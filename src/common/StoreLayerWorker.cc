#include "StoreLayerWorker.hh"
// #define PRINT_POP_INFO
std::mutex StoreLayerWorker::_store_layer_worker_mutex;
StoreLayerWorker::StoreLayerWorker(Config* conf): _conf(conf) {
    // create redis context
    assert(false && "should not be called");
    try {
        _processCtx = RedisUtil::createContext(_conf->_localIp);
        _localCtx = RedisUtil::createContext(_conf->_localIp);
    } catch (int e) {
        cerr << "initializing redis context error" << endl;
    }
    // _fs = FSUtil::createRBDFS(_conf);
    _dpHandler = new DedupHandler(_conf);
}

StoreLayerWorker::StoreLayerWorker(Config* conf, StoreLayer* store_handler): _conf(conf) {
    // create redis context
    try {
        _processCtx = RedisUtil::createContext(_conf->_localIp);
        _localCtx = RedisUtil::createContext(_conf->_localIp);
    } catch (int e) {
        cerr << "initializing redis context error" << endl;
    }

    // _fs = FSUtil::createFS(_conf);
    _dpHandler = new DedupHandler(_conf);
    _storeHandler = store_handler;
}


StoreLayerWorker::~StoreLayerWorker() {
    redisFree(_localCtx);
    redisFree(_processCtx);
    // FSUtil::deleteFS(_fs);
    if (_dpHandler) 
        delete _dpHandler;
}

void StoreLayerWorker::doProcess() {
    redisReply* rReply;
    while(true) {
        _store_layer_worker_mutex.lock();
        rReply = (redisReply*)redisCommand(_processCtx, "blpop store_layer_ag_request 0");
        assert(rReply!=NULL);
        assert(rReply->type != REDIS_REPLY_ERROR);
        assert(rReply->type != REDIS_REPLY_NIL);
        assert(rReply->type==REDIS_REPLY_ARRAY);
        assert(rReply->elements==2);
        char* reqStr = rReply->element[1]->str;
        AGCommand* agCmd = new AGCommand(reqStr);
        int type = agCmd->getType();
        switch(type) {
            case 12:
                popChunk(agCmd);
                break;
            case 14:
                flushStoreLayer();
                break;
            case 17:
                popSelectiveDedupChunk(agCmd);
                break;
            case 21:
                popUpdateChunk(agCmd);
                break;
            case 23:
                popBatchChunk(agCmd);
                break;
            case 19: 
                readPacket(agCmd);
                break;
            case 28:
                popChunkAndFlushToCephImmediately(agCmd);
                break;
            default:
                std::cout << "[StoreLayerWorker] unknown type: " << type << std::endl;
                exit(-1);
                break;
        }  
        freeReplyObject(rReply);
        _store_layer_worker_mutex.unlock();
    }

}

void StoreLayerWorker::registerFp(char* fp, int size, unsigned int ip, int pktid, int containerid, int conoff) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    char* buf = (char*)calloc(sizeof(int)*3, sizeof(char));
    int tmppktid = htonl(pktid);
    int tmpcid = htonl(containerid);
    int tmpcoff = htonl(conoff);
    memcpy(buf, (void*)&tmppktid, sizeof(int));
    memcpy(buf+sizeof(int), (void*)&tmpcid, sizeof(int));
    memcpy(buf+2*sizeof(int), (void*)&tmpcoff, sizeof(int));
    rReply = (redisReply*)redisCommand(sendCtx, "SET %s %b", to_string(hash_fp).c_str(), buf, sizeof(int)*3);
    assert(rReply!=NULL);
    assert(rReply->type==REDIS_REPLY_STATUS);
    assert(strcmp(rReply->str,"OK")==0);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

vector<int> StoreLayerWorker::lookupFp(char* fp, int size, unsigned int ip, bool batch) {
    vector<int> res = vector<int>();
    // send reply
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    rReply = (redisReply*)redisCommand(sendCtx, "GET %s", to_string(hash_fp).c_str());
    if(rReply->type != REDIS_REPLY_NIL) {
        char* buf = rReply->str;
        int tmppid, tmpcid, tmpcoff;
        memcpy((void*)&tmppid, buf, sizeof(int));
        memcpy((void*)&tmpcid, buf+sizeof(int), sizeof(int));
        memcpy((void*)&tmpcoff, buf+2*sizeof(int), sizeof(int));
        res.push_back(ntohl(tmppid));
        res.push_back(ntohl(tmpcid));
        res.push_back(ntohl(tmpcoff));
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);
    return res;
}



/**
 * 1 receive a chunk
 * 2 lookup in redis of the local node£¬ if non-exist, push to store layer, register in local redis
 * 3 return con_id and con_off to sender for packet recipe
 * NOTE: chunk is in local redis, pushed by pushingWorker
*/
void StoreLayerWorker::popChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    int buf_size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    #ifdef PRINT_POP_INFO
    cout << "[popChunk] wait to receive chunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << endl;
    #endif
    // 1 read chunk
    if(buf_size==0) {                   // receive a last chunk, tell store layer to flush
        // cout << "[popChunk] receive a last chunk, tell store layer to flush" << endl;
        DPDataChunk* chunk = new DPDataChunk();
        chunk->setLast(true);
        _storeHandler->push(chunk, nullptr, nullptr, poolname);
        delete chunk;
        return ;
    }
    redisReply* read_reply;
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());       // | fp | data |
    assert(read_reply!=NULL);
    assert(read_reply->type == REDIS_REPLY_ARRAY);
    char* buf = read_reply->element[1]->str;
    int datalen = buf_size - sizeof(fingerprint);
    assert(datalen == _conf->_chunkMaxSize);
    DPDataChunk* chunk = new DPDataChunk(datalen, chunkid, buf+sizeof(fingerprint), buf);
    #ifdef PRINT_POP_INFO
    cout << "[popChunk] receive a chunk filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << endl;
    #endif
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    // 2 lookup in redis of the local node, if non-exists, push to store layer and register in local redis
    vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_localIp, true);
    int con_id, con_off, dup;
    string chunkname = to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
    if(!res.empty()) {          // duplicate chunk
        con_id=res[1];
        con_off=res[2];
        dup = 1;
        registerChunkAppend( chunkname, filename, poolname, to_string(pktid), chunkid);
    } else {                    // non-exists chunk
        _storeHandler->push(chunk, &con_id, &con_off, poolname);
        registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_localIp, pktid, con_id, con_off);  
        registerChunk( chunkname, filename, poolname, to_string(pktid), chunkid); 
        dup = 0; 
    }
    delete chunk;


    // 3 send chunk writefinish reply, return con_id and con_off
    char* con_info = new char[sizeof(int)*3];
    int tmp_con_id = htonl(con_id);
    int tmp_con_off = htonl(con_off);
    int tmp_dup = htonl(dup);
    memcpy(con_info, (void*)&tmp_con_id, sizeof(int));
    memcpy(con_info+sizeof(int), (void*)&tmp_con_off, sizeof(int));
    memcpy(con_info+sizeof(int)*2, (void*)&tmp_dup, sizeof(int));
    redisReply* write_done_reply;
    redisContext* write_done_ctx = RedisUtil::createContext(_conf->_localIp);
    string write_done_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    write_done_reply = (redisReply*)redisCommand(write_done_ctx, "rpush %s %b", write_done_key.c_str(), con_info, sizeof(int)*3);
    assert(write_done_reply!=NULL);
    assert(write_done_reply->type == REDIS_REPLY_INTEGER);
    assert(write_done_reply->integer == 1);
    freeReplyObject(write_done_reply);
    redisFree(write_done_ctx);
    delete [] con_info;
    #ifdef PRINT_POP_INFO
    cout << "[popChunk] writefinish reply of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done, con_id:" << con_id << " con_off:" << con_off << endl;
    #endif
}

void StoreLayerWorker::registerChunk(string chunkname, string filename, string poolname, string pktid, int chunk_id) {  
    string chunk_info = filename + ":" + poolname + ":" + pktid + ":" + to_string(chunk_id);      
    redisReply *register_chunk_rReply = (redisReply *)redisCommand(_localCtx, 
                                            "SET %s %b", ("register:"+chunkname).c_str(), chunk_info.c_str(), chunk_info.size());
    assert(register_chunk_rReply != NULL);
    assert(register_chunk_rReply->type == REDIS_REPLY_STATUS);
    assert(string(register_chunk_rReply->str)== "OK");
    freeReplyObject(register_chunk_rReply);
    // update refcnt of this chunk
    // TODO: update refcnt of chunk correctly
    redisReply* update_refcnt_reply = (redisReply*)redisCommand(_localCtx, "INCR %s", ("refcnt:"+chunkname).c_str());
    assert(update_refcnt_reply!=NULL);
    assert(update_refcnt_reply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(update_refcnt_reply);
}

void StoreLayerWorker::registerChunkAppend(string chunkname, string filename, string poolname, string pktid, int chunk_id){
    string chunk_info = ":" + filename + ":" + poolname + ":" + pktid + ":" + to_string(chunk_id);       
    redisReply *register_chunk_rReply = (redisReply *)redisCommand(_localCtx,  "APPEND %s %b", ("register:"+chunkname).c_str(), chunk_info.c_str(), chunk_info.size());
    assert(register_chunk_rReply != NULL);
    freeReplyObject(register_chunk_rReply);
    // update refcnt of this chunk
    // TODO: update refcnt of chunk correctly
    redisReply* update_refcnt_reply = (redisReply*)redisCommand(_localCtx, "INCR %s", ("refcnt:"+chunkname).c_str());
    assert(update_refcnt_reply!=NULL);
    assert(update_refcnt_reply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(update_refcnt_reply);
}


/**
 * push a last chunk to store layer
*/
void StoreLayerWorker::flushStoreLayer() {
    std::cout << "[StoreLayerWorker] flushStoreLayer" << std::endl;
    DPDataChunk* chunk = new DPDataChunk();
    chunk->setLast(true);
    _storeHandler->push(chunk, nullptr, nullptr, "test");
    delete chunk;

    string wkey = "flushdone";
    redisContext* ctx = RedisUtil::createContext(_conf->_localIp);
    redisReply* rReply = (redisReply*)redisCommand(ctx, "RPUSH %s 1", wkey.c_str());
    assert(rReply!=NULL);
    assert(rReply->type==REDIS_REPLY_INTEGER);
    assert(rReply->integer>=1);
    freeReplyObject(rReply);
    redisFree(ctx);
}


void StoreLayerWorker::popSelectiveDedupChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    int buf_size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    // cout << "[popSelectiveDedupChunk] wait to receive chunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " datalen:" << buf_size << endl;
    
    // 1 read chunk
    assert(buf_size!=0);
    redisReply* read_reply;
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());       // | fp | data |
    assert(read_reply!=NULL);
    assert(read_reply->type == REDIS_REPLY_ARRAY);
    char* buf = read_reply->element[1]->str;
    DPDataChunk* chunk = new DPDataChunk(buf, buf_size);
    // cout << "[popSelectiveDedupChunk] receive a chunk filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " datalen:" << buf_size << endl;
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    // 2 push directly
    int con_id, con_off;
    _storeHandler->push(chunk, &con_id, &con_off, poolname);
    delete chunk;

    // 3 send chunk writefinish reply, return con_id and con_off
    char* con_info = new char[sizeof(int)*2];
    int tmp_con_id = htonl(con_id);
    int tmp_con_off = htonl(con_off);
    memcpy(con_info, (void*)&tmp_con_id, sizeof(int));
    memcpy(con_info+sizeof(int), (void*)&tmp_con_off, sizeof(int));
    redisReply* write_done_reply;
    redisContext* write_done_ctx = RedisUtil::createContext(_conf->_localIp);
    string write_done_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    write_done_reply = (redisReply*)redisCommand(write_done_ctx, "rpush %s %b", write_done_key.c_str(), con_info, sizeof(int)*2);
    assert(write_done_reply!=NULL);
    assert(write_done_reply->type == REDIS_REPLY_INTEGER);
    assert(write_done_reply->integer == 1);
    freeReplyObject(write_done_reply);
    redisFree(write_done_ctx);
    delete [] con_info;
    // cout << "[popSelectiveDedupChunk] writefinish reply of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done, con_id:" << con_id << " con_off:" << con_off << endl;
}


void StoreLayerWorker::popUpdateChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    int buf_size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    assert(buf_size == sizeof(fingerprint) + sizeof(int) + sizeof(int) + _conf->_chunkMaxSize);
    cout << "[popUpdateChunk] wait to receive chunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << endl;
    redisReply* read_reply;
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());      // | fp | conid | conoff | data |
    assert(read_reply!=NULL);
    assert(read_reply->type == REDIS_REPLY_ARRAY);
    char* buf = read_reply->element[1]->str;
    int con_id, con_off, tmp_con_id, tmp_con_off;
    memcpy((void*)&tmp_con_id, buf+sizeof(fingerprint), sizeof(int));
    memcpy((void*)&tmp_con_off, buf+sizeof(fingerprint)+sizeof(int), sizeof(int));
    con_id = ntohl(tmp_con_id);
    con_off = ntohl(tmp_con_off);
    int datalen = buf_size - sizeof(fingerprint) - sizeof(int) - sizeof(int);
    cout << "[popUpdateChunk] will persist chunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " to con_id:" << con_id << " con_off:" << con_off << endl;
    char* buffer = new char[sizeof(int) + datalen];
    int tmp_len = htonl(datalen);
    memcpy(buffer, (void*)&tmp_len, sizeof(int));
    memcpy(buffer+sizeof(int), buf+sizeof(fingerprint)+sizeof(int)+sizeof(int), datalen);
    _storeHandler->writeChunk(con_id, con_off, "test", buffer, datalen+sizeof(int));
    
    
    freeReplyObject(read_reply);
    redisFree(read_ctx);


    char* con_info = new char[sizeof(int)*3];
    redisReply* write_done_reply;
    redisContext* write_done_ctx = RedisUtil::createContext(_conf->_localIp);
    string write_done_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    write_done_reply = (redisReply*)redisCommand(write_done_ctx, "rpush %s %b", write_done_key.c_str(), con_info, sizeof(int)*3);
    assert(write_done_reply!=NULL);
    assert(write_done_reply->type == REDIS_REPLY_INTEGER);
    assert(write_done_reply->integer == 1);
    freeReplyObject(write_done_reply);
    redisFree(write_done_ctx);
    cout << "[popUpdateChunk] writefinish reply of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done" << endl;
    delete [] con_info;

}

/**
 * 1 receive a batch chunk
 * 2 for each chunk, lookup and persist, get conid and conff
 * 3 when all chunks lookup and persist done, send writefinish reply to sender and return conid and conoff
*/
void StoreLayerWorker::popBatchChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    int buf_size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    std::cout << "[StoreLayerWorker] popBatchChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " bufsize:" << buf_size << " start" << endl;
    
    // 1 read chunks
    redisReply* read_reply;
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = "chunksbuffer:" + filename + ":" + poolname + ":" + to_string(pktid);
    read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());       // | fingerprint | datalen | data | ...
    assert(read_reply!=NULL);
    assert(read_reply->type == REDIS_REPLY_ARRAY);
    char* buf = read_reply->element[1]->str;
    int chunk_cnt = buf_size / (sizeof(int) + sizeof(fingerprint) + sizeof(int) + _conf->_chunkMaxSize);
    std::cout << "[StoreLayerWorker] popBatchChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " bufsize:" << buf_size << " chunk_cnt:" << chunk_cnt << " start"<< endl;
    char* con_info_stream = new char [sizeof(int)*3*chunk_cnt];
    memset(con_info_stream, 0, sizeof(int)*3*chunk_cnt);
    int off = 0;
    for(int i=0;i<chunk_cnt;i++) {      // | chunkid | fingerprint | datalen | data |

        // 2 read one chunk
        // chunkid
        int tmp_chunkid;
        memcpy((void*)&tmp_chunkid, buf+off, sizeof(int));
        int chunkid = ntohl(tmp_chunkid);
        // datalen
        int tmp_datalen;
        memcpy((void*)&tmp_datalen, buf+off+sizeof(int)+sizeof(fingerprint), sizeof(int));
        int datalen = ntohl(tmp_datalen);
        DPDataChunk* chunk = new DPDataChunk(datalen, chunkid, buf+off+sizeof(int) + sizeof(fingerprint)+sizeof(int), buf+off+sizeof(int));
        off += (sizeof(int) + sizeof(fingerprint) + sizeof(int) + _conf->_chunkMaxSize);
        // std::cout << "[StoreLayerWorker] pop chunkid:" << chunkid << " hash_fp:" << Router::hash(chunk->getFp(), sizeof(fingerprint)) << " from stream" << endl;
        
        // 3 lookup in local redis, if not exists, push to store and get conid and conoff
        vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_localIp, true);
        int con_id, con_off, dup;
        string chunkname = to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
        if(!res.empty()) {          // duplicate chunk
            con_id=res[1];
            con_off=res[2];
            dup = 1;
            registerChunkAppend( chunkname, filename, poolname, to_string(pktid), chunkid);
        } else {                    // non-exists chunk
            _storeHandler->push(chunk, &con_id, &con_off, poolname);
            registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_localIp, pktid, con_id, con_off);  
            registerChunk( chunkname, filename, poolname, to_string(pktid), chunkid); 
            dup = 0; 
        }
        delete chunk;

        // 4 record conid and conoff
        // std::cout << "[StoreLayerWorker] persist chunkid:" << chunkid << " hash_fp:" << Router::hash(chunk->getFp(), sizeof(fingerprint)) << " in conid:" << con_id << " conoff:" << con_off << endl;
        int tmp_con_id = htonl(con_id);
        int tmp_con_off = htonl(con_off);
        memcpy(con_info_stream+sizeof(int)*3*i, (void*)&tmp_chunkid, sizeof(int));
        memcpy(con_info_stream+sizeof(int)*3*i+sizeof(int), (void*)&tmp_con_id, sizeof(int));
        memcpy(con_info_stream+sizeof(int)*3*i+sizeof(int)+sizeof(int), (void*)&tmp_con_off, sizeof(int));
    }
    
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    // 5 send writefinish reply to sender and return conid and conoff
    std::cout << "[StoreLayer] lookup and persist of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done, send finish reply to sender" << endl;
    redisReply* write_done_reply;
    redisContext* write_done_ctx = RedisUtil::createContext(_conf->_localIp);
    string write_done_key = "writechunksfinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    write_done_reply = (redisReply*)redisCommand(write_done_ctx, "rpush %s %b", write_done_key.c_str(), con_info_stream, sizeof(int)*3*chunk_cnt);
    assert(write_done_reply!=NULL);
    assert(write_done_reply->type == REDIS_REPLY_INTEGER);
    assert(write_done_reply->integer == 1);
    freeReplyObject(write_done_reply);
    redisFree(write_done_ctx);
    delete [] con_info_stream;

}


void StoreLayerWorker::readPacket(AGCommand* agCmd) {
    ReadBuf* readbuf = new ReadBuf(_conf);
    // readbuf->_mutex.lock();
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    unsigned int client_ip = agCmd->getIp();
    cout << "[readPacket] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " start" << std::endl;
    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    redisContext* sendCtx = RedisUtil::createContext(_conf->_localIp);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    assert(rReply!=NULL);
    assert(rReply->type == REDIS_REPLY_STRING);
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    vector<DPDataChunk*>* fpSequence = new vector<DPDataChunk*>();
    for (int i = 0; i < recipe->size(); i++) {
        fpSequence->push_back(new DPDataChunk());
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);

    // read chunk
    for(int i = 0; i < recipe->size(); i++) {
        int con_id = (*recipe)[i]->_containerId;
        string conName = "container_" + to_string((*recipe)[i]->_containerId);
        // cout << "[readPacket] read pktid:" << pktid << "chunkid:" << i << " from container " << conName << endl;
        readbuf->_mutex.lock();
        if (readbuf->find(conName) == NULL) {
            // cout << "[readPacket] container " << conName << " is not in readbuf, need to read" << endl;
            // string key=conName + "_poolname";             // key : container_[id]_poolname
            // redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[(*recipe)[i]->_containerId % _conf->_agentNum]);         
            // redisReply* rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
            // assert(rReply != NULL);
            // assert(rReply->type == REDIS_REPLY_STRING);
            // string poolname = string(rReply->str);
            string poolname = "test";
            // cout << "[readPacket] start read container " << conName << endl;
            char* buf = _storeHandler->readContainerTask(con_id, poolname, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
            // cout << "[readPacket] read " << conName << " done, insert it to readbuf" << endl;
            readbuf->insert(conName,buf);
            // freeReplyObject(rReply);
            // redisFree(sendCtx);
            // cout << "[readPacket] read container " << conName << " done" << endl;
        } else {
            // cout << "[readPacket] container " << conName << " is already in readbuf" << endl;
        }
        readbuf->_mutex.unlock();
        int tmpoff = (*recipe)[i]->_offset;
        int tmplen, dataLen;
        char* raw = readbuf->find(conName);
        // cout << "[readPacket] try to read pktid:" << pktid << " chunkid:" << i << " from container " << conName << endl;
        memcpy((void*)&tmplen, raw + tmpoff, sizeof(int));
        dataLen = ntohl(tmplen);
        (*fpSequence)[i]->setData(raw + tmpoff + sizeof(int), dataLen);
        // cout << "[readPacket] read pktid:" << pktid << " chunkid:" << i << " from container " << conName << " done" << endl;
    }
    // readbuf->_mutex.unlock();
    delete readbuf;
    // combine chunks to packet
    int pkt_len = 0;
    for(int i = 0; i < fpSequence->size(); i++) {
        pkt_len += (*fpSequence)[i]->getDatalen();
    }
    char* pkt_raw = new char[pkt_len+sizeof(int)];
    int tmplen = htonl(pkt_len);
    memcpy(pkt_raw, (void*)&tmplen, sizeof(int));
    int off = sizeof(int);
    for(int i = 0; i < fpSequence->size(); i++) {
        memcpy(pkt_raw+off, (*fpSequence)[i]->getData(), (*fpSequence)[i]->getDatalen());
        off += (*fpSequence)[i]->getDatalen();
    }
    redisContext* sendCtx1 = RedisUtil::createContext(client_ip);
    string key1 = "readpacket:" +filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply1 = (redisReply*)redisCommand(sendCtx1, "rpush %s %b", key1.c_str(), (void*)pkt_raw, pkt_len+sizeof(int));
    assert(rReply1!=NULL);
    assert(rReply1->type == REDIS_REPLY_INTEGER);
    // assert(rReply1->integer == 1);
    freeReplyObject(rReply1);
    redisFree(sendCtx1);
    delete []pkt_raw;
    for(int i = 0; i < recipe->size();i ++) {
        delete (*recipe)[i];
    }
    delete recipe;
    for(int i = 0; i < fpSequence->size(); i++) {
        delete (*fpSequence)[i];
    }
    delete fpSequence;
    cout << "[readPacket] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done and send to client" << std::endl;

}

/**
 * 1 receive a chunk
 * 2 lookup in redis of the local node£¬ if non-exist, push to store layer to get a conid and conoff, register in local redis
 * 3 return con_id and con_off to sender for packet recipe
 * NOTE: chunk is in local redis, pushed by pushingWorker
*/
void StoreLayerWorker::popChunkAndFlushToCephImmediately(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    int buf_size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
// #ifdef PRINT_POP_INFO
    cout << "[popChunkAndFlushToCephImmediately] wait to receive chunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << endl;
// #endif
    // 1 read chunk
    redisReply* read_reply;
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());       // | fp | data |
    assert(read_reply!=NULL);
    assert(read_reply->type == REDIS_REPLY_ARRAY);
    char* buf = read_reply->element[1]->str;
    int datalen = buf_size - sizeof(fingerprint);
    DPDataChunk* chunk = new DPDataChunk(datalen, chunkid, buf+sizeof(fingerprint), buf);
// #ifdef PRINT_POP_INFO
    cout << "[popChunkAndFlushToCephImmediately] receive a chunk filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << endl;
// #endif
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    int con_id, con_off;
    // 2 push to store layer, flush to ceph immediately and register in local redis
    string chunkname = to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
    _storeHandler->pushAndFlushToCephImmediately(chunk, &con_id, &con_off, poolname);

    registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_localIp, pktid, con_id, con_off);  
    registerChunk( chunkname, filename, poolname, to_string(pktid), chunkid); 
    int dup = 0; 
    delete chunk;


    // 3 send chunk writefinish reply, return con_id and con_off
    char* con_info = new char[sizeof(int)*3];
    int tmp_con_id = htonl(con_id);
    int tmp_con_off = htonl(con_off);
    int tmp_dup = htonl(dup);
    memcpy(con_info, (void*)&tmp_con_id, sizeof(int));
    memcpy(con_info+sizeof(int), (void*)&tmp_con_off, sizeof(int));
    memcpy(con_info+sizeof(int)*2, (void*)&tmp_dup, sizeof(int));
    redisReply* write_done_reply;
    redisContext* write_done_ctx = RedisUtil::createContext(_conf->_localIp);
    string write_done_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    write_done_reply = (redisReply*)redisCommand(write_done_ctx, "rpush %s %b", write_done_key.c_str(), con_info, sizeof(int)*3);
    assert(write_done_reply!=NULL);
    assert(write_done_reply->type == REDIS_REPLY_INTEGER);
    assert(write_done_reply->integer == 1);
    freeReplyObject(write_done_reply);
    redisFree(write_done_ctx);
    delete [] con_info;
// #ifdef PRINT_POP_INFO
    cout << "[popChunkAndFlushToCephImmediately] writefinish reply of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done, con_id:" << con_id << " con_off:" << con_off << endl;
// #endif
}
