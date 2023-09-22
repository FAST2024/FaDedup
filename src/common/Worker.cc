#include "Worker.hh"
// #define PRINT_PUSH_INFO
std::mutex Worker::_worker_mutex;

Worker::Worker(Config* conf, StoreLayer* store_handler, WorkerBuffer* worker_buffer): _conf(conf) {
    // create redis context
    try {
        _processCtx = RedisUtil::createContext(_conf->_localIp);
        _localCtx = RedisUtil::createContext(_conf->_localIp);
    } catch (int e) {
        cerr << "initializing redis context error" << endl;
    }

    _fs = FSUtil::createRBDFS(_conf);
    _dpHandler = new DedupHandler(_conf);
    _storeHandler = store_handler;
    _worker_buffer = worker_buffer;
}

// BaseFS* Worker::_fs;

Worker::~Worker() {
    redisFree(_localCtx);
    redisFree(_processCtx);
    FSUtil::deleteFS(_fs);
    if (_dpHandler) 
        delete _dpHandler;
}

void Worker::doProcess() {
    redisReply* rReply;
    while(true) {
        // cout << "[Worker] doProcess waiting..." << endl;
        rReply = (redisReply*)redisCommand(_processCtx, "blpop ag_request 0");
        assert(rReply != NULL);
        assert(rReply->type != REDIS_REPLY_ERROR);
        assert(rReply->type != REDIS_REPLY_NIL);
        assert(rReply->type == REDIS_REPLY_ARRAY);
        assert(rReply->elements == 2);
        if (rReply->type == REDIS_REPLY_NIL) {
            cerr << "[Worker] doProcess get feed back empty queue " << endl;
        }
        else if (rReply->type == REDIS_REPLY_ERROR) {
            cerr << "[Worker] doProcess get feed back ERROR happens " << endl;
        }
        // TransmitInterface* ds = new TCPAcceptor(_acc.accept());
        // size_t cmdLen;
        // ds->Receive(sizeof(cmdLen), &cmdLen);
        // char* buf = new char[cmdLen];
        else {
        // ds->Receive(cmdLen, buf);
            char* reqStr = rReply->element[1]->str;
            AGCommand* agCmd = new AGCommand(reqStr);
            int type = agCmd->getType();
            // cout << "[Worker] doProcess receive a request of type " << type << endl;

        switch (type) {
            case 6: registerFile(agCmd); break;
            case 8: clientReorder(); break;
            case 11: clientWriteFullWithRBD(agCmd); break;
            case 15: clientWriteFullAndSelectiveDedup(agCmd); break;
            case 22: clientWriteFullWithRBDAndBatchPush(agCmd); break;
            case 27: clientWriteFullWithRBDAndPushSuperChunk(agCmd); break;
            case 18: clientReadFullWithRBDCombineByClient(agCmd); break;
            case 24: updateChunk(agCmd); break;
            case 26: simpleUpdateChunk(agCmd); break;
            case 25: clearReadBuf(); break;
            default:
                cout << "[Worker] receive unknown command, type:" << type << endl;
                exit(-1);
                break;
        }

        }
        freeReplyObject(rReply);
    }
}

void Worker::clientWriteFull(AGCommand* agCmd) {
    cout << "[Worker] clientWriteFull" << endl;
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int filesize = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    // send ack to client
    // thread ackThrd = thread([=]{
    //     redisContext* ackCtx = RedisUtil::createContext(sender);
    //     string ackKey = "ack:" + filename+":"+poolname+":"+to_string(pktid);
    //     cout << ackKey << endl;
    //     redisReply* rReply = (redisReply*)redisCommand(ackCtx, "rpush %s 1", ackKey.c_str());
    //     freeReplyObject(rReply);
    //     redisFree(ackCtx);
    // });
    // ackThrd.detach();
    if (mode == "online") onlineWriteFull(filename, pktid, poolname, filesize, sender);
    else if (mode == "offline") offlineWriteFull(filename, pktid, poolname, filesize);
}

void Worker::onlineWriteFull(string filename, int pktid, string poolname, int filesize, unsigned int sender) {
    cout << "[Worker] onlineWriteFull" << endl;

    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 0. create threads for promoting chunk into FS
    // string objname = filename + to_string(pktid);
    // StoreOutputStream* objstream = new StoreOutputStream(_conf, objname, poolname, _fs);

    // thread persistThread = thread([=]{objstream->writeObjFull();});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Lookup and Register FP table in the corresponding node
    BlockingQueue<DPDataChunk*>* lookupQueue = new BlockingQueue<DPDataChunk*>();
    string objname = filename+":"+poolname+":"+to_string(pktid);
    PacketRecipe* pRecipe = new PacketRecipe;
    thread lookupThread = thread([=]{lookupWorker(hashQueue, lookupQueue, objname, pRecipe);});

    // 4. Persist chunk in the corresponding node
    thread persistFullThread = thread([=]{persistFullWorker(lookupQueue, filename, poolname);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    lookupThread.join();
    persistFullThread.join();

    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    delete pRecipe;
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete lookupQueue;
}

void Worker::loadWorker(BlockingQueue<DPDataPacket*>* readQueue, string keybase, int pktid) {
    cout << "[loadWorker] loading data..." << endl;
    redisContext* readCtx = RedisUtil::createContext(_conf->_localIp);
    string key = keybase + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(readCtx, "blpop %s 0", key.c_str());
    assert(rReply != NULL);
    assert(rReply->type == REDIS_REPLY_ARRAY);
    assert(rReply->elements == 2);
    char* content = rReply->element[1]->str;
    DPDataPacket* pkt = new DPDataPacket(content);
    assert(pkt != nullptr);
    assert(pkt->getDatalen() == _conf->_pktSize);
    readQueue->push(pkt);
    freeReplyObject(rReply);
    redisFree(readCtx);
    cout << "[loadWorker] finish loading" << endl;
}

void Worker::chunkingWorker(BlockingQueue<DPDataPacket*>* readQueue, BlockingQueue<DPDataChunk*>* chunkQueue) {
    cout << "[chunkingWorker] start chunking..." << endl;
    int chunk_id = 0;
    DPDataPacket* pkt = readQueue->pop();
    int pos = 0;
    char* data = pkt->getData();
    int datasize = pkt->getDatalen();
    char* leftbuf = new char[_conf->_pktSize+_conf->_chunkMaxSize];
    memcpy(leftbuf, data, datasize);
    int leftlen = datasize;
    int leftoff = 0;
    int chunk_total_size = 0;
    while (leftlen > 0) {
        // cout << "chunk id:" << chunk_id << endl;
        int chunk_size = 0;
        if (leftlen < _conf->_chunkAvgSize)
            chunk_size = leftlen;
        else
            chunk_size = _dpHandler->chunking(leftbuf+leftoff, leftlen);
        chunk_total_size += chunk_size;
        DPDataChunk* new_chunk = new DPDataChunk(chunk_size, chunk_id++); 
        new_chunk->setData(leftbuf+leftoff, chunk_size);
        leftlen -= chunk_size;
        leftoff += chunk_size;
        chunkQueue->push(new_chunk);
    }
    delete pkt;
    DPDataChunk* lastChunk = new DPDataChunk();
    lastChunk->setLast(true);
    chunkQueue->push(lastChunk);
    delete leftbuf;
    cout << "[chunkingWorker] finish chunking..." << endl;
}

void Worker::hashingWorker(BlockingQueue<DPDataChunk*>* chunkQueue, BlockingQueue<DPDataChunk*>* hashQueue) {
    cout << "[hashingWorker] start hashing..." << endl;
    while(1) {
        DPDataChunk* chunk = chunkQueue->pop();
        if (chunk->isLast()) {
            hashQueue->push(chunk);
            break;
        }
        _dpHandler->hashing(chunk->getData(), chunk->getDatalen(), chunk->getFp());
        hashQueue->push(chunk);
    }
    cout << "[hashingWorker] finish hashing..." << endl;
}

void Worker::lookupWorker(BlockingQueue<DPDataChunk*>* hashQueue, 
                        BlockingQueue<DPDataChunk*>* lookupQueue, 
                        string key,
                        PacketRecipe* pRecipe) {
    cout << "[lookupWorker] start lookup..." << endl;
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        if (chunk->isLast()) {
            lookupQueue->push(chunk);
            // register packet recipe
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(pRecipe->getRecipe(key), stream, alloc_len);
            // vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            freeReplyObject(rReply);
            delete stream;
            break;
        }
        // route to a specific node
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        // send protocol to lookup fp in the corresponding node, and register in the node
        bool res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id]);
        // push into packet recipe
        
        
        string chunkname = to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
        int ipId = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        std::stringstream ss(key);
        std::string item;
        std::vector<std::string> items;
        while (std::getline(ss, item, ':')) {
            items.push_back(item);
        }
        assert(items.size()==3);
	registerFilesInNode(items[0],items[1],node_id); 
        if (!res) { 
            registerOriginChunkPoolname(chunkname, items[1], ipId);
            registerChunk(chunkname, items[0], items[1], items[2], chunk->getId(), ipId); 
             
            pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), res, items[1].c_str()));
            lookupQueue->push(chunk); 
         } else {  
            string conInfo;
            if(getConInfo(chunkname,ipId,conInfo)){
            	std::stringstream ss(conInfo);
        	std::string info;
        	std::vector<std::string> infos;
        	while (std::getline(ss, info, ':')) {
            		infos.push_back(info);
        	}
        	pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), res, 
        			infos[0].c_str(),infos[1].c_str(),stoi(infos[2]),(unsigned int)stoul(infos[3]),stoi(infos[4])));
	    }
            else{
            	string origin_chunk_poolname;
            	getOriginChunkPoolname(chunkname, ipId, origin_chunk_poolname);
            	pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), res, origin_chunk_poolname.c_str()));
            }
            // cout << "dup chunk: " << chunk->getId() << endl;
            registerChunkAppend(chunkname, items[0], items[1], items[2], chunk->getId(), ipId);
            delete chunk;
        }
    }
    cout << "[lookupWorker] finish lookup..." << endl;
}

void Worker::persistFullWorker(BlockingQueue<DPDataChunk*>* lookupQueue, string filename, string poolname) {
    cout << "[persistWorker] start promoting..." << endl;
    while(1) {
        DPDataChunk* chunk = lookupQueue->pop();
        if (chunk->isLast()) {
            delete chunk;
            break;
        }
        string chunkname = to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
        int ipId = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        persistFullChunkTask(chunkname, poolname, chunk->getData(), chunk->getDatalen());
        // string chunkname = filename + ":" + poolname + ":" + "c" + to_string(Router::hash(chunk->getFp(), sizeof(fingerprint)));
        
        // persistFullChunkTask(chunkname, poolname, chunk->getData(), chunk->getDatalen());
        delete chunk;
    }
    cout << "[persistWorker] finish promoting..." << endl;
}

void Worker::offlineWriteFull(string filename, int pktid, string poolname, int filesize) {
    cout << "[Worker] offlineWriteFull" << endl;

    // 1. Chunking


    // 2. Hashing


    // 3. Routing


    // 4. Register FP in the corresponding node


    // 5. persist chunk in the corresponding node

}



void Worker::clientWritePartial(AGCommand* agCmd) {
    cout << "[Worker] clientWritePartial" << endl;
}

void Worker::onlineWritePartial(string filename, string poolname, int size, int offset) {
    cout << "[Worker] onlineWritePartial" << endl;
}

void Worker::offlineWritePartial(string filename, string poolname, int size, int offset) {
    cout << "[Worker] offlineWritePartial" << endl;
}

void Worker::clientReadFull(AGCommand* agCmd) {
    cout << "[Worker] clientReadFull" << endl;
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    // get file recipe
    string key = "pktnum:" + filename + ":" + poolname;
    // cout << "key: " << key << endl;
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "EXISTS %s", key.c_str());
    // char* response = rReply->element[1]->str;
    int exists = rReply->integer;
    if (exists == 0) {
        cout << "[ERROR] file not exists" << endl;
        return;
    }
    freeReplyObject(rReply);
    rReply = (redisReply*)redisCommand(sendCtx, "GET %s", key.c_str());
    int pktnum, tmpnum;
    char* content = rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);

    freeReplyObject(rReply);
    redisFree(sendCtx); 

    // return pktnum to client
    string skey = "pktnumReply:" + key;
    tmpnum = htonl(pktnum);
    redisReply* rReply1 = (redisReply*)redisCommand(_localCtx, "rpush %s %b", skey.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(rReply1);

    // read each obj in each corresponding node
    readFullObj(filename, poolname, pktnum);
}



bool Worker::lookupFp(char* fp, int size, unsigned int ip) {
    // cout << "[Worker] lookupFp" << endl;
    size_t hash_fp = Router::hash(fp, size);
    int new_refcnt=registerFpAdd(fp, size, ip);
    return new_refcnt!=1;
    // cout << "[Worker] finish lookupFp" << endl;
}

void Worker::registerFp(char* fp, int size, unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    rReply = (redisReply*)redisCommand(sendCtx, "SET %s 1", to_string(hash_fp).c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);
}


int Worker::registerFpAdd(char* fp, int size, unsigned int ip) {
    redisContext* inc_refcnt_Ctx = RedisUtil::createContext(ip);
    redisReply* inc_refcnt_rReply;
    size_t hash_fp = Router::hash(fp, size);
    inc_refcnt_rReply = (redisReply*)redisCommand(inc_refcnt_Ctx, "INCR %s", to_string(hash_fp).c_str());
    assert(inc_refcnt_rReply!=NULL);
    int new_refcnt = inc_refcnt_rReply->integer;
    assert(new_refcnt>=1);
#ifdef PRINT_FINGERPRINT_REFCNT
    cout << "[registerFpAdd] register hash_fp : " << hash_fp << " cnt : " << new_refcnt << endl;
#endif
    freeReplyObject(inc_refcnt_rReply);
    redisFree(inc_refcnt_Ctx);
    return new_refcnt;
}


int Worker::registerFpSub(char* fp, int size, unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    rReply = (redisReply*)redisCommand(sendCtx, "DECR %s", to_string(hash_fp).c_str());
    assert(rReply!=NULL);
    int new_refcnt = rReply->integer;
    assert(new_refcnt>=0);
#ifdef PRINT_FINGERPRINT_REFCNT
    cout << "[registerFpSub] register hash_fp : " << hash_fp << " cnt : " << new_refcnt << endl;
#endif
    freeReplyObject(rReply);
    redisFree(sendCtx);
    if(new_refcnt==0) {
        redisContext* delete_fp_refcnt_sendCtx = RedisUtil::createContext(ip);
        redisReply* delete_fp_refcnt_rReply;
        delete_fp_refcnt_rReply = (redisReply*)redisCommand(delete_fp_refcnt_sendCtx, "DEL %s", to_string(hash_fp).c_str());
        assert(delete_fp_refcnt_rReply!=NULL);
        assert(delete_fp_refcnt_rReply->type==REDIS_REPLY_INTEGER);
        assert(delete_fp_refcnt_rReply->integer==1);
        freeReplyObject(delete_fp_refcnt_rReply);
        redisFree(delete_fp_refcnt_sendCtx);
    }
    return new_refcnt;
}

void Worker::registerFp(char* fp, int size, unsigned int ip, int pktid, int containerid, int conoff) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    char* buf = (char*)calloc(sizeof(int)*3, sizeof(char));
    // cout << "[register] pktid: " << pktid << endl;
    // cout << "[register] conid: " << containerid << endl;
    // cout << "[register] conoff: " << conoff << endl;
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
    // rReply = (redisReply*)redisCommand(sendCtx, "SET %d %b", hash_fp, pktid, containerid, conoff);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

void Worker::registerFp(string chunkname, unsigned int ip, int pktid, int containerid, int conoff){
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    char* buf = (char*)calloc(sizeof(int)*3, sizeof(char));
    // cout << "[register] pktid: " << pktid << endl;
    // cout << "[register] conid: " << containerid << endl;
    // cout << "[register] conoff: " << conoff << endl;
    int tmppktid = htonl(pktid);
    int tmpcid = htonl(containerid);
    int tmpcoff = htonl(conoff);
    memcpy(buf, (void*)&tmppktid, sizeof(int));
    memcpy(buf+sizeof(int), (void*)&tmpcid, sizeof(int));
    memcpy(buf+2*sizeof(int), (void*)&tmpcoff, sizeof(int));
    rReply = (redisReply*)redisCommand(sendCtx, "SET %s %b", chunkname.c_str(), buf, sizeof(int)*3);
    // rReply = (redisReply*)redisCommand(sendCtx, "SET %s %b", to_string(hash_fp).c_str(), pktid, containerid, conoff);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}


vector<int> Worker::lookupFp(char* fp, int size, unsigned int ip, bool batch) {
    // cout << "[Worker] lookupFp" << endl;
    vector<int> res = vector<int>();
    // send reply
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply;
    size_t hash_fp = Router::hash(fp, size);
    rReply = (redisReply*)redisCommand(sendCtx, "GET %s", to_string(hash_fp).c_str());
    // res = rReply->integer ? true: false;
    // res = rReply->str;
    // cout << "reply type: " << rReply->type << endl;
    if(rReply->type != REDIS_REPLY_NIL) {
        // cout << "reply type: " << rReply->type << endl;
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
    // if not exists
    // if (!res) {
    //     // resgister
    //     registerFp(fp, size, ip, pktid, containerid);
    // }
    redisFree(sendCtx);
    return res;
    // cout << "[Worker] finish lookupFp" << endl;
}

void Worker::persistFullChunkTask(string filename, string poolname, char* buf, int size) {
    // cout << "[Worker] persistFullChunk" << endl;
    BaseFile* file = new BaseFile(filename, poolname);
    int res = _fs->writeFile(file, buf, size);
    if (res != SUCCESS) {
        cout << "[ERROR] fail to writefull!" << endl;
    }
    delete file;
    // cout << "[Worker] finish persistFullChunk" << endl;
}

void Worker::registerFile(AGCommand* agCmd) {
    // cout << "[Worker] registerFile" << endl;
    string key = agCmd->getFilename();
    int pktnum = agCmd->getPktNum();
    int tmpnum = htonl(pktnum);
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", key.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(rReply);
    // cout << "[Worker] finish registerFile" << endl;
}

void Worker::readFullObj(string filename, string poolname, int pktnum) {
    // cout << "[Worker] readFullObj" << endl;

    thread readFullPacktThrds[pktnum];
    // unordered_map<string, mutex*>* readMutex = new unordered_map<string, mutex*>;
    for(int i = 0; i < pktnum; i++) {
        readFullPacktThrds[i] = thread([=]{readFullPacketWorker(filename, poolname, i);});
        // readPacketWorker(readQueue, filename, poolname, i);
    }

    for (int i = 0; i < pktnum; i++) {
        readFullPacktThrds[i].join();
    }

    // for(auto&it: *readMutex) {
    //     delete it.second;
    // }
    // delete readMutex;
    // cout << "[Worker] finish readFullObj" << endl;
}

static void readChunkTask(string chunkname, string poolname, vector<DPDataChunk*>* fpSequence, BaseFS* fs, int cid, int clen) {
    // cout << "[Worker] readChunkTask" << endl;
    // BaseFile* file = fs->openFile(chunkname, poolname, "read");
    // cout << "chunkname: " << chunkname << endl;
    BaseFile* file = new BaseFile(chunkname, poolname);
    int hasread = 0;
    char* buf = (char*)calloc(clen, sizeof(char));
    if(!buf) {
        cerr << "[ERROR] Worker::readChunkTask malloc buffer error!" << endl;
        return;
    }
    while(hasread < clen) {
        int len = fs->readFile(file, buf+hasread, clen-hasread, hasread);
        if(len <= 0) break;
        hasread += len;
    }
    // assert(hasread == clen);
    assert(cid >= 0 && cid < (*fpSequence).size());
    (*fpSequence)[cid]->setData(buf, hasread);
    free(buf);
    delete file;
    // cout << "[Worker] finish readChunkTask" << endl;
}

static void readChunkTask(string chunkname, string poolname, DPDataChunk* chunk, BaseFS* fs, int clen) {
    // cout << "[Worker] readChunkTask" << endl;
    // BaseFile* file = fs->openFile(chunkname, poolname, "read");
    // cout << "chunkname: " << chunkname << endl;
    BaseFile* file = new BaseFile(chunkname, poolname);
    int hasread = 0;
    char* buf = (char*)calloc(clen, sizeof(char));
    if (!buf) {
        cerr << "[ERROR] Worker::readChunkTask malloc buffer error!" << endl;
        return;
    }
    while (hasread < clen) {
        int len = fs->readFile(file, buf + hasread, clen - hasread, hasread);
        if (len <= 0) break;
        hasread += len;
    }
    // assert(hasread == clen);
    chunk->setData(buf, hasread);
    free(buf);
    delete file;
    // cout << "[Worker] finish readChunkTask" << endl;
}

void Worker::readFullPacketWorker(string filename, 
                            string poolname,
                            int pktid) {
    // cout << "[Worker] readFullPacketWorker" << endl;
    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    vector<DPDataChunk*>* fpSequence = new vector<DPDataChunk*>();
    for (int i = 0; i < recipe->size(); i++) {
        fpSequence->push_back(new DPDataChunk());
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);
    // create a thread pool
    // ThreadPool thrdPool;
    // thrdPool.start(_conf->_readThrdNum);
    // cout << "recipe size:" << recipe->size() << endl;
    for(int i = 0; i < recipe->size(); i++) {
        string chunkname = to_string(Router::hash((*recipe)[i]->_fp, sizeof(fingerprint)));
        // string chunkname = filename + ":" + poolname + ":" + "c" + to_string(Router::hash((*recipe)[i]->_fp, sizeof(fingerprint)));
        // auto task = bind(readChunkTask, chunkname, poolname, fpSequence, _fs, i, (*recipe)[i]->_chunksize);
        // thrdPool.addTask(task);
        // if((*recipe)[i]->_dup) {
        //     if(readMutex->find(chunkname) == readMutex->end()) {
        //         readMutex->insert(make_pair(chunkname, new mutex));
        //     }
        //     (*readMutex)[chunkname]->lock();
        //     readChunkTask(chunkname, poolname, fpSequence, _fs, i, (*recipe)[i]->_chunksize);
        //     (*readMutex)[chunkname]->unlock();
        // }
        // else
        string origin_chunk_poolname((*recipe)[i]->_origin_chunk_poolname);
        // cout << "read chunk from pool " << origin_chunk_poolname << endl;
        readChunkTask(chunkname, origin_chunk_poolname, fpSequence, _fs, i, (*recipe)[i]->_chunksize);
    }
    // thrdPool.finish();
    // combine fpSequence into a packet
    int pkt_len = 0;
    for(int i = 0; i < fpSequence->size(); i++) {
        pkt_len += (*fpSequence)[i]->getDatalen();
    }
    // cout << "pkt_len: " << pkt_len << endl;
    char* pkt_raw = new char[pkt_len+sizeof(int)];
    // int tmpid = htonl(pktid);
    // memcpy(pkt_raw, (void*)&tmpid, sizeof(int));
    int tmplen = htonl(pkt_len);
    memcpy(pkt_raw, (void*)&tmplen, sizeof(int));
    int off = sizeof(int);
    // cout << "fpSequence: " << fpSequence->size() << endl;
    for(int i = 0; i < fpSequence->size(); i++) {
        memcpy(pkt_raw+off, (*fpSequence)[i]->getData(), (*fpSequence)[i]->getDatalen());
        off += (*fpSequence)[i]->getDatalen();
    }
    // cout << "[worker] send packet to client" << endl;
    // send packet to client
    redisContext* sendCtx1 = RedisUtil::createContext(_conf->_localIp);
    // cout << key << endl;
    // printf("%s\n", pkt_raw);
    string key1 = "readfile:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply1 = (redisReply*)redisCommand(sendCtx1, "rpush %s %b", key1.c_str(), (void*)pkt_raw, pkt_len+sizeof(int));
    freeReplyObject(rReply1);
    redisFree(sendCtx1);
    delete pkt_raw;
    for(int i = 0; i < recipe->size();i ++) {
        delete (*recipe)[i];
    }
    delete recipe;
    for(int i = 0; i < fpSequence->size(); i++) {
        delete (*fpSequence)[i];
    }
    delete fpSequence;
    // cout << "[Worker] finish readPacketWorker" << endl;
}


void Worker::clientWriteBatch(AGCommand* agCmd) {
    cout << "[Worker] clientWriteBatch" << endl;
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int filesize = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if (mode == "online") onlineWriteBatch(filename, pktid, poolname, filesize, sender);
    else if (mode == "offline") offlineWriteBatch(filename, pktid, poolname, filesize);
}

void Worker::onlineWriteBatch(string filename, int pktid, string poolname, int filesize, unsigned int sender) {
    cout << "[Worker] onlineWriteBatch" << endl;

    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Lookup and Register FP table in the corresponding node
    BlockingQueue<DPDataChunk*>* lookupQueue = new BlockingQueue<DPDataChunk*>();
    thread lookupThread = thread([=]{lookupWorker(hashQueue, lookupQueue);});

    // 4. Pushing non-exist chunks into container
    BlockingQueue<Container*>* persistQueue = new BlockingQueue<Container*>();
    key = filename + ":" + poolname + ":" + to_string(pktid);
    PacketRecipe* pRecipe = new PacketRecipe;
    thread batchThread = thread([=]{batchWorker(lookupQueue, persistQueue, key, pRecipe, pktid);});

    // 5. Persist container in the corresponding node
    thread persistFullThread = thread([=]{persistFullWorker(persistQueue, poolname);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    lookupThread.join();
    batchThread.join();
    persistFullThread.join();

    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    delete pRecipe;
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete lookupQueue;
    delete persistQueue;
}

void Worker::lookupWorker(BlockingQueue<DPDataChunk*>* hashQueue,
                        BlockingQueue<DPDataChunk*>* lookupQueue) {
    cout << "[lookupWorker] lookupWorker" << endl;
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        if (chunk->isLast()) {
            lookupQueue->push(chunk);
            break;
        }
        // route to a specific node
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        // send protocol to lookup fp in the corresponding node, and register in the node
        vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], true);
        if(!res.empty()) {
            chunk->setDup(true);
            chunk->setPktId(res[0]);
            chunk->setConId(res[1]);
            chunk->setConOff(res[2]);
        }
        else
            chunk->setDup(false);
        
        lookupQueue->push(chunk);
    }
    cout << "[lookupWorker] finish lookup..." << endl;
}


/**
 * look up whether this chunk is a duplicate chunk
 * if yes, get the corresponding container info, register the chunk in packet recipe, register it to the chunk it refers to 
 * else, push it to store layer, get container info from store layer, register the chunk in packet recipe, register it to the chunk it refers to
 * at last, store packet recipe in corresponding node
*/
void Worker::lookupToStoreLayerWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[lookupToStoreLayerWorker] lookupToStoreLayerWorker : filename : " << filename << " poolname : " << poolname << " pktid : " << pktid << " begin" << endl;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid); // key to construct packet recipe
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        if (chunk->isLast()) {
            _storeHandler->push(chunk, nullptr, nullptr,poolname); // push last chunk to store layer
            delete chunk;
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            std::cout << "[lookupToStoreLayerWorker] lookupToStoreLayerWorker : encode and write recipe of filename : " << filename << " poolname : " << poolname << " pktid : " << pktid << " done" << std::endl;
            freeReplyObject(rReply);
            break;
        }

        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], true);
        if(!res.empty()) {              // duplicate chunk, get its container information and register
            chunk->setDup(true);
            chunk->setPktId(res[0]);
            chunk->setConId(res[1]);
            chunk->setConOff(res[2]);
            pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), chunk->getConId(), chunk->getConOff()));
            size_t hash_fp = Router::hash(chunk->getFp(), sizeof(fingerprint));
            int ipId = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
            registerChunkAppend(to_string(hash_fp), filename, poolname, to_string(pktid), chunk->getId(), ipId);            
            // cout << "[lookupToStoreLayerWorker] lookupStoreLayerWorker : duplicate hash_fp " << hash_fp << " refer to con_id " << chunk->getConId() << " con_off " << chunk->getConOff() << endl;
        }
        else {                          // unduplicate chunk, get ite container information from store layer and register
            chunk->setDup(false);
            int con_id, con_off;
            _storeHandler->push(chunk, &con_id, &con_off, poolname);                                                        // push unduplicate chunk to store layer and get container id and offset
            pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), con_id, con_off));
            registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], pktid, con_id, con_off);            // unduplicate chunk, register it fp in corresponding node
            size_t hash_fp = Router::hash(chunk->getFp(), sizeof(fingerprint));     
            int ipId = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
            registerChunk(to_string(hash_fp), filename, poolname, to_string(pktid), chunk->getId(), ipId);      // register it in the corresponding container                                               
            // cout << "[lookupToStoreLayerWorker] lookupToStoreLayerWorker : unduplicate hash_fp " << hash_fp << " put in con_id " << con_id << " con_off " << con_off << endl;
        }
    }
    cout << "[lookupToStoreLayerWorker] lookupToStoreLayerWorker : filename : " << filename << " poolname : " << poolname << " pktid : " << pktid << " done" << endl;
}

void Worker::batchWorker(BlockingQueue<DPDataChunk*>* lookupQueue,
                     BlockingQueue<Container*>* persistQueue,
                     string key,
                     PacketRecipe* pRecipe,
                     int pktid) {
    cout << "[batchWorker] batchWorker" << endl;
    int conId = 0;
    string name = key + ":con:" + to_string(conId++);
    Container* conn = new Container(name, _conf->_containerSize, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
    unordered_map<size_t, pair<int, int>> fptable;
    while(1) {
        DPDataChunk* chunk = lookupQueue->pop();
        if (chunk->isLast()) {
            delete chunk;
            if(!conn->empty())
                persistQueue->push(conn);
            Container* new_conn = new Container("", 0, 0);
            new_conn->setLast(true);
            persistQueue->push(new_conn);
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            // register packet recipe
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            freeReplyObject(rReply);
            break;
        }
        if(chunk->getDup())                                            
            pRecipe->append(key, new WrappedFP(chunk->getFp(), 
                                            chunk->getDatalen(), 
                                            chunk->getDup(), 
                                            chunk->getPktId(), 
                                            chunk->getConId(), 
                                            chunk->getConOff(), 
                                            _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize)));
        else {                                                         
            pRecipe->append(key, new WrappedFP(chunk->getFp(),
                                            chunk->getDatalen(),
                                            chunk->getDup(),
                                            pktid,
                                            conId-1,
                                            conn->getOff(),
                                            _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize)));
            int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
            registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], pktid, conId-1, conn->getOff());
            conn->push(chunk->getData(), chunk->getDatalen());
            if(conn->full()) {
                persistQueue->push(conn);
                string new_name = key + ":con:" + to_string(conId++);
                Container* new_conn = new Container(new_name, _conf->_containerSize, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
                conn = new_conn;
            }
        }
        delete chunk;
    }
    cout << "[batchWorker] finish batching" << endl;
}

void Worker::persistFullWorker(BlockingQueue<Container*>* persistQueue,
                            string poolname) {
    cout << "[persistWorker] persistWorker" << endl;
    while(1) {
        Container* con = persistQueue->pop();
        if(con->isLast()) {
            delete con;
            break;
        }
        persistFullChunkTask(con->getName(), poolname, con->getRaw(), con->getLen());
        delete con;
    }

    cout << "[persistWorker] finish persisting" << endl;
}

void Worker::persistFullWorker(BlockingQueue<Container*>* persistQueue) {
    cout << "[persistWorker] persistWorker" << endl;
    while (1) {
        Container* con = persistQueue->pop();
        if (con->isLast()) {
            delete con;
            break;
        }
        string poolname;
        stringstream ss(con->getName());
        getline(ss, poolname, ':');
        getline(ss, poolname, ':');
        persistFullChunkTask(con->getName(), poolname, con->getRaw(), con->getLen());
        delete con;
    }

    cout << "[persistWorker] finish persisting" << endl;
}

void Worker::offlineWriteBatch(string filename, int pktid, string poolname, int filesize) {

}

void Worker::clientReadBatch(AGCommand* agCmd) {
    cout << "[Worker] clientReadBatch" << endl;
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    // get file recipe
    string key = "pktnum:" + filename + ":" + poolname;
    // cout << "key: " << key << endl;
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "EXISTS %s", key.c_str());
    // char* response = rReply->element[1]->str;
    int exists = rReply->integer;
    if (exists == 0) {
        cout << "[ERROR] file not exists" << endl;
        return;
    }
    freeReplyObject(rReply);
    rReply = (redisReply*)redisCommand(sendCtx, "GET %s", key.c_str());
    int pktnum, tmpnum;
    char* content = rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);

    freeReplyObject(rReply);
    redisFree(sendCtx); 

    // return pktnum to client
    string skey = "pktnumReply:" + key;
    tmpnum = htonl(pktnum);
    redisReply* rReply1 = (redisReply*)redisCommand(_localCtx, "rpush %s %b", skey.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(rReply1);

    readBatchObj(filename, poolname, pktnum);
}

void Worker::readBatchObj(string filename, string poolname, int pktnum) {
    // cout << "[Worker] readBatchObj" << endl;
    thread readBatchPktThrds[pktnum];
    for(int i = 0; i < pktnum; i++) {
        readBatchPktThrds[i] = thread([=]{readBatchPacketWorker(filename, poolname, i);});
    }

    for(int i = 0; i < pktnum; i++) {
        readBatchPktThrds[i].join();
    }
    // cout << "[Worker] finish readBatchObj" << endl;
}

static char* readContainerTask(string contName, string poolname, BaseFS* fs, int clen) {
    // cout << "[Worker] readContainerTask" << endl;
    BaseFile* file = new BaseFile(contName, poolname);
    int hasread = 0;
    char* buf = (char*)calloc(clen, sizeof(char));
    if(!buf) {
        cerr << "[ERROR] Worker::readChunkTask malloc buffer error!" << endl;
        return nullptr;
    }
    while(hasread < clen) {
        int len = fs->readFile(file, buf+hasread, clen-hasread, hasread);
        if(len <= 0) break;
        hasread += len;
    }
    // free(buf);
    delete file;
    // cout << "[Worker] finish readContainerTask" << endl;
    return buf;
}

static char* readContainerTask(int con_id, string poolname, BaseFS* fs, int clen) {
    cout << "[Worker] readContainerTask conid:" << con_id << " len:" << clen << endl;
    char* buf = (char*)calloc(clen, sizeof(char));
    if(!buf) {
        cerr << "[ERROR] Worker::readChunkTask malloc buffer error!" << endl;
        return nullptr;
    }
    fs->readFileInImage(con_id, poolname, clen, buf);
    // free(buf);
    cout << "[Worker] finish readContainerTask conid:" << con_id <<  endl;
    return buf;
}
void Worker::readBatchPacketWorker(string filename, string poolname, int pktid) {
    // cout << "[Worker] readBatchPacketWorker" << endl;
    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    vector<DPDataChunk*>* fpSequence = new vector<DPDataChunk*>();
    for (int i = 0; i < recipe->size(); i++) {
        fpSequence->push_back(new DPDataChunk());
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);

    ReadBuf readbuf(_conf);
    for(int i = 0; i < recipe->size(); i++) {
        string conName = string((*recipe)[i]->_chunk_store_filename) + ":" + string((*recipe)[i]->_origin_chunk_poolname) + ":" + to_string((*recipe)[i]->_conIp) + ":con:" + to_string((*recipe)[i]->_containerId);
        // cout << conName <<" fp:" << Router::hash((*recipe)[i]->_fp,sizeof(fingerprint)) << endl;
        if (readbuf.find(conName) == NULL) {
            char* buf = readContainerTask(conName, string((*recipe)[i]->_origin_chunk_poolname), _fs, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
            readbuf.insert(conName,buf);
        }
        int tmpoff = (*recipe)[i]->_offset;
        int tmplen, dataLen;
        char* raw = readbuf.find(conName);
        memcpy((void*)&tmplen, raw + tmpoff, sizeof(int));
        dataLen = ntohl(tmplen);
        (*fpSequence)[i]->setData(raw + tmpoff + sizeof(int), dataLen);
    }
    // combine fpSequence into a packet
    int pkt_len = 0;
    for(int i = 0; i < fpSequence->size(); i++) {
        pkt_len += (*fpSequence)[i]->getDatalen();
    }
    // cout << "pkt_len: " << pkt_len << endl;
    char* pkt_raw = new char[pkt_len+sizeof(int)];
    // int tmpid = htonl(pktid);
    // memcpy(pkt_raw, (void*)&tmpid, sizeof(int));
    int tmplen = htonl(pkt_len);
    memcpy(pkt_raw, (void*)&tmplen, sizeof(int));
    int off = sizeof(int);
    // cout << "fpSequence: " << fpSequence->size() << endl;
    for(int i = 0; i < fpSequence->size(); i++) {
        memcpy(pkt_raw+off, (*fpSequence)[i]->getData(), (*fpSequence)[i]->getDatalen());
        off += (*fpSequence)[i]->getDatalen();
    }
    // cout << "[worker] send packet to client" << endl;
    // send packet to client
    redisContext* sendCtx1 = RedisUtil::createContext(_conf->_localIp);
    // cout << key << endl;
    // printf("%s\n", pkt_raw);
    string key1 = "readfile:" +filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply1 = (redisReply*)redisCommand(sendCtx1, "rpush %s %b", key1.c_str(), (void*)pkt_raw, pkt_len+sizeof(int));
    freeReplyObject(rReply1);
    redisFree(sendCtx1);
    delete pkt_raw;
    for(int i = 0; i < recipe->size();i ++) {
        delete (*recipe)[i];
    }
    delete recipe;
    for(int i = 0; i < fpSequence->size(); i++) {
        delete (*fpSequence)[i];
    }
    delete fpSequence;
    // cout << "[Worker] finish readBatchPacketWorker" << endl;
}

void Worker::clientDeleteFull(AGCommand* agCmd) {
    cout << "[clientDeleteFull] clientDeleteFull begin" << endl;
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();

    string inquire_pktnum_key = "pktnum:" + filename + ":" + poolname;
    int ipId = hash<string>()(inquire_pktnum_key) % (_conf->_agentNum);                                           
    cout << "[clientDeleteFull] route to node " << ipId << " to  get file recipe pkynum, key is " << inquire_pktnum_key << endl;

    redisContext* inquire_pktnum_sendCtx=RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_pktnum_rReply = (redisReply*)redisCommand(inquire_pktnum_sendCtx, "GET %s", inquire_pktnum_key.c_str()); 
    assert(inquire_pktnum_rReply!=NULL);
    assert(inquire_pktnum_rReply->type==REDIS_REPLY_STRING);    
    int pktnum, tmpnum;
    char* content = inquire_pktnum_rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);                                                                         
    freeReplyObject(inquire_pktnum_rReply);
    redisFree(inquire_pktnum_sendCtx);
    cout << "[clientDeleteFull] get file recipe pktnum successfully, pktnum is " << pktnum << endl;

    cout << "[clientDeleteFull] delete all packet begin" << endl;
    
    for(int i=0;i<pktnum;i++) {
        deletePacketWorker(filename, poolname, i);
    }
    cout << "[clientDeleteFull] delete all packet done" << endl;

    string delete_done_key="deletefile:" + filename + ":" + poolname;
    redisContext* delete_done_sendCtx = RedisUtil::createContext(_conf->_localIp); 
    redisReply* delete_done_reply=(redisReply*)redisCommand(delete_done_sendCtx,"rpush %s %b",delete_done_key.c_str(), 
                                                            (void*)&delete_done_key, delete_done_key.size());
    cout << "[deletePacketWorker] send delete done reply to client done" << endl;
    cout << "[clientDeleteFull] clientDeleteFull done" << endl;
    freeReplyObject(delete_done_reply);
    redisFree(delete_done_sendCtx); 
}


void Worker::deleteChunkTask(string chunkname, string poolname, char* fp, BaseFS* fs) {
    // cout << "[deleteChunkTask] delete chunk begin, chunkname : " << chunkname << endl;

    int node_id = Router::chooseNode(fp, sizeof(fingerprint), _conf->_agentNum);            
    int new_refcnt=registerFpSub(fp, sizeof(fingerprint), _conf->_agentsIPs[node_id]);
    if(new_refcnt==0) {
        cout << "[deleteChunkTask] delete chunkname" << chunkname << " and delete register " << endl;
        registerChunkDelete(chunkname, node_id);                                                 
        BaseFile* file=new BaseFile(chunkname, poolname);
        assert(fs->removeFile(file)==SUCCESS);
    }
    // cout << "[deleteChunkTask] delete chunk done, chunkname : " << chunkname << endl;
}


void Worker::deleteChunkTask(string chunkname, string poolname, BaseFS* fs) {
    // cout << "[deleteChunkTask] delete chunk begin, chunkname : " << chunkname << endl;
    BaseFile* file = new BaseFile(chunkname, poolname);
    assert(fs->removeFile(file) == SUCCESS);
    // cout << "[deleteChunkTask] delete chunk done, chunkname : " << chunkname << endl;
}



#include <unordered_map>
void Worker::deletePacketWorker(string filename, string poolname, int pktid) {
    string key=filename + ":" + poolname + ":" + to_string(pktid);
    cout << "[deletePacketWorker] deletePacketWorker begin, delete packet is " << key << endl;


    string recipeKey = key+":"+"recipe";
    int ipId=hash<string>()(key) % (_conf->_agentNum);                                  
    cout << "[deletePacketWorker] route to node " << ipId << " to  get packet recipe, key is " << recipeKey << endl;
    redisContext* inquire_recipe_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_recipe_rReply = (redisReply*)redisCommand(inquire_recipe_sendCtx, "GET %s", recipeKey.c_str());
    assert(inquire_recipe_rReply!=NULL);
    assert(inquire_recipe_rReply->type==REDIS_REPLY_STRING);
    cout << "[deletePacketWorker] get recipe of packet [" << key << "] done" << endl;
    
    char* stream = inquire_recipe_rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    cout << "[deletePacketWorker] decode recipe of packet [" << key << "] done" << endl;
    freeReplyObject(inquire_recipe_rReply);
    redisFree(inquire_recipe_sendCtx);

    cout << "[deletePacketWorker] chunk num is " << recipe->size() << endl;

#ifdef PRINT_FINGERPRINT_REFCNT  

    std::unordered_map<std::string,int> map_;
    for(int i=0;i<recipe->size();i++) {
        string chunkname = filename + ":" + poolname + ":" + "c" + to_string(Router::hash((*recipe)[i]->_fp, sizeof(fingerprint)));
        int node_id = Router::chooseNode((*recipe)[i]->_fp, sizeof(fingerprint), _conf->_agentNum);
   
        redisContext* check_exist_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* check_exist_rReply;
        size_t hash_fp = Router::hash((*recipe)[i]->_fp, sizeof(fingerprint));
        check_exist_rReply = (redisReply*)redisCommand(check_exist_sendCtx, "EXISTS %s", to_string(hash_fp).c_str());
        assert(check_exist_rReply!=NULL);
        bool res = check_exist_rReply->integer ? true: false;
        assert(res);
        freeReplyObject(check_exist_rReply);
        redisFree(check_exist_sendCtx);
   
        redisContext* inquire_refcnt_Ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* inquire_refcnt_rReply;
        inquire_refcnt_rReply = (redisReply*)redisCommand(inquire_refcnt_Ctx, "GET %s", to_string(hash_fp).c_str());
        assert(inquire_refcnt_rReply!=NULL);
        map_[chunkname]++;
        freeReplyObject(inquire_refcnt_rReply);
        redisFree(inquire_refcnt_Ctx);
    }
    cout << "[deletePacketWorker] count fingerprint of this packet" << endl;
    for(auto& iter:map_) {
        cout << "[deletePacketWorker] chunkname is " << iter.first << " refcnt is " << iter.second << endl;
    }
#endif

    for(int i=0;i<recipe->size();i++) {
        // string chunkname = filename + ":" + poolname + ":" + "c" + to_string(Router::hash((*recipe)[i]->_fp, sizeof(fingerprint)));
        string chunkname = to_string(Router::hash((*recipe)[i]->_fp, sizeof(fingerprint)));
        deleteChunkTask(chunkname, poolname, (*recipe)[i]->_fp, _fs);                                         
    }


    string recipe_key=filename+":"+poolname+":"+to_string(pktid) + ":" + "recipe";
    cout << "[deletePacketWorker] delete recipe, key is " << recipe_key << " begin" << endl;
    redisContext* delete_recipe_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* delete_recipe_rReply = (redisReply*)redisCommand(delete_recipe_sendCtx, "DEL %s", recipeKey.c_str());
    assert(delete_recipe_rReply!=NULL);
    assert(delete_recipe_rReply->type==REDIS_REPLY_INTEGER);
    assert(delete_recipe_rReply->integer==1);
    cout << "[deletePacketWorker] delete recipe, key is " << recipe_key << " done" << endl;
    freeReplyObject(delete_recipe_rReply);
    redisFree(delete_recipe_sendCtx);
    cout << "[deletePacketWorker] delete all chunk of packet [" << key << "] done" << endl; 
    

}


void Worker::registerChunk(string chunkname, string filename, string poolname, string pktid, int chunk_id, int ipId) {
    string chunk_info=filename + ":" + poolname + ":" + pktid + ":" + to_string(chunk_id);    
    redisContext* register_chunk_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);         
    redisReply *register_chunk_rReply = (redisReply *)redisCommand(register_chunk_sendCtx, 
                                            "SET %s %b", ("register:"+chunkname).c_str(), chunk_info.c_str(), chunk_info.size());
    assert(register_chunk_rReply != NULL);
    assert(register_chunk_rReply->type == REDIS_REPLY_STATUS);
    assert(string(register_chunk_rReply->str)== "OK");
    freeReplyObject(register_chunk_rReply);
    redisFree(register_chunk_sendCtx);
}


void Worker::registerChunkDelete(string chunkname, int ipId) {
    redisContext* register_chunk_delete_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);         
    redisReply* register_chunk_delete_rReply = (redisReply *)redisCommand(register_chunk_delete_sendCtx, "DEL %s", ("register:"+chunkname).c_str());
    assert(register_chunk_delete_rReply != NULL);
    assert(register_chunk_delete_rReply->type==REDIS_REPLY_INTEGER);
    assert(register_chunk_delete_rReply->integer==1);
    freeReplyObject(register_chunk_delete_rReply);
    redisFree(register_chunk_delete_sendCtx);
}



void Worker::registerChunkAppend(string chunkname, string filename, string poolname, string pktid, int chunk_id, int ipId) {
    string chunk_info = ":" + filename + ":" + poolname + ":" + pktid + ":" + to_string(chunk_id);
    redisContext* register_chunk_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);         
    redisReply *register_chunk_rReply = (redisReply *)redisCommand(register_chunk_sendCtx, 
                                            "APPEND %s %b", ("register:"+chunkname).c_str(), chunk_info.c_str(), chunk_info.size());
    assert(register_chunk_rReply != NULL);
    freeReplyObject(register_chunk_rReply);
    redisFree(register_chunk_sendCtx);
}

void Worker::registerOriginChunkPoolname(string chunkname, string poolname, int ipId) {
    string key="origin_chunk_poolname:" + chunkname;
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);         
    redisReply* rReply = (redisReply *)redisCommand(sendCtx, "SET %s %b", key.c_str(), poolname.c_str(), poolname.size());
    assert(rReply != NULL);
    assert(rReply->type == REDIS_REPLY_STATUS);
    assert(string(rReply->str) == "OK");
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

void Worker::getOriginChunkPoolname(string chunkname, int ipId, string& origin_chunk_poolname) {
    string key="origin_chunk_poolname:" + chunkname;
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);   
    redisReply* rReply;
    while(1) {      
        rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
        assert(rReply != NULL);
        if(rReply->type!=REDIS_REPLY_NIL) {
            break;
        }
        freeReplyObject(rReply);
    }
    assert(rReply->type == REDIS_REPLY_STRING);
    origin_chunk_poolname = string(rReply->str);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

/**
 * logical address reordering on the local node
*/
void Worker::clientReorder() {
    
   unordered_map<string,vector<ChunkInfo*> > chunkInfos;
   vector<vector<string> > chunks_fp_with_locality;
   localRecognitionWorker(chunkInfos,chunks_fp_with_locality);
   
   string key = "reorderstart";
   redisContext* reorder_start_recvCtx = RedisUtil::createContext(_conf -> _localIp);
   redisReply* reorder_start_done_rReply = (redisReply*)redisCommand(reorder_start_recvCtx, "blpop %s 0", key.c_str());
   freeReplyObject(reorder_start_done_rReply);
   redisFree(reorder_start_recvCtx);

   createConCosWorker(chunkInfos,chunks_fp_with_locality);
       // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(_conf->_localIp);
    string wkey = "reorderfinish";
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    cout << "[clientReorder] send reorder done reply to client" << endl;

    for(auto iter:chunkInfos) {
        for(int i = 0; i < (iter.second).size(); i++){
        	delete (iter.second)[i];
        }
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

/*
 * local recognition 
 * 1 look for the chunkfps need to be reordered
 * 2 local recognition and get the sorted chunkfps
*/
void Worker::localRecognitionWorker(unordered_map<string,vector<ChunkInfo*> > &chunkInfos, vector<vector<string> > &chunks_fp_with_locality){
    //read the fingerprints of chunks
    redisContext* get_hashfp_Ctx = RedisUtil::createContext(_conf->_localIp);
    redisReply* hashfp_register_rReply = (redisReply*)redisCommand(get_hashfp_Ctx, "keys register:*");
    redisReply* hashfp_dupregister_rReply = (redisReply*)redisCommand(get_hashfp_Ctx, "keys dupregister:*");
    assert(hashfp_register_rReply->type != REDIS_REPLY_ERROR);
    assert(hashfp_dupregister_rReply->type != REDIS_REPLY_ERROR);
    cout << "[Worker::clientReorder] " << "the number of register hashfp: " << hashfp_register_rReply->elements << " dupregister hashfp: " << hashfp_dupregister_rReply->elements << endl;
    
    //choose the chunks which need to be reordered, and collect their details.
    		//key:hash_fp,value:ChunkInfo
    vector<string> chunknames;					//the chunknames of the chunks need to be reordered
    for (int i = 0; i < hashfp_register_rReply->elements; i++) {
    	string register_chunkname = hashfp_register_rReply->element[i]->str;
        vector<ChunkInfo*> info = lookupChunkInfo(register_chunkname);
        if (info.empty()) continue;
        string chunkname;
        stringstream ss(register_chunkname);
        getline(ss,chunkname,':');
        getline(ss,chunkname,':');
        if (chunkInfos.find(chunkname) == chunkInfos.end()) {
            chunkInfos[chunkname] = info;
            chunknames.push_back(chunkname);
	} else chunkInfos[chunkname].insert(chunkInfos[chunkname].end(),info.begin(),info.end());
    }
    for (int i = 0; i < hashfp_dupregister_rReply->elements; i++) {
    	string register_chunkname = hashfp_dupregister_rReply->element[i]->str;
        vector<ChunkInfo*> info = lookupChunkInfo(register_chunkname);
        if (info.empty()) continue;
        string chunkname;
        stringstream ss(register_chunkname);
        getline(ss,chunkname,':');
        getline(ss,chunkname,':');
        if (chunkInfos.find(chunkname) == chunkInfos.end()) {
            chunkInfos[chunkname] = info;
            chunknames.push_back(chunkname);
	} else chunkInfos[chunkname].insert(chunkInfos[chunkname].end(),info.begin(),info.end());
    }   
    freeReplyObject(hashfp_register_rReply);
    freeReplyObject(hashfp_dupregister_rReply);
    redisFree(get_hashfp_Ctx);
    
    //locality recognition
    LocalRecognition* _localrecognition = new LocalRecognition(_conf, chunknames, chunkInfos);
    _localrecognition->LocalRecognitionWorker();
    chunks_fp_with_locality = _localrecognition->getChunksFPWithLocality();
    delete _localrecognition;
    
    string key = "localrecognitionfinish";
    cout << _conf -> _localIp << endl;
    unsigned int tmpnum = htonl(_conf -> _localIp);
    redisContext* local_recognition_done_recvCtx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    redisReply* local_recognition_done_rReply= (redisReply*)redisCommand(local_recognition_done_recvCtx, "rpush %s %b", key.c_str(),(void*)&tmpnum,sizeof(tmpnum));
    freeReplyObject(local_recognition_done_rReply);
    redisFree(local_recognition_done_recvCtx);
}


/*
 * read the chunks, push the chunks and update the packet recipe
*/

void Worker::createConCosWorker(unordered_map<string,vector<ChunkInfo*> > &chunkInfos, vector<vector<string> > &chunks_fp_with_locality){
  
    string recipe_name;
    ReadBuf read_container_buf(_conf);
    unordered_map<string, int> recipeIp; 	//key:recipe_name, value: the node_id of the recipe
    string poolname; 
    unordered_map<string, vector<WrappedFP*>*> recipeBuf;
    for(int i = 0; i < chunks_fp_with_locality.size(); i++) {
        cout << "the index of chunks_fp_with_locality: " << i << " ;the size of chunks_fp_with_locality[i]: " << chunks_fp_with_locality[i].size() << endl;
    	for(int j = 0; j < chunks_fp_with_locality[i].size(); j++) {

    	    string chunkname = chunks_fp_with_locality[i][j];
    	    
    	    //read packet recipe
    	    int pktid = (chunkInfos[chunkname][0]->pkt_chunk_id).begin()->first;
    	    int chunkid = ((chunkInfos[chunkname][0]->pkt_chunk_id).begin()->second)[0];
    	    poolname = chunkInfos[chunkname][0]->_poolname;
    	    recipe_name = chunkInfos[chunkname][0]->_filename+":"+chunkInfos[chunkname][0]->_poolname+":"+to_string(pktid);
    	    string key_recipe = recipe_name + ":recipe";
            if(recipeBuf.find(recipe_name) == recipeBuf.end()) {
    	    	int ipId = hash<string>()(recipe_name) % (_conf->_agentNum);
            	vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
            	readPktRecipe(recipe_name, recipe,ipId);
            	recipeIp[recipe_name] = ipId;
            	recipeBuf[recipe_name] = recipe;
    	    }
            
            //read container to build chunk and push it to the store lay
            int conid = (*recipeBuf[recipe_name])[chunkid]->_containerId;
    	    int conoff = (*recipeBuf[recipe_name])[chunkid]->_offset;
    	    string conName = "container_" + to_string(conid);
            if (read_container_buf.find(conName) == NULL) {
            	string key=conName + "_poolname";             			// key : container_[id]_poolname
            	redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[conid % _conf->_agentNum]);         
            	redisReply* rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
            	assert(rReply != NULL);
            	assert(rReply->type == REDIS_REPLY_STRING);
            	string poolname = string(rReply->str);
            	char* buf = readContainerTask(conid, poolname, _fs, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
            	read_container_buf.insert(conName,buf);
            	freeReplyObject(rReply);
    	     	redisFree(sendCtx);
            }
            int tmplen, dataLen;
            char* raw = read_container_buf.find(conName);
        
            memcpy((void*)&tmplen, raw + conoff, sizeof(int));
            dataLen = ntohl(tmplen);
            DPDataChunk* chunk = new DPDataChunk();
            chunk->setData(raw + conoff + sizeof(int), dataLen);
            chunk->setLast(false);
            _storeHandler->push(chunk, &conid, &conoff, poolname);
            
            //update the packet recipe     
            vector<ChunkInfo*> info = chunkInfos[chunkname];
            for (int l = 0; l < info.size(); l++) {
            	string name = info[l]->_filename + ":" + info[l]->_poolname;
            	unordered_map<int, vector<int> > pkt_chunk_id = info[l]->pkt_chunk_id;
            	for (unordered_map<int, vector<int> >::iterator iter = pkt_chunk_id.begin(); iter != pkt_chunk_id.end(); iter++) {
                   string update_recipe_name = name + ":" + to_string(iter->first);
                   if (recipeBuf.find(update_recipe_name) == recipeBuf.end()) {
                        int ipId = hash<string>()(update_recipe_name) % (_conf->_agentNum);
                   	 vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
                   	 readPktRecipe(update_recipe_name,recipe,ipId);
                   	 recipeIp[update_recipe_name] = ipId;
                   	 recipeBuf[update_recipe_name] = recipe;
                    }
                   vector<int> chunkids = iter->second;
                   for (int j = 0; j < chunkids.size(); j++) {
                       (*recipeBuf[update_recipe_name])[chunkids[j]]->_offset = conoff;
                       (*recipeBuf[update_recipe_name])[chunkids[j]]->_containerId = conid;
                        //cout << recipe_name << ":" << chunkids[j] << ":" << conid << endl;
                   }                   
            	}
            }
            registerFp(chunkname,_conf->_localIp, 0, conid, conoff);
            delete chunk;
	}
	DPDataChunk* lastChunk = new DPDataChunk();
    	lastChunk->setLast(true);
    	_storeHandler->push(lastChunk, nullptr, nullptr, poolname);
    	delete lastChunk;
    }   
    read_container_buf.clear();

    //store the contents of recipeBuf in redis 
    for (unordered_map<string, vector<WrappedFP*>*>::iterator iter = recipeBuf.begin(); iter != recipeBuf.end(); iter++) {
        redisContext* recipe_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[recipeIp[iter->first]]);
        int alloc_len = sizeof(int) + (iter->second)->size() * (PacketRecipeSerializer::RECIPE_SIZE);
        char* stream = new char[alloc_len];
        PacketRecipeSerializer::encode(iter->second, stream, alloc_len);
        redisReply* rReply = (redisReply*)redisCommand(recipe_sendCtx,
            "SET %s %b", (iter->first + ":recipe").c_str(), (void*)stream, alloc_len);
        delete [] stream;
        freeReplyObject(rReply);
        redisFree(recipe_sendCtx);
    }
    
    for (unordered_map<string, vector<WrappedFP*>*>::iterator iter = recipeBuf.begin(); iter != recipeBuf.end(); iter++){
        for(int i = 0; i < (iter->second)->size();i ++) {
            delete (*(iter->second))[i];
        }
        delete iter->second;
    }
  
    cout << "[createConCosWorker] finish create container" << endl;   
}

/**
 * read packet recipe
*/
void Worker::readPktRecipe(string recipename, vector<WrappedFP*>* recipe, int ipId){
    string recipeKey = recipename + ":recipe";
    redisContext* inquire_recipe_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_recipe_rReply = (redisReply*)redisCommand(inquire_recipe_sendCtx, "GET %s", recipeKey.c_str());
    assert(inquire_recipe_rReply != NULL);
    if(inquire_recipe_rReply->type != REDIS_REPLY_STRING) {
    	cout << ipId << endl;
    }
    assert(inquire_recipe_rReply->type == REDIS_REPLY_STRING);

    char* str = inquire_recipe_rReply->str;
    PacketRecipeSerializer::decode(str, recipe);
    freeReplyObject(inquire_recipe_rReply);
    redisFree(inquire_recipe_sendCtx);
}

/**
 * write packet recipe
*/
void Worker::writePktRecipe(string recipename, vector<WrappedFP*>* recipe, int ipId){
    redisContext* recipe_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    int alloc_len = sizeof(int) + recipe->size() * (PacketRecipeSerializer::RECIPE_SIZE);
    char* stream = new char[alloc_len];
    PacketRecipeSerializer::encode(recipe, stream, alloc_len);
    redisReply* rReply = (redisReply*)redisCommand(recipe_sendCtx,
            "SET %s %b", (recipename + ":recipe").c_str(), (void*)stream, alloc_len);
    delete [] stream;
    freeReplyObject(rReply);
    redisFree(recipe_sendCtx);
}

vector<ChunkInfo*> Worker::lookupChunkInfo(string register_chunkname) {
    string u_chunkname = "u:" + register_chunkname;
    redisContext* get_chunkinfo_Ctx = RedisUtil::createContext(_conf -> _localIp);
    
    redisReply* union_rReply = (redisReply*)redisCommand(get_chunkinfo_Ctx, "SETNX %s 1", u_chunkname.c_str());
    vector<ChunkInfo*> info;
    // cout << "union_rReply->integer" << union_rReply->integer << endl;
    if (union_rReply->integer) {
        redisReply* chunkinfo_rReply = (redisReply*)redisCommand(get_chunkinfo_Ctx, "GET %s", register_chunkname.c_str());
        assert(chunkinfo_rReply != NULL);
        assert(chunkinfo_rReply->type == REDIS_REPLY_STRING);

        info = splitChunkInfo(chunkinfo_rReply->str, 4);
        freeReplyObject(chunkinfo_rReply);
    }

    freeReplyObject(union_rReply);
    redisFree(get_chunkinfo_Ctx);
    return info;
}


/**
 * split the information of the chunks
*/
vector<ChunkInfo*> Worker::splitChunkInfo(string Information, int size) {
    // cout << "information:" << Information << endl;
    vector<string> info;
    stringstream input(Information);
    string temp, storepool;
    vector<ChunkInfo*> res;
    unordered_map<string, int> index;
    int count = 0;
    while (getline(input, temp, ':'))
    {
        info.push_back(temp);
        if (++count == 2) storepool = temp;
        if (!(count % size)) {
            string name = info[0] + ":" + info[1];
            if (index.find(name) == index.end()) {
                ChunkInfo* new_info = new ChunkInfo(info[0], info[1], storepool);
                (new_info->pkt_chunk_id)[stoi(info[2])].push_back(stoi(info[3]));
                index[name] = res.size();
                res.push_back(new_info);
            }
            else {
                res[index[name]]->_count++;
                (res[index[name]]->pkt_chunk_id)[stoi(info[2])].push_back(stoi(info[3]));
            }
            info.clear();
        }
    }
    // cout << "pktnum" << (res[0]->pkt_chunk_id).size() << endl;
    if (res.size() > 1) std::sort(res.begin(), res.end());
    return res;
}

/*
 * register the container information
*/
void Worker::registerConInfo(string chunkname,int ipId,string filename, string poolname,int conId,int conOff){
    string u_chunkname = "u:"+chunkname;
    string conInfo = filename + ":" + poolname + ":" + to_string(conId) + ":" + to_string(_conf->_localIp) + ":" + to_string(conOff);
    redisContext* register_chunkinfo_Ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(register_chunkinfo_Ctx, 
        				"SET %s %b",u_chunkname.c_str(),conInfo.c_str(),conInfo.size());
    freeReplyObject(rReply);
    redisFree(register_chunkinfo_Ctx);
}

/*
 * delete container information
*/
void Worker::deleteRegisterConInfo(string chunkname,int ipId){
    string u_chunkname = "u:"+chunkname;
    redisContext* remove_chunkinfo_Ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(remove_chunkinfo_Ctx, "DEL %s %b",u_chunkname.c_str());
    assert(rReply != NULL);
    assert(rReply->type==REDIS_REPLY_INTEGER);
    assert(rReply->integer==1);
    freeReplyObject(rReply);
    redisFree(remove_chunkinfo_Ctx);
}

/*
 * get the container information that registered
*/
bool Worker::getConInfo(string chunkname,int ipId,string& conInfo){
    string key="u:" + chunkname;
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);   
    redisReply* rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
    assert(rReply != NULL);
    if(rReply->type==REDIS_REPLY_NIL) {
    	freeReplyObject(rReply);
    	redisFree(sendCtx);
	return false;
    }
    assert(rReply->type == REDIS_REPLY_STRING);
    conInfo = string(rReply->str);
    freeReplyObject(rReply);
    redisFree(sendCtx);
    return true;
}

/*
 * register files which is the node
*/
void Worker::registerFilesInNode(string filename,string poolname,int nodeid){
    string key="files";
    string name = filename + ":" + poolname; 
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[nodeid]);   
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "SADD %s %b", key.c_str(), name.c_str(), name.size());
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

/**
 * push a packet to store layer for aggregate and write
*/
void Worker::clientWriteFullAndRandomAggregate(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int filesize = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if(mode == "online") {
        onlineWriteFullAndRandomAggregate(filename, pktid, poolname, filesize, sender);
    } else {
        std::cout << "not implemented" << std::endl;
        exit(-1);
    }
}

void Worker::onlineWriteFullAndRandomAggregate(string filename, int pktid, string poolname, int filesize, unsigned int sender) {
    cout << "[Worker] onlineWriteFullAndRandomAggregate : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " begin "<< endl;

    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Lookup, register in the corresponding node, push non-exists chunk to store layer
    thread lookupToStoreLayerThread = thread([=]{lookupToStoreLayerWorker(hashQueue, filename, poolname, pktid);}); 


    // 3. Lookup and Register FP table in the corresponding node
    // BlockingQueue<DPDataChunk*>* lookupQueue = new BlockingQueue<DPDataChunk*>();
    // thread lookupThread = thread([=]{lookupWorker(hashQueue, lookupQueue);});

    // 4. Push non-exist chunk to store layer, construct packet recipe
    // thread persistFullToStoreLayerThread = thread([=]{persistFullToStoreLayerWorker(lookupQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    // lookupThread.join();
    // persistFullToStoreLayerThread.join();
    lookupToStoreLayerThread.join();

    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    cout << "[Worker] onlineWriteFullAndRandomAggregate : send write done reply of filename :" << filename << " poolname:" << poolname << " pktid:" << pktid << " to client" << endl;
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    // delete lookupQueue;
}

void PrintRecipe(vector<WrappedFP*>* recipe) {
    cout << "[PrintRecipe] begin " << endl;
    for(int i = 0; i < recipe->size(); i++) {
        size_t hash_fp = Router::hash(recipe->at(i)->_fp, sizeof(fingerprint));
        cout << "hash_fp:" << hash_fp << " dup:" << recipe->at(i)->_dup << " con_id:" << recipe->at(i)->_containerId << " con_off:" << recipe->at(i)->_offset << endl;
    }
    cout << "[PrintRecipe] done" << endl;
}


/**
 * push all non-exist chunk of the packet to store layer for aggerate and write, construct packet recipe
 * NOTE: Whether or not the chunk exists and infomation of duplicate chunk has already been given by lookupWorker
*/
void Worker::persistFullToStoreLayerWorker(BlockingQueue<DPDataChunk*>* lookupQueue, string filename, string poolname, int pktid) {
    cout << "[persistFullToStoreLayerWorker] persistFullToStoreLayerWorker : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid); // key to construct packet recipe
    while(1) {
        DPDataChunk* chunk = lookupQueue->pop();
        if (chunk->isLast()) {
            _storeHandler->push(chunk, nullptr, nullptr,poolname); // push last chunk to store layer
            delete chunk;
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            // PrintRecipe(recipe);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            // std::cout << "[persistFullToStoreLayerWorker] persistFullToStoreLayerWorker : encode and write recipe of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << std::endl;
            freeReplyObject(rReply);
            break;
        }

        if(chunk->getDup()) {                                           // duplicate chunk
            pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), chunk->getConId(), chunk->getConOff()));
            size_t hash_fp = Router::hash(chunk->getFp(), sizeof(fingerprint));
            // cout << "[persistFullToStoreLayerWorker] persistFullToStoreLayerWorker : duplicate hash_fp: " << hash_fp << " refer to con_id:" << chunk->getConId() << " con_off:" << chunk->getConOff() << endl;

        } else {                                                        // unduplicate chunk, push the chunk to store layer for aggerate and write
            int con_id, con_off;
            _storeHandler->push(chunk, &con_id, &con_off, poolname); 
            size_t hash_fp = Router::hash(chunk->getFp(), sizeof(fingerprint));
            pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), con_id, con_off));
            int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
            registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], pktid, con_id, con_off);
            // cout << "[persistFullToStoreLayerWorker] persistFullToStoreLayerWorker : unduplicate hash_fp:" << hash_fp << " put in con_id:" << con_id << " con_off:" << con_off << endl;
        }
        delete chunk;
    }
    cout << "[persistFullToStoreLayerWorker] persistFullToStoreLayerWorker done" << endl;
}


void Worker::clientReadFullAfterRandomAggregate(AGCommand* agCmd) {
    cout << "[Worker] clientReadFullAfterRandomAggregate" << endl;
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    // get pktnum of this file
    string inquire_pktnum_key = "pktnum:" + filename + ":" + poolname; 
    
    int ipId = hash<string>()(inquire_pktnum_key) % (_conf->_agentNum);
    // cout << "[Worker] clientReadFullAfterRandomAggregate : inquire pktnum in node : " << ipId << " key : " << inquire_pktnum_key << endl;
    redisContext* inquire_pktnum_Ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_pktnum_rReply = (redisReply*)redisCommand(inquire_pktnum_Ctx, "EXISTS %s", inquire_pktnum_key.c_str());
    assert(inquire_pktnum_rReply != NULL);
    assert(inquire_pktnum_rReply->type == REDIS_REPLY_INTEGER);
    // cout << "[Worker] clientReadFullAfterRandomAggregate : inquire pktnum done, exists : " << inquire_pktnum_rReply->integer << endl;
    assert(inquire_pktnum_rReply->integer == 1);
    freeReplyObject(inquire_pktnum_rReply);
    
    inquire_pktnum_rReply = (redisReply*)redisCommand(inquire_pktnum_Ctx, "GET %s", inquire_pktnum_key.c_str());
    assert(inquire_pktnum_rReply != NULL);
    assert(inquire_pktnum_rReply->type == REDIS_REPLY_STRING);

    int pktnum, tmpnum;
    char* content = inquire_pktnum_rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);

    freeReplyObject(inquire_pktnum_rReply);
    redisFree(inquire_pktnum_Ctx); 
    // cout << "[Worker] clientReadFullAfterRandomAggregate : inquire pktnum done, pktnum is " << pktnum << endl;

    // return pktnum to client
    string send_pktnum_key = "pktnumReply:" + inquire_pktnum_key;
    tmpnum = htonl(pktnum);
    redisReply* send_pktnum_rReply = (redisReply*)redisCommand(_localCtx, "rpush %s %b", send_pktnum_key.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(send_pktnum_rReply);
    readFullObjAfterRandomAggregate(filename, poolname, pktnum);
}

void Worker::readFullObjAfterRandomAggregate(string filename, string poolname, int pktnum) {
    cout << "[Worker] readFullObjAfterRandomAggregate : read all packet of " << filename << " " << poolname << " begin" << endl;
    thread readPktThrds[pktnum];
    for(int i = 0; i < pktnum; i++) {
        readPktThrds[i] = thread([=]{readFullPacketAfterRandomAggregateWorker(filename, poolname, i);});
    }
    for(int i = 0; i < pktnum; i++) {
        readPktThrds[i].join();
    }
    cout << "[Worker] readFullObjAfterRandomAggregate : read all packet of " << filename << " " << poolname << " done" << endl;
}

void Worker::readFullPacketAfterRandomAggregateWorker(string filename, string poolname, int pktid) {
    cout << "[Worker] readFullPacketAfterRandomAggregateWorker : read " << filename << " " << poolname << " " << pktid << " begin" << endl;
    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    assert(rReply!=NULL);
    assert(rReply->type == REDIS_REPLY_STRING);
    // cout << "[Worker] readFullPacketAfterRandomAggregateWorker : get recipe of " << filename << " " << poolname << " " << pktid << " done" << endl;
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    vector<DPDataChunk*>* fpSequence = new vector<DPDataChunk*>();
    for (int i = 0; i < recipe->size(); i++) {
        fpSequence->push_back(new DPDataChunk());
    }
    freeReplyObject(rReply);
    redisFree(sendCtx);

    ReadBuf readbuf(_conf);
    for(int i = 0; i < recipe->size(); i++) {
        int con_id = (*recipe)[i]->_containerId;
        string conName = "container_" + to_string((*recipe)[i]->_containerId);
        if (readbuf.find(conName) == NULL) {
            string key=conName + "_poolname";             // key : container_[id]_poolname
            redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[(*recipe)[i]->_containerId % _conf->_agentNum]);         
            redisReply* rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
            assert(rReply != NULL);
            assert(rReply->type == REDIS_REPLY_STRING);
            string poolname = string(rReply->str);
            // cout << "[Worker] read container " << conName << " in pool " << poolname << endl;
            char* buf = readContainerTask(con_id, poolname, _fs, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
            readbuf.insert(conName,buf);
        }
        int tmpoff = (*recipe)[i]->_offset;
        int tmplen, dataLen;
        char* raw = readbuf.find(conName);
        memcpy((void*)&tmplen, raw + tmpoff, sizeof(int));
        dataLen = ntohl(tmplen);
        (*fpSequence)[i]->setData(raw + tmpoff + sizeof(int), dataLen);
    }
    // combine fpSequence into a packet
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
    redisContext* sendCtx1 = RedisUtil::createContext(_conf->_localIp);
    string key1 = "readfile:" +filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply1 = (redisReply*)redisCommand(sendCtx1, "rpush %s %b", key1.c_str(), (void*)pkt_raw, pkt_len+sizeof(int));
    freeReplyObject(rReply1);
    redisFree(sendCtx1);
    delete pkt_raw;
    for(int i = 0; i < recipe->size();i ++) {
        delete (*recipe)[i];
    }
    delete recipe;
    for(int i = 0; i < fpSequence->size(); i++) {
        delete (*fpSequence)[i];
    }
    delete fpSequence;
    cout << "[Worker] readFullPacketAfterRandomAggregateWorker : read " << filename << " " << poolname << " " << pktid << " done" << endl;
}


void Worker::clientWriteFullWithRBD(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if(mode == "online") {
        onlineWriteFullWithRBD(filename, poolname, pktid, size, sender);
    } else {
        std::cout << "not implemented" << std::endl;
        exit(-1);
    }
}

void Worker::onlineWriteFullWithRBD(string filename, string poolname, int pktid, int size, unsigned int sender) {
    cout << "[Worker] onlineWriteFullWithRBD : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " begin "<< endl;
    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Push every chunk to corresponding node for lookup and persist
    BlockingQueue<DPDataChunk*>* pushQueue = new BlockingQueue<DPDataChunk*>();
    thread pushThread = thread([=]{pushingWorker(hashQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    pushThread.join();
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete pushQueue;
    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    cout << "[Worker] onlineWriteFullWithRBD : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " done "<< endl;
}

/**
 * 1 push every chunk(duplicate and unduplicate) of this packet from hashQueue to corresponding node for lookup and persist
 * 2 wait for lookup and persist done, get conid and cooff from store layer
 * 3 construct packet recipe and store in local node
 * NOTE: node_id is specified by hash_fp % agentNum, register hash_fp in node_id and register packet recipe in local node
*/
void Worker::pushingWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[pushingWorker] pushingWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    std::vector<thread> push_threads;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        // 3 receive last chunk, construct packet recipe and store in local node
        if(chunk->isLast()) {                   // last chunk of this packet, send empty chunk to all node, register packet recipe in this node
            cout << "[pushingWorker] pushingWorker: pop a last chunk from hashQueue" << endl;
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            assert(rReply!=NULL);
            assert(rReply->type == REDIS_REPLY_STATUS);
            assert(strcmp(rReply->str, "OK")==0);
            freeReplyObject(rReply);
            delete stream;
            delete chunk;
            delete pRecipe;
            break;
        }
        #ifdef PRINT_PUSH_INFO
        cout << "[pushingWorker] pop a chunk from hashQueue filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << endl;
        #endif
        // 1 send chunk data to store layer of corresponding node for lookup and persist
        int chunkid = chunk->getId();
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        // RedisUtil::AskRedisContext(_conf->_agentsIPs[node_id]);
        registerFilesInNode(filename, poolname, node_id);
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType12(12, filename, poolname, pktid, chunkid, sizeof(fingerprint)+chunk->getDatalen(), _conf->_localIp);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
        delete agCmd;
        string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        char* buf = new char[sizeof(fingerprint) + chunk->getDatalen()];
        memcpy(buf, chunk->getFp(), sizeof(fingerprint));
        memcpy(buf+sizeof(fingerprint), chunk->getData(), chunk->getDatalen());
        redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+chunk->getDatalen());  // | fp | data |
        assert(write_reply!=NULL);
        assert(write_reply->type == REDIS_REPLY_INTEGER);
        assert(write_reply->integer == 1);
        #ifdef PRINT_PUSH_INFO
        cout << "[pushingWorker] route filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " to lookup and persist"<< endl;
        #endif
        freeReplyObject(write_reply);
        redisFree(write_ctx);
        delete [] buf;
        

        // 2 wait for lookup and persist done, return conid and conoff and construct packet recipe
        string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        // cout << "[pushingWorker] route filename: " << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " and wait for lookup and persist" << endl;
        redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
        assert(wait_reply!=NULL);
        assert(wait_reply->type == REDIS_REPLY_ARRAY);
        char* con_info = wait_reply->element[1]->str;
        int con_id, con_off, tmp_con_id, tmp_con_off;
        memcpy((void*)&tmp_con_id, con_info, sizeof(int));
        memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
        con_id = ntohl(tmp_con_id);
        con_off = ntohl(tmp_con_off);
        #ifdef PRINT_PUSH_INFO
        cout << "[pushingWorker] lookup and persist filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << "chunkid:" << chunk->getId() << " done" << endl;
        #endif
        pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), con_id, con_off));
        freeReplyObject(wait_reply);
        redisFree(wait_ctx);
        delete chunk;
        // RedisUtil::FreeRedisContext(_conf->_agentsIPs[node_id]);
    }

    cout << "[pushingWorker] pushingWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << endl;
}




void Worker::clientReadFullWithRBD(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    cout << "[Worker] clientReadFullWithRBD: filename:" << filename << " poolname:" << poolname << endl;
    // get pktnum of this file
    string inquire_pktnum_key = "pktnum:" + filename + ":" + poolname; 
    int ipId = hash<string>()(inquire_pktnum_key) % (_conf->_agentNum);
    redisContext* inquire_pktnum_Ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_pktnum_rReply = (redisReply*)redisCommand(inquire_pktnum_Ctx, "EXISTS %s", inquire_pktnum_key.c_str());
    assert(inquire_pktnum_rReply != NULL);
    assert(inquire_pktnum_rReply->type == REDIS_REPLY_INTEGER);
    assert(inquire_pktnum_rReply->integer == 1);
    freeReplyObject(inquire_pktnum_rReply);
    
    inquire_pktnum_rReply = (redisReply*)redisCommand(inquire_pktnum_Ctx, "GET %s", inquire_pktnum_key.c_str());
    assert(inquire_pktnum_rReply != NULL);
    assert(inquire_pktnum_rReply->type == REDIS_REPLY_STRING);

    int pktnum, tmpnum;
    char* content = inquire_pktnum_rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);

    freeReplyObject(inquire_pktnum_rReply);
    redisFree(inquire_pktnum_Ctx); 
    cout << "[Worker] clientReadFullAfterRandomAggregate : inquire pktnum done, pktnum is " << pktnum << endl;

    // return pktnum to client
    string send_pktnum_key = "pktnumReply:" + inquire_pktnum_key;
    tmpnum = htonl(pktnum);
    redisReply* send_pktnum_rReply = (redisReply*)redisCommand(_localCtx, "rpush %s %b", send_pktnum_key.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(send_pktnum_rReply);
    readFullObjAfterRandomAggregate(filename, poolname, pktnum);
}


void Worker::clientWriteFullAndSelectiveDedup(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if(mode == "online") {
        onlineWriteFullAndSelectiveDedup(filename, poolname, pktid, size, sender);
    } else {
        std::cout << "offline mode is not implemented" << std::endl;
        exit(-1);
    }
}

/**
 * 1 after route to store layer and get its information, if chunk is duplicate, push it to worker buffer
 * 2 when worker buffer is full, select some chunks of a packet
 * 3 store these chunk in local node
 * 4 update packet recipe
*/
void Worker::onlineWriteFullAndSelectiveDedup(string filename, string poolname, int pktid, int size, unsigned int sender) {
    cout << "[Worker] onlineWriteFullAndSelectiveDedup : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " begin "<< endl;
    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Push every chunk to corresponding node for lookup and persist
    BlockingQueue<DPDataChunk*>* pushQueue = new BlockingQueue<DPDataChunk*>();
    thread pushThread = thread([=]{pushingAndSelectiveDedupWorker(hashQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    pushThread.join();
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete pushQueue;
    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    cout << "[Worker] onlineWriteFullAndSelectiveDedup : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " done "<< endl;
}

/**
 * 1 push every chunk(duplicate and unduplicate) of this packet from hashQueue to corresponding node for lookup and persist
 * 2 wait for lookup and persist done, get conid and cooff from store layer
 * 3 if this chunk is duplicate, push it to worker buffer
 * 4 construct packet recipe and store in local node
 * 5 when receive a last chunk, check whether worker buffer is full, if full, select some chunk from a packet and push them to store layer, modify packet recipe
*/
void Worker::pushingAndSelectiveDedupWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[pushingAndSelectiveDedupWorker] pushingAndSelectiveDedupWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    std::vector<thread> push_threads;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    while(1) {                                          // request a buffer in worker buffer
        if(_worker_buffer->request(filename, poolname, pktid)) {
            break;
        }
        sleep(1);
    }
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        // 3 receive last chunk, construct packet recipe and store in local node
        if(chunk->isLast()) {                   // last chunk of this packet, send empty chunk to all node, register packet recipe in this node
            cout << "[pushingAndSelectiveDedupWorker] pushingAndSelectiveDedupWorker: pop a last chunk from hashQueue" << endl;
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            assert(rReply!=NULL);
            assert(rReply->type == REDIS_REPLY_STATUS);
            assert(strcmp(rReply->str, "OK")==0);
            freeReplyObject(rReply);
            delete stream;
            delete chunk;
            _worker_buffer->pushLast();
            break;
        }
        // cout << "[pushingWorker] pop a chunk from hashQueue filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << endl;
        
        // 1 send chunk data to store layer of corresponding node for lookup and persist
        int chunkid = chunk->getId();
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        // RedisUtil::AskRedisContext(_conf->_agentsIPs[node_id]);
        registerFilesInNode(filename, poolname, node_id);
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType12(12, filename, poolname, pktid, chunkid, sizeof(fingerprint)+chunk->getDatalen(), _conf->_localIp);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
        delete agCmd;
        string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        char* buf = new char[sizeof(fingerprint) + chunk->getDatalen()];
        memcpy(buf, chunk->getFp(), sizeof(fingerprint));
        memcpy(buf+sizeof(fingerprint), chunk->getData(), chunk->getDatalen());
        redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+chunk->getDatalen());  // | fp | data |
        assert(write_reply!=NULL);
        assert(write_reply->type == REDIS_REPLY_INTEGER);
        assert(write_reply->integer == 1);

        freeReplyObject(write_reply);
        redisFree(write_ctx);
        delete [] buf;
        

        // 2 wait for lookup and persist done, return conid and conoff and construct packet recipe
        string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        // cout << "[pushingAndSelectiveDedupWorker] route filename: " << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " and wait for lookup and persist" << endl;
        redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
        assert(wait_reply!=NULL);
        assert(wait_reply->type == REDIS_REPLY_ARRAY);
        char* con_info = wait_reply->element[1]->str;
        int con_id, con_off, dup, tmp_con_id, tmp_con_off, tmp_dup;
        memcpy((void*)&tmp_con_id, con_info, sizeof(int));
        memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
        memcpy((void*)&tmp_dup, con_info+sizeof(int)+sizeof(int), sizeof(int));
        con_id = ntohl(tmp_con_id);
        con_off = ntohl(tmp_con_off);
        dup = ntohl(tmp_dup);
        // cout << "[pushingAndSelectiveDedupWorker] lookup and persist filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << "chunkid:" << chunk->getId() << " dup:" << dup << " done" << endl;
        if(dup) {           // is duplicate chunk, push it to worker buffer
            _worker_buffer->push(chunk->getFp(), chunk->getData(), chunk->getDatalen(), filename, poolname, pktid, chunk->getId(), node_id);
        }
        pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), con_id, con_off));
        freeReplyObject(wait_reply);
        redisFree(wait_ctx);
        delete chunk;
        // RedisUtil::FreeRedisContext(_conf->_agentsIPs[node_id]);
    }

    cout << "[pushingAndSelectiveDedupWorker] pushingAndSelectiveDedupWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << endl;
}


void Worker::clientReadFullWithRBDCombineByClient(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    cout << "[Worker] clientReadFullWithRBDCombineByClient: filename:" << filename << " poolname:" << poolname << endl;
    
    // get pktnum of this file
    string inquire_pktnum_key = "pktnum:" + filename + ":" + poolname; 
    int ipId = hash<string>()(inquire_pktnum_key) % (_conf->_agentNum);
    redisContext* inquire_pktnum_ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* inquire_pktnum_reply = (redisReply*)redisCommand(inquire_pktnum_ctx, "GET %s", inquire_pktnum_key.c_str());
    assert(inquire_pktnum_reply != NULL);
    assert(inquire_pktnum_reply->type == REDIS_REPLY_STRING);
    int pktnum, tmpnum;
    char* content = inquire_pktnum_reply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    pktnum = ntohl(tmpnum);
    freeReplyObject(inquire_pktnum_reply);
    redisFree(inquire_pktnum_ctx); 
    cout << "[Worker] clientReadFullWithRBDCombineByClient : inquire pktnum done, pktnum is " << pktnum << endl;

    // return pktnum to client
    string send_pktnum_key = "pktnumReply:" + inquire_pktnum_key;
    tmpnum = htonl(pktnum);
    redisReply* send_pktnum_rReply = (redisReply*)redisCommand(_localCtx, "rpush %s %b", send_pktnum_key.c_str(), (void*)&tmpnum, sizeof(tmpnum));
    freeReplyObject(send_pktnum_rReply);

    // read all packet and sent to client
    cout << "[Worker] send readPacket commend to all worker, pktnum is " << pktnum;
    for(int i=0;i<pktnum;i++) {
        std::string key = filename + ":" + poolname + ":" + to_string(i);
        int ipId = hash<string>()(key) % (_conf->_agentNum);
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType19(19, filename, i, poolname,  "", 0, _conf->_localIp);
        // agCmd->sendTo(_conf->_agentsIPs[ipId]);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[ipId]);
        delete agCmd;
    }
}

void Worker::readPacket(AGCommand* agCmd) {
    ReadBuf* readbuf = new ReadBuf(_conf);
    string filename = agCmd->getFilename();
    string poolname = agCmd->getPoolname();
    int pktid = agCmd->getPktId();
    unsigned int client_ip = agCmd->getIp();
    cout << "[readPacket] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << "start" << std::endl;
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
        cout << "[readPacket] read pktid:" << pktid << "chunkid:" << i << " from container " << conName << endl;
        if (readbuf->find(conName) == NULL) {
            cout << "[readPacket] container " << conName << " is not in readbuf, need to read" << endl;
            string key=conName + "_poolname";             // key : container_[id]_poolname
            redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[(*recipe)[i]->_containerId % _conf->_agentNum]);         
            redisReply* rReply = (redisReply *)redisCommand(sendCtx, "GET %s", key.c_str());
            assert(rReply != NULL);
            assert(rReply->type == REDIS_REPLY_STRING);
            string poolname = string(rReply->str);
            cout << "[readPacket] start read container " << conName << endl;
            char* buf = readContainerTask(con_id, poolname, _fs, _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize));
            cout << "[readPacket] read " << conName << " done, insert it to readbuf" << endl;
            readbuf->insert(conName,buf);
            cout << "[readPacket] read container " << conName << " done" << endl;
        } else {
            cout << "[readPacket] container " << conName << " is already in readbuf" << endl;
        }
        int tmpoff = (*recipe)[i]->_offset;
        int tmplen, dataLen;
        char* raw = readbuf->find(conName);
        cout << "[readPacket] try to read pktid:" << pktid << " chunkid:" << i << " from container " << conName << endl;
        memcpy((void*)&tmplen, raw + tmpoff, sizeof(int));
        dataLen = ntohl(tmplen);
        (*fpSequence)[i]->setData(raw + tmpoff + sizeof(int), dataLen);
        cout << "[readPacket] read pktid:" << pktid << " chunkid:" << i << " from container " << conName << " done" << endl;
    }
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
    assert(rReply1->integer == 1);
    freeReplyObject(rReply1);
    redisFree(sendCtx1);
    delete pkt_raw;
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


void Worker::updatePacket(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    cout << "[updatePacket] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " size:" << size << " start" << std::endl;

    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Push every chunk to corresponding node for lookup and persist
    BlockingQueue<DPDataChunk*>* pushQueue = new BlockingQueue<DPDataChunk*>();
    thread update_and_pushThread = thread([=]{updateAndPushWorker(hashQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    update_and_pushThread.join();
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "updatefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    cout << "[updatePacket] filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " size:" << size << " done "<< endl;
}

/**
 * 1 get packet recipe and decode
 * 2 check whether the chunk need to be modified
 * 3 if need, chech whether new hash_fp has been register
 * 4.1 if yes, chech whether origin chunk is refered by only one chunk
 * 4.1.1 if yes, delete corresponding register info, and modify packet recipe
 * 4.1.2 if no, just modify packet recipe
 * 4.2 if no, check whether only one chunk refer to origin chunk
 * 4.2.1 if yes, modify corresponding container directly and modify packet recipe
 * 4.2.2 if no, push it to store layer and modify packet recipe
 */ 
void Worker::updateAndPushWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    
    // 1 get and decode packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    assert(rReply!=NULL);
    assert(rReply->type == REDIS_REPLY_STRING);
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    int new_hashfp_registered_and_old_only_one_refered=0;
    int new_hashfp_registered_and_old_more_than_one_refered=0;
    int new_hashfp_not_registered_and_old_only_one_refered=0;
    int new_hashfp_not_registered_and_old_more_than_one_refered=0;
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        if(chunk->isLast()) {
            cout << "[updateAndPushWorker] pop a last chunk from hashQueue" << endl;
            int alloc_len = sizeof(int) + recipe->size() * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            assert(rReply!=NULL);
            assert(rReply->type == REDIS_REPLY_STATUS);
            assert(strcmp(rReply->str, "OK")==0);
            freeReplyObject(rReply);
            delete stream;
            delete chunk;
            break;
        }

        // 2 check whether the chunk need to be modified
        int chunkid = chunk->getId();
        if(memcmp((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint))==0) {
            delete chunk;
            continue;
        }

        // 3 check whether new hash_fp has been register
        cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " need to update" << endl;
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], true);
        if(!res.empty()) {                                  // 4.1 new hash_fp has been registered, just modify packet recipe
            std::cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is modified and refer to another registered already chunk" << std::endl;
            (*recipe)[chunkid]->_containerId = res[1];
            (*recipe)[chunkid]->_offset = res[2];
            memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
            if(checkChunkRefered((*recipe)[chunkid]->_fp)) {// if old chunk if refered only by one chunk, delete corresponding register info
                new_hashfp_registered_and_old_only_one_refered++;
            } else {
                new_hashfp_registered_and_old_more_than_one_refered++;
            }
        } else {                                            // 4.2 new hash_fp is registered the first time, check whether origin chunk is refered by more than one chunk
            if(checkChunkRefered((*recipe)[chunkid]->_fp)) {// 4.2.1 origin chunk is refered by only one chunk, modify corresponding container directly and do not need to modify packet recipe
                std::cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is refered only by one chunk, will write new chunk to origin container and conoff directly" << std::endl;
                int con_id = (*recipe)[chunkid]->_containerId;
                int con_off = (*recipe)[chunkid]->_offset;
                int origin_node_id = Router::chooseNode((*recipe)[chunkid]->_fp, sizeof(fingerprint), _conf->_agentNum);
                // assert(origin_node_id == con_id/128);
                cout << "[updateAndPushWorker] route filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << origin_node_id << " conid:" << con_id << " conoff:" << con_off << " and wait for persist" << endl;
                AGCommand* agCmd = new AGCommand();                         // | fingerprint | conid | conoff | data |
                agCmd->buildType21(21, filename, poolname, pktid, chunkid, sizeof(fingerprint) + sizeof(int) + sizeof(int) + chunk->getDatalen(), _conf->_localIp);
                agCmd->sendToStoreLayer(_conf->_agentsIPs[origin_node_id]);
                delete agCmd;
                string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
                redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[origin_node_id]);
                char* buf = new char[sizeof(fingerprint) + sizeof(int) + sizeof(int) + chunk->getDatalen()];
                memcpy(buf, chunk->getFp(), sizeof(fingerprint));
                int tmp_con_id = htonl(con_id);
                int tmp_con_off = htonl(con_off);
                memcpy(buf+sizeof(fingerprint), (void*)&tmp_con_id, sizeof(int));
                memcpy(buf+sizeof(fingerprint)+sizeof(int), (void*)&tmp_con_off, sizeof(int));
                memcpy(buf+sizeof(fingerprint)+sizeof(int)+sizeof(int), chunk->getData(), chunk->getDatalen());
                redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+sizeof(int)+sizeof(int)+chunk->getDatalen());  // | fp | data |
                assert(write_reply!=NULL);
                assert(write_reply->type == REDIS_REPLY_INTEGER);
                assert(write_reply->integer == 1);

                freeReplyObject(write_reply);
                redisFree(write_ctx);
                delete [] buf;

                // wait for persist done
                string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
                redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[origin_node_id]);
                redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
                cout << "[updateAndPushWorker] receive writefinish replypersist filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " done" << endl;
                assert(wait_reply!=NULL);
                assert(wait_reply->type == REDIS_REPLY_ARRAY);
                freeReplyObject(wait_reply);
                redisFree(wait_ctx);
                registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], pktid, con_id, con_off); 
                memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
                delete chunk;
                new_hashfp_not_registered_and_old_only_one_refered++;
            } else {                                        // 4.2.2 origin chunk is refered by more than one chunk, push new chunk to store layer and modify packet recipe
                std::cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is refered by more than one chunk, will write to another container" << std::endl;
                int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
                AGCommand* agCmd = new AGCommand();
                agCmd->buildType12(12, filename, poolname, pktid, chunkid, sizeof(fingerprint)+chunk->getDatalen(), _conf->_localIp);
                agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
                delete agCmd;
                string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
                redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
                char* buf = new char[sizeof(fingerprint) + chunk->getDatalen()];
                memcpy(buf, chunk->getFp(), sizeof(fingerprint));
                memcpy(buf+sizeof(fingerprint), chunk->getData(), chunk->getDatalen());
                redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+chunk->getDatalen());  // | fp | data |
                assert(write_reply!=NULL);
                assert(write_reply->type == REDIS_REPLY_INTEGER);
                assert(write_reply->integer == 1);

                freeReplyObject(write_reply);
                redisFree(write_ctx);
                delete [] buf;
                
                // wait for lookup and persist done, return conid and conoff and construct packet recipe
                string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
                redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
                cout << "[pushingWorker] route filename: " << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " and wait for lookup and persist" << endl;
                redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
                assert(wait_reply!=NULL);
                assert(wait_reply->type == REDIS_REPLY_ARRAY);
                char* con_info = wait_reply->element[1]->str;
                int con_id, con_off, tmp_con_id, tmp_con_off;
                memcpy((void*)&tmp_con_id, con_info, sizeof(int));
                memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
                con_id = ntohl(tmp_con_id);
                con_off = ntohl(tmp_con_off);
                (*recipe)[chunkid]->_containerId = con_id;
                (*recipe)[chunkid]->_offset = con_off;
                memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
                freeReplyObject(wait_reply);
                redisFree(wait_ctx);
                delete chunk;
                new_hashfp_not_registered_and_old_more_than_one_refered++;
            }
        }    
    }

    cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << endl;
    cout << "new_hashfp_registered_and_old_only_one_refered:" << new_hashfp_registered_and_old_only_one_refered << endl;
    cout << "new_hashfp_registered_and_old_more_than_one_refered:" << new_hashfp_registered_and_old_more_than_one_refered << endl;
    cout << "new_hashfp_not_registered_and_old_only_one_refered:" << new_hashfp_not_registered_and_old_only_one_refered << endl;
    cout << "new_hashfp_not_registered_and_old_more_than_one_refered:" << new_hashfp_not_registered_and_old_more_than_one_refered << endl;
}

/**
 * check whether a chunk if refered by more than one chunk
 * if true, delete chunk register info, return true
 * NOTE: called when update and check the old chunk, fp is origin chunk fp
*/
bool Worker::checkChunkRefered(char* fp) {
    int node_id = Router::chooseNode(fp, sizeof(fingerprint), _conf->_agentNum);
    string chunkname = to_string(Router::hash(fp, sizeof(fingerprint)));
    redisContext* get_chunk_register_info_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* get_chunk_register_info_reply = (redisReply*)redisCommand(get_chunk_register_info_ctx, "DECR %s", ("refcnt:" + chunkname).c_str());
    assert(get_chunk_register_info_reply!=NULL);
    assert(get_chunk_register_info_reply->type == REDIS_REPLY_INTEGER);
    int new_cnt = get_chunk_register_info_reply->integer;
    freeReplyObject(get_chunk_register_info_reply);
    redisFree(get_chunk_register_info_ctx);
    if(new_cnt == 0) {
        return true;
    } else {
        return false;
    }

    // redisContext* get_chunk_register_info_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    // // redisReply* get_chunk_refcnt_reply = (redisReply*)redisCommand(get_chunk_register_info_ctx, "GET %s", ("refcnt:" + chunkname).c_str());
    // redisReply* get_chunk_register_info_reply = (redisReply*)redisCommand(get_chunk_register_info_ctx, "GET %s", ("refcnt:" + chunkname).c_str());
    // assert(get_chunk_register_info_reply!=NULL);
    // assert(get_chunk_register_info_reply->type != REDIS_REPLY_ERROR);
    // if(get_chunk_register_info_reply->type == REDIS_REPLY_NIL) {
    //     freeReplyObject(get_chunk_register_info_reply);
    //     redisFree(get_chunk_register_info_ctx);
    //     return true;
    // }
    // assert(get_chunk_register_info_reply->type != REDIS_REPLY_NIL);
    // // assert(get_chunk_register_info_reply->type == REDIS_REPLY_STRING);
    // assert(get_chunk_register_info_reply->type == REDIS_REPLY_INTEGER);
    // int cnt = get_chunk_register_info_reply->integer;

    // redisReply* del_refcnt_reply = (redisReply*)redisCommand(get_chunk_register_info_ctx, "DEL %s", ("refcnt:" + c


    // if(cnt == 1) {
    //     freeReplyObject(get_chunk_register_info_reply);
    //     redisFree(get_chunk_register_info_ctx);
    //     return true;
    // } else {
    //     freeReplyObject(get_chunk_register_info_reply);
    //     redisFree(get_chunk_register_info_ctx);
    //     return false;
    // }



    // std::istringstream ss(get_chunk_register_info_reply->str);
    // std::string temp_filename, temp_poolname, temp_pktid, temp_chunkid;
    // int cnt = 0;
    // while (std::getline(ss, temp_filename, ':')) {
    //     assert(std::getline(ss, temp_poolname, ':'));
    //     assert(std::getline(ss, temp_pktid, ':'));
    //     assert(std::getline(ss, temp_chunkid, ':'));
    //     cnt++;
    //     if(cnt>1) {
    //         break;
    //     }
    // }
    // freeReplyObject(get_chunk_register_info_reply);
    // redisFree(get_chunk_register_info_ctx);
    // assert(cnt>=1);
    // if(cnt == 1) {          // this chunk is refered only by one chunk, delete chunk info and hash_fp
    //     redisContext* delete_chunk_register_info_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    //     redisReply* delete_chunk_register_info_reply = (redisReply*)redisCommand(delete_chunk_register_info_ctx, "DEL %s", ("register:" + chunkname).c_str());
    //     assert(delete_chunk_register_info_reply!=NULL);
    //     assert(delete_chunk_register_info_reply->type == REDIS_REPLY_INTEGER);
    //     assert(delete_chunk_register_info_reply->integer == 1);
    //     freeReplyObject(delete_chunk_register_info_reply);
    //     redisFree(delete_chunk_register_info_ctx);
    //     redisContext* delete_hashfp_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    //     redisReply* delete_hashfp_reply = (redisReply*)redisCommand(delete_hashfp_ctx, "DEL %s", chunkname.c_str());
    //     assert(delete_hashfp_reply!=NULL);
    //     assert(delete_hashfp_reply->type == REDIS_REPLY_INTEGER);
    //     assert(delete_hashfp_reply->integer == 1);
    //     freeReplyObject(delete_hashfp_reply);
    //     redisFree(delete_hashfp_ctx);
    //     return true;
    // } else {
    //     return false;
    // }
}


void Worker::clientWriteFullWithRBDAndBatchPush(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if(mode != "online") {
        std::cout << "not implemented" << std::endl;
        exit(-1);
    }

    cout << "[Worker] clientWriteFullWithRBDAndBatchPush : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " begin "<< endl;
    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Push every chunk to corresponding node for lookup and persist
    BlockingQueue<DPDataChunk*>* pushQueue = new BlockingQueue<DPDataChunk*>();
    thread pushThread = thread([=]{batchPushingWorker(hashQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    pushThread.join();
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete pushQueue;
    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    cout << "[Worker] clientWriteFullWithRBDAndBatchPush : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " done "<< endl;
}



/**
 * 1 push every chunk(duplicate and unduplicate) of this packet from hashQueue a chunk buffer, compute node_id 
 * 2 when pop a last chunk, push chunk buffer to store layer worker
 * 3 wait for store layer worker to lookup and persist done
 * 4 construct packet recipe and store in local node
 * NOTE: node_id is specified by hash_fp % agentNum, register hash_fp in node_id and register packet recipe in local node
*/
void Worker::batchPushingWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[Worker] batchPushingWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    std::vector<thread> push_threads;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    std::unordered_map<int, std::vector<DPDataChunk*>> chunk_buffer;        // nodeid -> chunks
    std::unordered_map<int, DPDataChunk*> recipe_buffer;                    // chunid -> chunk
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        
        // 2 receive last chunk, push chunk buffer to store layer
        if(chunk->isLast()) {   
            delete chunk;  
            assert(recipe_buffer.size() == _conf->_pktSize / _conf->_chunkMaxSize); 
            cout << "[Worker] batchPushingWorker : pop a last chunk from hashQueue, will send chunk buffer to store layer" << endl;
            sendChunkBufferToStoreLayer(chunk_buffer, filename, poolname, pktid);
            cout << "[Worker] batchPushingWorker : store layer lookup and persist done, will construct packet recipe and store in local node" << endl;
            for(int chunkid = 0; chunkid < recipe_buffer.size(); chunkid++) {
                DPDataChunk* chunk = recipe_buffer[chunkid];
                assert(chunk->getId() == chunkid);
                pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), chunk->getConId(), chunk->getConOff()));
                // std::cout << "[Worker] register packet recipe chunkid:" << chunk->getId() << " in con_id:" << chunk->getConId() << " con_off:" << chunk->getConOff() << std::endl;
                delete chunk;
            }
            
            // 4 construct packet recipe and store in local node
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            assert(rReply!=NULL);
            assert(rReply->type == REDIS_REPLY_STATUS);
            assert(strcmp(rReply->str, "OK")==0);
            freeReplyObject(rReply);
            delete stream;
            break;
        }

        // 1 push chunk to chunk buffer
        int chunkid = chunk->getId();
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        chunk_buffer[node_id].push_back(chunk);
        recipe_buffer[chunkid] = chunk;
    }

    cout << "[pushingWorker] pushingWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << endl;
}

/**
 * for each chunk buffer
 * 1 combine to stream and send to store layer
 * 2 wait for store layer to lookup and persist done
 * 3 decode conid and conoff
 * stream: | chunkid | fingerprint | datalen | data | ...
 */
    
void Worker::sendChunkBufferToStoreLayer(std::unordered_map<int, std::vector<DPDataChunk*>> chunk_buffer, string filename, string poolname, int pktid) {
    for(auto iter:chunk_buffer) {
        // 1 combine to stream and send to store layer
        int node_id = iter.first;                           // node which these chunks route to
        auto chunks = iter.second;
        int chunk_cnt = chunks.size();
        int stream_len = chunk_cnt * (sizeof(int) + sizeof(fingerprint) + sizeof(int) + _conf->_chunkMaxSize);
        char* stream = new char[stream_len];
        // std::cout << "[Worker] sendChunkBufferToStoreLayer : send chunks to node:" << node_id << " chunk_cnt:" << chunk_cnt << " buf_size:" << stream_len << std::endl;
        
        int off = 0;
        for(auto chunk:chunks) {        // | chunkid | fingerprint | datalen | data |
            // std::cout << "[Worker] sendChunkBufferToStoreLayer : push chunkid:" << chunk->getId() << " hash_fp:" << Router::hash(chunk->getFp(), sizeof(fingerprint)) << " to stream" << std::endl;
            // chunkid
            int temp_chunkid = htonl(chunk->getId());
            memcpy(stream+off, (void*)&temp_chunkid, sizeof(int));
            // fingerprint
            memcpy(stream+off+sizeof(int), chunk->getFp(), sizeof(fingerprint));
            // datalen
            int tmp_datalen = htonl(chunk->getDatalen());
            memcpy(stream+off+sizeof(int)+sizeof(fingerprint), (void*)&tmp_datalen, sizeof(int));
            // data
            memcpy(stream+off+sizeof(int)+sizeof(fingerprint)+sizeof(int), chunk->getData(), chunk->getDatalen());
            off += (sizeof(int) + sizeof(fingerprint) + sizeof(int) + _conf->_chunkMaxSize);
        }
        
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType23(23, filename, poolname, pktid, stream_len, _conf->_localIp);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
        delete agCmd;
        string write_key = "chunksbuffer:" + filename + ":" + poolname + ":" + to_string(pktid);
        redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), stream, stream_len); 
        assert(write_reply!=NULL);
        assert(write_reply->type == REDIS_REPLY_INTEGER);
        assert(write_reply->integer == 1);
        freeReplyObject(write_reply);
        redisFree(write_ctx);
        delete [] stream;
        // std::cout << "[Worker] sendChunkBufferToStoreLayer : send chunks to node:" << node_id << " chunk_cnt:" << chunk_cnt << " buf size:" << stream_len << " done, wait for lookup and persist done" << std::endl;
        
        // 2 wait for store layer lookup and persist done
        string wait_key = "writechunksfinish:" + filename + ":" + poolname + ":" + to_string(pktid);
        redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
        assert(wait_reply!=NULL);
        assert(wait_reply->type == REDIS_REPLY_ARRAY);
        char* con_info = wait_reply->element[1]->str;
        // std::cout << "[Worker] sendChunkBufferToStoreLayer: receive con_info_stream from node:" << node_id << std::endl;
        
        // 3 decode conid and conoff
        off = 0;
        for(int i=0;i<chunk_cnt;i++) {
            int chunk_id, con_id, con_off, tmp_chunkid, tmp_con_id, tmp_con_off;
            memcpy((void*)&tmp_chunkid, con_info+off, sizeof(int));
            memcpy((void*)&tmp_con_id, con_info+off+sizeof(int), sizeof(int));
            memcpy((void*)&tmp_con_off, con_info+off+sizeof(int)+sizeof(int), sizeof(int));
            chunk_id = ntohl(tmp_chunkid);
            con_id = ntohl(tmp_con_id);
            con_off = ntohl(tmp_con_off);
            assert(chunks[i]->getId()==chunk_id);
            chunks[i]->setConId(con_id);
            chunks[i]->setConOff(con_off);
            off+= (sizeof(int)+sizeof(int)+sizeof(int));
            // std::cout << "[Worker] persist chunkid:" << chunk_id << " in conid:" << con_id << " conoff:" << con_off << std::endl;
        }
        freeReplyObject(wait_reply);
        redisFree(wait_ctx);
    }
}


/**
 * update chunk
 * 1 get packet recipe
 * 2 check whether new chunk is registered
 * 3.1 if yes, modify packet recipe directly
 * 3.2 else, check whether old chunk is refered by more than one chunk
 * 3.2.1 if yes, push new chunk to store layer and modify packet recipe
 * 3.2.2 else, push new chunk to store layer to store it in origin container and offset
 * 3 update packet recpie
 * TODO: update refcnt of chunk correctly
*/

void Worker::updateChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    string poolname = agCmd->getPoolname();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    cout << "[Worker] updateChunk :  filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " size:" << size << " start" << std::endl;

    assert(size == _conf->_chunkMaxSize);
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    redisReply* read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());
    char* content = read_reply->element[1]->str;
    DPDataChunk* chunk = new DPDataChunk(size, chunkid, content);
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    // std::cout << "[Worker] updateChunk : load chunk done" << std::endl;
    _dpHandler->hashing(chunk->getData(), chunk->getDatalen(), chunk->getFp());

    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* get_recipe_ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* get_recipe_reply = (redisReply*)redisCommand(get_recipe_ctx, "GET %s", recipeKey.c_str());
    assert(get_recipe_reply!=NULL);
    assert(get_recipe_reply->type == REDIS_REPLY_STRING);
    char* stream = get_recipe_reply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    freeReplyObject(get_recipe_reply);
    redisFree(get_recipe_ctx);
    // std::cout << "[Worker] updateChunk : get packet recipe done" << std::endl;
    int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
    vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], true);

    if(!res.empty()) {                                  // new hash_fp has been registered, just modify packet recipe
        std::cout << "[Worker] updateChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is modified and refer to another registered already chunk con_id:" << res[1] << " con_off:" << res[2] << std::endl;
        (*recipe)[chunkid]->_containerId = res[1]; 
        (*recipe)[chunkid]->_offset = res[2];
        memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
        checkChunkRefered((*recipe)[chunkid]->_fp);     // if old chunk if refered only by one chunk, delete corresponding register info
    } else {                                            // new hash_fp is registered the first time, check whether origin chunk is refered by more than one chunk
        if(checkChunkRefered((*recipe)[chunkid]->_fp)) {// origin chunk is refered by only one chunk, modify corresponding container directly and do not need to modify packet recipe
            std::cout << "[Worker] updateChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is refered only by one chunk, will write new chunk to origin container and conoff directly" << std::endl;
            int con_id = (*recipe)[chunkid]->_containerId;
            int con_off = (*recipe)[chunkid]->_offset;
            int origin_node_id = Router::chooseNode((*recipe)[chunkid]->_fp, sizeof(fingerprint), _conf->_agentNum);
            // cout << "[Worker] updateChunk : route filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << origin_node_id << " conid:" << con_id << " conoff:" << con_off << " and wait for persist" << endl;
            
            AGCommand* agCmd = new AGCommand();                         // | fingerprint | conid | conoff | data |
            agCmd->buildType21(21, filename, poolname, pktid, chunkid, sizeof(fingerprint) + sizeof(int) + sizeof(int) + chunk->getDatalen(), _conf->_localIp);
            agCmd->sendToStoreLayer(_conf->_agentsIPs[origin_node_id]);
            delete agCmd;
            string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
            redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[origin_node_id]);
            char* buf = new char[sizeof(fingerprint) + sizeof(int) + sizeof(int) + chunk->getDatalen()];
            memcpy(buf, chunk->getFp(), sizeof(fingerprint));
            int tmp_con_id = htonl(con_id);
            int tmp_con_off = htonl(con_off);
            memcpy(buf+sizeof(fingerprint), (void*)&tmp_con_id, sizeof(int));
            memcpy(buf+sizeof(fingerprint)+sizeof(int), (void*)&tmp_con_off, sizeof(int));
            memcpy(buf+sizeof(fingerprint)+sizeof(int)+sizeof(int), chunk->getData(), chunk->getDatalen());
            redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+sizeof(int)+sizeof(int)+chunk->getDatalen());  // | fp | data |
            assert(write_reply!=NULL);
            assert(write_reply->type == REDIS_REPLY_INTEGER);
            assert(write_reply->integer == 1);

            freeReplyObject(write_reply);
            redisFree(write_ctx);
            delete [] buf;

            // wait for persist done
            string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
            redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[origin_node_id]);
            redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
            // cout << "[Worker] updateChunk : receive writefinish reply, persist filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " done" << endl;
            assert(wait_reply!=NULL);
            assert(wait_reply->type == REDIS_REPLY_ARRAY);
            freeReplyObject(wait_reply);
            redisFree(wait_ctx);
            registerFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], pktid, con_id, con_off); 
            memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
        } else {                                        // origin chunk is refered by more than one chunk, push new chunk to store layer and modify packet recipe
            std::cout << "[updateAndPushWorker] filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is refered by more than one chunk, will write to another container" << std::endl;
            int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
            AGCommand* agCmd = new AGCommand();
            agCmd->buildType28(28, filename, poolname, pktid, chunkid, sizeof(fingerprint)+chunk->getDatalen(), _conf->_localIp);
            agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
            delete agCmd;
            string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
            redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
            char* buf = new char[sizeof(fingerprint) + chunk->getDatalen()];
            memcpy(buf, chunk->getFp(), sizeof(fingerprint));
            memcpy(buf+sizeof(fingerprint), chunk->getData(), chunk->getDatalen());
            redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+chunk->getDatalen());  // | fp | data |
            assert(write_reply!=NULL);
            assert(write_reply->type == REDIS_REPLY_INTEGER);
            assert(write_reply->integer == 1);

            freeReplyObject(write_reply);
            redisFree(write_ctx);
            delete [] buf;
            
            // wait for lookup and persist done, return conid and conoff and construct packet recipe
            string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
            redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
            // cout << "[pushingWorker] route filename: " << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " and wait for lookup and persist" << endl;
            redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
            assert(wait_reply!=NULL);
            assert(wait_reply->type == REDIS_REPLY_ARRAY);
            char* con_info = wait_reply->element[1]->str;
            int con_id, con_off, tmp_con_id, tmp_con_off;
            memcpy((void*)&tmp_con_id, con_info, sizeof(int));
            memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
            con_id = ntohl(tmp_con_id);
            con_off = ntohl(tmp_con_off);
            (*recipe)[chunkid]->_containerId = con_id;
            (*recipe)[chunkid]->_offset = con_off;
            memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
            freeReplyObject(wait_reply);
            redisFree(wait_ctx);
        }
    } 


    // update packet recipe
    int alloc_len = sizeof(int) + recipe->size() * (PacketRecipeSerializer::RECIPE_SIZE);
    char* new_stream = new char[alloc_len];
    PacketRecipeSerializer::encode(recipe, new_stream, alloc_len);
    redisReply* store_recipe_reply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)new_stream, alloc_len);
    assert(store_recipe_reply!=NULL);
    assert(store_recipe_reply->type == REDIS_REPLY_STATUS);
    assert(strcmp(store_recipe_reply->str, "OK")==0);
    freeReplyObject(store_recipe_reply);
    delete new_stream;
    delete chunk;
    for(auto iter = recipe->begin(); iter != recipe->end(); iter++) {
        delete *iter;
    }
    delete recipe;
    // std::cout << "[Worker] updateChunk : update packet recipe done" << std::endl;
    // send finish reply to client
    redisContext* finish_ctx = RedisUtil::createContext(sender);
    string wkey = "updatefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    redisReply* finish_reply = (redisReply*)redisCommand(finish_ctx, "rpush %s 1", wkey.c_str());
    freeReplyObject(finish_reply);
    redisFree(finish_ctx);

    cout << "[updatePacket] filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<  " size:" << size << " done "<< endl;
}



/**
 * simple update chunk
 * 1 get packet recipe in local redis
 * 2 check whether new chunk has been persisted
 * 3.1 if yes, just modify packet recipe
 * 3.2 else, route the chunk to corresponding node(computed by hash_fp) to persist, update packet recipe
 * TODO: update refcnt of new chunk correctly
*/

void Worker::simpleUpdateChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    int chunkid = agCmd->getChunkId();
    string poolname = agCmd->getPoolname();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    cout << "[Worker] simpleUpdateChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid << " size:" << size << " start" << std::endl;

    assert(size == _conf->_chunkMaxSize);
    redisContext* read_ctx = RedisUtil::createContext(_conf->_localIp);
    string read_key = filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    redisReply* read_reply = (redisReply*)redisCommand(read_ctx, "blpop %s 0", read_key.c_str());
    char* content = read_reply->element[1]->str;
    DPDataChunk* chunk = new DPDataChunk(size, chunkid, content);
    freeReplyObject(read_reply);
    redisFree(read_ctx);

    // std::cout << "[Worker] simpleUpdateChunk : load chunk done" << std::endl;
    _dpHandler->hashing(chunk->getData(), chunk->getDatalen(), chunk->getFp());

    // get packet recipe
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* get_recipe_ctx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* get_recipe_reply = (redisReply*)redisCommand(get_recipe_ctx, "GET %s", recipeKey.c_str());
    assert(get_recipe_reply!=NULL);
    assert(get_recipe_reply->type == REDIS_REPLY_STRING);
    char* stream = get_recipe_reply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    freeReplyObject(get_recipe_reply);
    redisFree(get_recipe_ctx);
    // std::cout << "[Worker] simpleUpdateChunk : get packet recipe done" << std::endl;
    int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
    vector<int> res = lookupFp(chunk->getFp(), sizeof(fingerprint), _conf->_agentsIPs[node_id], true);

    if(!res.empty()) {                                  // new hash_fp has been registered, just modify packet recipe
        std::cout << "[Worker] simpleUpdateChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is modified and refer to another already registered chunk con_id:" << res[1] << " con_off:" << res[2] << std::endl;
        (*recipe)[chunkid]->_containerId = res[1]; 
        (*recipe)[chunkid]->_offset = res[2];
        memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
    } else {                                            // new hash_fp is registered the first time, persist this new chunk 
        std::cout << "[Worker] simpleUpdateChunk : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<" is modified to a non-registered chunk, push new chunk to store layer" << std::endl;
        int node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType28(28, filename, poolname, pktid, chunkid, sizeof(fingerprint)+chunk->getDatalen(), _conf->_localIp);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[node_id]);
        delete agCmd;
        string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        char* buf = new char[sizeof(fingerprint) + chunk->getDatalen()];
        memcpy(buf, chunk->getFp(), sizeof(fingerprint));
        memcpy(buf+sizeof(fingerprint), chunk->getData(), chunk->getDatalen());
        redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, sizeof(fingerprint)+chunk->getDatalen());  // | fp | data |
        assert(write_reply!=NULL);
        assert(write_reply->type == REDIS_REPLY_INTEGER);
        assert(write_reply->integer == 1);

        freeReplyObject(write_reply);
        redisFree(write_ctx);
        delete [] buf;
        
        // wait for lookup and persist done, return conid and conoff and construct packet recipe
        string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        cout << "[Worker] simpleUpdateChunk : route filename: " << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk->getId() << " to node:" << node_id << " and wait for lookup and persist" << endl;
        redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
        assert(wait_reply!=NULL);
        assert(wait_reply->type == REDIS_REPLY_ARRAY);
        char* con_info = wait_reply->element[1]->str;
        int con_id, con_off, tmp_con_id, tmp_con_off;
        memcpy((void*)&tmp_con_id, con_info, sizeof(int));
        memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
        con_id = ntohl(tmp_con_id);
        con_off = ntohl(tmp_con_off);
        (*recipe)[chunkid]->_containerId = con_id;
        (*recipe)[chunkid]->_offset = con_off;
        cout << "[Worker] simpleUpdateChunk : persist new chunk done, con_id:" << con_id << " con_off:" << con_off << endl;
        memcpy((*recipe)[chunkid]->_fp, chunk->getFp(), sizeof(fingerprint));
        freeReplyObject(wait_reply);
        redisFree(wait_ctx);
    } 


    // update packet recipe
    int alloc_len = sizeof(int) + recipe->size() * (PacketRecipeSerializer::RECIPE_SIZE);
    char* new_stream = new char[alloc_len];
    PacketRecipeSerializer::encode(recipe, new_stream, alloc_len);
    redisReply* store_recipe_reply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)new_stream, alloc_len);
    assert(store_recipe_reply!=NULL);
    assert(store_recipe_reply->type == REDIS_REPLY_STATUS);
    assert(strcmp(store_recipe_reply->str, "OK")==0);
    freeReplyObject(store_recipe_reply);
    delete new_stream;
    delete chunk;
    for(auto iter = recipe->begin(); iter != recipe->end(); iter++) {
        delete *iter;
    }
    delete recipe;
    std::cout << "[Worker] simpleUpdateChunk : update packet recipe done" << std::endl;
    // std::cout << "[Worrker] simpleUpdateChunk : chunk_id0 in con_id:" << recipe->at(0)->_containerId << " con_off:" << recipe->at(0)->_offset << std::endl;
    // send finish reply to client
    redisContext* finish_ctx = RedisUtil::createContext(sender);
    string wkey = "updatefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
    redisReply* finish_reply = (redisReply*)redisCommand(finish_ctx, "rpush %s 1", wkey.c_str());
    freeReplyObject(finish_reply);
    redisFree(finish_ctx);

    cout << "[Worker] simpleUpdateChunk : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunkid <<  " size:" << size << " done "<< endl;
}





void Worker::clientWriteFullWithRBDAndPushSuperChunk(AGCommand* agCmd) {
    string filename = agCmd->getFilename();
    int pktid = agCmd->getPktId();
    string poolname = agCmd->getPoolname();
    string mode = agCmd->getMode();
    int size = agCmd->getFilesize();
    unsigned int sender = agCmd->getIp();
    if(mode != "online") {
        std::cout << "not implemented" << std::endl;
        exit(-1);
    }

    cout << "[Worker] clientWriteFullWithRBDAndPushSuperChunk : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " begin "<< endl;
    // 0. create threads for loading data from DPDataPacket
    BlockingQueue<DPDataPacket*>* loadQueue = new BlockingQueue<DPDataPacket*>();
    string key = filename + ":" + poolname;
    thread loadThread = thread([=]{loadWorker(loadQueue, key, pktid);});

    // 1. Chunking
    BlockingQueue<DPDataChunk*>* chunkQueue = new BlockingQueue<DPDataChunk*>();
    thread chunkThread = thread([=]{chunkingWorker(loadQueue, chunkQueue);});

    // 2. Hashing
    BlockingQueue<DPDataChunk*>* hashQueue = new BlockingQueue<DPDataChunk*>();
    thread hashThread = thread([=]{hashingWorker(chunkQueue, hashQueue);});

    // 3. Push every chunk to corresponding node for lookup and persist
    BlockingQueue<DPDataChunk*>* pushQueue = new BlockingQueue<DPDataChunk*>();
    thread pushThread = thread([=]{PushingSuperChunkWorker(hashQueue, filename, poolname, pktid);});

    loadThread.join();
    chunkThread.join();
    hashThread.join();
    pushThread.join();
    delete loadQueue;
    delete chunkQueue;
    delete hashQueue;
    delete pushQueue;
    // send finish reply to client
    redisContext* sendCtx = RedisUtil::createContext(sender);
    string wkey = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "rpush %s 1", wkey.c_str());
    freeReplyObject(rReply);
    redisFree(sendCtx);

    cout << "[Worker] clientWriteFullWithRBDAndPushSuperChunk : filename:" <<  filename << " poolname:" << poolname << " pktid:" << pktid << " done "<< endl;
}


/**
 * 1 push every chunk(duplicate and unduplicate) of this packet from hashQueue a chunk buffer
 * 2 when chunk buffer is full, send it to store layer to lookup and persist, record conid and conff
 * 3 when pop a last chunk, register packet recipe
 * NOTE: node_id is the node which the first chunk need to route to, and look up and persist all chunks of chunkbuffer in this node
*/
void Worker::PushingSuperChunkWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid) {
    cout << "[Worker] PushingSuperChunkWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " begin" << endl;
    PacketRecipe* pRecipe = new PacketRecipe();
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    std::unordered_map<int, std::vector<DPDataChunk*>> chunk_buffer;        // nodeid -> chunks
    std::unordered_map<int, DPDataChunk*> recipe_buffer;                    // chunid -> chunk
    int first_chunk_node_id = -1;
    while(1) {
        DPDataChunk* chunk = hashQueue->pop();
        
        // 3 receive last chunk
        if(chunk->isLast()) {   
            delete chunk;  
            assert(recipe_buffer.size() == _conf->_pktSize / _conf->_chunkMaxSize); 
            assert(chunk_buffer.size() == 0);  
            assert(first_chunk_node_id == -1);   
            for(int chunkid = 0; chunkid < recipe_buffer.size(); chunkid++) {
                DPDataChunk* chunk = recipe_buffer[chunkid];
                assert(chunk->getId() == chunkid);
                pRecipe->append(key, new WrappedFP(chunk->getFp(), chunk->getDatalen(), chunk->getDup(), chunk->getConId(), chunk->getConOff()));
                delete chunk;
            }
            
            // 4 construct packet recipe and store in local node
            vector<WrappedFP*>* recipe = pRecipe->getRecipe(key);
            int alloc_len = sizeof(int) + pRecipe->getRecipeSize(key) * (PacketRecipeSerializer::RECIPE_SIZE);
            char* stream = new char[alloc_len];
            PacketRecipeSerializer::encode(recipe, stream, alloc_len);
            string recipeKey = key + ":" + "recipe";
            redisReply* rReply = (redisReply*)redisCommand(_localCtx, "SET %s %b", recipeKey.c_str(), (void*)stream, alloc_len);
            assert(rReply!=NULL);
            assert(rReply->type == REDIS_REPLY_STATUS);
            assert(strcmp(rReply->str, "OK")==0);
            freeReplyObject(rReply);
            delete stream;
            break;
        }

        // 1 push chunk to chunk buffer
        int chunkid = chunk->getId();
        if(first_chunk_node_id == -1) {
            assert(chunk_buffer.size()==0);
            first_chunk_node_id = Router::chooseNode(chunk->getFp(), sizeof(fingerprint), _conf->_agentNum);
        }
        chunk_buffer[first_chunk_node_id].push_back(chunk);
        recipe_buffer[chunkid] = chunk;
        
        // 2 when chunk buffer if full, route chunk buffer to corresponding node, wait for lookup and persist done, record conid and conff
        if(chunk_buffer[first_chunk_node_id].size() == _conf->_super_chunk_size) {
            cout << "[Worker] PushingSuperChunkWorker : chunk buffer is full, will send chunks of filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " to store layer of node_id:" << first_chunk_node_id << endl;
            assert(chunk_buffer.size() == 1);
            sendChunkBufferToStoreLayer(chunk_buffer, filename, poolname, pktid);
            cout << "[Worker] PushingSuperChunkWorker : store layer lookup and persist done" << endl;
            first_chunk_node_id = -1;
            chunk_buffer.clear();
        }
    }

    cout << "[Worker] PushingSuperChunkWorker: filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " done" << endl;
}

void Worker::clearReadBuf(){
    ReadBuf* readbuftmp = new ReadBuf(_conf);
    cout << "[clearReadBuf] begin to clear readbuf of node:" << _conf->_localIp << " io_counts:" << readbuftmp->size() << endl;
    string key = "readbuf_clear_done";
    int io_cnt = readbuftmp->size();
    int tmp_io_cnt = htonl(io_cnt);
    redisReply* reply = (redisReply*)redisCommand(_localCtx, "rpush %s %b", key.c_str(), (void*)&tmp_io_cnt, sizeof(int));
    assert(reply!=NULL);
    assert(reply->type == REDIS_REPLY_INTEGER);
    assert(reply->integer == 1);
    freeReplyObject(reply);
    readbuftmp->clear();
    delete readbuftmp;
    cout << "[clearReadBuf] done" << endl;
}
