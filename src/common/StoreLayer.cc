#include "StoreLayer.hh"
#define USE_RBD

StoreLayer::StoreLayer(Config* conf) {
    _conf=conf;
    try {
        _processCtx = RedisUtil::createContext(_conf->_localIp);
        _localCtx = RedisUtil::createContext(_conf->_localIp);
    } catch (int e) {
        cerr << "initializing redis context error" << endl;
        exit(-1);
    }
#ifdef USE_RBD
    _rbd_fs = FSUtil::createRBDFS(_conf);
#else
    _fs = FSUtil::createFS(_conf);
#endif
    _cur_container_size = 0;
    _max_container_size = _conf->_containerSize;
    _max_len = _conf->_containerSize*(sizeof(int) + _conf->_chunkMaxSize);
    _cur_container_id = 0;                              // next container id to be written
    _cur_container_off = 0;                             // next offset in buffer
    _buffer = new char[_max_len];
    memset(_buffer, 0, _max_len);
    _cur_container_id = -1;
    for(int i=0;i<conf->_agentNum;i++) {
        if(conf->_localIp == conf->_agentsIPs[i]) {
            _cur_container_id = i * 256;
            break;
        }
    }
    assert(_cur_container_id != -1);
    cout << "[StoreLayer] StoreLayer initialize done, max container size is " << _max_container_size << " max len is " << _max_len << " cur container id is " << _cur_container_id << endl;
    _rbd_fs->createNewImage(_cur_container_id/256);
}

StoreLayer::~StoreLayer() {
    redisFree(_localCtx);
    redisFree(_processCtx);
#ifdef USE_RBD
    FSUtil::deleteFS(_rbd_fs);
#else
    FSUtil::deleteFS(_fs);
#endif
    delete[] _buffer;
}

/**
 * 1 push a chunk to buffer
 * 2 if buffer is full or push a lask chunk, flush it to ceph
*/
void StoreLayer::push(DPDataChunk* chunk, int* con_id, int* con_off, string poolname){
    _buffer_mutex.lock();
    _poolname = poolname;
    if(chunk->isLast()) {
        // cout << "receive last chunk, flush con_id: " << _cur_container_id << " to ceph, cur con_off:" << _cur_container_off << endl;
#ifdef USE_RBD
        flushWithRBD();
#else
        flush();
#endif
        _buffer_mutex.unlock();
        return ;
    }
    if(_cur_container_size == _max_container_size){         // buffer is full, write buffer to ceph as a container
        cout << "[StoreLayer] buffer is full, flush con_id: " << _cur_container_id << " to ceph" << endl;
#ifdef USE_RBD
        flushWithRBD();
#else
        flush();
#endif
    }

    *con_id = _cur_container_id;
    *con_off = _cur_container_off;

    // push chunk size to buffer
    int tmplen = htonl(chunk->getDatalen());
    if(_cur_container_off + sizeof(tmplen) > _max_len){
        cout << "[ERROR] container out of bound when pushing datasize!" << endl;
        exit(-1);
    }
    memcpy(_buffer+_cur_container_off, (void*)&tmplen, sizeof(tmplen));
    _cur_container_off+=sizeof(tmplen);

    if(_cur_container_off + chunk->getDatalen() > _max_len){
        cout << "[ERROR] container out of bound when pushing data!" << endl;
        exit(-1);
    }
    memcpy(_buffer+_cur_container_off, chunk->getData(), chunk->getDatalen());
    _cur_container_off+=chunk->getDatalen();

    _cur_container_size ++;
    _buffer_mutex.unlock();
}

/**
 * 1 write content in buffer to ceph as a container
 * 2 register container poolname in redis
 * 3 update container id, container size and container off
 * NOTE: called by Push() when buffer is full or push last chunk, mutex has been locked by Push()
*/
void StoreLayer::flush(){
    assert(false && "should not be called");
    // flush buffer to ceph
    string chunkname = "container_" + to_string(_cur_container_id); // container name : container_[id]
    BaseFile* file = new BaseFile(chunkname, _poolname);
    int res = _fs->writeFile(file, _buffer, _max_len);
    assert(res==SUCCESS);
    delete file;

    // register container poolname in redis
    string key=chunkname + "_poolname";             // key : container_[id]_poolname
    redisContext* store_container_poolname_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[_cur_container_id % _conf->_agentNum]);         
    redisReply* store_container_poolname_rReply = (redisReply *)redisCommand(store_container_poolname_sendCtx, "SET %s %b", key.c_str(), _poolname.c_str(), _poolname.size());
    assert(store_container_poolname_rReply != NULL);
    assert(store_container_poolname_rReply->type == REDIS_REPLY_STATUS);
    assert(string(store_container_poolname_rReply->str) == "OK");
    freeReplyObject(store_container_poolname_rReply);
    redisFree(store_container_poolname_sendCtx);

    // update container id, container size and container off
    updateContainerId();
    _cur_container_size = 0;
    _cur_container_off = 0;
    memset(_buffer, 0, _max_len);
}

/**
 * 1 write content in buffer to image
 * 2 register container poolname in redis
 * 3 update container id, container size and container off
 * NOTE: called by Push() when buffer is full or push last chunk, mutex has been locked by Push()
*/
void StoreLayer::flushWithRBD() {
    // flush buffer to ceph
    string chunkname = "container_" + to_string(_cur_container_id); // container name : container_[id]
    int res = _rbd_fs->writeFileToImage(_cur_container_id, _poolname, _max_len, _buffer);
    assert(res==SUCCESS);

    // register container poolname in redis
    string key=chunkname + "_poolname";             // key : container_[id]_poolname
    redisContext* store_container_poolname_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[_cur_container_id % _conf->_agentNum]);         
    redisReply* store_container_poolname_rReply = (redisReply *)redisCommand(store_container_poolname_sendCtx, "SET %s %b", key.c_str(), _poolname.c_str(), _poolname.size());
    assert(store_container_poolname_rReply != NULL);
    assert(store_container_poolname_rReply->type == REDIS_REPLY_STATUS);
    assert(string(store_container_poolname_rReply->str) == "OK");
    freeReplyObject(store_container_poolname_rReply);
    redisFree(store_container_poolname_sendCtx);

    // update container id, container size and container off
    // updateContainerId();
    _cur_container_id++;
    assert(_cur_container_id % 256 != 0);
    _cur_container_size = 0;
    _cur_container_off = 0;
    memset(_buffer, 0, _max_len);
}
/**
 * update container id, which is stored in redis of admin
*/
void StoreLayer::updateContainerId(){
    redisContext* update_container_id_Ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    redisReply* update_container_id_rReply;
    update_container_id_rReply = (redisReply*)redisCommand(update_container_id_Ctx, "INCR cur_container_id");
    assert(update_container_id_rReply!=NULL);
    assert(update_container_id_rReply->type==REDIS_REPLY_INTEGER);
    _cur_container_id = update_container_id_rReply->integer;
    freeReplyObject(update_container_id_rReply);
    redisFree(update_container_id_Ctx);
}

/**
 * try to lock image_id, if lock success, return, else sleep 1s and try again
 * NOTE: called before try to create a new image
*/
void StoreLayer::lock(int image_id) {
    while(true) {
        redisReply* lock_reply;
        redisContext* lock_ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
        string lock_key = "lock_" + to_string(image_id);
        lock_reply = (redisReply*)redisCommand(lock_ctx, "SETNX %s %b", lock_key.c_str(), "1", 1);
        assert(lock_reply!=NULL);
        assert(lock_reply->type == REDIS_REPLY_INTEGER);
        if(lock_reply->integer == 1) {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            cout << "[StoreLayer] lock image_id:" << image_id << " success" << endl;
            break;
        } else {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            sleep(1);
        }
    }
}

/**
 * unlock image_id and increase image_id_created
 * NOTE: called after create a new image
*/
void StoreLayer::unlock(int image_id) {
    redisReply* unlock_reply;
    redisContext* unlock_ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    string lock_key = "lock_" + to_string(image_id);
    unlock_reply = (redisReply*)redisCommand(unlock_ctx, "DEL %s", lock_key.c_str());
    assert(unlock_reply!=NULL);
    assert(unlock_reply->type == REDIS_REPLY_INTEGER);
    assert(unlock_reply->integer == 1);
    freeReplyObject(unlock_reply);
    redisFree(unlock_ctx);
    cout << "[StoreLayer] unlock image_id:" << image_id << " success" << endl;
}

/**
 * init container id, which is stored in redis of admin, each image has 256 containers
*/
void StoreLayer::initContainerId(){
    lock();
    redisContext* init_container_id_Ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    redisReply* init_container_id_rReply;
    init_container_id_rReply = (redisReply*)redisCommand(init_container_id_Ctx, "GET init_container_id");
    
    if(init_container_id_rReply->type==REDIS_REPLY_NIL) {
    	int tmpnum = htonl(_cur_container_id);
    	init_container_id_rReply = (redisReply*)redisCommand(init_container_id_Ctx, "SET init_container_id %b",(void*)&tmpnum,sizeof(tmpnum));
    	freeReplyObject(init_container_id_rReply);
    	redisFree(init_container_id_Ctx);
    	unlock();
    	return;
    }
    int tmpnum;
    char* content = init_container_id_rReply->str;
    memcpy((void*)&tmpnum, content, sizeof(int));
    _cur_container_id = ntohl(tmpnum) + 256;
    std::cout << "[StoreLayer] after initContainerId, cur container id is " << _cur_container_id << std::endl;
    //_cur_container_id = init_container_id_rReply->integer + 256;
    tmpnum = htonl(_cur_container_id);
    init_container_id_rReply = (redisReply*)redisCommand(init_container_id_Ctx, "SET init_container_id %b",(void*)&tmpnum,sizeof(tmpnum));
    
    freeReplyObject(init_container_id_rReply);
    redisFree(init_container_id_Ctx);
    unlock();
}

/**
 * try to lock container_id, if lock success, return, else sleep 1s and try again
 * NOTE: called before try to init a new container id
*/
void StoreLayer::lock() {
    while(true) {
        redisReply* lock_reply;
        redisContext* lock_ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
        string lock_key = "lock_container";
        lock_reply = (redisReply*)redisCommand(lock_ctx, "SETNX %s 1", lock_key.c_str());
        assert(lock_reply!=NULL);
        assert(lock_reply->type == REDIS_REPLY_INTEGER);
        if(lock_reply->integer == 1) {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            cout << "[StoreLayer] lock container success" << endl;
            break;
        } else {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            sleep(1);
        }
    }
}

/**
 * unlock and increase init_container_id
 * NOTE: called after init a new container
*/
void StoreLayer::unlock() {
    redisReply* unlock_reply;
    redisContext* unlock_ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    string lock_key = "lock_container";
    unlock_reply = (redisReply*)redisCommand(unlock_ctx, "DEL %s", lock_key.c_str());
    assert(unlock_reply!=NULL);
    assert(unlock_reply->type == REDIS_REPLY_INTEGER);
    assert(unlock_reply->integer == 1);
    freeReplyObject(unlock_reply);
    redisFree(unlock_ctx);
    cout << "[StoreLayer] unlock container success" << endl;
}
