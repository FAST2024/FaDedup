#include "DPOutputStream.hh"

DPOutputStream::DPOutputStream(Config* conf,
                                 string filename,
                                 string poolname,
                                 string mode,
                                 int filesize) {
    _conf = conf;
    _filename = filename;
    _poolname = poolname;
    _mode = mode;
    _filesize = filesize;
    _localCtx = RedisUtil::createContext(_conf->_localIp);
    _pktid = 0;
    _replyid = 0;
}

DPOutputStream::~DPOutputStream() {
    redisFree(_localCtx);
}

bool DPOutputStream::lookupFile(string filename, string poolname) {
    string key = "pktnum:" + filename + ":" + poolname;
    // cout << "key: " << key << endl;
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "EXISTS %s", key.c_str());
    // char* response = rReply->element[1]->str;
    int exists = rReply->integer;
    if (exists == 1) {
    	redisFree(sendCtx);
    	freeReplyObject(rReply);
        return true;
    }
    redisFree(sendCtx);
    freeReplyObject(rReply);
    return false;
}

void DPOutputStream::write(char* buf, int len, bool batch) {
    /*
    * DPOutputStream write packet to local redis in this format
    * |key = filename|
    * |value = |datalen|data|
    */
    AGCommand* agCmd = new AGCommand();
    if (batch) 
        agCmd->buildType3(3, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    else
        agCmd->buildType0(0, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);
    // cout << "key: " << key << endl;

    int ipId = hash<string>()(key) % (_conf->_agentNum);

    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // wait for a ack reply
    redisReply* rReply;
    // string ackKey = "ack:" + key;
    // cout << ackKey << endl;
    // rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", ackKey.c_str());
    // freeReplyObject(rReply);
    
    // redisAppendCommand(writeCtx, "RPUSH %s %b", key.c_str(), buf, len);
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
}


/**
 * write the packet to store_layer for aggregate
*/
void DPOutputStream::writeAndRandomAggregate(char* buf, int len) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType9(9, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);

    int ipId = hash<string>()(key) % (_conf->_agentNum);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // wait for a ack reply
    redisReply* rReply;
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
}

/**
 * write with ceph rbd
*/
void DPOutputStream::writeWithRBD(char* buf, int len) {
    AGCommand* agCmd = new AGCommand();
    assert(agCmd != nullptr);
    agCmd->buildType11(11, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);

    int ipId = hash<string>()(key) % (_conf->_agentNum);
    // RedisUtil::AskRedisContext(_conf->_agentsIPs[ipId]);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // send packet to corresponding node
    redisReply* rReply;
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    assert(rReply != nullptr);
    assert(rReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
    // RedisUtil::FreeRedisContext(_conf->_agentsIPs[ipId]);
}

/**
 * write with ceph rbd
*/
void DPOutputStream::writeAndSelectiveDedup(char* buf, int len) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType15(15, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);

    int ipId = hash<string>()(key) % (_conf->_agentNum);
    // RedisUtil::AskRedisContext(_conf->_agentsIPs[ipId]);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // send packet to corresponding node
    redisReply* rReply;
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
    // RedisUtil::FreeRedisContext(_conf->_agentsIPs[ipId]);
}


/**
 * write with ceph rbd
*/
void DPOutputStream::writeWithRBDAndBatchPush(char* buf, int len) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType22(22, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);

    int ipId = hash<string>()(key) % (_conf->_agentNum);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // send packet to corresponding node
    redisReply* rReply;
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
}

/**
 * write with rbd, push a super chunk one time
*/
void DPOutputStream::writeWithRBDAndPushSuperChunk(char* buf, int len) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType27(27, _filename, _pktid, _poolname, _mode, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(_pktid++);

    int ipId = hash<string>()(key) % (_conf->_agentNum);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);

    // send packet to corresponding node
    redisReply* rReply;
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
}


void DPOutputStream::registerFile(int pktnum) {
    AGCommand* agCmd = new AGCommand();
    string key = "pktnum:" + _filename + ":" + _poolname;
    // cout << "key: " << key << endl;
    agCmd->buildType6(6, key, pktnum);
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    agCmd->sendTo(_conf->_agentsIPs[ipId]);
    delete agCmd;
}

void DPOutputStream::close() {
    for (int i = 0; i < _pktid; i++) {
        // wait for finish
        string wkey = "writefinish:"+_filename+":"+_poolname+":"+to_string(i);
        redisAppendCommand(_localCtx, "blpop %s 0", wkey.c_str());
    }

    thread waitThrds[_pktid];
    char buf[102];
    memset(buf, 0, sizeof(buf));
    const char *lable = "|/-\\";
    for (int i = 0; i < _pktid; i++) {
        int idx = ((double)i / (double)_pktid) * 100;
        printf("\033[40;32m[%-101s][%d%%][%c]\033[0m\r", buf, idx, lable[i % 4]);
        fflush(stdout);
        memset(buf, '=', idx);
        redisReply* rReply; 
        redisGetReply(_localCtx, (void**)&(rReply));
        freeReplyObject((void*)(rReply));
    }
    memset(buf, '=', 100);
    printf("\033[40;32m[%-101s][%d%%][%c]\033[0m\r", buf, 100, lable[0]);
    cout << endl;
    
    // // writerfull done, notify all store layer to flush
    // for(int i=0;i<_conf->_agentNum;i++) {
    //     AGCommand* agCmd = new AGCommand();
    //     agCmd->buildType14(14);
    //     agCmd->sendToStoreLayer(_conf->_agentsIPs[i]);
    //     // delete agCmd;
    // }
    // // wait for all store layer to flush done 
    // for (int i = 0; i < _conf->_agentNum; i++) {
    //     string wkey = "flushdone";
    //     redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[i]);
    //     redisReply* rReply = (redisReply*)redisCommand(ctx, "blpop %s 0", wkey.c_str());
    //     assert(rReply!=NULL);
    //     assert(rReply->type==REDIS_REPLY_ARRAY);
    //     assert(string(rReply->element[1]->str) == "1");        
    //     freeReplyObject(rReply);
    //     redisFree(ctx);
    // }
}
