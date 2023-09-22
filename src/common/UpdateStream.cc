#include "UpdateStream.hh"

UpdateStream::UpdateStream(Config* conf, 
                            string filename, 
                            string poolname,
                            int offset,
                            int size) {
    _conf = conf;
    _filename = filename;
    _poolname = poolname;
    _localCtx = RedisUtil::createContext(_conf->_localIp);
    _offset = offset;
    _size = size;
}

UpdateStream::~UpdateStream() {
    redisFree(_localCtx);
}

bool UpdateStream::lookupFile(string filename, string poolname) {
    string key = "pktnum:" + filename + ":" + poolname;
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "EXISTS %s", key.c_str());
    int exists = rReply->integer;
    freeReplyObject(rReply);
    redisFree(sendCtx);
    if (exists == 1) {
        return true;
    }
    return false;
}

void UpdateStream::updatePacket(char* buf, int len, int update_offset) {
    assert(update_offset % _conf->_pktSize == 0);
    AGCommand* agCmd = new AGCommand();
    int pktid = update_offset / _conf->_pktSize;
    agCmd->buildType20(20, _filename, _poolname, pktid, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(pktid);
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    cout << "[UpdateStream] update filename:" << _filename << " poolname:" << _poolname << " pktid:" << pktid << ", route to nodeid:" << ipId << endl;
    agCmd->sendTo(_conf->_agentsIPs[ipId]);
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", key.c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;
}

void UpdateStream::updateChunk(char* buf, int len, int update_offset) {
    assert(update_offset % _conf->_chunkMaxSize == 0);
    AGCommand* agCmd = new AGCommand();
    int pktid = update_offset / _conf->_pktSize;
    int chunkid = (update_offset - pktid * _conf->_pktSize) / _conf->_chunkMaxSize;
    agCmd->buildType24(24, _filename, _poolname, pktid, chunkid, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(pktid);
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    cout << "[UpdateStream] updateChunk filename:" << _filename << " poolname:" << _poolname << " pktid:" << pktid << " chunkid:" << chunkid << ", route to nodeid:" << ipId << endl;
    agCmd->sendTo(_conf->_agentsIPs[ipId]);
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", (key+":"+to_string(chunkid)).c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;

    string wkey = "updatefinish:"+_filename+":"+_poolname+":"+to_string(pktid)+":"+to_string(chunkid);
    redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    cout << "[UpdateStream] update filename:" << _filename << " poolname:" << _poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done"<< endl;
    
    
    // for(int i=0;i<_conf->_agentNum;i++) {
    //     AGCommand* agCmd = new AGCommand();
    //     agCmd->buildType14(14);
    //     agCmd->sendToStoreLayer(_conf->_agentsIPs[i]);
    //     delete agCmd;
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


void UpdateStream::simpleUpdateChunk(char* buf, int len, int update_offset) {
    assert(update_offset % _conf->_chunkMaxSize == 0);
    AGCommand* agCmd = new AGCommand();
    int pktid = update_offset / _conf->_pktSize;
    int chunkid = (update_offset - pktid * _conf->_pktSize) / _conf->_chunkMaxSize;
    agCmd->buildType26(26, _filename, _poolname, pktid, chunkid, len, _conf->_localIp);
    
    string key = _filename + ":" + _poolname + ":" + to_string(pktid);
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    cout << "[UpdateStream] simleUpdateChunk filename:" << _filename << " poolname:" << _poolname << " pktid:" << pktid << " chunkid:" << chunkid << ", route to nodeid:" << ipId << endl;
    agCmd->sendTo(_conf->_agentsIPs[ipId]);
    redisContext* writeCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(writeCtx, "rpush %s %b", (key+":"+to_string(chunkid)).c_str(), buf, len);
    freeReplyObject(rReply);
    redisFree(writeCtx);
    delete agCmd;

    string wkey = "updatefinish:"+_filename+":"+_poolname+":"+to_string(pktid)+":"+to_string(chunkid);
    redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    cout << "[UpdateStream] simpleUpdate filename:" << _filename << " poolname:" << _poolname << " pktid:" << pktid << " chunkid:" << chunkid << " done"<< endl;
    
    // // update done, notify store layer to flush chunks to ceph
    // for(int i=0;i<_conf->_agentNum;i++) {
    //     AGCommand* agCmd = new AGCommand();
    //     agCmd->buildType14(14);
    //     agCmd->sendToStoreLayer(_conf->_agentsIPs[i]);
    //     delete agCmd;
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

void UpdateStream::close() {
    int pkt_num = _offset / _conf->_pktSize;
    for(int i = _offset / _conf->_pktSize ; i<(_offset+_size)/_conf->_pktSize; i++) {
        string wkey = "updatefinish:"+_filename+":"+_poolname+":"+to_string(i);
        redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
        cout << "[UpdateStream] update filename:" << _filename << " poolname:" << _poolname << " pktid:" << i << " done"<< endl;
    }
    
    // update done, notify all store layer to flush
    for(int i=0;i<_conf->_agentNum;i++) {
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType14(14);
        agCmd->sendToStoreLayer(_conf->_agentsIPs[i]);
        delete agCmd;
    }
    // wait for all store layer to flush done 
    for (int i = 0; i < _conf->_agentNum; i++) {
        string wkey = "flushdone";
        redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[i]);
        redisReply* rReply = (redisReply*)redisCommand(ctx, "blpop %s 0", wkey.c_str());
        assert(rReply!=NULL);
        assert(rReply->type==REDIS_REPLY_ARRAY);
        assert(string(rReply->element[1]->str) == "1");        
        freeReplyObject(rReply);
        redisFree(ctx);
    }
}
