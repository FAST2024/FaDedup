#include "DPDeleteStream.hh"

DPDeleteStream::DPDeleteStream(Config* conf, 
                                string filename, 
                                string poolname,
                                int start_pos,
                                size_t size) {
    _conf=conf;
    _filename=filename;
    _poolname=poolname;
    _localCtx=RedisUtil::createContext(_conf->_localIp);

    // delete partial
    _start_pos=start_pos;
    _size=size;
    
}



DPDeleteStream::~DPDeleteStream() {
    redisFree(_localCtx);
}

bool DPDeleteStream::lookupFile(string filename, string poolname) {
    string key="pktnum:" + filename + ":" + poolname;
    std::cout << "[lookupFile] key: " << key << std::endl;
    int ipId = hash<string>()(key) % (_conf->_agentNum);
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "EXISTS %s", key.c_str());
    int exists = rReply->integer;
    if (exists == 1) {
        return true;
    }
    return false;
}


void DPDeleteStream::deleteFullWorker() {
    cout << "[DPDeleteStream::deleteFullWorker] begin" << endl;

    AGCommand* deletefull_agCmd=new AGCommand();
    deletefull_agCmd->buildType7(7,_filename, _poolname);
    deletefull_agCmd->sendTo(_conf->_localIp);
    delete deletefull_agCmd;
    cout << "[DPDeleteStream::deleteFullWorker] send deletefull request to local agent, wait for deletefull done" << endl;

    redisContext* deletefull_done_recvCtx = RedisUtil::createContext(_conf->_localIp);
    string key = "deletefile:" + _filename + ":" + _poolname;
    redisCommand(deletefull_done_recvCtx, "blpop %s 0", key.c_str());
    redisFree(deletefull_done_recvCtx);

    string delete_pktnum_key = "pktnum:" + _filename + ":" + _poolname;
    int ipId = hash<string>()(delete_pktnum_key) % (_conf->_agentNum);
    cout << "[DPDeleteStream::deleteFullWorker] send delete pktnum request to node " << ipId << " key is " << delete_pktnum_key << endl;
    redisContext* delete_pktnum_sendCtx = RedisUtil::createContext(_conf->_agentsIPs[ipId]);
    redisReply* delete_pktnum_rReply = (redisReply*)redisCommand(delete_pktnum_sendCtx, "DEL %s", delete_pktnum_key.c_str());
    assert(delete_pktnum_rReply!=NULL);
    assert(delete_pktnum_rReply->type==REDIS_REPLY_INTEGER);
    assert(delete_pktnum_rReply->integer==1);
    freeReplyObject(delete_pktnum_rReply);
    redisFree(delete_pktnum_sendCtx);
    cout << "[DPDeleteStream::deleteFullWorker] delete pktnum done" << endl;
    cout << "[DPDeleteStream::deleteFullWorker] delete filename : " << _filename << " poolname : " << _poolname << " done " << endl; 
}


