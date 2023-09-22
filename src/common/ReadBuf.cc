#include "ReadBuf.hh"

std::mutex ReadBuf::_mutex;
unordered_map<string,char*> ReadBuf::_localBuf;

ReadBuf::ReadBuf(Config* conf){
    _conf=conf;
    _localCtx=RedisUtil::createContext(_conf->_localIp);

}

ReadBuf::~ReadBuf(){
    redisFree(_localCtx);
}

char* ReadBuf::find(string conName){
    if(_localBuf.find(conName) == _localBuf.end()) {
		return NULL;
    } else {
		return _localBuf[conName];
	}
}


void ReadBuf::insert(string conName,char* conData){
	// assert(_localBuf.find(conName) == _localBuf.end());
    if(_localBuf.find(conName) != _localBuf.end()){
    	cout << "The container is already written to the readbuf!" << endl;
    	//return;
    }
    _localBuf[conName] = conData;
    redisReply * rReply = (redisReply *)redisCommand(_localCtx, "DEL %s", conName.c_str());
    freeReplyObject(rReply);
}

void ReadBuf::clear(){
    for(unordered_map<string,char*>::iterator iter = _localBuf.begin(); iter!=_localBuf.end(); ++iter){
    	free(iter->second);
    }
    _localBuf.clear();
}
int ReadBuf::size(){
    return _localBuf.size();
}
