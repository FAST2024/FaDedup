#include "DPReorderStream.hh"

DPReorderStream::DPReorderStream(Config* conf) {
    _conf=conf;
    _localCtx=RedisUtil::createContext(_conf->_localIp);
}



DPReorderStream::~DPReorderStream() {
    redisFree(_localCtx);
}

/**
 * reorder data that is localized on each node
*/
void DPReorderStream::reorderWorker() {
    cout << "[DPReorderStream::reorderWorker] begin" << endl;

    // send the reorder request to all agents
    AGCommand* reorder_agCmd=new AGCommand();
    reorder_agCmd->buildType8(8);
    for (int i = 0; i < _conf -> _agentNum; i++) {
    	reorder_agCmd->sendTo(_conf->_agentsIPs[i]);
    }
    delete reorder_agCmd;
    cout << "[DPReorderStream::reorderWorker] send reorder request to all of agents, wait for local recognition done" << endl;

    
    int cur_agent_count = 0;
    while(cur_agent_count < _conf -> _agentNum) {
    	string key = "localrecognitionfinish";
    	redisContext* local_recognition_done_recvCtx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    	redisReply* local_recognition_done_rReply = (redisReply*)redisCommand(local_recognition_done_recvCtx, "blpop %s 0", key.c_str());
    	//cout << key.c_str() << endl;
    	unsigned int ip;
    	char* content = local_recognition_done_rReply -> element[1] -> str;
    	memcpy((void*)&ip, content, sizeof(unsigned int));
    	ip = ntohl(ip);
    	    	
    	key = "reorderstart";
    	redisContext* reorder_start_recvCtx = RedisUtil::createContext(ip);
    	redisReply* reorder_start_done_rReply = (redisReply*)redisCommand(reorder_start_recvCtx, "rpush %s 1", key.c_str());
    	
    	key = "reorderfinish";
    	redisContext* reorder_done_recvCtx = RedisUtil::createContext(ip);
    	redisReply* reorder_done_rReply = (redisReply*)redisCommand(reorder_done_recvCtx, "blpop %s 0", key.c_str());	
    	cout << "[DPReorderStream::reorderWorker] reorder node " << ip << " done" << endl; 
    	cur_agent_count++;
    	
    	freeReplyObject(local_recognition_done_rReply);
    	redisFree(local_recognition_done_recvCtx);
    	freeReplyObject(reorder_start_done_rReply);
    	redisFree(reorder_start_recvCtx);
    	freeReplyObject(reorder_done_rReply);
    	redisFree(reorder_done_recvCtx);
    }	
}
 
