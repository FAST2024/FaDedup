#include "DPInputStream.hh"

DPInputStream::DPInputStream(Config* conf, 
                               string filename,
                               string poolname,
                               bool batch) {
  _conf = conf;
  _filename = filename;
  _poolname = poolname;
  _localCtx = RedisUtil::createContext(_conf->_localIp);
  _batch = batch;
  init(_batch);
}

/**
 * called when readfull after random aggregate
*/
DPInputStream::DPInputStream(Config* conf,
                              string filename, 
                              string poolname, 
                              bool batch, 
                              bool random_aggregate) {
  _conf = conf;
  _filename = filename;
  _poolname = poolname;
  _localCtx = RedisUtil::createContext(_conf->_localIp);
  init(false, true);
}

DPInputStream::DPInputStream(Config* conf, 
                              string filename,
                              string poolname,
                              bool batch,
                              bool random_aggregate,
                              bool withRBD) {
  _conf = conf;
  _filename = filename;
  _poolname = poolname;
  _localCtx = RedisUtil::createContext(_conf->_localIp);
  init(false, false, true);
}


/**
 * readfull with rbd and combined by client
*/
DPInputStream::DPInputStream(Config* conf, 
                              string filename,
                              string poolname,
                              bool batch,
                              bool random_aggregate,
                              bool withRBD,
                              bool combined_by_client) {
  assert(!batch && !random_aggregate && !withRBD && combined_by_client);
  _conf = conf;
  _filename = filename;
  _poolname = poolname;
  _localCtx = RedisUtil::createContext(_conf->_localIp);
  init(false, false, false, true);
}


DPInputStream::~DPInputStream() {
  redisFree(_localCtx);
}

void DPInputStream::init(bool batch) {
    AGCommand* agCmd = new AGCommand();
    if (!batch)
      agCmd->buildType2(2, _filename, _poolname);
    else
      agCmd->buildType4(4, _filename, _poolname);
    agCmd->sendTo(_conf->_localIp);
    delete agCmd;

    // get file recipe (file -> pktnum)
    string key = _filename+":"+_poolname;
    string wkey = "pktnumReply:pktnum:"+key;
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    char* content = rReply -> element[1] -> str;
    int tmpnum;
    memcpy((void*)&tmpnum, content, sizeof(int));
    _pktnum = ntohl(tmpnum);
    // cout << "[client] pktnum: " << _pktnum << endl;

    freeReplyObject(rReply);

    _readQueue = new BlockingQueue<DPDataPacket*>();

    _collectThread = thread([=]{readFullWorker(_readQueue, key);});
    // _collectThread.join();
}

/**
 * called when readfull after random aggregate
*/
void DPInputStream::init(bool batch, bool random_aggregate) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType10(10, _filename, _poolname);
    agCmd->sendTo(_conf->_localIp);
    delete agCmd;

    // get file recipe (file -> pktnum)
    string key = _filename+":"+_poolname;
    string wkey = "pktnumReply:pktnum:"+key;
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    char* content = rReply -> element[1] -> str;
    int tmpnum;
    memcpy((void*)&tmpnum, content, sizeof(int));
    _pktnum = ntohl(tmpnum);
    cout << "[client] inquire pktnum done, pktnum: " << _pktnum << endl;

    freeReplyObject(rReply);

    _readQueue = new BlockingQueue<DPDataPacket*>();

    _collectThread = thread([=]{readFullWorker(_readQueue, key);});
}

void DPInputStream::init(bool batch, bool random_aggregate, bool withRBD) {
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType13(13, _filename, _poolname);
    agCmd->sendTo(_conf->_localIp);
    delete agCmd;

    // get file recipe (file -> pktnum)
    string key = _filename+":"+_poolname;
    string wkey = "pktnumReply:pktnum:"+key;
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    char* content = rReply -> element[1] -> str;
    int tmpnum;
    memcpy((void*)&tmpnum, content, sizeof(int));
    _pktnum = ntohl(tmpnum);
    cout << "[client] inquire pktnum done, pktnum: " << _pktnum << endl;

    freeReplyObject(rReply);

    _readQueue = new BlockingQueue<DPDataPacket*>();

    _collectThread = thread([=]{readFullWorker(_readQueue, key);});
}


void DPInputStream::init(bool batch, bool random_aggregate, bool withRBD, bool combined_by_client) {
    assert(!batch && !random_aggregate && !withRBD && combined_by_client);
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType18(18, _filename, _poolname);
    agCmd->sendTo(_conf->_localIp);
    delete agCmd;


    string key = _filename+":"+_poolname;
    string wkey = "pktnumReply:pktnum:"+key;
    // cout << "wkey: " << wkey << endl;
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
    char* content = rReply -> element[1] -> str;
    int tmpnum;
    memcpy((void*)&tmpnum, content, sizeof(int));
    _pktnum = ntohl(tmpnum);
    cout << "[client] inquire pktnum done, pktnum: " << _pktnum << endl;

    freeReplyObject(rReply);

    _readQueue = new BlockingQueue<DPDataPacket*>();

    _collectThread = thread([=]{readFullWorker(_readQueue, key, true);});
}

void DPInputStream::readFullWorker(BlockingQueue<DPDataPacket*>* readQueue, string keybase) {
    // cout << "[DPInputStream] readFullWorker" << endl;
    redisReply* rReply;
    redisContext* recvCtx = RedisUtil::createContext(_conf->_localIp);
    for (int i = 0; i < _pktnum; i++) {
      string key = "readfile:"+keybase + ":" + to_string(i);
      // cout << "key: " << key << endl;
      redisAppendCommand(recvCtx, "blpop %s 0", key.c_str());
    }
    for (int i = 0; i < _pktnum; i++) {
      // string key = keybase + ":" + to_string(i);
      // cout << "key: " << key << endl;
      // rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", key.c_str());
      redisGetReply(recvCtx, (void**)&rReply);
      if(rReply->type == REDIS_REPLY_ERROR)
        cout << "rReply error info: " << rReply->str << endl;
      char* content = rReply->element[1]->str;
      int tmpid = 0;
      // memcpy((void*)&tmpid, content, sizeof(int));
      // int pktid = ntohl(tmpid);
      DPDataPacket* pkt = new DPDataPacket(content);
      // int curDataLen = pkt->getDatalen();
      readQueue->push(pkt);
      freeReplyObject(rReply);
    }
    redisFree(recvCtx);
    // cout << "[DPInputStream] finish readFullWorker" << endl;
    readbufClear();
}

void DPInputStream::readbufClear(){
    std::cout << "[client] wait for the readbuf to clear" << std::endl;
    int io_cnt_sum = 0;
    for(int i = 0; i < _conf -> _agentNum; i++) {
    	AGCommand* agCmd = new AGCommand();
    	agCmd->buildType25(25);
    	agCmd->sendTo(_conf->_agentsIPs[i]);
    	delete agCmd;

      string key = "readbuf_clear_done";
      redisReply* reply;
      redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[i]);
      reply = (redisReply*)redisCommand(ctx, "blpop %s 0", key.c_str());
      assert(reply!=NULL);
      assert(reply->type == REDIS_REPLY_ARRAY);
      assert(reply->elements == 2);
      int tmp_iocnt;
      memcpy((void*)&tmp_iocnt, reply->element[1]->str, sizeof(int));
      int io_cnt = ntohl(tmp_iocnt);
      // cout << "[client] agent " << i << " readbuf clear done, io_cnt: " << io_cnt << endl;
      io_cnt_sum += io_cnt;

      freeReplyObject(reply);
      redisFree(ctx);
    }
    cout << "[client] read io cnt sum:" << io_cnt_sum << endl;
}

void DPInputStream::readFullWorker(BlockingQueue<DPDataPacket*>* readQueue, string keybase, bool combined_by_client) {
    assert(combined_by_client);
    std::cout << "[client] wait for all packet read done, pktnum is " << _pktnum << std::endl;
    for (int i = 0; i < _pktnum; i++) {
      string key = "readpacket:"+keybase + ":" + to_string(i);
      redisContext* recvCtx = RedisUtil::createContext(_conf->_localIp);
      redisReply* rReply = (redisReply*)redisCommand(recvCtx, "blpop %s 0", key.c_str());
      assert(rReply!=NULL);
      assert(rReply->type == REDIS_REPLY_ARRAY);
      assert(rReply->elements == 2); 
      char* content = rReply->element[1]->str;
      int tmpid = 0;
      DPDataPacket* pkt = new DPDataPacket(content);
      readQueue->push(pkt);
      freeReplyObject(rReply);
      redisFree(recvCtx);
    }
    readbufClear();
}

size_t DPInputStream::output2file(string saveas) {
    // cout << "[DPInputStream] output2file" << endl;
  ofstream ofs(saveas);
  ofs.close();
  ofs.open(saveas, ios::app);
  size_t fileLen = 0;
  for(int i = 0; i < _pktnum; i++) {
    DPDataPacket* curPkt = _readQueue->pop();
    int len = curPkt->getDatalen();
    if (len) {
      ofs.write(curPkt->getData(), len);
    }
    else
      break;
    fileLen += len;
    delete curPkt;
  }
  ofs.close();
  assert(_readQueue->getSize() == 0);
  delete _readQueue;
  // cout << "[DPInputStream] finish output2file" << endl;
  return fileLen;
}

void DPInputStream::close() {
  _collectThread.join();
}


bool DPInputStream::lookupFile(string filename, string poolname) {
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