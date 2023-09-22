#include "AgCommand.hh"

AGCommand::AGCommand() {
  _agCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
  _cmLen = 0;
  _rKey = "ag_request";
  _buf = nullptr;
  _fp = nullptr;
}

AGCommand::AGCommand(int cmdsize) {
  _agCmd = (char*)calloc(cmdsize, sizeof(char));
  _cmLen = 0;
  _rKey = "ag_request";
  _buf = nullptr;
  _fp = nullptr;
}

AGCommand::~AGCommand() {
  if (_agCmd) {
    free(_agCmd);
    _agCmd = 0;
  }
  _cmLen = 0;
}

AGCommand::AGCommand(char* reqStr) {
  _agCmd = reqStr;
  _cmLen = 0; 

  // parse type
  _type = readInt();

  switch(_type) {
    case 0: resolveType0(); break;
    case 2: resolveType2(); break;
    case 3: resolveType3(); break;
    case 4: resolveType4(); break;
    case 5: resolveType5(); break;
    case 6: resolveType6(); break;
    case 7: resolveType7(); break;
    case 8: resolveType8(); break;
    case 9: resolveType9(); break;
    case 10: resolveType10(); break;
    case 11: resolveType11(); break;
    case 12: resolveType12(); break;
    case 13: resolveType13(); break;
    case 14: resolveType14(); break;
    case 15: resolveType15(); break;
    case 16: resolveType16(); break;
    case 17: resolveType17(); break;
    case 18: resolveType18(); break;
    case 19: resolveType19(); break;
    case 20: resolveType20(); break;
    case 21: resolveType21(); break;
    case 22: resolveType22(); break;
    case 23: resolveType23(); break;
    case 24: resolveType24(); break;
    case 26: resolveType26(); break;
    case 27: resolveType27(); break;
    case 25: resolveType25(); break;
    case 28: resolveType28(); break;
    default: assert(false && "undefined agcommand type"); break;
  }
  _agCmd = nullptr;
  _cmLen = 0;
}

void AGCommand::setRkey(string key) {
  _rKey = key;
} 

void AGCommand::writeInt(int value) {
  int tmpv = htonl(value);
  memcpy(_agCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4;
}

void AGCommand::writeString(string s) {
  int slen = s.length();
  int tmpslen = htonl(slen);
  // string length
  memcpy(_agCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
  // string
  memcpy(_agCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}

int AGCommand::readInt() {
  int tmpint;
  memcpy((char*)&tmpint, _agCmd + _cmLen, sizeof(int)); _cmLen += sizeof(int);
  return ntohl(tmpint);
}

string AGCommand::readString() {
  string toret;
  int slen = readInt();
  char* sname = (char*)calloc(sizeof(char), slen+1);
  memcpy(sname, _agCmd + _cmLen, slen); _cmLen += slen;
  toret = string(sname);
  free(sname);
  return toret;
}

void AGCommand::writeCharPointer(char* buf, int len) {
  int tmplen = htonl(len);
  // buf length
  memcpy(_agCmd + _cmLen, (void*)&tmplen, sizeof(int)); _cmLen += sizeof(int);
  // buf
  memcpy(_agCmd + _cmLen, buf, len); _cmLen += len;
}

char* AGCommand::readCharPointer() {
  int len = readInt();
  char* tmpbuf = new char[len];
  memcpy(tmpbuf, _agCmd + _cmLen, len); _cmLen += len;
  return tmpbuf;
}

int AGCommand::getType() {
  return _type;
}

char* AGCommand::getCmd() {
  return _agCmd;
}

int AGCommand::getCmdLen() {
  return _cmLen;
}

string AGCommand::getFilename() {
    return _filename;
}

int AGCommand::getPktId() {
    return _pktid;
}

int AGCommand::getChunkId() {
    return _chunkid;
}
string AGCommand::getPoolname() {
    return _poolname;
}

string AGCommand::getMode() {
    return _mode;
}

int AGCommand::getFilesize() {
    return _file_size;
}

char* AGCommand::getFp() {
  return _fp;
}

bool AGCommand::getRes() {
  return _res ? true: false;
}

unsigned int AGCommand::getIp() {
  return _ip;
}

char* AGCommand::getBuf() {
  return _buf;
}

int AGCommand::getSize() {
  return _size;
}

int AGCommand::getPktNum() {
  return _pkt_num;
}

void AGCommand::sendTo(unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _agCmd, _cmLen);
    assert(rReply != NULL);
    assert(rReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

void AGCommand::sendToStoreLayer(unsigned int ip) {
    string key = "store_layer_ag_request";
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", key.c_str(), _agCmd, _cmLen);
    assert(rReply != NULL);
    assert(rReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(rReply);
    redisFree(sendCtx);
}

void AGCommand::buildType0(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType0() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType2(int type, string filename, string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType2() {
  _filename = readString();
  _poolname = readString();
}

void AGCommand::buildType3(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode,
                    int filesizeMB,
                    unsigned int ip) {
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesizeMB;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType3() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType4(int type,
                    string filename,
                    string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType4() {
  _filename = readString();
  _poolname = readString();
}

void AGCommand::buildType5(int type,
                          string filename,
                          string poolname,
                          char* buf,
                          int size,
                          unsigned int ip) {
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _buf = buf;
  _size = size;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeCharPointer(_buf, _size);
  writeInt(_size);
  writeInt(_ip);
}

void AGCommand::resolveType5() {
  _filename = readString();
  _poolname = readString();
  _buf = readCharPointer();
  _size = readInt();
  _ip = (unsigned int) readInt();
}

void AGCommand::buildType6(int type, string filename, int pkt_num) {
  _type = type;
  _filename = filename;
  _pkt_num = pkt_num;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pkt_num);
}

void AGCommand::resolveType6() {
  _filename = readString();
  _pkt_num = readInt();
}

void AGCommand::buildType7(int type, string filename, string poolname) {
  _type=type;
  _filename=filename;
  _poolname=poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType7() {
  _filename = readString();
  _poolname = readString();
}

/**
 * build a reorder request to agent
*/
void AGCommand::buildType8(int type) {
  _type=type;

  writeInt(_type);
}
void AGCommand::resolveType8() {

}


/**
 * build a writefull and aggregate request to agent
*/
void AGCommand::buildType9(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType9() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType10(int type,
                    string filename,
                    string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType10() {
  _filename = readString();
  _poolname = readString();
}

void AGCommand::buildType11(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType11() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType12(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType12() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType13(int type,
                    string filename,
                    string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType13() {
  _filename = readString();
  _poolname = readString();
}

/**
 * notify store layer to flush
*/
void AGCommand::buildType14(int type) {
  _type = type;
  writeInt(_type);
}

void AGCommand::resolveType14() {
  
}
/**
 * writefullAndSelectiveDedup
*/
void AGCommand::buildType15(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType15() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

/**
 * readfullAfterWriteSelectiveDedup
*/
void AGCommand::buildType16(int type,
                    string filename,
                    string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType16() {
  _filename = readString();
  _poolname = readString();
}

// popSelectiveDedupChunk
void AGCommand::buildType17(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType17() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType18(int type,
                    string filename,
                    string poolname) {
  _type = type;
  _filename = filename;
  _poolname = poolname;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
}

void AGCommand::resolveType18() {
  _filename = readString();
  _poolname = readString();
}

/**
 * send to agent to read a packet and return to client directly
*/
void AGCommand::buildType19(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType19() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}


// update
void AGCommand::buildType20(int type,
                    string filename,
                    string poolname,
                    int pktid,
                    int size,
                    unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _file_size = size;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType20() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}


void AGCommand::buildType21(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType21() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}


void AGCommand::buildType22(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType22() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

void AGCommand::buildType23(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType23() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}


void AGCommand::buildType24(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType24() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

/**
 * clear readbuf
*/
void AGCommand::buildType25(int type) {
  _type=type;

  writeInt(_type);
}
void AGCommand::resolveType25() {

}



void AGCommand::buildType26(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType26() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}


void AGCommand::buildType27(int type,
                           string filename,
                           int pktid,
                           string poolname,
                           string mode, 
                           int filesize,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _pktid = pktid;
  _poolname = poolname;
  _mode = mode;
  _file_size = filesize;
  _ip = ip;

  writeInt(_type);
  writeString(_filename);
  writeInt(_pktid);
  writeString(_poolname);
  writeString(_mode);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType27() {
  _filename = readString();
  _pktid = readInt();
  _poolname = readString();
  _mode = readString();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}

/**
 * simple update chunk and flush to ceph immediate
*/
void AGCommand::buildType28(int type,
                           string filename,
                           string poolname,
                           int pktid,
                           int chunkid,
                           int size,
                           unsigned int ip) {
  // set up corresponding parameters
  _type = type;
  _filename = filename;
  _poolname = poolname;
  _pktid = pktid;
  _chunkid = chunkid;
  _file_size = size;
  _ip = ip;
  

  writeInt(_type);
  writeString(_filename);
  writeString(_poolname);
  writeInt(_pktid);
  writeInt(_chunkid);
  writeInt(_file_size);
  writeInt(_ip);
}

void AGCommand::resolveType28() {
  _filename = readString();
  _poolname = readString();
  _pktid = readInt();
  _chunkid = readInt();
  _file_size = readInt();
  _ip = (unsigned int)readInt();
}