#ifndef _STORE_INPUT_STREAM_HH_
#define _STORE_INPUT_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "DPDataChunk.hh"
#include "DPDataPacket.hh"
#include "../storage/BaseFS.hh"
#include "BlockingQueue.hh"

using namespace std;

class StoreInputStream {
private:
    Config* _conf;
    string _objname;
    string _poolname;
    BlockingQueue<DPDataPacket*>* _queue;
    
    BaseFS* _fs;
    BaseFile* _file;
public:
    StoreInputStream(Config* conf, string objname, string poolname, BaseFS* fs);
    ~StoreInputStream();

};

#endif