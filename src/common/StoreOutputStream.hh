#ifndef _STORE_OUTPUT_STREAM_HH_
#define _STORE_OUTPUT_STREAM_HH_

#include "../inc/include.hh"
#include "BlockingQueue.hh"
#include "../common/Config.hh"
#include "../storage/BaseFile.hh"
#include "../storage/BaseFS.hh"
#include "DPDataChunk.hh"

class StoreOutputStream {
private:
    Config* _conf;
    string _objname;
    string _poolname;
    BlockingQueue<DPDataChunk*>* _queue;
    bool _finish;
    int _objsize;

    BaseFS* _basefs;
    BaseFile* _basefile;

public:
    StoreOutputStream(Config* conf, string objname, string poolname, BaseFS* fs);
    ~StoreOutputStream();
    void writeObjFull();
    void writeObjPartial();
    void enqueue(DPDataChunk* pkt);
    bool getFinish();
    BlockingQueue<DPDataChunk*>* getQueue(); 

};

#endif