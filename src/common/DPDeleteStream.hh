#ifndef _DP_DELETE_STREAM_HH_
#define _DP_DELETE_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "DPDataChunk.hh"
#include "DPDataPacket.hh"
#include "BlockingQueue.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"

class DPDeleteStream {
private:
    Config* _conf;
    string _filename;
    string _poolname;
    redisContext* _localCtx;
    BlockingQueue<DPDataPacket*>* _deleteQueue;
    int _pktnum;
    int _start_pos=0;
    size_t _size=0; 
    thread _collectThread;

public:
    DPDeleteStream(Config* conf, string filename, string poolname, int start_pos=0, size_t size=0);
    ~DPDeleteStream();
    void deleteFullWorker();
    bool lookupFile(string filename, string poolname);
    void deleteRegisterFile(int pktnum);
};

#endif