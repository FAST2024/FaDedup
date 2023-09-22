#ifndef _DP_IUPUT_STREAM_HH_
#define _DP_IUPUT_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "DPDataChunk.hh"
#include "DPDataPacket.hh"
#include "BlockingQueue.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"

class DPInputStream {
private:
    Config* _conf;
    string _filename;
    string _poolname;
    redisContext* _localCtx;

    BlockingQueue<DPDataPacket*>* _readQueue;
    int _filesize;
    int _pktnum;
    thread _collectThread;
    bool _batch;
    
    void readbufClear();
public:
    DPInputStream(Config* conf, 
                   string filename,
                   string poolname,
                   bool batch);
    DPInputStream(Config* conf, 
                    string filename, 
                    string poolname, 
                    bool batch, 
                    bool random_aggregate);
    DPInputStream(Config* conf, 
                    string filaname,
                    string poolname,
                    bool batch,
                    bool random_aggregate,
                    bool withRBD);
    DPInputStream(Config* conf, 
                    string filaname,
                    string poolname,
                    bool batch,
                    bool random_aggregate,
                    bool withRBD,
                    bool combined_by_client);
    ~DPInputStream();
    void init(bool batch);
    void init(bool batch, bool random_aggregate);
    void init(bool batch, bool random_aggregate, bool withRBD);
    void init(bool batch, bool random_aggregate, bool withRBD, bool combined_by_client);
    void readFullWorker(BlockingQueue<DPDataPacket*>* readQueue,
                   string keybase);
    void readFullWorker(BlockingQueue<DPDataPacket*>* readQueue,
                   string keybase, bool combined_by_client);
    size_t output2file(string saveas);
    bool lookupFile(string filename, string poolname);
    void close();

};

#endif
