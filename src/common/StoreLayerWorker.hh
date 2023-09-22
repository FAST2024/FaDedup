#ifndef _STORE_LAYER_WORKER_HH_
#define _STORE_LAYER_WORKER_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "DPOutputStream.hh"
#include "StoreInputStream.hh"
#include "StoreOutputStream.hh"
#include "../protocol/AgCommand.hh"
#include "../storage/CephFS.hh"
#include "../storage/CephRBDFS.hh"
#include "../storage/BaseFS.hh"
#include "../storage/FSUtil.hh"
#include "../util/RedisUtil.hh"
#include "BlockingQueue.hh"
#include "DPDataPacket.hh"
#include "../dedup/DedupHandler.hh"
#include "Router.hh"
#include "PacketRecipe.hh"
#include "WrappedFP.hh"
#include "PacketRecipeSerializer.hh"
#include "ThreadPool.hh"
#include "Container.hh"
#include "ChunkInfo.hh"
#include "ReadBuf.hh"
#include "StoreLayer.hh"
using namespace std;

class StoreLayerWorker {
private:
    Config* _conf;


    // BaseFS* _fs;

    DedupHandler* _dpHandler;

    StoreLayer* _storeHandler;
    redisContext* _processCtx;
    redisContext* _localCtx;

public:
    static std::mutex _store_layer_worker_mutex;
    StoreLayerWorker(Config* conf);
    StoreLayerWorker(Config* conf, StoreLayer* storeHandler);
    ~StoreLayerWorker();

    void doProcess();
    // deal with client request
    void popChunk(AGCommand* agCmd);
    vector<int> lookupFp(char* fp, int size, unsigned int ip, bool batch);
    void registerFp(char* fp, int size, unsigned int ip, int pktid, int containerid, int conoff);
    
    void registerChunk(string chunkname, string filename, string poolname, string pktid, int chunk_id);
    void registerChunkAppend(string chunkname, string filename, string poolname, string pktid, int chunk_id);
    void flushStoreLayer();

    void popSelectiveDedupChunk(AGCommand* agCmd);
    void popUpdateChunk(AGCommand* agCmd);
    void popBatchChunk(AGCommand* agCmd);
    void readPacket(AGCommand* agCmd);
    void popChunkAndFlushToCephImmediately(AGCommand* agCmd);
};

#endif
