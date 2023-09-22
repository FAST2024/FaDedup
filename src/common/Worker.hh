#ifndef _WORKER_HH_
#define _WORKER_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "DPOutputStream.hh"
#include "StoreInputStream.hh"
#include "StoreOutputStream.hh"
#include "../protocol/AgCommand.hh"
#include "../storage/CephFS.hh"
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
#include "WorkerBuffer.hh"
#include "LocalRecognition.hh"
using namespace std;

class Worker {
private:
    Config* _conf;


    BaseFS* _fs;

    DedupHandler* _dpHandler;

    StoreLayer* _storeHandler;
    redisContext* _processCtx;
    redisContext* _localCtx;
    WorkerBuffer* _worker_buffer;

public:
    static std::mutex _worker_mutex;
    Worker(Config* conf, StoreLayer* storeHandler, WorkerBuffer* worker_buffer);
    ~Worker();

    void doProcess();
    // deal with client request
    void clientWriteFull(AGCommand* agCmd);
    void clientWriteBatch(AGCommand* agCmd);
    void clientWritePartial(AGCommand* agCmd);
    void clientReadFull(AGCommand* agCmd);
    void clientReadBatch(AGCommand* agCmd);
    void clientWriteFullAndRandomAggregate(AGCommand* agCmd);
    void clientReadFullAfterRandomAggregate(AGCommand* agCmd);
    void clientWriteFullWithRBD(AGCommand* agCmd);
    void clientReadFullWithRBD(AGCommand* agCmd);
    void clientWriteFullAndSelectiveDedup(AGCommand* agCmd);
    void clientReadFullWithRBDCombineByClient(AGCommand* agCmd);
    void readPacket(AGCommand* agCmd);
    void updatePacket(AGCommand* agCmd);
    void updateChunk(AGCommand* agCmd);
    void simpleUpdateChunk(AGCommand* agCmd);
    void clientWriteFullWithRBDAndBatchPush(AGCommand* agCmd);
    void clientWriteFullWithRBDAndPushSuperChunk(AGCommand* agCmd);
    void onlineWriteFull(string filename, int pktid, string poolname, int filesize, unsigned int sender);
    void offlineWriteFull(string filename, int pktid, string poolname, int filesize);
    void onlineWriteBatch(string filename, int pktid, string poolname, int filesize, unsigned int sender);
    void offlineWriteBatch(string filename, int pktid, string poolname, int filesize);
    void onlineWritePartial(string filename, string poolname, int size, int offset);
    void offlineWritePartial(string filename, string poolname, int size, int offset);
    void onlineWriteFullAndRandomAggregate(string filename, int pktid, string poolname, int filesize, unsigned int sender);
    void onlineWriteFullWithRBD(string filename, string poolname, int pktid, int size, unsigned int sender);
    void onlineWriteFullAndSelectiveDedup(string filename, string poolname, int pktid, int size, unsigned int sender);
    bool lookupFp(char* fp, int size, unsigned int ip);
    vector<int> lookupFp(char* fp, int size, unsigned int ip, bool batch);
    void registerFp(char* fp, int size, unsigned int ip);
    void registerFp(char* fp, int size, unsigned int ip, int pktid, int containerid, int conoff);
    int registerFpAdd(char* fp, int size, unsigned int ip);
    int registerFpSub(char* fp, int size, unsigned int ip);
    void persistFullChunkTask(string filename, string poolname, char* buf, int size);
    void registerFile(AGCommand* cmd);
    void readFullObj(string filename, string poolname, int pktnum);
    void readBatchObj(string filename, string poolname, int pktnum);
    void readFullObjAfterRandomAggregate(string filename, string poolname, int pktnum);

    // load data from redis
    void loadWorker(BlockingQueue<DPDataPacket*>* readQueue,
                    string keybase,
                    int pktid);

    // chunk
    void chunkingWorker(BlockingQueue<DPDataPacket*>* readQueue,
                        BlockingQueue<DPDataChunk*>* chunkQueue);

    // hash
    void hashingWorker(BlockingQueue<DPDataChunk*>* chunkQueue,
                        BlockingQueue<DPDataChunk*>* hashQueue);

    // Lookup
    void lookupWorker(BlockingQueue<DPDataChunk*>* hashQueue,
                        BlockingQueue<DPDataChunk*>* lookupQueue,
                        string filename,
                        PacketRecipe* pRecipe);

    void lookupWorker(BlockingQueue<DPDataChunk*>* hashQueue,
                        BlockingQueue<DPDataChunk*>* lookupQueue);
                        
    void batchWorker(BlockingQueue<DPDataChunk*>* lookupQueue,
                     BlockingQueue<Container*>* persistQueue,
                     string key,
                     PacketRecipe* pRecipe,
                     int pktid);

    // Persist
    void persistFullWorker(BlockingQueue<DPDataChunk*>* lookupQueue,
                            string objname,
                            string poolname);

    void persistFullWorker(BlockingQueue<Container*>* persistQueue,
                            string poolname);
     
    void persistFullWorker(BlockingQueue<Container*>* persistQueue);
    // read packet
    void readFullPacketWorker(string filename, 
                            string poolname,
                            int pktid);
    void readBatchPacketWorker(string filename, 
                            string poolname,
                            int pktid);
    void lookupToStoreLayerWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    void persistFullToStoreLayerWorker(BlockingQueue<DPDataChunk*>* lookupQueue, string filename, string poolname, int pktid);
    void readFullPacketAfterRandomAggregateWorker(string filename, string poolname, int pktid);
    void pushingWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    void pushingAndSelectiveDedupWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    void updateAndPushWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    void batchPushingWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    void PushingSuperChunkWorker(BlockingQueue<DPDataChunk*>* hashQueue, string filename, string poolname, int pktid);
    // transfer packet
    // void transferWorker(BlockingQueue<DPDataPacket*>* readQueue);
    void clientDeleteFull(AGCommand* agCmd);
    void deletePacketWorker(string filename, string poolname, int pktid);
    void deleteChunkTask(string chunkname, string poolname, char* fp, BaseFS* fs);
    void deleteChunkTask(string chunkname, string poolname, BaseFS* fs);
    void registerChunk(string chunkname, string filename, string poolname, string pktid, int chunk_id, int ipId);
    void registerChunkDelete(string chunkname, int ipId);
    void registerChunkAppend(string chunkname, string filename, string poolname, string pktid, int chunk_id, int ipId);
    void registerOriginChunkPoolname(string chunkname, string poolname, int ipId);
    void getOriginChunkPoolname(string chunkname, int ipId, string& origin_chunk_poolname);
    
    void clientReorder();
    void localRecognitionWorker(unordered_map<string,vector<ChunkInfo*> > &chunkInfos, vector<vector<string> > &chunks_fp_with_locality);
    void createConCosWorker(unordered_map<string,vector<ChunkInfo*> > &chunkInfos, vector<vector<string> > &chunks_fp_with_locality);
    vector<ChunkInfo*> lookupChunkInfo(string register_chunkname);
    vector<ChunkInfo*> splitChunkInfo(string Information, int size);
    bool getConInfo(string chunkname,int ipId,string& conInfo);
    void registerConInfo(string chunkname,int ipId,string filename, string poolname,int conId,int conOff);
    void deleteRegisterConInfo(string chunkname,int ipId);
    void registerFilesInNode(string filename,string poolname,int ipId);
    void readPktRecipe(string recipename, vector<WrappedFP*>* recipe,int ipId);
    void writePktRecipe(string recipename, vector<WrappedFP*>* recipe, int ipId);
    void registerFp(string chunkname, unsigned int ip, int pktid, int containerid, int conoff);

    bool checkChunkRefered(char* fp);
    void sendChunkBufferToStoreLayer(std::unordered_map<int, std::vector<DPDataChunk*>> chunk_buffer, string filename, string poolname, int pktid);
    void clearReadBuf();
    
   
};

#endif
