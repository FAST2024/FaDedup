#ifndef _WORKER_BUFFER_HH_
#define _WORKER_BUFFER_HH_

#include "../inc/include.hh"
#include "DPDataChunk.hh"
#include "Config.hh"
#include "../storage/CephFS.hh"
#include "../storage/BaseFS.hh"
#include "../storage/FSUtil.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"
#include "WrappedFP.hh"
#include "PacketRecipeSerializer.hh"
#include "Router.hh"
#include <mutex>
#include <limits>

class WorkerBuffer {
private:
    Config* _conf;
    const int _MAX_PACKET_SIZE = 8;
    int _packet_size = 0;
    int _done_packet_size = 0;
    std::unordered_map<std::string, std::unordered_map<int,std::vector<int>>> _route_info;     // filename:poolname:pktid
    std::unordered_map<std::string, char*> _data_buffer;                            // filename:poolname:pktid:chunkid
    std::unordered_map<std::string, int> _data_len;
    std::unordered_map<std::string, fingerprint> _data_fp;
    std::mutex _buffer_mutex;
    void selectDedupChunk(std::string& pkt_key, int* src_node_id, int* dst_node_id);
    void adjustPacketRecipe(std::string filename, std::string poolname, int pktid, std::vector<std::tuple<int,int,int>> new_wrapfp);
    void flush();
    void registerDuplicateChunkDelete(char* fp, std::string filename, std::string poolname, int pktid, int chunkid, int node_id);
    void registerDuplicateChunk(char* fp, std::string filename, std::string poolname, int pktid, int chunkid, int node_id);
    void registerFilesInNode(string filename,string poolname,int nodeid);

public:
    WorkerBuffer(Config* conf);
    ~WorkerBuffer();
    void push(fingerprint fp, char* data, int data_len, std::string filename, std::string poolname, int pktid, int chunk_id, int node_id);
    bool request(std::string filename, std::string poolname, int pktid);
    void pushLast();
};



#endif
