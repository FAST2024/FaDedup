#ifndef _CONFIG_HH_
#define _CONFIG_HH_

#include "../inc/include.hh"

using namespace std;

class Config {
public:
    Config(string path);
    ~Config();

    string _sys_conf_path;
    string _ceph_conf_path;

    unsigned int _localIp;
    IPAddress _localAddr;
    int _agentNum;
    vector<IPAddress> _agentsAddrs;
    vector<unsigned int> _agentsIPs;
    int port;

    int _agWorkerThreadNum;
    int _pktSize;

    int _chunkAvgSize;
    int _chunkMaxSize;
    int _chunkMinSize;

    string _chunkAlg;
    string _hashAlg;

    string _cacheAlg;
    int _cacheTransform;
    int _cacheCapacity;

    int _readThrdNum;
    int _containerSize;
    double _dedupRatio;
    bool _localityAware;
    int _super_chunk_size = 256;
    int _redirect_degree = 1;
    WriteMode _write_mode = WriteMode::Normal;
    UpdateMode _update_mode = UpdateMode::Normal;
    
private:
    int parseConf(string path);
};

#endif