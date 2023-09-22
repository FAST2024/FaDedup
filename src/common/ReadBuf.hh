#ifndef _DP_READ_BUF_HH_
#define _DP_READ_BUF_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "../util/RedisUtil.hh"

class ReadBuf {
private:
    Config* _conf;
    static unordered_map<string,char*> _localBuf;
    redisContext* _localCtx;
    
public:
    static std::mutex _mutex;
    ReadBuf(Config* conf);
    ~ReadBuf();
    char* find(string conName);
    void insert(string conName,char* conData);
    void clear();
    int size();
};

#endif
