#ifndef _DP_OUTPUT_STREAM_HH_
#define _DP_OUTPUT_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"

class DPOutputStream {
private:
    Config* _conf;
    string _filename;
    string _poolname;
    string _mode;
    int _filesize;
    redisContext* _localCtx;
    int _pktid;
    int _replyid;

public:
    DPOutputStream(Config* conf, string filename, string poolname, string mode, int filesize);
    ~DPOutputStream();
    // void init();
    void write(char* buf, int len, bool batch);
    void close();
    void registerFile(int pktnum);
    bool lookupFile(string filename, string poolname);
    void writeAndRandomAggregate(char* buf, int len);
    void writeWithRBD(char* buf, int len);
    void writeAndSelectiveDedup(char* buf, int len);
    void writeWithRBDAndBatchPush(char* buf, int len);
    void writeWithRBDAndPushSuperChunk(char* buf, int len);
};

#endif