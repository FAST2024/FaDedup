#ifndef _UPDATE_STREAM_HH_
#define _UPDATE_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"

class UpdateStream {
private:
    Config* _conf;
    string _filename;
    string _poolname;
    int _offset;
    int _size;
    redisContext* _localCtx;


public:
    UpdateStream(Config* conf, string filename, string poolname, int offset, int size);
    ~UpdateStream();
    // void init();
    void close();
    bool lookupFile(string filename, string poolname);
    void updatePacket(char* buf, int len, int update_offset);
    void updateChunk(char* buf, int len, int update_offset);
    void simpleUpdateChunk(char* buf, int len, int update_offset);
};

#endif