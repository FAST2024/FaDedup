#ifndef _DP_REORDER_STREAM_HH_
#define _DP_REORDER_STREAM_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AgCommand.hh"

class DPReorderStream {
private:
    Config* _conf;
    redisContext* _localCtx;

public:
    DPReorderStream(Config* conf);
    ~DPReorderStream();
    void reorderWorker();
};

#endif
