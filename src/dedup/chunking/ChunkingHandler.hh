#ifndef _CHUNKING_HANDLER_HH_
#define _CHUNKING_HANDLER_HH_

#include "../../inc/include.hh"
#include "../../common/Config.hh"
#include "chunking.hh"

class ChunkingHandler {
public:
    Config* _conf;
    int (*chunking)(char* buf, int size);

    ChunkingHandler(Config* conf);
    ~ChunkingHandler();
};

#endif