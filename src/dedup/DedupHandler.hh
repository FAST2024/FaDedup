#ifndef _DEDUP_HANDLER_HH_
#define _DEDUP_HANDLER_HH_

#include "../inc/include.hh"
#include "chunking/ChunkingHandler.hh"
#include "hashing/HashingHandler.hh"
#include "../common/Config.hh"

class DedupHandler {
private:
    ChunkingHandler* chunker;
    HashingHandler* hasher;
    Config* _conf;

public:
    DedupHandler(Config* conf);
    ~DedupHandler();
    int chunking(char* data, int size);
    void hashing(char *data, int32_t size, fingerprint fp);

};

#endif