#include "DedupHandler.hh"

DedupHandler::DedupHandler(Config* conf): _conf(conf) {
    chunker = new ChunkingHandler(conf);
    hasher = new HashingHandler(conf);
}

DedupHandler::~DedupHandler() {
    if(chunker)
        delete chunker;
    if(hasher)
        delete hasher;
}

int DedupHandler::chunking(char* data, int size) {
    return chunker->chunking(data, size);
}

void DedupHandler::hashing(char *data, int32_t size, fingerprint fp) {
    hasher->hashing(data, size, fp);
}