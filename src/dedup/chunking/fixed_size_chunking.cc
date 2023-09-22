#include "chunking.hh"

static int chunk_size = 0;

void fsc_init(Config* conf) {
    chunk_size = conf->_chunkAvgSize;
}

int fixed_size_chunking(char *p, int size) {
    return chunk_size > size ? size : chunk_size;
}