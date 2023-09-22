#include "ChunkingHandler.hh"

ChunkingHandler::ChunkingHandler(Config* conf):_conf(conf) {
    if (_conf->_chunkAlg == "rabin") {
        rabin_init(_conf);
        chunking = rabin_chunk_data;
    }
    else if (_conf->_chunkAlg == "fixed") {
        fsc_init(_conf);
        chunking = fixed_size_chunking;
    }
    else if (_conf->_chunkAlg == "fastcdc") {
        fastcdc_init(_conf);
        chunking = fastcdc_chunk_data;
    }
    else {
        cout << "[ERROR] unrecognized chunking algorithm: " << _conf->_chunkAlg << endl;
    }
}

ChunkingHandler::~ChunkingHandler() {

}

