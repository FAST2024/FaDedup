#ifndef _WRAPPED_FP_HH_
#define _WRAPPED_FP_HH_

#include "../inc/include.hh"

class WrappedFP {
public:
    char* _fp;
    bool _dup;
    int _chunksize;
    int _pktid;
    int _containerId;
    int _offset;
    char _origin_chunk_poolname[32];
    char _chunk_store_filename[32];
    unsigned int _conIp;
    // int _containerSize;
    WrappedFP() {
        _fp = new char[sizeof(fingerprint)];
        _dup = false;
        _chunksize = 0;
        _pktid = 0;
        _containerId = 0;
        _offset = 0;
        // _containerSize = 0;
    }
    WrappedFP(char* fp) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = false;
        _chunksize = 0;
        _pktid = 0;
        _containerId = 0;
        _offset = 0;
        // _containerSize = 0;
    }
    WrappedFP(char* fp, int chunksize) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = false;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = 0;
        _offset = 0;
        // _containerSize = 0;
    }
    WrappedFP(char* fp, int chunksize, bool dup) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = 0;
        _offset = 0;
        // _containerSize = 0;
    }
    WrappedFP(char* fp, int chunksize, bool dup, const char* origin_chunk_poolname) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = 0;
        _offset = 0;
        // _containerSize = 0;
        memset(_origin_chunk_poolname, '\0', 32);
        memcpy(_origin_chunk_poolname, origin_chunk_poolname, strlen(origin_chunk_poolname));
        // std::cout << "new WrappedFp : origin_chunk_poolname : " << _origin_chunk_poolname << std::endl;

    }

    WrappedFP(char* fp, int chunksize, bool dup, const char* chunk_store_filename, const char* origin_chunk_poolname, int conId,unsigned int conIp, int conOff) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = conId;
        _offset = conOff;
        _conIp = conIp;
        // _containerSize = 0;
        memset(_origin_chunk_poolname, '\0', 32);
        memcpy(_origin_chunk_poolname, origin_chunk_poolname, strlen(origin_chunk_poolname));
        memset(_chunk_store_filename, '\0', 32);
        memcpy(_chunk_store_filename, chunk_store_filename, strlen(chunk_store_filename));
        // std::cout << "new WrappedFp : origin_chunk_poolname : " << _origin_chunk_poolname << std::endl;

    }
    
    WrappedFP(char* fp, int chunksize, bool dup, int cid, int off) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = cid;
        _offset = off;
        // _containerSize = 0;
    }
    WrappedFP(char* fp, int chunksize, bool dup, int cid, int off, int conSize) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = 0;
        _containerId = cid;
        _offset = off;
        // _containerSize = conSize;
    }
    WrappedFP(char* fp, int chunksize, bool dup, int pid, int cid, int off, int conSize) {
        _fp = new char[sizeof(fingerprint)];
        memcpy(_fp, fp, sizeof(fingerprint));
        _dup = dup;
        _chunksize = chunksize;
        _pktid = pid;
        _containerId = cid;
        _offset = off;
        // _containerSize = conSize;
    }
    void deepCopy(char* fp, int len) {
        if (_fp)
            memcpy(_fp, fp, len);
        else {
            _fp = new char[len];
            memcpy(_fp, fp, len);
        }
    }
    // void setContainerSize(int size) {
    //     _containerSize = size;
    // }
    ~WrappedFP() {
        if (_fp)
            delete _fp;
    }

};

#endif
