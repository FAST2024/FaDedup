#ifndef _CHUNK_INFO_HH_
#define _CHUNK_INFO_HH_

#include "../inc/include.hh"

class ChunkInfo {
public:
    string _filename;
    string _poolname;
    string _storepool;
    unordered_map<int,vector<int> > pkt_chunk_id;
    int _count;
    ChunkInfo() {
        _count = 0;
    }
    ChunkInfo(string filename, string poolname,string storepool) {
    	_filename = filename;
    	_poolname = poolname;
    	 _storepool =  storepool;
        _count = 0;
    }
    bool operator< (const ChunkInfo& t) const
    {
	return _count > t._count;
    }
    ~ChunkInfo() {
        
    }
};

#endif
