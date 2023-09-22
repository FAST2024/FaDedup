#ifndef _LOCAL_RECOGNITION_HH_
#define _LOCAL_RECOGNITION_HH_

#include "../inc/include.hh"
#include "Config.hh"
#include "../util/RedisUtil.hh"
#include "ChunkInfo.hh"

class LocalRecognition {
private:
    Config* _conf;
    redisContext* _localCtx;
    vector<string> _chunks_fp;
    unordered_map<string,int> _files_index;			//key:filename+poolname; value: index;
    unordered_map<string,string> _fileinfo_of_chunks; 	//key:chunk fingerprint; value: binary representation of files
    unordered_map<string,vector<ChunkInfo*> > _chunkinfos;
    // vector<vector<double> > _locality_with_cos;
    vector<vector<string> > _chunks_fp_with_locality;
    
    struct CompareTuple {
  	bool operator()(const tuple<int,int,double>& a,const tuple<int,int,double>& b) const {
    	    return get<2>(a) < get<2>(b);
    	}
    };
    priority_queue<tuple<int,int,double>, vector<tuple<int,int,double>>, CompareTuple> _locality_with_cos_sortedqueue;
    int _files_count = 0;
    
    void init();
    void createFileInfoString();
    int dot(string a, string b);
    double VectorLength(string s);
    void CalculateCosineSimilarity();
    void merge(int index1, int dirt1, int index2, int dirt2);
    
public:
    LocalRecognition(Config* conf);
    LocalRecognition(Config* conf,vector<string> chunks_fp,unordered_map<string,vector<ChunkInfo*> > chunkinfos);
    ~LocalRecognition();
    
    void LocalRecognitionWorker();
    vector<vector<string> > getChunksFPWithLocality();
    
};

#endif
