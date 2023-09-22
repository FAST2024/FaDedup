#include "LocalRecognition.hh"

LocalRecognition::LocalRecognition(Config* conf) {
    _conf=conf;
    _localCtx=RedisUtil::createContext(_conf->_localIp);
    init();
}

LocalRecognition::LocalRecognition(Config* conf,
		vector<string> chunks_fp,
		unordered_map<string,vector<ChunkInfo*> > chunkinfos){
    _conf=conf;
    _localCtx=RedisUtil::createContext(_conf->_localIp);
    _chunks_fp = chunks_fp;
    _chunkinfos = chunkinfos;
    init();		

}

LocalRecognition::~LocalRecognition() {
    redisFree(_localCtx);
}


void LocalRecognition::init() {
    string name = "files";
    redisReply* files_rReply = (redisReply*)redisCommand(_localCtx, "SMEMBERS %s",name.c_str());
    assert(files_rReply->type == REDIS_REPLY_ARRAY);
    for(int i = 0; i < files_rReply->elements; i++){
    	string tmp = files_rReply->element[i]->str;
    	_files_index[tmp] = _files_count++;
    }
    freeReplyObject(files_rReply);
    cout << "[LocalRecognition::init] " << "the number of files is: " << _files_count << endl;
    
    createFileInfoString();
}

/*
 * For each chunk, register the files that the chunk appears in, if the chunk exists in the file, then regiser the corresponding position as 1, otherwise register it as 0
*/
void LocalRecognition::createFileInfoString(){
    for(int i = 0; i < _chunks_fp.size(); i++) {
    	string file_info = string(_files_count,'0');
    	string chunkname = _chunks_fp[i];
    	vector<ChunkInfo*> chunkinfo = _chunkinfos[chunkname];
    	for(int j = 0; j < chunkinfo.size(); j++){
    	    string name = chunkinfo[j]->_filename + ":" + chunkinfo[j]->_poolname;
    	    file_info[_files_index[name]] = '1';
	}
	_fileinfo_of_chunks[chunkname] = file_info;
	
	// cout << "chunkname: " << chunkname << " file_info: " << file_info << endl;
    }
    
}

void LocalRecognition::CalculateCosineSimilarity(){
    int size = _chunks_fp.size();
    //_locality_with_cos = vector<vector<double> >(size, vector<double>(size) );
    for(int i = 0; i < size; i++) {
        for(int j = i + 1; j < size; j++) {
     	    string fileinfo_i = _fileinfo_of_chunks[_chunks_fp[i]];
            string fileinfo_j = _fileinfo_of_chunks[_chunks_fp[j]];
            // _locality_with_cos[i][j] = (dot(fileinfo_i,fileinfo_j) * 1.0) / (VectorLength(fileinfo_i) * VectorLength(fileinfo_j));
            double tmp = (dot(fileinfo_i,fileinfo_j) * 1.0) / (VectorLength(fileinfo_i) * VectorLength(fileinfo_j));
            //cout << dot(fileinfo_i,fileinfo_j) * 1.0 << endl;
            //cout << VectorLength(fileinfo_i) * VectorLength(fileinfo_j) << endl;
            //cout << "chunkname:" << _chunks_fp[i] << " " << _chunks_fp[j] << " "<< _locality_with_cos[i][j] << endl;
            //assert(_locality_with_cos[i][j] <= 1.0);
            //assert(_locality_with_cos[i][j] >= 0);
            assert(tmp <= 1.1);
            assert(tmp >= 0);
            if(tmp != 0) _locality_with_cos_sortedqueue.push(make_tuple(i,j,tmp));
     	    
     	}
    }
    cout << "[LocalRecognition::CalculateCosineSimilarity] " << "CalculateCosineSimilarity done" << endl;
}

int LocalRecognition::dot(string a, string b){
    int ans = 0;
    assert(a.size() == b.size());
    for(int i = 0; i < a.size(); i++){
    	    ans += (a[i] - '0') * (b[i] - '0');
    }
    //cout << "a: " << a << " b: " << b << " ans: " << ans << endl;
    return ans;
}

double LocalRecognition::VectorLength(string s){
    int ans = 0;
    for(int i = 0; i < s.size(); i++) {
    	ans += pow((s[i] - '0'),2);
    }
    //cout << "sqrt ans: " << sqrt(1.0 * ans) << endl;
    return sqrt(1.0 * ans);
}

/*
 * based on the cosine similarity, merge the chunkfp
*/
static::vector<int> status = vector<int>(200000, 0);
void LocalRecognition::LocalRecognitionWorker() {
    cout << "[LocalRecognition::LocalRecognitionWorker] " << "begin local recognition" << endl;
    CalculateCosineSimilarity();
    
    //0:the chunkfp is not exist, 1:the chunkfp is at either end, 2:the chunkfp is in the middle
    // vector<int> status = vector<int>(_chunks_fp.size(), 0);
    	
    while(!_locality_with_cos_sortedqueue.empty()){
    	tuple<int, int, double> _cur = _locality_with_cos_sortedqueue.top();
    	// cout << get<2>(_cur) << endl;
    	// cout << "current status: " << "chunkname " << _chunks_fp[get<0>(_cur)] << " status " << status[get<0>(_cur)] << "; chunkname" << _chunks_fp[get<1>(_cur)] << " status " << status[get<1>(_cur)] << endl;
    	_locality_with_cos_sortedqueue.pop();
    	if(get<2>(_cur) == 0) break;
    	if(status[get<0>(_cur)] == 2 || status[get<1>(_cur)] == 2) continue;    	
    	else if(status[get<0>(_cur)] == 0 && status[get<1>(_cur)] == 0) {
    	    _chunks_fp_with_locality.push_back({_chunks_fp[get<0>(_cur)],_chunks_fp[get<1>(_cur)] });
    	    status[get<0>(_cur)] = 1;
    	    status[get<1>(_cur)] = 1;
	}
	else if(status[get<0>(_cur)] == 1 && status[get<1>(_cur)] == 1){
	    vector<pair<int, int> > index(2); 	//pair: the index of the chunk, left(0) or right(1);
	    for(int i = 0; i < _chunks_fp_with_locality.size(); i++){
	    	if(_chunks_fp[get<0>(_cur)] == _chunks_fp_with_locality[i][0]) index[0] = make_pair(i,0);
	    	else if(_chunks_fp[get<0>(_cur)] == _chunks_fp_with_locality[i][_chunks_fp_with_locality[i].size() - 1]) index[0] = make_pair(i,1);
	    	
	    	if(_chunks_fp[get<1>(_cur)] == _chunks_fp_with_locality[i][0]) index[1] = make_pair(i,0);
	    	else if(_chunks_fp[get<1>(_cur)] == _chunks_fp_with_locality[i][_chunks_fp_with_locality[i].size() - 1]) index[1] = make_pair(i,1);
	    }
	    
	    if(index[0].first != index[1].first) {
	    	merge(index[0].first, index[0].second, index[1].first, index[1].second);
	    	status[get<0>(_cur)] = 2;
	    	status[get<1>(_cur)] = 2;
	    }
	}
	else if(status[get<0>(_cur)] == 1){
	    for(int i = 0; i < _chunks_fp_with_locality.size(); i++){
	    	if(_chunks_fp[get<0>(_cur)] == _chunks_fp_with_locality[i][0]) {
	    	    _chunks_fp_with_locality[i].insert(_chunks_fp_with_locality[i].begin(),1,_chunks_fp[get<1>(_cur)]);
	    	    status[get<0>(_cur)] = 2;
	    	    status[get<1>(_cur)] = 1;
	    	    break;
	    	}

	    	else if(_chunks_fp[get<0>(_cur)] == _chunks_fp_with_locality[i][_chunks_fp_with_locality[i].size() - 1]){
	    	    _chunks_fp_with_locality[i].insert(_chunks_fp_with_locality[i].end(),1,_chunks_fp[get<1>(_cur)]);
	    	    status[get<0>(_cur)] = 2;
	    	    status[get<1>(_cur)] = 1;
	    	    break;
		}  	
	    }
	}
	else if(status[get<1>(_cur)] == 1){
	    for(int i = 0; i < _chunks_fp_with_locality.size(); i++){
	    	if(_chunks_fp[get<1>(_cur)] == _chunks_fp_with_locality[i][0]) {
	    	    _chunks_fp_with_locality[i].insert(_chunks_fp_with_locality[i].begin(),1,_chunks_fp[get<0>(_cur)]);
	    	    status[get<1>(_cur)] = 2;
	    	    status[get<0>(_cur)] = 1;
	    	    break;
	    	}

	    	else if(_chunks_fp[get<1>(_cur)] == _chunks_fp_with_locality[i][_chunks_fp_with_locality[i].size() - 1]){
	    	    _chunks_fp_with_locality[i].insert(_chunks_fp_with_locality[i].end(),1,_chunks_fp[get<0>(_cur)]);
	    	    status[get<1>(_cur)] = 2;
	    	    status[get<0>(_cur)] = 1;
	    	    break;
		}  	
	    }
	}
	// cout << "changed status: " << "chunkname " << _chunks_fp[get<0>(_cur)] << " status " << status[get<0>(_cur)] << "; chunkname " << _chunks_fp[get<1>(_cur)] << " status " << status[get<1>(_cur)] << endl;
    }
    std::fill(status.begin(), status.begin()+_chunks_fp.size(), 0);
    cout << "[LocalRecognition::LocalRecognitionWorker] " << "local recognition done" << endl;
}

void LocalRecognition::merge(int index1, int dirt1, int index2, int dirt2) {
    if(dirt1 > dirt2) {
    	_chunks_fp_with_locality[index1].insert(_chunks_fp_with_locality[index1].end(),_chunks_fp_with_locality[index2].begin(),_chunks_fp_with_locality[index2].end());
    	_chunks_fp_with_locality.erase(_chunks_fp_with_locality.begin() + index2);
    }
    else if(dirt1 < dirt2) {
    	_chunks_fp_with_locality[index2].insert(_chunks_fp_with_locality[index2].end(),_chunks_fp_with_locality[index1].begin(),_chunks_fp_with_locality[index1].end());
    	_chunks_fp_with_locality.erase(_chunks_fp_with_locality.begin() + index1);
    }
    else if(dirt1 == 0) {
    	for(int i = 0 ; i < _chunks_fp_with_locality[index2].size(); i++) {
    	    _chunks_fp_with_locality[index1].insert(_chunks_fp_with_locality[index1].begin(),1,_chunks_fp_with_locality[index2][i]);
    	}
    	_chunks_fp_with_locality.erase(_chunks_fp_with_locality.begin() + index2);
    }
    else {
    	for(int i = _chunks_fp_with_locality[index2].size() - 1; i >= 0; i--) {
    	    _chunks_fp_with_locality[index1].insert(_chunks_fp_with_locality[index1].end(),1,_chunks_fp_with_locality[index2][i]);
    	}
    	_chunks_fp_with_locality.erase(_chunks_fp_with_locality.begin() + index2);
    }
}

vector<vector<string> > LocalRecognition::getChunksFPWithLocality(){
    return _chunks_fp_with_locality;
}
