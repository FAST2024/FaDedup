#include "WorkerBuffer.hh"

WorkerBuffer::WorkerBuffer(Config* conf) {
    _conf = conf;
}

WorkerBuffer::~WorkerBuffer() {
    for(auto iter:_data_buffer) {
        delete [] iter.second;
    }
}

/**
 * push a duplicate chunk to worker buffer as candidate for selective dedup
 * 1 check whether worker buffer is full, if full, flush it
 * 2 push the chunk and register
 * NOTE: lock mutex first
*/
void WorkerBuffer::push(fingerprint fp,char* data, int data_len, std::string filename, std::string poolname, int pktid, int chunk_id, int node_id) {
    _buffer_mutex.lock();
    // std::cout << "[WorkerBuffer] push duplicate chunk filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk_id << " dst node_id:" << node_id << " datalen:" << data_len << std::endl; 
    std::string pkt_key = filename + ":" + poolname + ":" + to_string(pktid);
    _route_info[pkt_key][node_id].push_back(chunk_id);
    std::string chunk_key = pkt_key + ":" + to_string(chunk_id);
    char* buf = nullptr;
    buf = new char[data_len];
    assert(buf!=nullptr);
    memcpy(buf, data, data_len);
    assert(_data_len.count(chunk_key)==0);
    assert(_data_buffer.count(chunk_key)==0);
    assert(_data_fp.count(chunk_key)==0);
    _data_len[chunk_key] = data_len;
    _data_buffer[chunk_key] = buf;
    memcpy(_data_fp[chunk_key], fp, sizeof(fingerprint));
    _buffer_mutex.unlock();
    // std::cout << "[WorkerBuffer] push duplicate chunk filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " chunkid:" << chunk_id << " dst node_id:" << node_id << " datalen:" << data_len << " done" << std::endl; 
}

/**
 * 1 select some chunk and a node
 * 2 route these to store layer of corresponding node
 * 3 update packet recipe of corresponding packet
 * 4 clear buffer
 * NOTE: called by push(), locked already 
*/
void WorkerBuffer::flush() {
    // 1 select chunk and src node and dst node
    std::cout << "[WorkerBuffer] worker buffer is full, select duplicate chunk to store in local node" << std::endl;
    assert(_route_info.size() == _MAX_PACKET_SIZE);
    for(int i=0;i<_conf->_redirect_degree;i++) {
        std::string selected_key;
        int src_node_id = -1, dst_node_id = -1;
        selectDedupChunk(selected_key, &src_node_id, &dst_node_id);
        
        std::stringstream ss(selected_key);         // filename:poolname:pktid
        std::string item;
        std::vector<std::string> items;
        while (std::getline(ss, item, ':')) {
            items.push_back(item);
        }
        assert(items.size()==3);
        std::string filename = items[0];
        std::string poolname = items[1];
        int pktid = std::stoi(items[2]);
        assert(_route_info.count(selected_key)!=0);
        assert(_route_info[selected_key].size() == _conf->_agentNum);
        std::cout << "[WorkerBuffer] redirect:" << i << " select filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " from node_id:" << src_node_id << " to node_id:" << dst_node_id << " chunk cnt:" << _route_info[selected_key][src_node_id].size() << std::endl;
        std::cout << "[WorkerBuffer] node0 cnt:" << _route_info[selected_key][0].size() << " node1 cnt:" << _route_info[selected_key][1].size() << " node2 cnt:" << _route_info[selected_key][2].size() << " node3 cnt:" << _route_info[selected_key][3].size() << 
                    " node4 cnt:" << _route_info[selected_key][4].size() << " node5 cnt:" << _route_info[selected_key][5].size() << " node6 cnt:" << _route_info[selected_key][6].size() << " node7 cnt:" << _route_info[selected_key][7].size() << std::endl;
        
        // 2 route chunks to dst node to persist
        std::vector<std::tuple<int,int,int>> new_wrapfp;
        for(int i=0;i<_route_info[selected_key][src_node_id].size();i++) {
            int chunk_id = _route_info[selected_key][src_node_id][i];
            // std::cout << "[WorkerBuffer] route chunkid:" << chunk_id << " from src_node_id:" << src_node_id << " to dst_node_id:" << dst_node_id << std::endl;
            std::string chunk_key = selected_key + ":" + to_string(chunk_id);
            int data_len = _data_len[chunk_key];
            AGCommand* agCmd = new AGCommand();
            agCmd->buildType17(17, filename, poolname, pktid, chunk_id, data_len, _conf->_localIp);
            // RedisUtil::AskRedisContext(_conf->_agentsIPs[dst_node_id]);
            agCmd->sendToStoreLayer(_conf->_agentsIPs[dst_node_id]);
            delete agCmd;
            
            string write_key = "chunkbuffer:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunk_id);
            redisContext* write_ctx = RedisUtil::createContext(_conf->_agentsIPs[dst_node_id]);
            char* buf = new char[data_len];
            memcpy(buf, _data_buffer[chunk_key], data_len);
            redisReply* write_reply = (redisReply*)redisCommand(write_ctx, "rpush %s %b", write_key.c_str(), buf, data_len); 
            assert(write_reply!=NULL);
            assert(write_reply->type == REDIS_REPLY_INTEGER);
            assert(write_reply->integer == 1);
            freeReplyObject(write_reply);
            redisFree(write_ctx);
            delete [] buf;
            

            // wait for persist done, return conid and conoff and construct packet recipe
            string wait_key = "writefinish:" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunk_id);
            redisContext* wait_ctx = RedisUtil::createContext(_conf->_agentsIPs[dst_node_id]);
            redisReply* wait_reply = (redisReply*)redisCommand(wait_ctx, "blpop %s 0", wait_key.c_str());
            assert(wait_reply!=NULL);
            assert(wait_reply->type == REDIS_REPLY_ARRAY);
            char* con_info = wait_reply->element[1]->str;
            int con_id, con_off, tmp_con_id, tmp_con_off;
            memcpy((void*)&tmp_con_id, con_info, sizeof(int));
            memcpy((void*)&tmp_con_off, con_info+sizeof(int), sizeof(int));
            con_id = ntohl(tmp_con_id);
            con_off = ntohl(tmp_con_off);
            new_wrapfp.push_back({chunk_id, con_id, con_off});
            freeReplyObject(wait_reply);
            redisFree(wait_ctx);

            assert(_data_fp.count(chunk_key)!=0);
            // delete hash_fp in src node
            registerDuplicateChunkDelete(_data_fp[chunk_key], filename, poolname, pktid, chunk_id, src_node_id);
            // register hash_fp in dst node
            registerDuplicateChunk(_data_fp[chunk_key], filename, poolname, pktid, chunk_id, dst_node_id);
            // RedisUtil::FreeRedisContext(_conf->_agentsIPs[dst_node_id]);
        }

        // 3 update packet recipe
        adjustPacketRecipe(filename, poolname, pktid, new_wrapfp);

        _route_info.erase(selected_key);
    }
    
    // 4 route done, clear worker buffer
    for(auto iter:_data_buffer) {
        delete [] iter.second;
    }
    _route_info.clear();
    _data_buffer.clear();
    _data_len.clear();
    _data_fp.clear();
    _packet_size = 0;
    _done_packet_size = 0;
    std::cout << "[WorkerBuffer] flush done" << std::endl;
}

/**
 * select some chunk and a dst node
 * NOTE: called by flush(), locked already. 1 packet -- 1024 chunk
 */
void WorkerBuffer::selectDedupChunk(std::string& selected_key, int* src_node_id, int* dst_node_id) {
    // select src node
    int cnt = std::numeric_limits<int>::max();
    for(auto iter:_route_info) {
        // assert(_route_info.size() == _conf->_agentNum);
        for(int i=0;i<_conf->_agentNum;i++) {
            if((iter.second)[i].size() < cnt) {
                cnt = (iter.second)[i].size();
                selected_key = iter.first;
                *src_node_id = i;
            }
        }
    }
    // select dst node
    cnt = std::numeric_limits<int>::max();;
    for(int i=0;i<_conf->_agentNum;i++) {
        if(i==*src_node_id) {
            continue;
        }
        if(_route_info[selected_key][i].size() < cnt) {
            cnt = _route_info[selected_key][i].size();
            *dst_node_id = i;
        }
    }
    assert(*dst_node_id!=-1);
    assert(*src_node_id!=-1);
    std::cout << "[selectDedupChunk] select:" << selected_key << " route chunks in node_id:" << *src_node_id << " cnt:" << _route_info[selected_key][*src_node_id].size()
        << " to node_id:" << *dst_node_id << " cnt:" << _route_info[selected_key][*dst_node_id].size() << std::endl;
}

/**
 * 1 get packet recipe and decode
 * 2 update
 * 3 encode and store
 */
void WorkerBuffer::adjustPacketRecipe(std::string filename, std::string poolname, int pktid, std::vector<std::tuple<int,int,int>> new_wrapfp) {
    std::cout << "[WorkerBuffer] start adjustPacketRecipe filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << std::endl;
    // 1 get old packet recipe, is stored in local redis
    string key = filename + ":" + poolname + ":" + to_string(pktid);
    string recipeKey = key + ":" + "recipe";
    redisContext* sendCtx = RedisUtil::createContext(_conf->_localIp);
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "GET %s", recipeKey.c_str());
    assert(rReply!=NULL);
    assert(rReply->type == REDIS_REPLY_STRING);
    char* stream = rReply->str;
    vector<WrappedFP*>* recipe = new vector<WrappedFP*>();
    PacketRecipeSerializer::decode(stream, recipe);
    freeReplyObject(rReply);
    redisFree(sendCtx);
    // 2 update
    for(int i=0;i<new_wrapfp.size();i++) {
        int chunkid = std::get<0>(new_wrapfp[i]);
        assert(chunkid < recipe->size());
        int con_id = std::get<1>(new_wrapfp[i]);
        int con_off = std::get<2>(new_wrapfp[i]);
        // std::cout << "[WorkerBuffer] adjustPacketRecipe: modify pktid:" << pktid << " chunkid:" << chunkid << " to con_id:" << con_id << " con_off:" << con_off << std::endl;
        (*recipe)[chunkid]->_containerId = con_id;
        (*recipe)[chunkid]->_offset = con_off;
    }

    // 3 encode and store packet recipe in local redis
    int alloc_len = sizeof(int) + recipe->size() * (PacketRecipeSerializer::RECIPE_SIZE);
    char* new_stream = new char[alloc_len];
    PacketRecipeSerializer::encode(recipe, new_stream, alloc_len);
    redisContext* store_ctx = RedisUtil::createContext(_conf->_localIp);
    redisReply* store_reply = (redisReply*)redisCommand(store_ctx, "SET %s %b", recipeKey.c_str(), (void*)new_stream, alloc_len);
    assert(store_reply!=NULL);
    assert(store_reply->type == REDIS_REPLY_STATUS);
    assert(strcmp(store_reply->str, "OK")==0);
    freeReplyObject(store_reply);
    redisFree(store_ctx);
    delete [] new_stream;
    delete recipe;
    std::cout << "[WorkerBuffer] adjustPacketRecipe done, filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << std::endl;
}

/**
 * request a buffer in worker buffer, if full, return false
*/
bool WorkerBuffer::request(std::string filename, std::string poolname, int pktid) {
    _buffer_mutex.lock();
    if(_packet_size == _MAX_PACKET_SIZE) {
        _buffer_mutex.unlock();
        return false;
    }
    std::cout << "[WorkerBuffer] request : filename:" << filename << " poolname:" << poolname << " pktid:" << pktid << " request successfully" << std::endl;
    std::string pkt_key = filename + ":" + poolname + ":" + to_string(pktid);
    assert(_route_info.count(pkt_key)==0);
    _packet_size++;
    _buffer_mutex.unlock();
    return true;
}

void WorkerBuffer::pushLast() {
    _buffer_mutex.lock();
    _done_packet_size++;
    std::cout << "[WorkerBuffer] receive a last flag" << std::endl;
    if(_done_packet_size == _MAX_PACKET_SIZE) {
        std::cout << "[WorkerBuffer] all packet in worker buffer is done, flush" << std::endl;
        flush();
    }
    _buffer_mutex.unlock();
}


void WorkerBuffer::registerDuplicateChunkDelete(char* fp, std::string filename, std::string poolname, int pktid, int chunkid, int node_id) {
    std::string chunkname = to_string(Router::hash(fp, sizeof(fingerprint)));
    std::string lock_key = "lock:register:" + chunkname;
    std::string key = "register:" + chunkname;
    // std::cout << "[WorkerBuffer] registerDuplicateChunkDelete hash_fp:" << chunkname << std::endl;
    // lock
    while(1) {
        redisContext* lock_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* lock_reply = (redisReply*)redisCommand(lock_ctx, "SETNX %s 1", lock_key.c_str());
        assert(lock_reply!=NULL);
        assert(lock_reply->type == REDIS_REPLY_INTEGER);
        if(lock_reply->integer == 1) {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            break;
        } else {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            sleep(1);
        }
    }
    // std::cout << "[WorkerBuffer] lock register successfully" << std::endl;
    
    // get
    redisContext* get_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* get_reply = (redisReply*)redisCommand(get_ctx, "GET %s", key.c_str());
    assert(get_reply!=NULL);
    assert(get_reply->type == REDIS_REPLY_STRING);
    
    std::string new_chunk_info;
    std::istringstream ss(get_reply->str);
    std::string temp_filename, temp_poolname, temp_pktid, temp_chunkid;
    bool found;
    while (std::getline(ss, temp_filename, ':')) {
        assert(std::getline(ss, temp_poolname, ':'));
        assert(std::getline(ss, temp_pktid, ':'));
        assert(std::getline(ss, temp_chunkid, ':'));
        if(temp_filename == filename &&
            temp_poolname == poolname &&
            to_string(pktid) == temp_pktid &&
            to_string(chunkid) == temp_chunkid) {
                found = true;
                continue;
            }
        if(new_chunk_info.empty()) {
            new_chunk_info = temp_filename + ":" + temp_poolname + ":" + temp_pktid + ":" + temp_chunkid;
        } else {
            new_chunk_info = new_chunk_info + ":" + temp_filename + ":" + temp_poolname + ":" + temp_pktid + ":" + temp_chunkid;
        }
    }
    assert(found);
    redisFree(get_ctx);
    freeReplyObject(get_reply);

    // set
    redisContext* set_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* set_reply = (redisReply*)redisCommand(set_ctx, "SET %s %b", key.c_str(), new_chunk_info.c_str(), new_chunk_info.size());
    assert(set_reply!=NULL);
    assert(set_reply->type == REDIS_REPLY_STATUS);
    assert(std::string(set_reply->str) == "OK");
    redisFree(set_ctx);
    freeReplyObject(set_reply);

    // unlock
    redisContext* unlock_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* unlock_reply = (redisReply*)redisCommand(unlock_ctx, "DEL %s", lock_key.c_str());
    assert(unlock_reply!=NULL);
    assert(unlock_reply->type == REDIS_REPLY_INTEGER);
    assert(unlock_reply->integer == 1);
    freeReplyObject(unlock_reply);
    redisFree(unlock_ctx);
    // std::cout << "[WorkerBuffer] registerDuplicateChunkDelete hash_fp:" << chunkname << " done" << std::endl;
}

void WorkerBuffer::registerDuplicateChunk(char* fp, std::string filename, std::string poolname, int pktid, int chunkid, int node_id) {
    std::string chunkname = to_string(Router::hash(fp, sizeof(fingerprint)));
    // std::cout << "[WorkerBuffer] registerDuplicateChunk hash_fp:" << chunkname << std::endl;
    std::string lock_key = "lock:dupregister:" + chunkname;
    std::string key = "dupregister:" + chunkname;
    // lock
    while(1) {
        redisContext* lock_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* lock_reply = (redisReply*)redisCommand(lock_ctx, "SETNX %s 1", lock_key.c_str());
        assert(lock_reply!=NULL);
        assert(lock_reply->type == REDIS_REPLY_INTEGER);
        if(lock_reply->integer == 1) {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            break;
        } else {
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            sleep(1);
        }
    }

    // set or append
    // std::cout << "[WorkerBuffer] lock dupregister successfully" << std::endl;
    redisContext* get_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* get_reply = (redisReply*)redisCommand(get_ctx, "GET %s", key.c_str());
    assert(get_reply!=NULL);
    if(get_reply->type == REDIS_REPLY_NIL) {
        // std::cout << "[WorkerBuffer] registerDuplicateChunk set" << std::endl;
        freeReplyObject(get_reply);
        redisFree(get_ctx);
        std::string chunk_info = filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        // std::cout << "[WorkerBuffer] registerDuplicateChunk chunkinfo:" << chunk_info << std::endl;
        redisContext* set_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* set_reply = (redisReply*)redisCommand(set_ctx, "SET %s %b",key.c_str(), chunk_info.c_str(), chunk_info.size());
        assert(set_reply!=NULL);
        assert(set_reply->type == REDIS_REPLY_STATUS);
        assert(std::string(set_reply->str) == "OK");
        // std::cout << "[WorkerBuffer] registerDuplicateChunk set done" << std::endl;
        freeReplyObject(set_reply);
        redisFree(set_ctx);
    } else if(get_reply->type == REDIS_REPLY_STRING) {
        // std::cout << "[WorkerBuffer] registerDuplicateChunk append" << std::endl;
        freeReplyObject(get_reply);
        redisFree(get_ctx);
        std::string chunk_info = ":" + filename + ":" + poolname + ":" + to_string(pktid) + ":" + to_string(chunkid);
        redisContext* append_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
        redisReply* append_reply = (redisReply*)redisCommand(append_ctx, "APPEND %s %b",key.c_str(), chunk_info.c_str(), chunk_info.size());
        assert(append_reply!=NULL);
        freeReplyObject(append_reply);
        redisFree(append_ctx);
    } else {
        assert(false && "GET error");
    }
    registerFilesInNode(filename,poolname,node_id);
    // std::cout << "[WorkerBuffer] registerDuplicate set/append done" << std::endl;
    
    
    // unlock
    redisContext* unlock_ctx = RedisUtil::createContext(_conf->_agentsIPs[node_id]);
    redisReply* unlock_reply = (redisReply*)redisCommand(unlock_ctx, "DEL %s", lock_key.c_str());
    assert(unlock_reply!=NULL);
    assert(unlock_reply->type == REDIS_REPLY_INTEGER);
    assert(unlock_reply->integer == 1);
    freeReplyObject(unlock_reply);
    redisFree(unlock_ctx);
}

void WorkerBuffer::registerFilesInNode(string filename,string poolname,int nodeid){
    string key="files";
    string name = filename + ":" + poolname; 
    redisContext* sendCtx = RedisUtil::createContext(_conf->_agentsIPs[nodeid]);   
    redisReply* rReply = (redisReply*)redisCommand(sendCtx, "SADD %s %b", key.c_str(), name.c_str(), name.size());
    freeReplyObject(rReply);
    redisFree(sendCtx);
}
