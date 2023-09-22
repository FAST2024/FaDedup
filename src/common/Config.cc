#include "Config.hh"

Config::Config(string path) {
    _sys_conf_path = path;
    parseConf(path);
}

Config::~Config() {

}

int Config::parseConf(string path) {
    fstream fs = fstream(path, ios::in);
    if (!fs.is_open()) {
        cerr << "cannot open the config file! " << endl;
        return ERR_CONFIG_FILE;
    }
    string line;
    string key;
    string value;
    while(getline(fs, line)) {
        if(line[0] == '#')
            continue;
        if (line.size() == 0)
            continue;
        auto pos = line.find(' ');
        key = line.substr(0, pos);
        if (key == "ceph_conf_path") 
            _ceph_conf_path = line.substr(pos+1, line.size());
        else if (key == "local_ip") {
            // IPAddress addr;
            // int ipPos = line.find(line.substr(pos+1, line.size()).find(":"));
            _localAddr.ip = line.substr(pos+1, line.size());
            // stringstream ss(line.substr(ipPos+1, line.size()));
            // ss >> addr.port;
            _localIp = inet_addr(line.substr(pos+1, line.size()).c_str());
        }   
        else if (key == "agents_ip") {
            stringstream ss(line.substr(pos+1, line.size()));
            string agent_ip;
            IPAddress agentAddr;
            while(ss >> agent_ip) {
                // int ipPos = agent_ip.find(":");
                agentAddr.ip = agent_ip;
                // stringstream sss(agent_ip.substr(ipPos+1, agent_ip.size()));
                // sss >> agentAddr.port;
                _agentsAddrs.push_back(agentAddr);
                _agentsIPs.push_back(inet_addr(agent_ip.c_str()));
            }
            // _agentNum = _agentsAddrs.size();
            _agentNum = _agentsIPs.size();
        }
        else if (key == "port") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> port;
            _localAddr.port = port;
            for(int i = 0; i < _agentNum; i++) {
                _agentsAddrs[i].port = port;
            }
        }    
        else if (key == "packet_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _pktSize;
        }
        else if (key == "agent_worker_number") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _agWorkerThreadNum;
        }
        else if (key == "chunk_avg_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _chunkAvgSize;
        }
        else if (key == "chunk_min_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _chunkMinSize;
        }
        else if (key == "chunk_max_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _chunkMaxSize;
        }
        else if (key == "chunk_algorithm") {
            _chunkAlg = line.substr(pos+1, line.size());
        }
        else if (key == "hash_algorithm") {
            _hashAlg = line.substr(pos+1, line.size());
        }
        else if (key == "cache_algorithm") {
            _cacheAlg = line.substr(pos+1, line.size());
        }
        else if (key == "cache_transform_time") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _cacheTransform;
        } 
        else if (key == "cache_capacity") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _cacheCapacity;
        }
        else if (key == "read_thread_num") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _readThrdNum;
        }
        else if (key == "container_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _containerSize;
        }
        else if (key == "dedup_ratio") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _dedupRatio;
        }
        else if (key == "locality_aware") {
            string res = line.substr(pos+1, line.size());
            if (res == "true") _localityAware = true;
            else _localityAware = false;
        }
        else if (key == "super_chunk_size") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _super_chunk_size;
        }
        else if (key == "redirect_degree") {
            stringstream ss(line.substr(pos+1, line.size()));
            ss >> _redirect_degree;
        } 
        else if ( key == "write_mode") {
            stringstream ss(line.substr(pos+1, line.size()));
            string mode;
            ss >> mode;
            if (mode == "Normal") {
                _write_mode = WriteMode::Normal;
            } else if (mode == "NormalMulti") {
                _write_mode = WriteMode::NormalMulti;
            } else if (mode == "SelectiveDedup") {
                _write_mode = WriteMode::SelectiveDedup;
            } else if (mode == "SelectiveDedupMulti") {
                _write_mode = WriteMode::SelectiveDedupMulti;
            } else if (mode == "BatchPush") {
                _write_mode = WriteMode::BatchPush;
            } else if (mode == "BatchPushMulti") {
                _write_mode = WriteMode::BatchPushMulti;
            } else if (mode == "SuperChunk") {
                _write_mode = WriteMode::SuperChunk;
            } else if (mode == "SuperChunkMulti") {
                _write_mode = WriteMode::SuperChunkMulti;
            } else {
                std::cout << "unidentified write mode, support mode: Normal, NormalMulti, SelectiveDedup, SelectiveDedupMulti, BatchPush, BatchPushMulti, SuperChunk, SuperChunkMulti" << std::endl;
                exit(-1);
            }
        } 
        else if (key == "update_mode") {
            stringstream ss(line.substr(pos+1, line.size()));
            string mode;
            ss >> mode;
            if (mode == "Normal") {
                _update_mode = UpdateMode::Normal;
            } else if (mode == "Remap") {
                _update_mode = UpdateMode::Remap;
            } else {
                std::cout << "unidentified update mode, support mode: Normal, Remap" << std::endl;
                exit(-1);
            }
        }
    }
 
    fs.close();
    return SUCCESS;
}