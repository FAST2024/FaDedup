#ifndef _PACKET_RECIPE_HH_
#define _PACKET_RECIPE_HH_

#include "../inc/include.hh"
#include "WrappedFP.hh"

using namespace std;

class PacketRecipe {
public:
    PacketRecipe() {}
    ~PacketRecipe() {
        for(auto it = _map.begin(); it != _map.end(); it++) {
            if(it->second) {
                for (int i = 0; i < it->second->size(); i++) {
                    if (it->second->at(i))
                        delete it->second->at(i);
                }
                delete it->second;
            }
        }
    }
    void append(string filename, WrappedFP* value) {
        if(_map.find(filename) == _map.end()) {
            _map[filename] = new vector<WrappedFP*>();
        }
        _map[filename]->push_back(value);
    }
    vector<WrappedFP*>* get(string filename) {
        if (_map.find(filename) == _map.end()) {
            return nullptr;
        }
        return _map[filename];
    }
    size_t getMapSize() {
        return _map.size();
    }
    size_t getRecipeSize(string key) {
        if(_map.find(key) == _map.end())
            return 0;
        return _map[key]->size();
    }
    vector<WrappedFP*>* getRecipe(string filename) {
        return _map[filename];
    }
private:
    unordered_map<string, vector<WrappedFP*>*> _map;
};

#endif