#ifndef _FILE_RECIPE_HH_
#define _FILE_RECIPE_HH_

#include "../inc/include.hh"

using namespace std;

class FileRecipe {
public:
    void insert(string filename, uint64_t pktnum) {
        _map.insert(make_pair(filename, pktnum));
    }
    uint64_t get(string filename) {
        if (_map.find(filename) != _map.end()) 
            return _map[filename];
        return 0;
    }
private:
    unordered_map<string, uint64_t> _map;
};

#endif