#ifndef _ROUTER_HH_
#define _ROUTER_HH_

#include "../inc/include.hh"

class Router {
public:
    static size_t hash(const char* str, int size) {
        std::size_t hash = 0;
        for (std::size_t i = 0; i < size; ++i) {
            hash = hash * 13131131313 + static_cast<std::size_t>(str[i]);
        }
        return hash;
    }
    static int chooseNode(char* fp, int size, int nodenum) {
        size_t hash_int = hash(fp, size);
        return hash_int % nodenum;
    }
};

#endif