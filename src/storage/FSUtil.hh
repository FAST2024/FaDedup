#ifndef _FS_UTIL_HH_
#define _FS_UTIL_HH_

#include "../inc/include.hh"
#include "BaseFS.hh"
#include "CephFS.hh"
#include "CephRBDFS.hh"
#include "../common/Config.hh"

using namespace std;

class FSUtil {
public:
    static BaseFS* createFS(Config* conf);
    static void deleteFS(BaseFS*);
    static BaseFS* createRBDFS(Config* conf);
};

#endif