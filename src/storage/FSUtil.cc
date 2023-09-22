#include "FSUtil.hh"

BaseFS* FSUtil::createFS(Config* conf) {
    BaseFS* ret;
    ret = new CephFS(conf);
    return ret;
}

void FSUtil::deleteFS(BaseFS* fshandler) {
    if(fshandler)
        delete fshandler;
}

/**
 * create a ceph rbd fs
 * called when creating store layer
*/
BaseFS* FSUtil::createRBDFS(Config* conf) {
    BaseFS* ret;
    ret = new CephRBDFS(conf);
    return ret;
}