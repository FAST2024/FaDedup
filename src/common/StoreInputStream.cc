#include "StoreInputStream.hh"

StoreInputStream::StoreInputStream(Config* conf, string objname, string poolname, BaseFS* fs) {
    _conf = conf;
    _objname = objname;
    _poolname = poolname;
    _queue = new BlockingQueue<DPDataPacket*>();

    _fs = fs;
    _file = _fs->openFile(objname, poolname, "read");
    if (!_file) {
        cerr << "[ERROR] failed to open file!" << endl;
    }
    else {
        
    }
}   