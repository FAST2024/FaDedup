#ifndef _CEPH_FILE_HH_
#define _CEPH_FILE_HH_

#include "../inc/include.hh"
#include "BaseFile.hh"

class CephFile: public BaseFile {
public: 
    // fstream* _file;
    string _poolname;

    CephFile(string filename, string poolname) {
        _filename = filename;
        // _file = file;
        _poolname = poolname;
    }

    ~CephFile() {
        // if (_file) {
        //     _file->close();
        //     delete _file;
        // }
    }
};

#endif