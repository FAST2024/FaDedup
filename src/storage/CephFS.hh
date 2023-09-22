#ifndef _CEPH_FS_HH_
#define _CEPH_FS_HH_

#include "../inc/include.hh"
#include "BaseFile.hh"
#include "BaseFS.hh"
#include "../common/Config.hh"
#include "CephFile.hh"

using namespace std;

class CephFS: public BaseFS {
private:
    librados::Rados _rados;
    unordered_map<string, librados::IoCtx*> _ioctx_exist;
public:
    CephFS(Config* conf);
    ~CephFS();
    int init();
    CephFile* openFile(string filename, string poolname, string mode);
    int writeFile(BaseFile* file, char* buffer, int len);
    int pWriteFile(BaseFile* file, int offset, char* buf, int len);
    // int flushFile(BaseFile* file);
    int closeFile(BaseFile* file);
    int readFile(BaseFile* file, char* buffer, int len, int offset);
    int pReadFile(BaseFile* file, int offset, char* buffer, int len);
    int getFileSize(BaseFile* file, uint64_t* psize, struct timespec* pts);
    int removeFile(BaseFile* file);
    int writeFileToImage(int con_id, string poolname, int len, char* buffer) { assert(false && "should not be called");};
    int readFileInImage(int con_id, string poolname, int len, char* buffer) { assert(false && "should not be called");};
    int createNewImage(int image_id) { assert(false && "should not be called");};
    int writeChunkToImage(int con_id, int con_off, string poolname, int len, char* buffer) { assert(false && "should not be called");};
};

#endif