#ifndef _CEPH_RBD_FS_HH_
#define _CEPH_RBD_FS_HH_

#include "../inc/include.hh"
#include "BaseFile.hh"
#include "BaseFS.hh"
#include "../common/Config.hh"
#include "CephFile.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class CephRBDFS: public BaseFS {
private:
    librados::Rados _rados;
    unordered_map<string, librados::IoCtx*> _ioctx_exist;
    std::mutex _ioctx_mutex;
    uint64_t _MAX_CONTAINER_SIZE;
    uint64_t _MAX_IMAGE_SIZE;
    int _cur_image_id = -1;                       // image id store layer is writing to 
    librbd::RBD _rbd;
    librbd::Image _cur_image;                // image store layer is writing to
public:
    CephRBDFS(Config* conf);
    ~CephRBDFS();
    int init();
    int writeFileToImage(int con_id, string poolname, int len, char* buffer);
    int readFileInImage(int con_id, string poolname, int len, char* buffer);
    int createNewImage(int con_id);
    CephFile* openFile(string filename, string poolname, string mode) { assert(false && "should not be called"); };
    int writeFile(BaseFile* file, char* buffer, int len) { assert(false && "should not be called"); };
    int closeFile(BaseFile* file) { assert(false && "should not be called"); };
    int readFile(BaseFile* file, char* buffer, int len, int offset) { assert(false && "should not be called"); };
    int pWriteFile(BaseFile* file, int offset, char* buf, int len) { assert(false && "should not be called"); }
    int pReadFile(BaseFile* file, int offset, char* buffer, int len) { assert(false && "should not be called"); }
    int getFileSize(BaseFile* file, uint64_t* psize, struct timespec* pts) { assert(false && "should not be called"); }
    int removeFile(BaseFile* file) { assert(false && "should not be called"); }
    int writeChunkToImage(int con_id, int con_off, string poolname, int len, char* buffer);
};

#endif
