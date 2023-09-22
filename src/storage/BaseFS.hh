#ifndef _BASE_FS_HH_
#define _BASE_FS_HH_

#include "../inc/include.hh"
#include "../common/Config.hh"
#include "BaseFile.hh"

using namespace std;

class BaseFS {
public:
    Config* _conf;

    virtual BaseFile* openFile(string filename, string poolname, string mode) = 0;
    virtual int writeFile(BaseFile* file, char* buf, int len) = 0;
    virtual int pWriteFile(BaseFile* file, int offset, char* buf, int len) = 0;
    // virtual int flushFile(BaseFile* file) = 0;
    virtual int closeFile(BaseFile* file) = 0;
    virtual int readFile(BaseFile* file, char* buffer, int len, int offset) = 0;
    virtual int pReadFile(BaseFile* file, int offset, char* buffer, int len) = 0;
    virtual int getFileSize(BaseFile* file, uint64_t* psize, struct timespec* pts) = 0;
    virtual int removeFile(BaseFile* file) = 0;
    virtual int writeFileToImage(int con_id, string poolname, int len, char* buffer)=0;
    virtual int readFileInImage(int con_id, string poolname, int len, char* buffer)=0;
    virtual int createNewImage(int image_id) = 0;
    virtual int writeChunkToImage(int con_id, int con_off, string poolname, int len, char* buffer) = 0;
};

#endif