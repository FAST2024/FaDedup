#ifndef _BASE_FILE_HH_
#define _BASE_FILE_HH_

#include "../inc/include.hh"

class BaseFile
{ 
public:
    std::string _filename;
    std::string _poolname;

    BaseFile() {}
    BaseFile(std::string filename, std::string poolname): _filename(filename), _poolname(poolname) {}
    ~BaseFile() {}
};


#endif