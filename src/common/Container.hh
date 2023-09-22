#ifndef _CONTAINER_HH_
#define _CONTAINER_HH_

#include "../inc/include.hh"

using namespace std;

class Container {
private:
    char* _raw;
    int _len;
    int _size;
    int _off;
    string _name;
    bool _last;
    int _chunkCapacity;

public:
    Container(string name, int cc, int len);
    ~Container();

    void push(void* data, int dataSize);
    void getRawToBuf(void* buf, int offset, int size);
    // void getData(void* buf, int offset);
    bool full();
    void setLast(bool last);
    bool isLast();
    string getName();
    int getOff();
    char* getRaw();
    int getLen();
    bool empty();
    
};

#endif
