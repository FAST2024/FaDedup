#ifndef _DP_DATA_CHUNK_HH_
#define _DP_DATA_CHUNK_HH_

#include "../inc/include.hh"
#include "Config.hh"

class DPDataChunk {
private:
    char* _data;
    int _len;
    fingerprint _fp;
    bool _last;
    int _id;
    bool _dup;
    int _pktid;
    int _conid;
    int _conoff;
public:
    DPDataChunk();
    DPDataChunk(char* data, int len);
    DPDataChunk(int len);
    DPDataChunk(int len, int id);
    DPDataChunk(int len, int id, char* data);
    DPDataChunk(int len, int id, char* data, char* fp);
    ~DPDataChunk();
    void setData(char* data, int len);
    int getDatalen();
    char* getData();
    char* getFp();
    void setLast(bool last);
    int getId();
    bool isLast();
    void setDup(bool dup);
    bool getDup();
    void setPktId(int pktid);
    void setConId(int conid);
    int getPktId();
    int getConId();
    void setConOff(int off);
    int getConOff();
};

#endif