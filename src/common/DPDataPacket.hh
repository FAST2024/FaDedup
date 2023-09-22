#ifndef _DP_DATA_PACKET_HH_
#define _DP_DATA_PACKET_HH_

#include "../inc/include.hh"
#include "Config.hh"

class DPDataPacket {
private:
    int _len;
    char* _raw;
    char* _data;

public:
    DPDataPacket();
    DPDataPacket(char* raw);
    DPDataPacket(int len);
    ~DPDataPacket();
    void setRaw(char* raw);

    int getDatalen();
    char* getData();
    char* getRaw();

};

#endif