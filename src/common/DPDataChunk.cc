#include "DPDataChunk.hh"

DPDataChunk::DPDataChunk() {
    _data = nullptr;
    _len = 0;
    _last = false;
    _dup = false;
}

DPDataChunk::DPDataChunk(char* data, int len) {
    _data = new char[len];
    memcpy(_data, data, len);
    _len = len;
    _last = false;
    _dup = false;
}

DPDataChunk::DPDataChunk(int len) {
    _len = len;
    _data = new char[_len];
    _last = false;
    _dup = false;
}

DPDataChunk::DPDataChunk(int len, int id) {
    _len = len;
    _data = new char[_len];
    _last = false;
    _id = id;
    _dup = false;
}

DPDataChunk::DPDataChunk(int len, int id, char* data, char* fp) {
    _len = len;
    _id = id;
    _data = new char[_len];
    memcpy(_data, data, len);
    memcpy(_fp, fp, sizeof(fingerprint));
    _last = false;
    _dup = false;
}

DPDataChunk::DPDataChunk(int len, int id, char* data) {
    _len = len;
    _id = id;
    _data = new char[_len];
    memcpy(_data, data, len);
    _last = false;
    _dup = false;
}
DPDataChunk::~DPDataChunk() {
    if(_data)
        delete _data;
}

int DPDataChunk::getDatalen() {
    return _len;
}

char* DPDataChunk::getData(){
    return _data;
}

char* DPDataChunk::getFp(){
    return _fp;
}

void DPDataChunk::setData(char* data, int len) {
    _len = len;
    if (_data)
        memcpy(_data, data, len);
    else {
        if (_len > 0) {
            _data = new char[_len];
            memcpy(_data, data, len);
        }
        else
            cout << "[ERROR] data len is unexpected zero" << endl;
    }
}

void DPDataChunk::setLast(bool last) {
    _last = last;
}

int DPDataChunk::getId() {
    return _id;
}

bool DPDataChunk::isLast() {
    return _last;
}

void DPDataChunk::setDup(bool dup) {
    _dup = dup;
}

bool DPDataChunk::getDup() {
    return _dup;
}

void DPDataChunk::setConId(int conid) {
    _conid = conid;
}

void DPDataChunk::setPktId(int pktid) {
    _pktid = pktid;
}

int DPDataChunk::getConId() {
    return _conid;
}

int DPDataChunk::getPktId() {
    return _pktid;
}

void DPDataChunk::setConOff(int off) {
    _conoff = off;
}

int DPDataChunk::getConOff() {
    return _conoff;
}