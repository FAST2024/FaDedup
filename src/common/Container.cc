#include "Container.hh"

Container::Container(string name, int cc, int len):_name(name), _chunkCapacity(cc), _len(len) {
    _raw = new char[_len];
    memset(_raw, 0, _len);
    _off = 0;
    _size = 0;
    _last = false;
}

Container::~Container() {
    if (_raw) delete _raw;
}

void Container::push(void* data, int dataSize) {
    // if (_off + fpSize > _len) {
    //     cout << "[ERROR] container out of bound when pushing fp!" << endl;
    //     return;
    // }
    // memcpy(_raw + _off, fp, fpSize);
    // _off += fpSize;

    int tmplen = htonl(dataSize);
    if (_off + sizeof(tmplen) > _len) {
        cout << "[ERROR] container out of bound when pushing datasize!" << endl;
        return;
    }
    memcpy(_raw + _off, (void*)&tmplen, sizeof(tmplen));
    _off += sizeof(tmplen);
    
    if (_off + dataSize > _len) {
        cout << "[ERROR] container out of bound when pushing data!" << endl;
        return;
    }
    memcpy(_raw + _off, data, dataSize);
    _off += dataSize;

    _size++;
}

void Container::getRawToBuf(void* buf, int offset, int size) {
    memcpy(buf, _raw + offset, size);
}

// void Container::getData(void* buf, int offset) {
//     int tmplen;
//     memcpy((void*)&tmplen, _raw + offset + sizeof(fingerprint), sizeof(int));
//     int len = ntohl(tmplen);
//     memcpy(buf, _raw + offset + sizeof(fingerprint) + sizeof(int), len);
// }

bool Container::full() {
    return _size == _chunkCapacity;
}

void Container::setLast(bool last) {
    _last = last;
}

bool Container::isLast() {
    return _last;
}

string Container::getName() {
    return _name;
}

int Container::getOff() {
    return _off;
}

char* Container::getRaw() {
    return _raw;
}

int Container::getLen() {
    return _len;
}

bool Container::empty() {
    return _size == 0;
}
