#include "DPDataPacket.hh"

DPDataPacket::DPDataPacket() {
}

DPDataPacket::DPDataPacket(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, sizeof(int));
  _len = ntohl(tmplen);

  _raw = (char*)calloc(_len+sizeof(int), sizeof(char));
  memcpy(_raw, raw, _len+sizeof(int));

  _data = _raw+sizeof(int);
}

DPDataPacket::DPDataPacket(int len) {
  _raw = (char*)calloc(len+sizeof(int), sizeof(char));
  _data = _raw+sizeof(int);
  _len = len;

  int tmplen = htonl(len) ;
  memcpy(_raw, (char*)&tmplen, sizeof(int));
}

DPDataPacket::~DPDataPacket() {
  if (_raw) free(_raw);
}

void DPDataPacket::setRaw(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, sizeof(int));
  _len = ntohl(tmplen);

  _raw = raw;
  _data = _raw + sizeof(int);
}

int DPDataPacket::getDatalen() {
  return _len;
}

char* DPDataPacket::getData() {
  return _data;
}

char* DPDataPacket::getRaw() {
  return _raw;
}
