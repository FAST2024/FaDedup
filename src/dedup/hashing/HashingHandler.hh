#ifndef _HASHING_HANDLER_HH_
#define _HASHING_HANDLER_HH_

#include "../../inc/include.hh"
#include "../../common/Config.hh"
#include "hashing.hh"

class HashingHandler {
public:
    Config* _conf;
    void (*hashing)(char *data, int32_t size, fingerprint fp);

    HashingHandler(Config* conf);
    ~HashingHandler();

};

#endif