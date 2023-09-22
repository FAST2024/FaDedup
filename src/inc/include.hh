#ifndef _COMMON_HH_
#define _COMMON_HH_

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <fstream>
#include <thread>
#include <set>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <memory>
#include <algorithm>
#include <sstream>
#include <random>
#include <utility>

#include <cassert>
#include <cstring>
#include <ctime>

#include <arpa/inet.h>
#include <hiredis/hiredis.h>
#include <rados/librados.hpp>
#include <rbd/librbd.hpp>
#include <openssl/sha.h>

#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <memory.h>
#include <assert.h>

#define MAX_COMMAND_LEN 4096
#define DEFAULT_PKT_BASE 4194304

enum {
    SUCCESS = 0, ERR_RADOS_INIT, ERR_RADOS_CONF, ERR_RADOS_CONN, ERR_RADOS_POOL_CREATE,
    ERR_RADOS_IOCTX_CREATE, ERR_RADOS_OPEN_FILE, ERR_RADOS_WRITE_FILE, ERR_RADOS_READ_FILE,
    ERR_RADOS_FLUSH_FILE, ERR_RADOS_CLOSE_FILE, ERR_CONFIG_FILE, ERR_RADOS_STAT_FILE,
    ERR_RADOS_POOL_NOT_EXIST
};

typedef char fingerprint[20];

typedef struct chunk {
    unsigned char* data;
    fingerprint fp;
}chunk;

typedef struct IPAddress {
    std::string ip;
    uint16_t port;
}IPAddress;

enum class WriteMode {
    Normal,
    NormalMulti,
    SelectiveDedup,
    SelectiveDedupMulti,
    BatchPush,
    BatchPushMulti,
    SuperChunk,
    SuperChunkMulti
};

enum class UpdateMode {
    Normal,
    Remap
};

#endif