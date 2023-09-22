#ifndef _STORE_LAYER_HH_
#define _STORE_LAYER_HH_

#include "../inc/include.hh"
#include "DPDataChunk.hh"
#include "Config.hh"
#include "../storage/CephFS.hh"
#include "../storage/BaseFS.hh"
#include "../storage/FSUtil.hh"
#include "../util/RedisUtil.hh"
#include "mutex"


class StoreLayer {
private:
    Config* _conf;
    BaseFS* _fs;
    BaseFS* _rbd_fs;
    redisContext* _processCtx;
    redisContext* _localCtx;
    
    size_t _cur_container_size;
    size_t _max_container_size;     // max num of chunk
    size_t _max_len;                // max bytes of buffer, includding chunk size
    int _cur_container_id;
    int _cur_container_off;
    char* _buffer;
    std::mutex _buffer_mutex;
    string _poolname;

    void flush();
    void flushWithRBD();
    void updateContainerId();
    void initContainerId();

public:
    StoreLayer(Config* conf);
    ~StoreLayer();
    void push(DPDataChunk* chunk, int* con_id, int* con_off, string filename);
    void lock(int image_id);
    void unlock(int image_id);
    void lock();
    void unlock();
    void writeChunk(int con_id, int con_off, string poolname, char* data, int len) {
        _buffer_mutex.lock();
        _rbd_fs->writeChunkToImage(con_id, con_off, poolname, len, data);
        _buffer_mutex.unlock();
    };
    // called by store layer worker when do readPacket
    char* readContainerTask(int con_id, string poolname, int clen) {
        char* buf = (char*)calloc(clen, sizeof(char));
        assert(buf!=nullptr);
        _rbd_fs->readFileInImage(con_id, poolname, clen, buf);
        return buf;
    }

    // called when simple update, flush data to ceph and update container id and container off
    void pushAndFlushToCephImmediately(DPDataChunk* chunk, int* con_id, int* con_off, string filename) {
        _buffer_mutex.lock();
        if(_cur_container_size == _max_container_size) {
             _cur_container_id++;
            assert(_cur_container_id % 256 != 0);
            _cur_container_size = 0;
            _cur_container_off = 0;
        }
        *con_id = _cur_container_id;
        *con_off = _cur_container_off; 
        // chunk len
        assert(chunk->getDatalen()!=0);
        int tmplen = htonl(chunk->getDatalen());
        // chunk content
        char* buffer = new char[chunk->getDatalen()+sizeof(tmplen)];

        if(_cur_container_off + sizeof(tmplen) > _max_len){
            cout << "[ERROR] container out of bound when pushing datasize!" << endl;
            exit(-1);
        }
        memcpy(buffer, (void*)&tmplen, sizeof(tmplen));
        _cur_container_off+=sizeof(tmplen);

        if(_cur_container_off + chunk->getDatalen() > _max_len){
            cout << "[ERROR] container out of bound when pushing data!" << endl;
            exit(-1);
        }
        memcpy(buffer+sizeof(tmplen), chunk->getData(), chunk->getDatalen());
        _cur_container_off+=chunk->getDatalen();
        _cur_container_size ++;
        _rbd_fs->writeChunkToImage(*con_id, *con_off, _poolname, chunk->getDatalen()+sizeof(tmplen), buffer);
        delete buffer;
        _buffer_mutex.unlock();
        
    }
};



#endif
