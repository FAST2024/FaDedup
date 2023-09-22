#include "CephRBDFS.hh"

CephRBDFS::CephRBDFS(Config* conf) {
    _conf = conf;
    _MAX_CONTAINER_SIZE =  conf->_containerSize*(sizeof(int) + conf->_chunkMaxSize);
    _MAX_IMAGE_SIZE = _MAX_CONTAINER_SIZE * 256 ;
    cout << "[CephRBDFS] CephRBDFS max container size:" << _MAX_CONTAINER_SIZE << " max image size:" << _MAX_IMAGE_SIZE << endl;
    // rados create
    init();
}

CephRBDFS::~CephRBDFS() {
    for (auto& it: _ioctx_exist) {
        delete it.second;
    }
    _rados.shutdown();
}


/**
 * 1 create a rados object and initialize it
 * 2 get config info, parse argv
 * 3 connect to cluster
*/
int CephRBDFS::init() {
    cout << "[CephRBDFS] initializing rados..." << endl;
    int ret = _rados.init("admin");
    if (ret < 0) {
        cerr << "[ERROR] failed to initialize rados! " << endl;
        return ERR_RADOS_INIT;
    }
    cout << "[CephRBDFS] initialized rados!" << endl;

    // rados parse config
    cout << "[CephRBDFS] rados reading config..." << endl;
    ret = _rados.conf_read_file(_conf->_ceph_conf_path.c_str());
    if (ret < 0) {
        std::cerr << "[ERROR] failed to parse config file " << _conf->_ceph_conf_path
                    << "! error" << ret << std::endl;
        return ERR_CONFIG_FILE;
    }
    cout << "[CephRBDFS] finish reading config!" << endl;

    // rados connect
    cout << "[CephRBDFS] rados connecting..." << endl;
    ret = _rados.connect();
    if (ret < 0) {
      cerr << "[ERROR] couldn't connect to cluster! " << ret << endl;
      return ERR_RADOS_CONN;
    }
    cout << "[CephRBDFS] rados connected!" << endl;
    cout << "[CephRBDFS] CephRBDFS created!" << endl;
    return SUCCESS;
}

/**
 * try to lock image_id, if lock success, return, else sleep 1s and try again
 * NOTE: called befor try to create a new image
*/
static void lock(int image_id, Config* conf) {
    while(true) {
        redisReply* lock_reply;
        redisContext* lock_ctx = RedisUtil::createContext(conf->_agentsIPs[0]);
        string lock_key = "lock_" + to_string(image_id);
        lock_reply = (redisReply*)redisCommand(lock_ctx, "SETNX %s %b", lock_key.c_str(), "1", 1);
        assert(lock_reply!=NULL);
        assert(lock_reply->type == REDIS_REPLY_INTEGER);
        if(lock_reply->integer == 1) {      // lock image success
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            std::cout << "[CephRBDFS] lock image_id:" << image_id << " success" << std::endl;
            break;
        } else {                            // lock image failed
            freeReplyObject(lock_reply);
            redisFree(lock_ctx);
            sleep(1);
        }
    }
}

/**
 * unlock image_id and increase image_id_created
 * NOTE: called after create a new image
*/
static void unlock(int image_id, Config* conf) {
    redisReply* unlock_reply;
    redisContext* unlock_ctx = RedisUtil::createContext(conf->_agentsIPs[0]);
    string lock_key = "lock_" + to_string(image_id);
    unlock_reply = (redisReply*)redisCommand(unlock_ctx, "DEL %s", lock_key.c_str());
    assert(unlock_reply!=NULL);
    assert(unlock_reply->type == REDIS_REPLY_INTEGER);
    assert(unlock_reply->integer == 1);
    freeReplyObject(unlock_reply);
    redisFree(unlock_ctx);
    std::cout << "[CephRBDFS] unlock image_id:" << image_id << " success" << std::endl;
}


/**
 * write container to image
 * 1 compute image need to write to and image offset
 * 2 check whether the image need to write to is current openning image of store layer
 *   if not, close current image, open the new image, if not exists, create
 * 3 write container to image
 * 4 close image
 * NOTE: image name to flush to : image_[con_id/256], image offset : id * container_size, if image not exist, lock,create,unlock
 * NOTE: image size: 256*max_container_size, container size: 4MB
*/
int CephRBDFS::writeFileToImage(int con_id, string poolname, int len, char* buffer) {
    
    librados::IoCtx* ictx;
    int ret;
    _ioctx_mutex.lock();
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        ret = _rados.pool_create(poolname.c_str());
        if (ret < 0) {
            cerr << "[ERROR] couldn't create pool! " << ret << endl;
            exit(-1);
        }
    }
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
        ictx = new librados::IoCtx;
        ret = _rados.ioctx_create(poolname.c_str(), *ictx);
        if (ret < 0) {
            cerr << "[ERROR] couldn't set up ioctx! " << ret << endl;
            exit(-1);
        }
        _ioctx_exist.insert(make_pair(poolname, ictx));
    }
    else {
        ictx = _ioctx_exist[poolname];
    }
    _ioctx_mutex.unlock();
    // 1 comput image id and image offset
    int write_image_id = con_id / 256;
    int write_image_offset = (con_id % 256) * _MAX_CONTAINER_SIZE;
    cout << "[CephRBDFS] write container:" << con_id << " to image id:" << write_image_id << " image offset:" << write_image_offset << endl;
    // 2 check whether the image need to write to is current openning image of store layer, if not, open close image and open new image, if not exists, create one
    string write_image_name = "image_" + to_string(write_image_id); 

    if(_cur_image_id != write_image_id) {   // close old image, try to create a new image
        if(_cur_image_id!=-1) {                         
            _cur_image.close();
        }
        // std::cout << "[CephRBD] try to create image:" << write_image_id << " if not exist" << std::endl;
        _cur_image_id = write_image_id;
        // lock
        // lock(write_image_id, _conf);
        // check whether the image has been created    
        // redisReply* reply;
        // redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
        // string key = write_image_name + "_created";
        // reply = (redisReply*)redisCommand(ctx, "SETNX %s 1", key.c_str());
        // assert(reply!=NULL);
        // assert(reply->type == REDIS_REPLY_INTEGER);
        // if(reply->integer == 1) {   // image not exists, create one
            // createNewImage(write_image_id);
        // } else {
            // std::cout << "[CephRBD] image:" << write_image_id << " has been created" << std::endl;
        // }
        // freeReplyObject(reply);
        // redisFree(ctx);
        // unlock
        // unlock(write_image_id, _conf);
        // open new image
        ret = _rbd.open(*ictx, _cur_image, write_image_name.c_str(), NULL);
        if( ret < 0 ) {
            std::cerr << "couldn't open the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
            exit(-1);            
        }
    }
    
    // open new image, if not exists, create a new image
    // ret = _rbd.open(*ictx, _cur_image, write_image_name.c_str(), NULL);
    // if( ret < 0 ) {
    //     std::cerr << "couldn't open the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
    //     exit(-1);            
    // }
   
    // 2 write data in container to cur_image
    char* new_buf = new char[len];
    memcpy(new_buf, buffer, len);
    ceph::bufferlist bl;
    bl.append(new_buf, len);
    ret = _cur_image.write(write_image_offset, len, bl);
    if(ret < 0) {
        std::cerr << "couldn't write to the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
        exit(-1);
    }
    // _cur_image.close();
    cout << "[CephRBDFS] write container:" << con_id << " to image id:" << write_image_id << " image offset:" << write_image_offset << " successfully" << endl;
    delete new_buf;
    return SUCCESS;
}


/**
 * 1 compute image name and read offset
 * 2 open image
 * 3 read data from image to buffer
*/
int CephRBDFS::readFileInImage(int con_id, string poolname, int len, char* buffer) {
    _ioctx_mutex.lock();
    int ret;
    librados::IoCtx* ictx;
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before readfull it!" << endl;
        exit(-1);
    }
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
        cout << "[CephRBDFS] create a new ioctx for pool:" << poolname << endl;
        ictx = new librados::IoCtx;
        ret = _rados.ioctx_create(poolname.c_str(), *ictx);
        if (ret < 0) {
            cerr << "[ERROR] couldn't set up ioctx! " << ret << endl;
            return ERR_RADOS_IOCTX_CREATE;
        }
        _ioctx_exist.insert(make_pair(poolname, ictx));
    }
    else {
        ictx = _ioctx_exist[poolname];
    }
    _ioctx_mutex.unlock();
    
    // 1 compute image name and read offset
    librbd::RBD read_rbd;
    librbd::Image read_image;
    string read_image_name = "image_" + to_string(con_id/256);
    int read_image_off = (con_id % 256) * _MAX_CONTAINER_SIZE;
    
    // 2 open image
    // cout << "[CephRBDFS] try to open container:" << con_id << " from image id:" << con_id/256 << " image offset:" << read_image_off << endl;
    assert(ictx != nullptr);
    ret = read_rbd.open(*ictx, read_image, read_image_name.c_str(), NULL);
    if (ret < 0) {
      std::cerr << "couldn't open the rbd image! error " << ret << " image name : " << read_image_name << std::endl;
      exit(-1);
    } 

    // 3 read image
    // cout << "[CephRBDFS] try to read container:" << con_id << " from image id:" << con_id/256 << " image offset:" << read_image_off << endl;
    ceph::bufferlist bl_r;
    int read;
    read = read_image.read(read_image_off, len, bl_r);
    if (read < 0) {
      std::cerr << "we couldn't read data from the image! error" << " image name : " << read_image_name <<std::endl;
      exit(-1);
    }
    assert(read==len);
    // cout << "[CephRBDFS] read container:" << con_id << " from image id:" << con_id/256 << " image offset:" << read_image_off << " successfully, copy it to buffer" << endl;
    memcpy(buffer, bl_r.c_str(), len);
    read_image.close();
    return SUCCESS;
}

/**
 * create a new image
 * lock has been held before
*/
int CephRBDFS::createNewImage(int image_id) {
    std::cout << "[CephRBDFS] creating a new image " << image_id << std::endl;
    string poolname = "test";
    librados::IoCtx* ictx;
    int ret;

    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        ret = _rados.pool_create(poolname.c_str());
        if (ret < 0) {
            cerr << "[ERROR] couldn't create pool! " << ret << endl;
            exit(-1);
        }
    }
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
        ictx = new librados::IoCtx;
        ret = _rados.ioctx_create(poolname.c_str(), *ictx);
        if (ret < 0) {
            cerr << "[ERROR] couldn't set up ioctx! " << ret << endl;
            exit(-1);
        }
        _ioctx_exist.insert(make_pair(poolname, ictx));
    }
    else {
        ictx = _ioctx_exist[poolname];
    }

    int order = 0;
    string new_image_name = "image_" + to_string(image_id);
    ret = _rbd.create(*ictx, new_image_name.c_str(), _MAX_IMAGE_SIZE, &order);
    if (ret < 0) {
        std::cerr << "couldn't create an rbd image! error " << ret << " image name : " << new_image_name << std::endl;
        exit(-1);
    } 
    std::cout << "[CephRBDFS] created a new image " << new_image_name << std::endl;
    redisReply* reply;
    redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
    string key = new_image_name + "_created";
    reply = (redisReply*)redisCommand(ctx, "SETNX %s 1", key.c_str());
    assert(reply!=NULL);
    assert(reply->type == REDIS_REPLY_INTEGER);
    assert(reply->integer == 1);   
    freeReplyObject(reply);
    redisFree(ctx);
    return SUCCESS;
}

/**
 * write a chunk to image
 * con_id: container id of the chunk 
 * con_off: offset of the chunk in container
*/
 int CephRBDFS::writeChunkToImage(int con_id, int con_off, string poolname, int len, char* buffer) {
    librados::IoCtx* ictx;
    int ret;

    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        ret = _rados.pool_create(poolname.c_str());
        if (ret < 0) {
            cerr << "[ERROR] couldn't create pool! " << ret << endl;
            exit(-1);
        }
    }
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
        ictx = new librados::IoCtx;
        ret = _rados.ioctx_create(poolname.c_str(), *ictx);
        if (ret < 0) {
            cerr << "[ERROR] couldn't set up ioctx! " << ret << endl;
            exit(-1);
        }
        _ioctx_exist.insert(make_pair(poolname, ictx));
    }
    else {
        ictx = _ioctx_exist[poolname];
    }

    // 1 comput image id and image offset
    int write_image_id = con_id / 256;
    int write_image_offset = (con_id % 256) * _MAX_CONTAINER_SIZE;
    cout << "[CephRBDFS] writeChunkToImage: container:" << con_id << " con_off:" << con_off << " to image id:" << write_image_id << " image offset:" << write_image_offset << endl;
    // 2 check whether the image need to write to is current openning image of store layer, if not, open close image and open new image, if not exists, create one
    string write_image_name = "image_" + to_string(write_image_id); 

    if(_cur_image_id != write_image_id) {   // close old image, try to create a new image
        if(_cur_image_id!=-1) {                         
            _cur_image.close();
        }
        std::cout << "[CephRBD] try to create image:" << write_image_id << " if not exist" << std::endl;
        _cur_image_id = write_image_id;
        // lock
        lock(write_image_id, _conf);
        // check whether the image has been created    
        redisReply* reply;
        redisContext* ctx = RedisUtil::createContext(_conf->_agentsIPs[0]);
        string key = write_image_name + "_created";
        reply = (redisReply*)redisCommand(ctx, "SETNX %s 1", key.c_str());
        assert(reply!=NULL);
        assert(reply->type == REDIS_REPLY_INTEGER);
        if(reply->integer == 1) {   // image not exists, create one
            createNewImage(write_image_id);
        } else {
            std::cout << "[CephRBD] image:" << write_image_id << " has been created" << std::endl;
        }
        freeReplyObject(reply);
        redisFree(ctx);
        // unlock
        unlock(write_image_id, _conf);
        // open new image, if not exists, create a new image
        ret = _rbd.open(*ictx, _cur_image, write_image_name.c_str(), NULL);
        if( ret < 0 ) {
            std::cerr << "couldn't open the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
            exit(-1);            
        }
    }
    
    // open new image, if not exists, create a new image
    // ret = _rbd.open(*ictx, _cur_image, write_image_name.c_str(), NULL);
    // if( ret < 0 ) {
    //     std::cerr << "couldn't open the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
    //     exit(-1);            
    // }
   
    // 2 write data in container to cur_image
    char* new_buf = new char[len];
    memcpy(new_buf, buffer, len);
    ceph::bufferlist bl;
    bl.append(new_buf, len);
    ret = _cur_image.write(write_image_offset + con_off, len, bl);
    if(ret < 0) {
        std::cerr << "couldn't write to the rbd image! error " << ret << " image name : " << write_image_name << std::endl;
        exit(-1);
    }
    // _cur_image.close();
    cout << "[CephRBDFS] writeChunkToImage: container:" << con_id << " to image id:" << write_image_id << " image offset:" << write_image_offset << " successfully" << endl;
    delete [] new_buf;
    return SUCCESS;
 }
