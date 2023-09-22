#include "CephFS.hh"

CephFS::CephFS(Config* conf) {
    _conf = conf;

    // rados create
    init();
}

CephFS::~CephFS() {
    for (auto& it: _ioctx_exist) {
        delete it.second;
    }
    _rados.shutdown();
}

int CephFS::init() {
    cout << "[CephFS] initializing rados..." << endl;
    int ret = _rados.init("admin");
    if (ret < 0) {
        cerr << "[ERROR] failed to initialize rados! " << endl;
        return ERR_RADOS_INIT;
    }
    cout << "[CephFS] initialized rados!" << endl;

    // rados parse config
    cout << "[CephFS] rados reading config..." << endl;
    ret = _rados.conf_read_file(_conf->_ceph_conf_path.c_str());
    if (ret < 0) {
        std::cerr << "[ERROR] failed to parse config file " << _conf->_ceph_conf_path
                    << "! error" << ret << std::endl;
        return ERR_CONFIG_FILE;
    }
    cout << "[CephFS] finish reading config!" << endl;

    // rados connect
    cout << "[CephFS] rados connecting..." << endl;
    ret = _rados.connect();
    if (ret < 0) {
      cerr << "[ERROR] couldn't connect to cluster! " << ret << endl;
      return ERR_RADOS_CONN;
    }
    cout << "[CephFS] rados connected!" << endl;
    cout << "[CephFS] CephFS created!" << endl;
    return SUCCESS;
}

CephFile* CephFS::openFile(string filename, string poolname, string mode) {
    fstream* fp = new fstream;
    if (mode == "read" || mode == "r") {
        fp->open(filename, ios::in);
    }
    else if (mode == "write" || mode == "w") {
        fp->open(filename, ios::out);
    }
    else if (mode == "rw" || mode == "wr") {
        fp->open(filename, ios::in | ios::out);
    }
    else if (mode == "binary" || mode == "b") {
        fp->open(filename, ios::binary);
    }
    else if (mode == "truncate" || mode == "trunc") {
        fp->open(filename, ios::trunc);
    }

    if(!fp->is_open()) {
        cerr << "[ERROR] file cannot open! " << filename << endl;
        return nullptr;
    }

    CephFile* file = new CephFile(filename, poolname);
    return file;
}

int CephFS::writeFile(BaseFile* file, char* buffer, int len) {
    string filename = file->_filename;
    string poolname = file->_poolname;
    
    librados::IoCtx* ictx;
    int ret;
    // if pool not exists, create it
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        ret = _rados.pool_create(poolname.c_str());
        if (ret < 0) {
            cerr << "[ERROR] couldn't create pool! " << ret << endl;
            return ERR_RADOS_POOL_CREATE;
        }
    }
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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
    librados::bufferlist bl;
    // librados::AioCompletion *write_completion = librados::Rados::aio_create_completion();
    char* new_buf = new char[len];
    memcpy(new_buf, buffer, len);
    bl.append(new_buf, len);
    ret = (*ictx).write_full(filename, bl);
    if (ret < 0) {
      std::cerr << "[ERROR] couldn't write object! " << ret << " chunkname is " << filename << std::endl;
      return ERR_RADOS_WRITE_FILE;
    }
    delete new_buf;
    // write_completion->wait_for_complete();
    // ret = write_completion->get_return_value();
    // cout << "[cephfs] ret: " << ret << endl;
    if (ret < 0) {
      std::cerr << "couldn't write object! error " << ret << " chunkname is " << filename << std::endl;
    //   write_completion->release();
      return -1;
    }
    // write_completion->release();
    return SUCCESS;
}

int CephFS::pWriteFile(BaseFile* file, int offset, char* buf, int len) {
    librados::bufferlist bl;
    string filename = file->_filename;
    string poolname = file->_poolname;
    int ret;
    // if pool not exists, return error
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before writesame it!" << endl;
        return ERR_RADOS_POOL_NOT_EXIST;
    }
    librados::IoCtx* ictx;
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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

    char* new_buf = new char[len];
    bl.append(new_buf);
    memcpy(new_buf, buf, len);
    ret = (*ictx).writesame(filename, bl, len, offset);
    if (ret < 0) {
      std::cerr << "[ERROR] couldn't write object! error " << ret << std::endl;
      return ERR_RADOS_WRITE_FILE;
    }
    return SUCCESS;
}

int CephFS::closeFile(BaseFile* file) {
    // static_cast<CephFile*>(file)->_file.close();
    return SUCCESS;
}

int CephFS::readFile(BaseFile* file, char* buffer, int len, int offset) {
    string filename = file->_filename;
    string poolname = file->_poolname;
    librados::bufferlist read_buf;
    int ret;
    // if pool not exists, return error
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before readfull it!" << endl;
        return ERR_RADOS_POOL_NOT_EXIST;
    }
    librados::IoCtx* ictx;
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
    int hasread = 0;
    // while(hasread < len) {
    ret = (*ictx).aio_read(filename, read_completion, &read_buf, len, offset);
    // cout << "ret: " << ret << endl;
    // ret = (*ictx).read(filename, read_buf, len-hasread, hasread);
    if (ret < 0) {
        std::cerr << "[ERROR] couldn't start read object! error " << ret << std::endl;
        read_completion->release();
        return -1;
    }
    read_completion->wait_for_complete();
    ret = read_completion->get_return_value();
    // cout << "ret: " << ret << endl;
    // if (ret == 0) {
        // assert(hasread == len);
    // }
    if (ret < 0) {
        cout << "chunkname: " << filename << " poolname: " << poolname << endl;
        std::cerr << "couldn't read object! error " << ret << std::endl;
        read_completion->release();
        return -1;
    }
    // std::string read_string;
    // read_buf.begin().copy(ret, read_string);
    read_buf.begin().copy(ret, buffer); 
    read_completion->release();

    // hasread += ret;
    // }
    
    
    return ret;
}

int CephFS::pReadFile(BaseFile* file, int offset, char* buffer, int len) {
    string filename = file->_filename;
    string poolname = file->_poolname;
    librados::bufferlist read_buf;
    int ret;
    // if pool not exists, return error
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before writesame it!" << endl;
        return ERR_RADOS_POOL_NOT_EXIST;
    }
    librados::IoCtx* ictx;
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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
    ret = (*ictx).read(filename, read_buf, len, offset);
    if (ret < 0) {
      std::cerr << "[ERROR] couldn't start read object! error " << ret << std::endl;
      return ERR_RADOS_READ_FILE;
    }
    read_buf.begin().copy(len, buffer);
    return SUCCESS;
}

int CephFS::getFileSize(BaseFile* file, uint64_t* psize, struct timespec* pts) {
    string filename = file->_filename;
    string poolname = file->_poolname;
    int ret;
    // if pool not exists, return error
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before writesame it!" << endl;
        return ERR_RADOS_POOL_NOT_EXIST;
    }
    librados::IoCtx* ictx;
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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
    ret = (*ictx).stat2(filename, psize, pts);
    if (ret < 0) {
        std::cerr << "[ERROR] couldn't start read object! error " << ret << std::endl;
      return ERR_RADOS_STAT_FILE;
    }
    return SUCCESS;
}

int CephFS::removeFile(BaseFile* file) {
    string filename = file->_filename;
    string poolname = file->_poolname;
    // if pool not exists, return error
    if (_rados.pool_lookup(poolname.c_str()) == -ENOENT) {
        cerr << "[ERROR] pool not exists, please writefull it before writesame it!" << endl;
        return ERR_RADOS_POOL_NOT_EXIST;
    }
    int ret;
    librados::IoCtx* ictx;
    if (_ioctx_exist.find(poolname) == _ioctx_exist.end()) {
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
    ret = (*ictx).remove(filename);
    if (ret < 0) {
        std::cerr << "[ERROR] couldn't remove object! error " << ret << std::endl;
      return ERR_RADOS_STAT_FILE;
    }
    return SUCCESS;
}