#include "StoreOutputStream.hh"

StoreOutputStream::StoreOutputStream(Config* conf, string objname, string poolname, BaseFS* fs) {
    _conf = conf;
    _objname = objname;
    _poolname = poolname;
    _queue = new BlockingQueue<DPDataChunk*>();
    _finish = false;
    _objsize = 0;

    // connnect to storage cluster
    _basefs = fs;
    _basefile = _basefs->openFile(_objname, _poolname, "write");
    if (!_basefile) {
        cout << "[ERROR] StoreOutputStream fails to connect to storage cluster!" << endl;
    }
}

StoreOutputStream::~StoreOutputStream() {
    if(_basefile) {
        _basefs->closeFile(_basefile);
        _basefile = nullptr;
    }
    if (_queue) {
        delete _queue;
    }
}

void StoreOutputStream::writeObjFull() {
    cout << "[StoreOutputStream] writeObj..." << endl;
    while (1) {
        DPDataChunk* curChunk = _queue->pop();
        if (curChunk == nullptr) {
            break;
        }
        _objsize += curChunk->getDatalen();
        // write to storage cluster
        _basefs->writeFile(_basefile, curChunk->getData(), curChunk->getDatalen());
        delete curChunk;
    }
    _finish = true;
}

void StoreOutputStream::enqueue(DPDataChunk* pkt) {
    _queue->push(pkt);
}

bool StoreOutputStream::getFinish() {
    return _finish;
}

BlockingQueue<DPDataChunk*>* StoreOutputStream::getQueue() {
    return _queue;
}