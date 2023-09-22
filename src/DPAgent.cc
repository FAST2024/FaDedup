#include "common/Config.hh"
#include "common/Worker.hh"
#include "common/StoreLayerWorker.hh"
#include "inc/include.hh"

using namespace std;

int main(int argc, char** argv) {
    string configPath = "../conf/sys.conf";
    Config* conf = new Config(configPath);
    StoreLayer* store_handler = new StoreLayer(conf);
    WorkerBuffer* worker_buffer = new WorkerBuffer(conf);
    Worker** workers = (Worker**)calloc(conf->_agentNum, sizeof(Worker*));
    StoreLayerWorker** store_layer_workers = (StoreLayerWorker**)calloc(conf->_agentNum, sizeof(StoreLayerWorker*));
    thread worker_thrds[conf->_agWorkerThreadNum];
    thread store_layer_worker_thrds[conf->_agWorkerThreadNum];
    for(int i = 0; i < conf->_agWorkerThreadNum; i++) {
        workers[i] = new Worker(conf, store_handler, worker_buffer);
        worker_thrds[i] = thread([=]{workers[i]->doProcess();});
    }
    for(int i = 0; i < conf->_agWorkerThreadNum; i++) {
        store_layer_workers[i] = new StoreLayerWorker(conf, store_handler);
        store_layer_worker_thrds[i] = thread([=]{store_layer_workers[i]->doProcess();});
    }
    cout << "DPAgents started..." << endl;

    for(int i = 0; i < conf->_agWorkerThreadNum; i++) {
        worker_thrds[i].join();
        store_layer_worker_thrds[i].join();
    }
    for(int i = 0; i < conf->_agWorkerThreadNum; i++) {
        delete workers[i];
        delete store_layer_workers[i];
    }
    free(workers);
    free(store_layer_workers);
    delete conf;
    delete store_handler;
    delete worker_buffer;

    return 0;
}