#include "inc/include.hh"
#include "common/Config.hh"
#include "common/DPOutputStream.hh"
#include "common/DPInputStream.hh"
#include "common/DPDeleteStream.hh"
#include "common/DPReorderStream.hh"
#include "common/UpdateStream.hh"

using namespace std;
using namespace chrono;

void usage() {
    cout << "usage: ./DPClient write filename" << endl;
    cout << "       ./DPClient read filename poolname saves" << endl;
    cout << "       ./DPClient startreorder" << endl;
    cout << "       ./DPClient update filename" << endl; 
}


void startreorder(void){
    cout << "[DPClient reorder] reorder begin" << endl;
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    DPReorderStream* reorder_stream = new DPReorderStream(conf);
    reorder_stream->reorderWorker();
    delete reorder_stream;
    delete conf;
    cout << "[DPClient reorder] reorder done" << endl;
}


void writefullWithRBD(string filename, string saveas, string poolname, string mode, int size) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(filename.c_str(), "rb");
    if(!fp) {
        cout << "[ERROR] file can not open!" << endl;
        return;
    }

    // create DPOutputStream
    DPOutputStream* outstream = new DPOutputStream(conf, saveas, poolname, mode, size);

    if(outstream->lookupFile(saveas, poolname)) {
        cout << "[ERROR] file exists, cannot writefull!" << endl;
        return;
    }

    // write file data into DPOutputStream
    int read_loop = (int)ceil(((double)size) / ((double)(conf->_pktSize)));
    int loop = read_loop;
    int have_read_size = 0;
    int cnt = 0;
    while(read_loop > 1) {
        char* buf = (char*)calloc(conf->_pktSize+sizeof(int), sizeof(char));
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));

        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, conf->_pktSize, fp);
        assert(read_size == conf->_pktSize);

        outstream->writeWithRBD(buf, conf->_pktSize+sizeof(int));

        free(buf);
        read_loop--;
        have_read_size += conf->_pktSize;
        cnt++;
    }
    if(read_loop == 1) {
        int last_size = size - have_read_size;
        char* buf = (char*)calloc(last_size+sizeof(int), sizeof(char));

        int tmplen = htonl(last_size);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        
        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, last_size, fp);
        assert(read_size == last_size);

        outstream->writeWithRBD(buf, last_size+sizeof(int));
        
        free(buf);
    }
    outstream->registerFile(loop);
    outstream->close();

    delete outstream;
    // fclose(fp);
    delete conf;
}

void writefullAndSelectiveDedup(string filename, string saveas, string poolname, string mode, int size) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(filename.c_str(), "rb");
    if(!fp) {
        cout << "[ERROR] file can not open!" << endl;
        return;
    }

    // create DPOutputStream
    DPOutputStream* outstream = new DPOutputStream(conf, saveas, poolname, mode, size);

    if(outstream->lookupFile(saveas, poolname)) {
        cout << "[ERROR] file exists, cannot writefull!" << endl;
        return;
    }

    // write file data into DPOutputStream
    int read_loop = (int)ceil(((double)size) / ((double)(conf->_pktSize)));
    int loop = read_loop;
    int have_read_size = 0;
    int cnt = 0;
    while(read_loop > 1) {
        char* buf = (char*)calloc(conf->_pktSize+sizeof(int), sizeof(char));
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));

        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, conf->_pktSize, fp);
        assert(read_size == conf->_pktSize);

        outstream->writeAndSelectiveDedup(buf, conf->_pktSize+sizeof(int));

        free(buf);
        read_loop--;
        have_read_size += conf->_pktSize;
        cnt++;
    }
    if(read_loop == 1) {
        int last_size = size - have_read_size;
        char* buf = (char*)calloc(last_size+sizeof(int), sizeof(char));

        int tmplen = htonl(last_size);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        
        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, last_size, fp);
        assert(read_size == last_size);

        outstream->writeAndSelectiveDedup(buf, last_size+sizeof(int));
        
        free(buf);
    }
    outstream->registerFile(loop);
    outstream->close();

    delete outstream;
    // fclose(fp);
    delete conf;
}

int readfullWithRBD(string filename, string poolname, string saveas) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    DPInputStream* instream = new DPInputStream(conf, filename, poolname, false, true);
    size_t fileLen = instream->output2file(saveas);
    instream->close();

    delete instream;
    delete conf;

    return fileLen;
}

int readfullWithRBDCombinedByClient(string filename, string poolname, string saveas) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    DPInputStream* instream = new DPInputStream(conf, filename, poolname, false, false, false, true);
    if(!instream->lookupFile(filename, poolname)) {
        cout << "[ERROR] read error, filename:" << filename << " poolname:" << poolname << " don't exist"  << endl;
        exit(-1);
    }
    size_t fileLen = instream->output2file(saveas);
    instream->close();

    delete instream;
    delete conf;

    return fileLen;
}

void writefullWithRBDAndBatchPush(string filename, string saveas, string poolname, string mode, int size) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(filename.c_str(), "rb");
    if(!fp) {
        cout << "[ERROR] file can not open!" << endl;
        return;
    }

    // create DPOutputStream
    DPOutputStream* outstream = new DPOutputStream(conf, saveas, poolname, mode, size);

    if(outstream->lookupFile(saveas, poolname)) {
        cout << "[ERROR] file exists, cannot writefull!" << endl;
        return;
    }

    // write file data into DPOutputStream
    int read_loop = (int)ceil(((double)size) / ((double)(conf->_pktSize)));
    int loop = read_loop;
    int have_read_size = 0;
    int cnt = 0;
    while(read_loop > 1) {
        char* buf = (char*)calloc(conf->_pktSize+sizeof(int), sizeof(char));
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));

        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, conf->_pktSize, fp);
        assert(read_size == conf->_pktSize);

        outstream->writeWithRBDAndBatchPush(buf, conf->_pktSize+sizeof(int));

        free(buf);
        read_loop--;
        have_read_size += conf->_pktSize;
        cnt++;
    }
    if(read_loop == 1) {
        int last_size = size - have_read_size;
        char* buf = (char*)calloc(last_size+sizeof(int), sizeof(char));

        int tmplen = htonl(last_size);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        
        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, last_size, fp);
        assert(read_size == last_size);

        outstream->writeWithRBDAndBatchPush(buf, last_size+sizeof(int));
        
        free(buf);
    }
    outstream->registerFile(loop);
    outstream->close();

    delete outstream;
    // fclose(fp);
    delete conf;
}


void writefullWithRBDAndPushSuperChunk(string filename, string saveas, string poolname, string mode, int size) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(filename.c_str(), "rb");
    if(!fp) {
        cout << "[ERROR] file can not open!" << endl;
        return;
    }

    // create DPOutputStream
    DPOutputStream* outstream = new DPOutputStream(conf, saveas, poolname, mode, size);

    if(outstream->lookupFile(saveas, poolname)) {
        cout << "[ERROR] file exists, cannot writefull!" << endl;
        return;
    }

    // write file data into DPOutputStream
    int read_loop = (int)ceil(((double)size) / ((double)(conf->_pktSize)));
    int loop = read_loop;
    int have_read_size = 0;
    int cnt = 0;
    while(read_loop > 1) {
        char* buf = (char*)calloc(conf->_pktSize+sizeof(int), sizeof(char));
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));

        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, conf->_pktSize, fp);
        assert(read_size == conf->_pktSize);

        outstream->writeWithRBDAndPushSuperChunk(buf, conf->_pktSize+sizeof(int));

        free(buf);
        read_loop--;
        have_read_size += conf->_pktSize;
        cnt++;
    }
    if(read_loop == 1) {
        int last_size = size - have_read_size;
        char* buf = (char*)calloc(last_size+sizeof(int), sizeof(char));

        int tmplen = htonl(last_size);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        
        fseek(fp, conf->_pktSize*cnt, SEEK_SET);
        int read_size = fread(buf+sizeof(int), 1, last_size, fp);
        assert(read_size == last_size);

        outstream->writeWithRBDAndPushSuperChunk(buf, last_size+sizeof(int));
        
        free(buf);
    }
    outstream->registerFile(loop);
    outstream->close();

    delete outstream;
    // fclose(fp);
    delete conf;
}

uint64_t writemulti(string filename, bool random_aggregate, bool rbd, bool selective_dedup, bool batch_push, bool super_chunk) {
    uint64_t write_size=0;
    std::ifstream read_file(filename);
    assert(read_file.is_open());
    std::string single_command;
    std::vector<std::vector<std::string>> commands;

    while (std::getline(read_file, single_command)) {
        std::istringstream iss(single_command);
        std::vector<std::string> argv;
        std::string arg;
        while (iss >> arg) {
            argv.push_back(arg);
        }
        commands.push_back(argv);
    }
    read_file.close();
    std::thread write_thread[commands.size()];
    for(int i=0;i<commands.size();i++) {
        auto argv=commands[i];
        if (argv.size() != 6) {
            exit(-1);
        }
        std::string filename(argv[1]);
        std::string saveas(argv[2]);
        std::string poolname(argv[3]);
        std::string mode(argv[4]);
        uint64_t size = stoi(argv[5]);
        write_size += size;
        if(mode!="online") {
            std::cout << "[ERROR] offline mode is not implemented" << std::endl;
            exit(-1);
        }
        cout << "[writemulti] filename:" << filename << " saveas:" << saveas <<  " poolname:" << poolname << " mode:" << mode << " size:" << size << endl;
        if(rbd) {
            write_thread[i]=std::thread([=]{writefullWithRBD(filename, saveas, poolname, mode, size);});
        } else if(selective_dedup){
            write_thread[i]=std::thread([=]{writefullAndSelectiveDedup(filename, saveas, poolname, mode, size);});
        } else if(batch_push) {
            write_thread[i]=std::thread([=]{writefullWithRBDAndBatchPush(filename, saveas, poolname, mode, size);});
        } else if(super_chunk) {
            write_thread[i]=std::thread([=]{writefullWithRBDAndPushSuperChunk(filename, saveas, poolname, mode, size);});
        } else {
            cout << "[ERROR] undifined writemulti mode" << endl;
            exit(-1);
        }
    }
    for(int i=0; i<commands.size(); i++) {
        write_thread[i].join();
    }
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);
    // writerfull done, notify all store layer to flush
    for(int i=0;i<conf->_agentNum;i++) {
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType14(14);
        agCmd->sendToStoreLayer(conf->_agentsIPs[i]);
        // delete agCmd;
    }
    // wait for all store layer to flush done 
    for (int i = 0; i < conf->_agentNum; i++) {
        string wkey = "flushdone";
        redisContext* ctx = RedisUtil::createContext(conf->_agentsIPs[i]);
        redisReply* rReply = (redisReply*)redisCommand(ctx, "blpop %s 0", wkey.c_str());
        assert(rReply!=NULL);
        assert(rReply->type==REDIS_REPLY_ARRAY);
        assert(string(rReply->element[1]->str) == "1");        
        freeReplyObject(rReply);
        redisFree(ctx);
    }
    return write_size;
}

void updatePacket(string filename, string poolname, int offset, int size, string new_filename) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(new_filename.c_str(), "rb");
    assert(fp);

    // create UpdateStream
    UpdateStream* update_stream = new UpdateStream(conf, filename, poolname, offset, size);
    assert(update_stream->lookupFile(filename, poolname));
    
    // write update data into UpdateStream
    int update_loop = (int)ceil(((double)size) / ((double)(conf->_pktSize)));
    int update_offset = offset;
    assert(size % conf->_pktSize == 0);
    for(int i=0; i<update_loop; i++, update_offset += conf->_pktSize) {
        char* buf = (char*)calloc(conf->_pktSize+sizeof(int), sizeof(char));
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        fseek(fp, conf->_pktSize*i, SEEK_SET);
        int update_size = fread(buf+sizeof(int), 1, conf->_pktSize, fp);
        assert(update_size == conf->_pktSize);
        update_stream->updatePacket(buf, conf->_pktSize+sizeof(int), update_offset);
        free(buf);
    }
    update_stream->close();

    delete update_stream;
    delete conf;
}

void updateChunk(string filename, string poolname, int offset, int size, string new_filename) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(new_filename.c_str(), "rb");
    assert(fp);

    // create UpdateStream
    UpdateStream* update_stream = new UpdateStream(conf, filename, poolname, offset, size);
    assert(update_stream->lookupFile(filename, poolname));
    
    // write update data into UpdateStream
    assert(size == conf->_chunkMaxSize);
    char* buf = (char*)malloc(conf->_chunkMaxSize);
    
    fseek(fp, 0, SEEK_SET);
    int update_size = fread(buf, 1, conf->_chunkMaxSize, fp);
    assert(update_size == conf->_chunkMaxSize);
    update_stream->updateChunk(buf, conf->_chunkMaxSize, offset);
    free(buf);
    fclose(fp);
    delete update_stream;
    delete conf;
}

void simpleUpdateChunk(string filename, string poolname, int offset, int size, string new_filename) {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);

    // open file
    FILE* fp = fopen(new_filename.c_str(), "rb");
    assert(fp);

    // create UpdateStream
    UpdateStream* update_stream = new UpdateStream(conf, filename, poolname, offset, size);
    assert(update_stream->lookupFile(filename, poolname));
    
    // write update data into UpdateStream
    assert(size == conf->_chunkMaxSize);
    char* buf = (char*)malloc(conf->_chunkMaxSize);
    
    fseek(fp, 0, SEEK_SET);
    int update_size = fread(buf, 1, conf->_chunkMaxSize, fp);
    assert(update_size == conf->_chunkMaxSize);
    update_stream->simpleUpdateChunk(buf, conf->_chunkMaxSize, offset);
    free(buf);
    fclose(fp);
    delete update_stream;
    delete conf;
} 


void updateChunkBatch(string filename, bool simple) {
    std::ifstream read_file(filename);
    assert(read_file.is_open());
    std::string single_command;
    std::vector<std::vector<std::string>> commands;

    while (std::getline(read_file, single_command)) {
        std::istringstream iss(single_command);
        std::vector<std::string> argv;
        std::string arg;
        while (iss >> arg) {
            argv.push_back(arg);
        }
        commands.push_back(argv);
    }
    read_file.close();
    cout << "[updateChunkBatch] receive update commands size:" << commands.size() << endl;
    for(int i=0;i<commands.size();i++) {
        auto argv=commands[i];
        if (argv.size() != 6) {
            exit(-1);
        }
        std::string filename(argv[1]);
        std::string poolname(argv[2]);
        int offset = stoi(argv[3]);
        int size = stoi(argv[4]);
        std::string new_filename(argv[5]);
        cout << "[updateChunkBatch] filename:" << filename << " poolname:" << poolname << " offset:" << offset << " size:" << size << " new_filename:" << new_filename << endl;
        if(simple) {
            simpleUpdateChunk(filename, poolname, offset, size, new_filename);
        } else {
            updateChunk(filename, poolname, offset, size, new_filename);
        } 
    }

}

void flushStoreLayer() {
    string confPath("../conf/sys.conf");
    Config* conf = new Config(confPath);
    for(int i=0;i<conf->_agentNum;i++) {
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType14(14);
        agCmd->sendToStoreLayer(conf->_agentsIPs[i]);
        delete agCmd;
    }
    for (int i = 0; i < conf->_agentNum; i++) {
        string wkey = "flushdone";
        redisContext* ctx = RedisUtil::createContext(conf->_agentsIPs[i]);
        redisReply* rReply = (redisReply*)redisCommand(ctx, "blpop %s 0", wkey.c_str());
        assert(rReply!=NULL);
        assert(rReply->type==REDIS_REPLY_ARRAY);
        assert(string(rReply->element[1]->str) == "1");        
        freeReplyObject(rReply);
        redisFree(ctx);
    }
}


int main(int argc, char** argv) {
    string configPath = "../conf/sys.conf";
    Config* conf = new Config(configPath);
    if (argc < 2) {
        usage();
        return -1;
    }

    string reqType(argv[1]);
    string filename, poolname, saveas, mode;
    uint64_t size;
    if(reqType == "write") {
        auto start = system_clock::now();
        switch(conf->_write_mode) {
            case WriteMode::Normal:
                if (argc != 7) {
                    usage();
                    exit(-1);
                }
                filename = string(argv[2]);
                saveas = string(argv[3]);
                poolname = string(argv[4]);
                mode = string(argv[5]);
                size = atoi(argv[6]);
                writefullWithRBD(filename, saveas, poolname, mode, size);
                flushStoreLayer();
                break;
            case WriteMode::NormalMulti:
                if(argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                size = writemulti(filename, false, true, false, false, false);
                break;
            case WriteMode::SelectiveDedup:
                if (argc != 7) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                saveas = string(argv[3]);
                poolname = string(argv[4]);
                mode = string(argv[5]);
                size = atoi(argv[6]);
                writefullAndSelectiveDedup(filename, saveas, poolname, mode, size);
                flushStoreLayer();
                break;
            case WriteMode::SelectiveDedupMulti:
                if(argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                size = writemulti(filename, false, false, true, false, false);
                break;
            case WriteMode::BatchPush:
                if (argc != 7) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                saveas = string(argv[3]);
                poolname = string(argv[4]);
                mode = string(argv[5]);
                size = atoi(argv[6]);
                writefullWithRBDAndBatchPush(filename, saveas, poolname, mode, size);
                flushStoreLayer();
                break;
            case WriteMode::BatchPushMulti:
                if(argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                size = writemulti(filename, false, false, false, true, false);
                break;
            case WriteMode::SuperChunk:
                if (argc != 7) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                saveas = string(argv[3]);
                poolname = string(argv[4]);
                mode = string(argv[5]);
                size = atoi(argv[6]);
                writefullWithRBDAndPushSuperChunk(filename, saveas, poolname, mode, size);
                flushStoreLayer();
                break;
            case WriteMode::SuperChunkMulti:
                if(argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                size = writemulti(filename, false, false, false, false, true);
                break;
            default:
                cout << "undefined write mode" << endl;
                exit(-1);
        }
        auto end = system_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        cout <<  "write time is: " 
            << double(duration.count()) * milliseconds::period::num
            << " ms" << endl;
        cout << "write throughput is:"
            << (double(size)/double(1024*1024)) / ((double(duration.count()) * seconds::period::num)/1000)
            << " MB/s" << endl;
    
    } else if(reqType == "read") {
        if (argc != 5) {
            usage();
            return -1;
        }
        string filename(argv[2]);
        string poolname(argv[3]);
        string saveas(argv[4]);
        auto start = system_clock::now();
        int size = readfullWithRBDCombinedByClient(filename, poolname, saveas);
        auto end = system_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        cout <<  "read time is: " 
            << double(duration.count()) * milliseconds::period::num
            << " ms" << endl;
        cout << "read throughput is:"
            << (double(size)/double(1024*1024)) / ((double(duration.count()) * seconds::period::num)/1000)
            << " MB/s" << endl;
    } else if(reqType == "startreorder") {
        if(argc != 2) {
            usage();
            return -1;
        }
        startreorder();
    } else if(reqType == "update") {
        string filename;
        auto start = system_clock::now();
        switch(conf->_update_mode) {
            case UpdateMode::Normal:
                if (argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                updateChunkBatch(filename, true);
                break;
            case UpdateMode::Remap:
                if (argc != 3) {
                    usage();
                    return -1;
                }
                filename = string(argv[2]);
                updateChunkBatch(filename, false);
                break;
            default:
                cout << "undefined update mode" << endl;
                exit(-1);
        }
        auto end = system_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        cout <<  "update time is: " << double(duration.count()) * milliseconds::period::num << " ms" << endl;
    } else {
        cout << "[ERROR] unrecognized request!" << endl;
        usage();
        return -1;
    }

    delete conf;
    return 0;
}

