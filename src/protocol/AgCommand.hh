#ifndef _AG_COMMAND_HH_
#define _AG_COMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class AGCommand {
private:
    char* _agCmd = 0;
    int _cmLen = 0;

    string _rKey;

    int _type;

    // type 0 (writeFull)
    string _filename;
    int _pktid;
    int _chunkid;
    string _poolname;
    string _mode;
    int _file_size;

    // type 1 (writePartial)


    // type 2 (read)


    // type 3 (lookup fp)
    char* _fp;
    int _res;
    unsigned int _ip;

    // type 5 (register fp)
    char* _buf;
    int _size;

    //type 6 (register file)
    int _pkt_num;

public:
    AGCommand();
    AGCommand(int cmdsize);
    ~AGCommand();
    AGCommand(char* reqStr);

    void setRkey(string key);

    // basic construction methods
    void writeInt(int value);
    void writeString(string s);
    int readInt();
    string readString();
    void writeCharPointer(char* fp, int len);
    char* readCharPointer();

    int getType();
    char* getCmd();
    int getCmdLen();
    string getFilename();
    int getPktId();
    int getChunkId();
    string getPoolname();
    string getMode();
    int getFilesize();
    char* getFp();
    bool getRes();
    unsigned int getIp();
    char* getBuf();
    int getSize();
    int getPktNum();

    // send methods
    void sendTo(unsigned int ip);
    void sendToStoreLayer(unsigned int ip);
    // build AGCommand
    // writefull command
    void buildType0(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode,
                    int filesizeMB,
                    unsigned int ip);
    // readfull command
    void buildType2(int type,
                    string filename,
                    string poolname);
    // writebatch fp command
    void buildType3(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode,
                    int filesizeMB,
                    unsigned int ip);
    // readbatch command
    void buildType4(int type,
                    string filename,
                    string poolname);

    // promote command
    void buildType5(int type,
                    string filename,
                    string poolname,
                    char* buf,
                    int size,
                    unsigned int ip);
    // register file
    void buildType6(int type,
                    string filename,
                    int pkt_num);
    // deletefull command
    void buildType7(int type,
                    string filename,
                    string poolname);
    // reorder command
    void buildType8(int type);

    // writefull and random aggregate command
    void buildType9(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode,
                    int filesizeMB,
                    unsigned int ip);

    // readfulll after random aggregate command
    void buildType10(int type,
                    string filename,
                    string poolname);

    // writefull with ceph rbd
    void buildType11(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode, 
                    int filesize,
                    unsigned int ip);
    void buildType12(int type, 
                    string filename, 
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
    void buildType13(int type,
                    string filename,
                    string poolname);
    void buildType14(int type);

    // writefullAndSelectiveDedup
    void buildType15(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode, 
                    int filesize,
                    unsigned int ip);
    // readfullAfterSelectiveDedup
    void buildType16(int type,
                    string filename,
                    string poolname);

    // popSelectiveDedupChunk
    void buildType17(int type, 
                    string filename, 
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
    void buildType18(int type,
                    string filename,
                    string poolname);
    void buildType19(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode,
                    int filesize,
                    unsigned int ip);
    // update packet
    void buildType20(int type,
                    string filename,
                    string poolname,
                    int pktid,
                    int size,
                    unsigned int ip);
    // pop update chunk
    void buildType21(int type, 
                    string filename, 
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
    // writefull with rbd and batch push
    void buildType22(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode, 
                    int filesize,
                    unsigned int ip);
    // send chunk buffer stream to store layer
    void buildType23(int type, 
                    string filename, 
                    string poolname,
                    int pktid,
                    int size,
                    unsigned int ip);
    // update chunk
    void buildType24(int type,
                    string filename,
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
      
    // simple update chunk
    void buildType26(int type,
                    string filename,
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
    // writefull with rbd and push super chunk
    void buildType27(int type,
                    string filename,
                    int pktid,
                    string poolname,
                    string mode, 
                    int filesize,
                    unsigned int ip);


    //clear readbuf;              
    void buildType25(int type);

    // simple update chunk flush to ceph immediate
    void buildType28(int type, 
                    string filename, 
                    string poolname,
                    int pktid,
                    int chunkid,
                    int size,
                    unsigned int ip);
    // resolve AGCommand
    void resolveType0();
    void resolveType2();
    void resolveType3();
    void resolveType4();
    void resolveType5();
    void resolveType6();
    void resolveType7();
    void resolveType8();
    void resolveType9();
    void resolveType10();
    void resolveType11();
    void resolveType12();
    void resolveType13();
    void resolveType14();
    void resolveType15();
    void resolveType16();
    void resolveType17();
    void resolveType18();
    void resolveType19();
    void resolveType20();
    void resolveType21();
    void resolveType22();
    void resolveType23();
    void resolveType24();
    void resolveType26();
    void resolveType27();
    void resolveType25();
    void resolveType28();
};

#endif
