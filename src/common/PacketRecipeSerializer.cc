#include "PacketRecipeSerializer.hh"

int PacketRecipeSerializer::RECIPE_SIZE = sizeof(fingerprint) + 4*sizeof(int) + sizeof(bool) + 32*2 + sizeof(unsigned int);

void PacketRecipeSerializer::encode(vector<WrappedFP*>* recipe, char* stream, int len) {
    assert(recipe != nullptr);
    int alloc_len = sizeof(int) + recipe->size() * (RECIPE_SIZE);
    assert(alloc_len == len);
    int off = 0;
    int tmplen = htonl(alloc_len);
    memcpy(stream+off, (void *)&tmplen, sizeof(int));
    off += sizeof(int);
    for(int i = 0; i < recipe->size(); i++) {
        memcpy(stream+off, (*recipe)[i]->_fp, sizeof(fingerprint));
        off += sizeof(fingerprint);

        int tmp = htonl((*recipe)[i]->_chunksize);
        memcpy(stream+off, (void*)&tmp, sizeof(int));
        off += sizeof(int);

        bool dup = (*recipe)[i]->_dup;
        memcpy(stream+off, (void*)&dup, sizeof(bool));
        off += sizeof(bool);

        int pid = htonl((*recipe)[i]->_pktid);
        memcpy(stream+off, (void*)&pid, sizeof(int));
        off += sizeof(int);

        int cid = htonl((*recipe)[i]->_containerId);
        memcpy(stream+off, (void*)&cid, sizeof(int));
        off += sizeof(int);

        int tmpoff = htonl((*recipe)[i]->_offset);
        memcpy(stream+off, (void*)&tmpoff, sizeof(int));
        off += sizeof(int);
        
        unsigned int conip = htonl((*recipe)[i]->_conIp);
        memcpy(stream+off, (void*)&conip, sizeof(unsigned int));
        off += sizeof(unsigned int);
        
        // cout << "encode origin_chunk_poolname: " << (*recipe)[i]->_origin_chunk_poolname << endl;
        memcpy(stream+off, (*recipe)[i]->_origin_chunk_poolname, 32);
        off += 32;
        
        memcpy(stream+off, (*recipe)[i]->_chunk_store_filename, 32);
        off += 32;
    }
}

void PacketRecipeSerializer::decode(char* stream, vector<WrappedFP*>* recipe) {
    int tmplen, len = 0;
    int off = 0;
    memcpy((void*)&tmplen, stream+off, sizeof(int)); 
    off += sizeof(int);
    len = ntohl(tmplen);
    int chunknum = (len - sizeof(int))/(RECIPE_SIZE);
    for(int i = 0; i < chunknum; i++) {
        recipe->push_back(new WrappedFP());
    }
    char* tmpfp = new char[sizeof(fingerprint)];
    for(int i = 0; i < chunknum; i++) {
        memcpy(tmpfp, stream+off, sizeof(fingerprint));
        off += sizeof(fingerprint);
        (*recipe)[i]->deepCopy(tmpfp, sizeof(fingerprint));

        int tmpsize;
        memcpy((void*)&tmpsize, stream+off, sizeof(int));
        off += sizeof(int);
        (*recipe)[i]->_chunksize = ntohl(tmpsize);

        bool dup;
        memcpy((void*)&dup, stream+off, sizeof(bool));
        off += sizeof(bool);
        (*recipe)[i]->_dup = dup;

        int pid;
        memcpy((void*)&pid, stream+off, sizeof(int));
        off += sizeof(int);
        (*recipe)[i]->_pktid = ntohl(pid);

        int cid;
        memcpy((void*)&cid, stream+off, sizeof(int));
        off += sizeof(int);
        (*recipe)[i]->_containerId = ntohl(cid);

        int tmpoff;
        memcpy((void*)&tmpoff, stream+off, sizeof(int));
        off += sizeof(int);
        (*recipe)[i]->_offset = ntohl(tmpoff);
        
        unsigned int conip;
        memcpy((void*)&conip, stream+off, sizeof(unsigned int));
        off += sizeof(unsigned int);
        (*recipe)[i]->_conIp = ntohl(conip);

        memcpy((*recipe)[i]->_origin_chunk_poolname, stream+off, 32);
        // std::cout << "decode origin_chunk_poolname: " << (*recipe)[i]->_origin_chunk_poolname << std::endl;
        off += 32;
        
        memcpy((*recipe)[i]->_chunk_store_filename, stream+off, 32);
        off += 32;
    }
    delete tmpfp;
}
