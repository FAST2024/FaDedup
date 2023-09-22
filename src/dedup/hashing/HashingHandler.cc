#include "HashingHandler.hh"

HashingHandler::HashingHandler(Config* conf): _conf(conf) {
    if(conf->_hashAlg == "sha1") {
        hashing = sha1;
    }
    else {
        cout << "[ERROR] unrecognized hashing algorithm: " << _conf->_hashAlg << endl;
    }
}

HashingHandler::~HashingHandler() {

}