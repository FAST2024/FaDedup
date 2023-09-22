#include "hashing.hh"

void sha1(char *data, int32_t size, fingerprint fp) {
    SHA_CTX ctx;
    SHA_Init(&ctx);
	SHA_Update(&ctx, data, size);
	SHA_Final((unsigned char*)fp, &ctx);
}