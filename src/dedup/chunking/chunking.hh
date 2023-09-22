#ifndef _CHUNKING_HH_
#define _CHUNKING_HH_

#include "../../inc/include.hh"
#include "../../common/Config.hh"

void rabin_init(Config* conf);
int rabin_chunk_data(char *p, int n);

void fsc_init(Config* conf);
int fixed_size_chunking(char *p, int size);

void fastcdc_init(Config* conf);
int fastcdc_chunk_data(char *p, int n);

#endif