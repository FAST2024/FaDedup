#ifndef _PACKET_RECIPE_SERIALIZER_HH_
#define _PACKET_RECIPE_SERIALIZER_HH_

#include "../inc/include.hh"
#include "PacketRecipe.hh"
#include "WrappedFP.hh"

class PacketRecipeSerializer {
public:
    static int RECIPE_SIZE;
public:
    static void encode(vector<WrappedFP*>* recipe, char* stream, int len);
    static void decode(char* stream, vector<WrappedFP*>* recipe);
};

#endif