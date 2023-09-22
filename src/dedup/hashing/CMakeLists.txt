aux_source_directory(. DIR_LIB_SRCS)
add_library(hashing ${DIR_LIB_SRCS})
target_link_libraries(hashing common crypto ssl)