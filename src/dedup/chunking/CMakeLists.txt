aux_source_directory(. DIR_LIB_SRCS)
add_library(chunking ${DIR_LIB_SRCS})
target_link_libraries(chunking common)