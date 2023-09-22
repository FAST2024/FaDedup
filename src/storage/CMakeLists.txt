aux_source_directory(. DIR_LIB_SRCS)
add_library(storage ${DIR_LIB_SRCS})
target_link_libraries(storage rados pthread common rbd)