aux_source_directory(. DIR_LIB_SRCS)
add_library(protocol ${DIR_LIB_SRCS})
target_link_libraries(protocol util)