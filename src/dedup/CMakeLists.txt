add_subdirectory(chunking)
add_subdirectory(hashing)

aux_source_directory(. DIR_LIB_SRCS)
add_library(dedup ${DIR_LIB_SRCS})
target_link_libraries(dedup chunking hashing common)