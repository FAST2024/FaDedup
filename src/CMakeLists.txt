# minimum version of cmake
cmake_minimum_required (VERSION 3.1)

# project name
project (dpdedup_exe)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../bin)

add_subdirectory(common)
add_subdirectory(dedup)
add_subdirectory(protocol)
add_subdirectory(util)
add_subdirectory(storage)

aux_source_directory(. DIR_SRCS)
add_executable(DPAgent DPAgent.cc)
add_executable(DPClient DPClient.cc)

target_link_libraries(DPAgent common storage pthread)
target_link_libraries(DPClient common)
