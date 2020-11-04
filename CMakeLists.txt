cmake_minimum_required(VERSION 3.10)
project(libcuckoo_improve)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

#add_executable(micro_benchmark micro_benchmark.cpp )

add_executable(ycsb ycsb.cpp)