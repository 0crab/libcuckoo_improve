cmake_minimum_required(VERSION 3.17)
project(libcuckoo_improve)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

#add_executable(microbenchmark microbenchmark.cpp )

add_executable(ycsb ycsb.cpp)