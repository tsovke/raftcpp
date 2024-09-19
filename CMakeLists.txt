cmake_minimum_required(VERSION 3.29)
project(raftcpp)

set(CMAKE_CXX_STANDARD 14)

include_directories(src)

add_executable(raftcpp
    src/noncopyable.h
    src/objfactory.h
    src/raft.cpp
    src/raft.h
    src/thread_pool.h
    test/test.cpp)
add_executable(thread_pool_test
        src/thread_pool.h
        test/thread_pool_test.cpp)
