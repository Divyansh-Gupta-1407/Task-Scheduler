
cmake_minimum_required(VERSION 3.16)

project(TaskScheduler LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(thread_pool_demo
    src/ThreadPool.cpp
    src/TaskGraph.cpp
    src/DagExecutor.cpp
    tests/test.cpp
)

target_include_directories(thread_pool_demo PRIVATE include)

target_link_libraries(thread_pool_demo PRIVATE Threads::Threads)

if(BUILD_TESTING)
    add_test(NAME scheduler_tests COMMAND thread_pool_demo)
endif()
