cmake_minimum_required(VERSION 3.16)

project(janus)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(janus
    src/main.cpp
    src/job.cpp
    src/scheduler.cpp
)

target_include_directories(janus
    PRIVATE
        include
)

target_link_libraries(janus
    PRIVATE
        Threads::Threads
)