cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")
project(YOUR_PROJECT_NAME)
add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")
set(CMAKE_CXX_STANDARD 11)
# include directories for dependencies and WRENCH libraries
include_directories(src/ /usr/local/include /usr/local/include/wrench)
# source files
set(SOURCE_FILES
        src/Simulator.cpp
        src/ZhangClusteringWMS.cpp
        src/ZhangClusteringWMS.h
        src/FixedClusteringWMS.cpp
        src/FixedClusteringWMS.h
        src/FixedClusteringScheduler.cpp
        src/FixedClusteringScheduler.h
        )

# wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(SIMGRID_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(LEMON_LIBRARY NAMES emon)
find_library(ZMQ_LIBRARY NAMES zmq)

# generating the executable
add_executable(simulator ${SOURCE_FILES})

target_link_libraries(simulator
        ${WRENCH_LIBRARY}
        ${SIMGRID_LIBRARY}
        ${PUGIXML_LIBRARY}
        ${LEMON_LIBRARY}
        ${ZMQ_LIBRARY}
        )

install(TARGETS simulator DESTINATION bin)

