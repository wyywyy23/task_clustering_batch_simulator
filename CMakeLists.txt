#SET(CMAKE_C_FLAGS_DEBUG "-D_DEBUG")
cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")
project(task_clustering_batch_simulator)
add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid REQUIRED)

# include directories for dependencies and WRENCH libraries
include_directories(src/ /usr/local/include /usr/local/include/wrench /opt/local/include ${SimGrid_INCLUDE_DIR})

# source files
set(SOURCE_FILES
        src/main.cpp
        src/Simulator.cpp
        src/Simulator.h
        src/Globals.h
        src/Util/WorkflowUtil.cpp
        src/Util/WorkflowUtil.h
        src/Util/ProxyWMS.cpp
        src/Util/ProxyWMS.h
        src/Util/PlaceHolderJob.cpp
        src/Util/PlaceHolderJob.h
        src/LevelByLevelAlgorithm/OngoingLevel.cpp
        src/LevelByLevelAlgorithm/OngoingLevel.h
        src/LevelByLevelAlgorithm/LevelByLevelWMS.cpp
        src/LevelByLevelAlgorithm/LevelByLevelWMS.h
        src/ZhangClusteringAlgorithms/ZhangWMS.cpp
        src/ZhangClusteringAlgorithms/ZhangWMS.h
        src/GlumeAlgorithm/GlumeWMS.cpp
        src/GlumeAlgorithm/GlumeWMS.h
        src/StaticClusteringAlgorithms/ClusteredJob.cpp
        src/StaticClusteringAlgorithms/ClusteredJob.h
        src/StaticClusteringAlgorithms/StaticClusteringWMS.cpp
        src/StaticClusteringAlgorithms/StaticClusteringWMS.h
        )

# wrench library and dependencies
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_TOOL_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SimGrid_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(LEMON_LIBRARY NAMES emon)
find_library(ZMQ_LIBRARY NAMES zmq)

# generating the executable
add_executable(simulator ${SOURCE_FILES})

target_link_libraries(simulator
        ${WRENCH_LIBRARY}
        ${WRENCH_PEGASUS_TOOL_LIBRARY}
        ${SimGrid_LIBRARY}
        ${PUGIXML_LIBRARY}
        ${LEMON_LIBRARY}
        ${ZMQ_LIBRARY}
        )


install(TARGETS simulator DESTINATION bin)
