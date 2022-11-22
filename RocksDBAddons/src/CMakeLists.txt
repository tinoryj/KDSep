#/
find_package(Boost REQUIRED COMPONENTS thread system serialization)
if(Boost_FOUND)
   include_directories(${BOOST_INCLUDE_DIRS})
   link_directories(${BOOST_LIBRARY_DIR})
endif()

include_directories(../include)

set(BOOST_LIBRARY_OBJ boost_thread boost_system boost_serialization)
set(SYSTEM_LIBRARY_OBJ pthread rt dl)
set(ROCKSDB_LIBRARY_OBJ rocksdb lz4 z snappy bz2 zstd)

set(LINK_OBJ ${ROCKSDB_LIBRARY_OBJ} ${BOOST_LIBRARY_OBJ} ${SYSTEM_LIBRARY_OBJ})

#-fno-builtin-memset

set (CMAKE_SYSTEM_NAME Linux )
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (
      STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}"
   )
endif ()

set(CMAKE_C_FLAGS_DEBUG "-Wall -std=c17 -g -ggdb -g2") 
set(CMAKE_CXX_FLAGS_DEBUG "-Wall -std=c++17 -g -ggdb -g2 -fno-omit-frame-pointer -fsanitize=leak")
set(CMAKE_C_FLAGS_RELEASE "-O3 -std=c17 -g -fPIC -frtti") 
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -std=c++17 -g -fPIC -frtti")

SET (CMAKE_C_COMPILER               "/usr/bin/clang")
SET (CMAKE_CXX_COMPILER             "/usr/bin/clang++")
SET (CMAKE_AR                       "/usr/bin/llvm-ar")
SET (CMAKE_LINKER                   "/usr/bin/llvm-ld")
SET (CMAKE_NM                       "/usr/bin/llvm-nm")
SET (CMAKE_OBJDUMP                  "/usr/bin/llvm-objdump")
SET (CMAKE_RANLIB                   "/usr/bin/llvm-ranlib")

add_library(DeltaKV STATIC 
    interface/deltaKVOptions.cpp
    interface/deltaKVInterface.cpp
    interface/mergeOperation.cpp
    utils/murmurHash.cpp
    utils/trie.cpp
    utils/messageQueue.cpp
    utils/lruCache.cpp
    utils/boostLruCache.cpp
)
target_link_libraries(DeltaKV ${LINK_OBJ})
add_executable(test mainTest.cc)
target_link_libraries(test DeltaKV ${LINK_OBJ})