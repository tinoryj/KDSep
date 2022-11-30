#/
find_package(Boost REQUIRED COMPONENTS thread system serialization)
if(Boost_FOUND)
   include_directories(${BOOST_INCLUDE_DIRS})
   link_directories(${BOOST_LIBRARY_DIR})
endif()

set(ROCKSDB_INCLUDE_DIR /opt/rocksdb-7.7.3/include)
set(ROCKSDB_STATIC_LIBRARY_OBJ /opt/rocksdb-7.7.3/librocksdb.a)

include_directories(../include ${ROCKSDB_INCLUDE_DIR})

set(BOOST_LIBRARY_OBJ boost_thread boost_system boost_serialization)
set(SYSTEM_LIBRARY_OBJ pthread rt dl)
set(ROCKSDB_LIBRARY_OBJ lz4 z snappy bz2 zstd uring)

set(LINK_OBJ ${ROCKSDB_LIBRARY_OBJ} ${BOOST_LIBRARY_OBJ} ${SYSTEM_LIBRARY_OBJ})

#-fno-builtin-memset

set (CMAKE_SYSTEM_NAME Linux )
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (
      STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}"
   )
endif ()

set(CMAKE_C_FLAGS_DEBUG "-Wall -std=c17 -fPIC -frtti -g -ggdb -g2") 
set(CMAKE_CXX_FLAGS_DEBUG "-Wall -std=c++17 -fPIC -frtti -g -ggdb -g2 -fno-omit-frame-pointer -fsanitize=leak")
set(CMAKE_C_FLAGS_RELEASE "-O3 -std=c17 -fPIC -frtti -g -fsanitize=leak") 
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -std=c++17 -fPIC -frtti -g -fsanitize=leak")

SET (CMAKE_C_COMPILER               "/usr/bin/clang")
SET (CMAKE_CXX_COMPILER             "/usr/bin/clang++")
SET (CMAKE_AR                       "/usr/bin/llvm-ar")
SET (CMAKE_LINKER                   "/usr/bin/llvm-ld")
SET (CMAKE_NM                       "/usr/bin/llvm-nm")
SET (CMAKE_OBJDUMP                  "/usr/bin/llvm-objdump")
SET (CMAKE_RANLIB                   "/usr/bin/llvm-ranlib")

add_library(DeltaKV STATIC 
      utils/murmurHash.cpp
      utils/lruCache.cpp
      utils/timer.cpp
      utils/fileOperation.cpp
      hashBasedStore/hashStoreInterface.cpp
      hashBasedStore/hashStoreFileManager.cpp
      hashBasedStore/hashStoreFileOperator.cpp
      indexBasedStore/indexStoreInterface.cpp
      interface/deltaKVOptions.cpp
      interface/deltaKVInterface.cpp
      interface/mergeOperation.cpp
)

target_link_libraries(DeltaKV ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
add_executable(test mainTest.cc)
target_link_libraries(test DeltaKV ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})