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

set (CMAKE_SYSTEM_NAME Linux )
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (
      STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}"
   )
endif ()

if(${CMAKE_BUILD_TYPE} STREQUAL "Release")
   message (STATUS "CMAKE_BUILD_TYPE as Release selected")
   add_compile_options(-std=c++17 -fPIC -frtti -g) 
else()
   message (STATUS "CMAKE_BUILD_TYPE as Debug selected")
   add_compile_options(-Wall -std=c++17 -fPIC -frtti -g -ggdb -g2 -fno-omit-frame-pointer -fsanitize=leak) 
endif ()

add_library(DeltaKV STATIC 
      utils/murmurHash.cpp
      utils/lruCache.cpp
      utils/statsRecorder.cc
      utils/timer.cpp
      utils/debug.cpp
      utils/fileOperation.cpp
      utils/mempool.cpp
      hashBasedStore/hashStoreInterface.cpp
      hashBasedStore/hashStoreFileManager.cpp
      hashBasedStore/hashStoreFileOperator.cpp
      indexBasedStore/deviceManager.cc
      indexBasedStore/ds/bitmap.cc
      indexBasedStore/ds/segmentPool.cc
      indexBasedStore/ds/lru.cc
      indexBasedStore/gcManager.cc
      indexBasedStore/indexStoreInterface.cpp
      indexBasedStore/indexStore.cpp
      indexBasedStore/configManager.cc
      indexBasedStore/indexStoreInterface.cpp
      indexBasedStore/logManager.cc
      indexBasedStore/rocksdbKeyManager.cc
      indexBasedStore/segmentGroupManager.cc
      indexBasedStore/valueManager.cc
      interface/deltaKVOptions.cpp
      interface/deltaKVInterface.cpp
      interface/lsmTreeInterface.cpp
      interface/mergeOperation.cpp
)

target_link_libraries(DeltaKV ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
# add_executable(deltaDump deltaStoreDump.cc)
# target_link_libraries(deltaDump DeltaKV ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
# add_executable(test mainTest.cc)
# target_link_libraries(test DeltaKV ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
