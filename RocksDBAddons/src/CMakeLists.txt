#/
find_package(Boost REQUIRED COMPONENTS thread system serialization)
if(Boost_FOUND)
   include_directories(${BOOST_INCLUDE_DIRS})
   link_directories(${BOOST_LIBRARY_DIR})
endif()

include_directories(../include)
link_directories(util)

set(BOOST_LIBRARY_OBJ boost_thread boost_system boost_serialization)
set(SYSTEM_LIBRARY_OBJ pthread rt dl)
set(ROCKSDB_LIBRARY_OBJ pthread rocksdb)

set(LINK_OBJ ${UTILS_OBJ} ${ROCKSDB_LIBRARY_OBJ} ${BOOST_LIBRARY_OBJ} ${SYSTEM_LIBRARY_OBJ})

#-fno-builtin-memset

set (CMAKE_SYSTEM_NAME Linux )
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (
      STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}"
   )
endif ()

set(CMAKE_C_FLAGS_DEBUG "-Wall -std=c1x -g -ggdb -g2") 
set(CMAKE_CXX_FLAGS_DEBUG "-Wall -g -ggdb -g2 -fno-omit-frame-pointer -fsanitize=leak")
set(CMAKE_C_FLAGS_RELEASE "-O3 -std=c1x") 
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -std=c1x")

SET (CMAKE_C_COMPILER               "/usr/bin/clang")
SET (CMAKE_CXX_COMPILER             "/usr/bin/clang++")
SET (CMAKE_AR                       "/usr/bin/llvm-ar")
SET (CMAKE_LINKER                   "/usr/bin/llvm-ld")
SET (CMAKE_NM                       "/usr/bin/llvm-nm")
SET (CMAKE_OBJDUMP                  "/usr/bin/llvm-objdump")
SET (CMAKE_RANLIB                   "/usr/bin/llvm-ranlib")

add_subdirectory(interface)
add_subdirectory(indexBasedStore)
add_subdirectory(hashBasedStore)
add_subdirectory(utils)