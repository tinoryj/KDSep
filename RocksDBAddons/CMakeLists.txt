cmake_minimum_required(VERSION 3.13)
project(DeltaKV)

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${DeltaKV_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${DeltaKV_SOURCE_DIR}/lib)

add_subdirectory(src)