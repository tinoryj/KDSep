#src/interface

add_library(deltaKVOptions STATIC deltaKVOptions.cpp)
add_library(deltaKVInterface STATIC deltaKVInterface.cpp)
add_library(mergeOperation STATIC mergeOperation.cpp)

set(INTERFACE_OBJ deltaKVOptions deltaKVInterface mergeOperation)
