#src/utils

add_library(murmurHash STATIC murmurHash.cpp)
add_library(trie STATIC trie.cpp)
add_library(messageQueue STATIC messageQueue.cpp)
add_library(lruCache STATIC lruCache.cpp)

set(UTILS_OBJ murmurHash trie messageQueue lruCache)
