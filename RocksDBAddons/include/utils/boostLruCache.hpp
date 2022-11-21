#pragma once

#include <bits/stdc++.h>
#include <boost/compute/detail/lru_cache.hpp>
#include <boost/thread.hpp>

using namespace std;

namespace DELTAKV_NAME_SPACE {

template <typename keyType, typename valueType>

class BOOSTLRUCache {
private:
    boost::compute::detail::lru_cache<keyType, valueType>* Cache_;
    uint64_t cacheSize_ = 0;

public:
    BOOSTLRUCache(uint64_t cacheSize);
    ~BOOSTLRUCache();
    void insertToCache(keyType& cacheKey, valueType& data);
    bool existsInCache(keyType& cacheKey);
    valueType* getFromCache(keyType& cacheKey);
};

}