#pragma once

#include <bits/stdc++.h>
#include <boost/compute/detail/lru_cache.hpp>
#include <boost/thread.hpp>

using namespace std;

namespace DELTAKV_NAMESPACE {

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

template <typename keyType, typename valueType>
BOOSTLRUCache<keyType, valueType>::BOOSTLRUCache(uint64_t cacheSize)
{
    cacheSize_ = cacheSize;
    Cache_ = new boost::compute::detail::lru_cache<keyType, valueType>(cacheSize_);
}

template <typename keyType, typename valueType>
BOOSTLRUCache<keyType, valueType>::~BOOSTLRUCache()
{
    delete Cache_;
}

template <typename keyType, typename valueType>
void BOOSTLRUCache<keyType, valueType>::insertToCache(keyType& cacheKey, valueType& data)
{
    Cache_->insert(cacheKey, data);
}

template <typename keyType, typename valueType>
bool BOOSTLRUCache<keyType, valueType>::existsInCache(keyType& cacheKey)
{
    if (Cache_->contains(cacheKey)) {
        return true;
    } else {
        return false;
    }
}

template <typename keyType, typename valueType>
valueType* BOOSTLRUCache<keyType, valueType>::getFromCache(keyType& cacheKey)
{
    boost::optional<valueType> optionalValue = Cache_->get(cacheKey);
    return &(optionalValue.get());
}

} // DELTAKV_NAMESPACE