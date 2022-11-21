#pragma once

#include "_lru11Cache.hpp"
#include <bits/stdc++.h>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

using namespace std;

namespace DELTAKV_NAME_SPACE {

class LRUCache {
private:
    lru11::Cache<string, uint32_t>* Cache_;
    uint8_t** memoryPool_;
    uint64_t cacheSize_ = 0;
    size_t currentIndex_ = 0;
    boost::shared_mutex mtx;

public:
    LRUCache(uint64_t cacheSize);
    ~LRUCache();
    void insertToCache(string& cacheKey, uint8_t* data, uint32_t length);
    bool existsInCache(string& cacheKey);
    uint8_t* getFromCache(string& cacheKey);
};

}