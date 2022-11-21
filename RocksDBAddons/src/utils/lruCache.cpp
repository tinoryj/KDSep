#include "utils/lruCache.hpp"

namespace DELTAKV_NAME_SPACE {

LRUCache::LRUCache(uint64_t cacheSize)
{
    cacheSize_ = cacheSize;
    this->Cache_ = new lru11::Cache<string, uint32_t>((cacheSize_ / (1024 * 1024)), 0);
    memoryPool_ = (uint8_t**)malloc(cacheSize_ * sizeof(uint8_t*));
    for (size_t i = 0; i < cacheSize_; i++) {
        memoryPool_[i] = (uint8_t*)malloc(cacheSize_ * sizeof(uint8_t));
    }
    currentIndex_ = 0;
}

void LRUCache::insertToCache(string& cacheKey, uint8_t* data, uint32_t length)
{
    {
        boost::unique_lock<boost::shared_mutex> t(this->mtx);
        if (Cache_->size() + 1 > cacheSize_) {
            // evict a item
            uint32_t replaceIndex = Cache_->pruneValue();
            memcpy(memoryPool_[replaceIndex], data, length);
            Cache_->insert(cacheKey, replaceIndex);
        } else {
            // directly using current index
            memcpy(memoryPool_[currentIndex_], data, length);
            Cache_->insert(cacheKey, currentIndex_);
            currentIndex_++;
        }
    }
}

bool LRUCache::existsInCache(string& cacheKey)
{
    bool flag = false;
    {
        // boost::shared_lock<boost::shared_mutex> t(this->mtx);
        flag = this->Cache_->contains(cacheKey);
    }
    return flag;
}

uint8_t* LRUCache::getFromCache(string& cacheKey)
{
    {
        // boost::shared_lock<boost::shared_mutex> t(this->mtx);
        uint32_t index = this->Cache_->get(cacheKey);
        return memoryPool_[index];
    }
}

LRUCache::~LRUCache()
{
    for (size_t i = 0; i < cacheSize_; i++) {
        free(memoryPool_[i]);
    }
    free(memoryPool_);
    delete Cache_;
}

}