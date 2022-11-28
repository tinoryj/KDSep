#pragma once

#include <bits/stdc++.h>
#include <boost/thread.hpp>

using namespace std;

namespace DELTAKV_NAMESPACE {

template <class Key, class Value>
class lru_cache {
public:
    typedef Key key_type;
    typedef Value value_type;
    typedef std::list<key_type> list_type;
    typedef std::map<
        key_type,
        std::pair<value_type, typename list_type::iterator>>
        map_type;

    lru_cache(size_t capacity)
        : m_capacity(capacity)
    {
    }

    ~lru_cache()
    {
    }

    size_t size()
    {
        return m_map.size();
    }

    size_t capacity()
    {
        return m_capacity;
    }

    bool empty()
    {
        return m_map.empty();
    }

    bool contains(key_type& key)
    {
        return m_map.find(key) != m_map.end();
    }

    void insert(key_type& key, value_type& value)
    {
        typename map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            // insert item into the cache, but first check if it is full
            if (size() >= m_capacity) {
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            m_list.push_front(key);
            m_map[key] = std::make_pair(value, m_list.begin());
        }
    }

    value_type& get(key_type& key)
    {
        // lookup value in the cache
        typename map_type::iterator i = m_map.find(key);
        // return the value, but first update its place in the most
        // recently used list
        typename list_type::iterator j = i->second.second;
        if (j != m_list.begin()) {
            // move item to the front of the most recently used list
            m_list.erase(j);
            m_list.push_front(key);

            // update iterator in map
            j = m_list.begin();
            value_type& value = i->second.first;
            m_map[key] = std::make_pair(value, j);

            // return the value
            return value;
        } else {
            // the item is already at the front of the most recently
            // used list so just return it
            return i->second.first;
        }
    }

    void clear()
    {
        m_map.clear();
        m_list.clear();
    }

private:
    void evict()
    {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --m_list.end();
        m_map.erase(*i);
        m_list.erase(i);
    }

private:
    map_type m_map;
    list_type m_list;
    size_t m_capacity;
};

template <typename keyType, typename valueType>
class AppendAbleLRUCache {
private:
    lru_cache<keyType, valueType>* Cache_;
    uint64_t cacheSize_ = 0;

public:
    AppendAbleLRUCache(uint64_t cacheSize);
    ~AppendAbleLRUCache();
    void insertToCache(keyType& cacheKey, valueType& data);
    bool existsInCache(keyType& cacheKey);
    valueType& getFromCache(keyType& cacheKey);
};

template <typename keyType, typename valueType>
AppendAbleLRUCache<keyType, valueType>::AppendAbleLRUCache(uint64_t cacheSize)
{
    cacheSize_ = cacheSize;
    Cache_ = new lru_cache<keyType, valueType>(cacheSize_);
}

template <typename keyType, typename valueType>
AppendAbleLRUCache<keyType, valueType>::~AppendAbleLRUCache()
{
    delete Cache_;
}

template <typename keyType, typename valueType>
void AppendAbleLRUCache<keyType, valueType>::insertToCache(keyType& cacheKey, valueType& data)
{
    Cache_->insert(cacheKey, data);
}

template <typename keyType, typename valueType>
bool AppendAbleLRUCache<keyType, valueType>::existsInCache(keyType& cacheKey)
{
    if (Cache_->contains(cacheKey)) {
        return true;
    } else {
        return false;
    }
}

template <typename keyType, typename valueType>
valueType& AppendAbleLRUCache<keyType, valueType>::getFromCache(keyType& cacheKey)
{
    return Cache_->get(cacheKey);
}

} // DELTAKV_NAMESPACE