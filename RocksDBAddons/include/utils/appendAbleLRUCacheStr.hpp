#pragma once

#include "common/dataStructure.hpp" 

using namespace std;

namespace DELTAKV_NAMESPACE {

typedef str_t key_type;
typedef vector<str_t>* value_type;
typedef std::list<key_type> list_type;
typedef std::unordered_map<key_type, std::pair<value_type, list_type::iterator>, 
        mapHashKeyForStr_t, mapEqualKeForStr_t> map_type;

class lru_cache_str_t {
public:

    lru_cache_str_t(size_t capacity)
        : m_capacity(capacity)
    {
    }

    ~lru_cache_str_t()
    {
        debug_error("deconstruct %lu\n", m_map.size());
        clear();
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

    void insert(key_type& key, value_type value)
    {
        map_type::iterator i = m_map.find(key);
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

    value_type get(key_type& key)
    {
        // lookup value in the cache
        map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            return nullptr;
        }

        str_t keyStored = i->first;
        // return the value, but first update its place in the most
        // recently used list
        list_type::iterator j = i->second.second;
        if (j != m_list.begin()) {
            // move item to the front of the most recently used list
            m_list.erase(j);
            m_list.push_front(keyStored);

            // update iterator in map
            j = m_list.begin();
            value_type value = i->second.first;
            m_map[keyStored] = std::make_pair(value, j);

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
        uint64_t valueSizes = 0;
        uint64_t keySizes = 0;
        m_list.clear();
        for (auto& it : m_map) {
            for (auto& it0 : *it.second.first) {
                valueSizes += it0.size_;
                delete[] it0.data_;
            }
            keySizes += it.first.size_;
            delete[] it.first.data_;
            delete it.second.first;
        }
        debug_error("clear, key total sizes %lu, value total sizes %lu\n", keySizes, valueSizes);
        m_map.clear();
    }

private:
    void evict()
    {
        // evict item from the end of most recently used list
        list_type::iterator i = --m_list.end();
        vector<char*> char_ptr_vec;
        vector<str_t>* str_vec_ptr = m_map[*i].first; 

        // values
        for (auto& it : *str_vec_ptr) {
            char_ptr_vec.push_back(it.data_);
        }
        // key
        char_ptr_vec.push_back(i->data_);

        m_map.erase(*i);
        m_list.erase(i);
        for (auto& it : char_ptr_vec) {
            delete[] it;
        }
        delete str_vec_ptr;
    }

private:
    map_type m_map;
    list_type m_list;
    size_t m_capacity;
};

class AppendAbleLRUCacheStrT {
private:
    lru_cache_str_t* Cache_;
    uint64_t cacheSize_ = 0;
    std::shared_mutex cacheMtx_;

public:
    AppendAbleLRUCacheStrT(uint64_t cacheSize) {
        cacheSize_ = cacheSize;
        Cache_ = new lru_cache_str_t(cacheSize_);
    }

    ~AppendAbleLRUCacheStrT() {
        delete Cache_;
    }
    void insertToCache(str_t& cacheKey, value_type data) {
        std::scoped_lock<std::shared_mutex> w_lock(cacheMtx_);
        Cache_->insert(cacheKey, data);
    }

    bool existsInCache(str_t& cacheKey) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        bool status = Cache_->contains(cacheKey);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    value_type getFromCache(str_t& cacheKey) {
        std::scoped_lock<std::shared_mutex> r_lock(cacheMtx_);
        value_type newValue = Cache_->get(cacheKey);
        return newValue;
    }
};

} // DELTAKV_NAMESPACE
