#include "utils/KDLRUCache.hpp"

using namespace std;

namespace KDSEP_NAMESPACE {

lru_cache_str_t::~lru_cache_str_t() {
//        debug_error("deconstruct %lu, data size %lu\n", m_map.size(), data_size);
    clear();
}

void lru_cache_str_t::clear() {
    m_list.clear();
    for (auto& it : m_map) {
        if (it.second.first.size_ > 0) {
            delete[] it.second.first.data_; // delete value
        }
        delete[] it.first.data_; // delete key
    }
    m_map.clear();
    data_size = 0;
}

KDLRUCache::~KDLRUCache() {
    size_t total_data_size = 0;
    size_t num_items = 0;
    size_t rss_before = getRss();
    for (int i = 0; i < shard_num_; i++) {
        total_data_size += shards_[i]->getDataSize();
        num_items += shards_[i]->getNumberOfItems();
        delete shards_[i];
    }
    delete[] shards_;
//    debug_error("total_data_size: %lu\n", total_data_size);
//    debug_error("total_items: %lu\n", num_items);
//    size_t rss_after = getRss();
//    debug_error("rss from %lu to %lu (diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
//    printf("total_data_size: %lu\n", total_data_size);
//    printf("total_items: %lu\n", num_items);
//    printf("rss from %lu to %lu (diff: %.4lf)\n", 
//           rss_before, rss_after, 
//           (rss_before - rss_after) / 1024.0 / 1024.0); 
}

} // KDSEP_NAMESPACE
