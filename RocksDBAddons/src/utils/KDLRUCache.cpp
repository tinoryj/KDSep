#pragma once

#include "utils/KDLRUCache.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

KDLRUCache::~KDLRUCache() {
    size_t total_data_size = 0;
    size_t rss_before = getRss();
    for (int i = 0; i < shard_num_; i++) {
        total_data_size += shards_[i]->getDataSize();
        delete shards_[i];
    }
    delete[] shards_;
    debug_error("total_data_size: %lu\n", total_data_size);
    size_t rss_after = getRss();
    debug_error("rss from %lu to %lu (diff: %.4lf)\n", 
           rss_before, rss_after, 
           (rss_before - rss_after) / 1024.0 / 1024.0); 
}

} // DELTAKV_NAMESPACE
