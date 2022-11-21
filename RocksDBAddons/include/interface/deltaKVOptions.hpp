#pragma once

#include "rocksdb/options.h"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class DeltaKVOptions {
public:
    DeltaKVOptions() = default;
    ~DeltaKVOptions() = default;

    rocksdb::Options rocksdbRawOptions_;

    enum class contentStoreMode {
        kAppendOnlyLogWithIndex = 0,
        kHashBasedBucketWithoutIndex = 1,
        kErrorUnknownStoreMode = 2,
    }; // Mainly used for deltaStore (Baseline and Current Design)

    enum class contentCacheMode {
        kLRUCache = 0,
        kAdaptiveReplacementCache = 1,
        kErrorUnknownCacheMode = 2,
    };

    // deltaStore options
    bool enable_deltaStore = false;
    bool enable_deltaStore_fileLvel_cache = false;
    bool enable_deltaStore_KDLevel_cahce = false;
    bool enable_deltaStore_garbage_collection = false;
    contentCacheMode deltaStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode deltaStore_base_store_mode = contentStoreMode::kAppendOnlyLogWithIndex;
    uint32_t extract_to_deltaStore_size_lower_bound = 0;
    uint32_t extract_to_deltaStore_size_upper_bound = 0x3f3f3f;
    uint32_t deltaStore_single_file_maximum_size = 1 * 1024 * 1024;
    uint32_t deltaStore_total_storage_maximum_size = 1024 * 1024 * 1024 * 1024;
    float deltaStore_garbage_collection_start_single_file_minimum_occupancy = 0.8;
    float deltaStore_garbage_collection_start_total_storage_minimum_occupancy = 0.8;
    float deltaStore_garbage_collection_force_single_file_minimum_occupancy = 0.95;
    float deltaStore_garbage_collection_force_total_storage_minimum_occupancy = 0.95;

    // valueStore options
    bool enable_valueStore = false;
    bool enable_valueStore_fileLvel_cache = false;
    bool enable_valueStore_KDLevel_cahce = false;
    bool enable_valueStore_garbage_collection = false;
    contentCacheMode valueStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode valueStore_base_store_mode = contentStoreMode::kAppendOnlyLogWithIndex;
    uint32_t extract_to_valueStore_size_lower_bound = 0;
    uint32_t extract_to_valueStore_size_upper_bound = 0x3f3f3f;
    uint32_t valueStore_single_file_maximum_size = 1 * 1024 * 1024;
    uint32_t valueStore_total_storage_maximum_size = 1024 * 1024 * 1024 * 1024;
    float valueStore_garbage_collection_start_single_file_minimum_occupancy = 0.8;
    float valueStore_garbage_collection_start_total_storage_minimum_occupancy = 0.8;
    float valueStore_garbage_collection_force_single_file_minimum_occupancy = 0.95;
    float valueStore_garbage_collection_force_total_storage_minimum_occupancy = 0.95;

    bool dumpOptions(string dumpPath);
};

} // namespace DELTAKV_NAMESPACE
