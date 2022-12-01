#pragma once

#include "common/dataStructure.hpp"
#include "interface/mergeOperation.hpp"
#include "rocksdb/options.h"
#include "utils/fileOperation.hpp"
#include "utils/loggerColor.hpp"
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
    bool enable_deltaStore_KDLevel_cache = false;
    bool enable_deltaStore_garbage_collection = false;
    contentCacheMode deltaStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode deltaStore_base_store_mode = contentStoreMode::kHashBasedBucketWithoutIndex;
    uint64_t deltaStore_KDLevel_cache_item_number = 1 * 1024 * 1024;
    uint64_t deltaStore_KDLevel_cache_peritem_value_number = 1 * 1024;
    uint64_t extract_to_deltaStore_size_lower_bound = 0;
    uint64_t extract_to_deltaStore_size_upper_bound = 0x3f3f3f;
    uint64_t deltaStore_single_file_maximum_size = 1 * 1024 * 1024;
    uint64_t deltaStore_total_storage_maximum_size = 1024 * 1024 * deltaStore_single_file_maximum_size;
    uint64_t deltaStore_thread_number_limit = 3;
    uint64_t deltaStore_file_flush_buffer_size_limit_ = 4096;
    float deltaStore_garbage_collection_start_single_file_minimum_occupancy = 0.8;
    float deltaStore_garbage_collection_start_total_storage_minimum_occupancy = 0.8;
    float deltaStore_garbage_collection_force_single_file_minimum_occupancy = 0.95;
    float deltaStore_garbage_collection_force_total_storage_minimum_occupancy = 0.95;

    // valueStore options
    bool enable_valueStore = false;
    bool enable_valueStore_fileLvel_cache = false;
    bool enable_valueStore_KDLevel_cache = false;
    bool enable_valueStore_garbage_collection = false;
    contentCacheMode valueStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode valueStore_base_store_mode = contentStoreMode::kAppendOnlyLogWithIndex;
    uint64_t valueStore_fileLvel_cache_size = 1 * 1024 * 1024 * 1024;
    uint64_t valueStore_KDLevel_cache_size = 1 * 1024 * 1024 * 1024;
    uint64_t extract_to_valueStore_size_lower_bound = 0;
    uint64_t extract_to_valueStore_size_upper_bound = 0x3f3f3f;
    uint64_t valueStore_single_file_maximum_size = 1 * 1024 * 1024;
    uint64_t valueStore_total_storage_maximum_size = 1024 * 1024 * valueStore_single_file_maximum_size;
    uint64_t valueStore_thread_number_limit = 3;
    float valueStore_garbage_collection_start_single_file_minimum_occupancy = 0.8;
    float valueStore_garbage_collection_start_total_storage_minimum_occupancy = 0.8;
    float valueStore_garbage_collection_force_single_file_minimum_occupancy = 0.95;
    float valueStore_garbage_collection_force_total_storage_minimum_occupancy = 0.95;

    // common options
    uint64_t deltaKV_thread_number_limit = 4;
    uint64_t hashStore_init_prefix_bit_number = 8;
    uint64_t hashStore_max_prefix_bit_number = 16;
    shared_ptr<DeltaKVMergeOperator> deltaKV_merge_operation_ptr;
    fileOperationType fileOperationMethod_ = kDirectIO;

    bool dumpOptions(string dumpPath);
    bool dumpDataStructureInfo(string dumpPath);
};

} // namespace DELTAKV_NAMESPACE