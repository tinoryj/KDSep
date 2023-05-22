#pragma once

#include "common/dataStructure.hpp"
#include "interface/mergeOperation.hpp"
#include "rocksdb/options.h"
#include "utils/KDLRUCache.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/appendAbleLRUCacheStrVector.hpp"
#include "utils/debug.hpp"
#include "utils/fileOperation.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

class KDSepOptions {
public:
    KDSepOptions() = default;
    ~KDSepOptions() = default;

    rocksdb::Options rocks_opt;

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
    bool enable_deltaStore_KDLevel_cache = false;
    bool enable_deltaStore_garbage_collection = false;
    contentCacheMode deltaStore_base_cache_mode = contentCacheMode::kLRUCache;
    contentStoreMode deltaStore_base_store_mode = contentStoreMode::kHashBasedBucketWithoutIndex;
    uint64_t deltaStore_KDCache_item_number_ = 1 * 1024 * 1024;
    bool deltaStore_KDLevel_cache_use_str_t = true;
    uint64_t deltaStore_KDLevel_cache_peritem_value_number = 1 * 1024;
    uint64_t extract_to_deltaStore_size_lower_bound = 0;
    uint64_t extract_to_deltaStore_size_upper_bound = 0x3f3f3f;
    uint64_t deltaStore_bucket_size_ = 1 * 1024 * 1024;
    uint64_t deltaStore_total_storage_maximum_size = 64 * 1024 * deltaStore_bucket_size_;
    uint64_t deltaStore_op_worker_thread_number_limit_ = 4;
    uint64_t deltaStore_gc_worker_thread_number_limit_ = 1;
    uint64_t deltaStore_file_flush_buffer_size_limit_ = 4096;
    uint64_t deltaStore_operationNumberForMetadataCommitThreshold_ = 10000;
    uint64_t deltaStore_operationNumberForForcedSingleFileGCThreshold_ = 10000;
    float deltaStore_garbage_collection_start_single_file_minimum_occupancy = 0.8;
    float deltaStore_gc_split_threshold_ = 0.4;
    float deltaStore_garbage_collection_start_total_storage_minimum_occupancy = 0.8;
    float deltaStore_garbage_collection_force_single_file_minimum_occupancy = 0.95;
    float deltaStore_garbage_collection_force_total_storage_minimum_occupancy = 0.95;
    uint64_t deltaStore_write_back_during_reads_threshold = 5;
    uint64_t deltaStore_write_back_during_reads_size_threshold = 5;
    uint64_t deltaStore_gc_write_back_delta_num = 5;
    uint64_t deltaStore_gc_write_back_delta_size = 1000;
    uint64_t deltaStore_init_k_ = 999;
    uint32_t deltaStore_mem_pool_object_number_ = 5;
    uint32_t deltaStore_mem_pool_object_size_ = 4096;
    uint64_t unsorted_part_size_threshold = 1024 * 1024 * 1024;

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
    uint64_t KDSep_thread_number_limit = 4;
    uint64_t deltaStore_max_bucket_number_ = 16;
    shared_ptr<KDSepMergeOperator> KDSep_merge_operation_ptr;
    fileOperationType fileOperationMethod_ = kDirectIO;
    bool enable_write_back_optimization_ = true;
    bool enable_parallel_lsm_interface_ = false;
    bool enable_crash_consistency = false;
    bool enable_bucket_merge = true;
    bool enable_batched_operations_ = false;
    bool enable_key_value_cache_ = false;
    bool enable_lsm_tree_delta_meta = false;
    uint64_t key_value_cache_object_number_ = 1000;
    uint64_t write_buffer_num = 5;
    uint64_t write_buffer_size = 2 * 1024 * 1024;
    bool rocksdb_sync_put = false;
    bool rocksdb_sync_merge = false;
    bool rocksdb_sync = false;

    bool enable_index_block = true;
    bool test_recovery = false;

    AppendAbleLRUCacheStrVector* keyToValueListCacheStr_ = nullptr;
    KDLRUCache* kd_cache = nullptr;
    //    boost::atomic<bool>* write_stall = nullptr;
    bool* write_stall = nullptr;
    std::queue<string>* wb_keys = nullptr;
    std::mutex* wb_keys_mutex = nullptr;

    // dump options
    bool dumpOptions(string dumpPath);
    bool dumpDataStructureInfo(string dumpPath);
};

} // namespace KDSEP_NAMESPACE
