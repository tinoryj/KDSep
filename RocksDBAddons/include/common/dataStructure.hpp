#pragma once

#include "boost/thread.hpp"
#include "utils/fileOperation.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>

using namespace std;

namespace DELTAKV_NAMESPACE {
typedef struct internalValueType {
    bool mergeFlag_; // true if the value request merge.
    bool valueSeparatedFlag_; // true if the value is stored outside LSM-tree
    uint32_t sequenceNumber_; // global sequence number
    uint32_t rawValueSize_; // store the raw value size, in case some delta are not separated.
} internalValueType;

typedef struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
} externalIndexInfo;

enum DBOperationType { kPutOp = 0,
    kMergeOp = 1 };

enum hashStoreFileCreateReason { kNewFile = 0,
    kInternalGCFile = 1,
    kSplitFile = 2,
    kMergeFile = 3 };

enum hashStoreFileOperationType { kPut = 0,
    kGet = 1,
    kMultiPut = 2 };

enum hashStoreFileGCType { kNew = 0, // newly created files (or only gc internal files)
    kMayGC = 1, // tried gc by start threshold, but could not done internal gc or split right now， waiting for force threshold
    kNoGC = 2, // tried gc by force threshold, but could not done internal gc or split, mark as not gc forever
    kNeverGC = 3, // if GC, the file will exceed trie bit number limit
    kShouldDelete = 4 }; // gc done, split/merge to new file, this file should be delete

typedef struct hashStoreFileMetaDataHandler {
    uint64_t target_file_id_ = 0;
    uint64_t previous_file_id_first_; // for merge, should contain two different previous file id
    uint64_t previous_file_id_second_; // for merge, should contain two different previous file id
    uint64_t current_prefix_used_bit_ = 0;
    hashStoreFileCreateReason file_create_reason_ = kNewFile;
    uint64_t total_object_count_ = 0;
    uint64_t total_object_bytes_ = 0;
    uint64_t total_on_disk_bytes_ = 0;
    uint64_t no_gc_wait_operation_number_ = 0;
    uint64_t temp_not_flushed_data_bytes_ = 0;
    hashStoreFileGCType gc_result_status_flag_ = kNew;
    int8_t file_ownership_flag_ = 0; // 0-> file not in use, 1->file belongs to user, -1->file belongs to GC
    FileOperation* file_operation_func_ptr_;
    std::shared_mutex fileOperationMutex_;
    unordered_map<string, uint32_t> bufferedUnFlushedAnchorsVec_;
} hashStoreFileMetaDataHandler;

typedef struct hashStoreWriteOperationHandler {
    string* key_str_;
    string* value_str_;
    uint32_t sequence_number_;
    bool is_anchor = false;
} hashStoreWriteOperationHandler;

typedef struct hashStoreBatchedWriteOperationHandler {
    vector<string>* key_str_vec_ptr_;
    vector<string>* value_str_vec_ptr_;
    vector<uint32_t>* sequence_number_vec_ptr_;
    vector<bool>* is_anchor_vec_ptr_;
} hashStoreBatchedWriteOperationHandler;

typedef struct hashStoreReadOperationHandler {
    string* key_str_;
    vector<string>* value_str_vec_;
} hashStoreReadOperationHandler;

enum operationStatus {
    kDone = 1,
    kNotDone = 2,
    kError = 3
};

typedef struct hashStoreOperationHandler {
    hashStoreFileMetaDataHandler* file_handler_;
    hashStoreWriteOperationHandler write_operation_;
    hashStoreReadOperationHandler read_operation_;
    hashStoreBatchedWriteOperationHandler batched_write_operation_;
    hashStoreFileOperationType opType_;
    operationStatus jobDone_ = kNotDone;
    hashStoreOperationHandler(hashStoreFileMetaDataHandler* file_handler) { file_handler_ = file_handler; };
} hashStoreOperationHandler;

typedef struct hashStoreFileHeader {
    uint64_t file_id_;
    uint64_t previous_file_id_first_ = 0xffffffffffffffff; // used for file create reason == kInternalGCFile || kSplitFile || kMergeFile
    uint64_t previous_file_id_second_ = 0xffffffffffffffff; // only used for file create reason == kMergeFile
    uint64_t current_prefix_used_bit_;
    hashStoreFileCreateReason file_create_reason_;
} hashStoreFileHeader;

typedef struct hashStoreRecordHeader {
    uint32_t key_size_;
    uint32_t value_size_;
    uint32_t sequence_number_;
    bool is_anchor_;
    bool is_gc_done_ = false; // to mark gc job done
} hashStoreRecordHeader;

// following enums are used for indexStore only
enum CodingScheme {
    RAID0,
    REPLICATION,
    RAID5,
    RDP,
    EVENODD,
    CAUCHY,
    DEFAULT
};

enum DataType {
    KEY,
    VALUE,
    META
};

enum DiskType {
    DATA,
    LOG,
    MIXED
};

enum RequestType {
    READ = 0x00,
    WRITE = 0x01,
    FLUSH = 0x10,
    COMMIT = 0x20,
    WIRTE_KEY = 0x03,
    READ_VALUE = 0x04,
    WIRTE_VALUE = 0x05,
};

enum class DebugLevel : int {
    NONE,
    ERROR,
    WARN,
    INFO,
    TRACE,
    ANY
};

enum DBType {
    LEVEL = 0x00,
};

enum GCMode {
    ALL, // 0
    LOG_ONLY, // 1
};

}
