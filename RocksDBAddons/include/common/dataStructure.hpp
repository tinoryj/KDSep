#pragma once

#include "boost/thread.hpp"
#include <bits/stdc++.h>

using namespace std;

typedef struct internalValueType {
    bool mergeFlag_; // true if the value request merge.
    bool valueSeparatedFlag_; // true if the value is stored outside LSM-tree
    uint32_t rawValueSize_; // store the raw value size, in case some delta are not separated.
} internalValueType;

typedef struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
} externalIndexInfo;

enum hashStoreFileCreateReason { kNewFile = 0,
    kGCFile = 1 };

enum hashStoreFileOperationType { kPut = 0,
    kGet = 1 };

enum hashStoreFileGCType { kNew = 0, // newly created files (or only gc internal files)
    kMayGC = 1, // tried gc by start threshold, but could not done internal gc or split right now， waiting for force threshold
    kNoGC = 2, // tried gc by force threshold, but could not done internal gc or split, mark as not gc forever
    kShouldDelete = 3 }; // gc done, split/merge to new file, this file should be delete

typedef struct hashStoreFileMetaDataHandler {
    uint64_t target_file_id_ = 0;
    uint64_t current_prefix_used_bit_ = 0;
    uint64_t total_object_count_ = 0;
    uint64_t total_object_bytes_ = 0;
    uint64_t temp_not_flushed_data_bytes_ = 0;
    hashStoreFileGCType gc_result_status_flag_ = kNew;
    int8_t file_ownership_flag_ = 0; // 0-> file not in use, 1->file belongs to user, -1->file belongs to GC
    fstream file_operation_stream_;
    boost::shared_mutex fileOperationMutex_;
} hashStoreFileMetaDataHandler;

typedef struct hashStoreWriteOperationHandler {
    string* key_str_;
    string* value_str_;
    bool is_anchor = false;
} hashStoreWriteOperationHandler;

typedef struct hashStoreReadOperationHandler {
    string* key_str_;
    vector<string>* value_str_vec_;
} hashStoreReadOperationHandler;

typedef struct hashStoreOperationHandler {
    hashStoreFileMetaDataHandler* file_handler_;
    hashStoreWriteOperationHandler write_operation_;
    hashStoreReadOperationHandler read_operation_;
    hashStoreFileOperationType opType_;
    bool jobDone = false;
    hashStoreOperationHandler(hashStoreFileMetaDataHandler* file_handler) { file_handler_ = file_handler; };
} hashStoreOperationHandler;

typedef struct hashStoreFileHeader {
    uint64_t file_id_;
    uint64_t previous_file_id_ = 0xffffffffffffffff; // only used for file create reason == kGCFile
    uint64_t current_prefix_used_bit_;
    hashStoreFileCreateReason file_create_reason_;
} hashStoreFileHeader;

typedef struct hashStoreRecordHeader {
    uint32_t key_size_;
    uint32_t value_size_;
    bool is_anchor_;
    bool is_gc_done_ = false; // to mark gc job done
} hashStoreRecordHeader;