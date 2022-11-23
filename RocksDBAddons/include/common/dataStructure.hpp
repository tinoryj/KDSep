#pragma once

#include "boost/thread.hpp"
#include <bits/stdc++.h>

using namespace std;

typedef struct externalValueType {
    bool mergeFlag_;
} externalValueType;

typedef struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
} externalIndexInfo;

enum hashStoreFileCreateReason { kNewFile = 0,
    kGCFile = 1 };

enum hashStoreFileOperationType { kPut = 0,
    kGet = 1 };

typedef struct hashStoreFileMetaDataHandler {
    uint64_t target_file_id_;
    uint64_t current_prefix_used_bit_;
    uint64_t total_object_count_;
    uint64_t total_object_bytes_;
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

typedef union hashStoreOperationHandler {
    hashStoreFileMetaDataHandler* file_handler;
    hashStoreWriteOperationHandler write_operation_;
    hashStoreReadOperationHandler read_operation_;
    hashStoreFileOperationType opType_;
    bool jobDone = false;
    hashStoreOperationHandler() {};
} hashStoreOperationHandler;

typedef struct hashStoreFileHeader {
    uint64_t file_id_;
    uint64_t current_prefix_used_in_int_;
    uint64_t current_prefix_used_bit_;
    hashStoreFileCreateReason file_create_reason_;
} hashStoreFileHeader;

typedef struct hashStoreRecordHeader {
    uint32_t key_size_;
    uint32_t value_size_;
    bool is_anchor_;
} hashStoreRecordHeader;