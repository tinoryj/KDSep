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

typedef struct hashStoreFileMetaDataHandler {
    uint64_t target_file_id_;
    uint64_t total_object_count_;
    uint64_t total_object_bytes_;
    fstream file_operation_stream_;
    boost::shared_mutex fileOperationMutex_;
} hashStoreFileMetaDataHandler;

typedef struct hashStoreOperationHandler {
    hashStoreFileMetaDataHandler* file_handler;
    string* key_str_;
    string* value_str_;
    bool is_anchor = false;
    bool jobDone = false;
} hashStoreOperationHandler;

enum hashStoreFileCreateReason { newFile = 0,
    gcFile = 1 };

typedef struct hashStoreFileHeader {
    uint64_t file_id_;
    uint64_t prefix_record_in_int_;
    hashStoreFileCreateReason file_create_reason_;
} hashStoreFileHeader;

typedef struct hashStoreRecordHeader {
    uint32_t key_size_;
    uint32_t value_size_;
    bool is_anchor_;
} hashStoreRecordHeader;