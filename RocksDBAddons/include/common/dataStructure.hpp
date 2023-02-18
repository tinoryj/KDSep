#pragma once

#include "boost/thread.hpp"
#include "utils/fileOperation.hpp"
#include "utils/mempool.hpp"
#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <shared_mutex>

using namespace std;

namespace DELTAKV_NAMESPACE {

class BucketKeyFilter;

typedef struct str_t {
    char* data_;
    uint32_t size_;
    str_t() { }
    str_t(char* data, uint32_t size)
        : data_(data)
        , size_(size)
    {
    }
} str_t;

typedef struct str_cpy_t {
    char* data_ = nullptr;
    uint32_t size_ = 0;
    str_cpy_t() {
        data_ = nullptr;
        size_ = 0;
    }
    str_cpy_t(char* data, uint32_t size)
    {
        data_ = new char[size];
        memcpy(data_, data, size);
        size_ = size;
    }
    ~str_cpy_t()
    {
        if (data_ != nullptr) {
            delete[] data_;
        }
    }
} str_cpy_t;

static unsigned int charBasedHashFunc(char* data, uint32_t n)
{
    unsigned int hash = 388650013;
    unsigned int scale = 388650179;
    unsigned int hardener = 1176845762;
    for (uint32_t i = 0; i < n; i++) {
        hash *= scale;
        hash += (data[i]);
    }
    return hash ^ hardener;
}

struct mapEqualKeForStr_t {
    bool operator()(str_t const& a, str_t const& b) const
    {
        if (a.size_ == b.size_) {
            return (memcmp(a.data_, b.data_, a.size_) == 0);
        }
        return false;
    }
};

struct mapHashKeyForStr_t {
    size_t operator()(str_t const& s) const
    {
        return charBasedHashFunc(s.data_, s.size_);
    }
};

struct mapEqualKeForMemPoolHandler_t {
    bool operator()(mempoolHandler_t const& a, mempoolHandler_t const& b) const
    {
        return (memcmp(a.keyPtr_, b.keyPtr_, a.keySize_) == 0);
    }
};

struct mapHashKeyForMemPoolHandler_t {
    size_t operator()(mempoolHandler_t const& s) const
    {
        return charBasedHashFunc(s.keyPtr_, s.keySize_);
    }
};

// Put:
//    false, true: value in vLog
//    false, false: value in LSM-tree
// Merge:
//    false, true: delta in dStore
//    false, false: delta in LSM-tree
//    true, true: delta for value position in LSM-tree
// Get:
//    true, true: delta in dStore, value in vLog
//    true, false: delta in dStore, value in LSM-tree
//    false, true: delta in LSM-tree, value in vLog
//    false, false: delta in LSM-tree
struct internalValueType {
    bool mergeFlag_ = false; // true if the value request merge.
    bool valueSeparatedFlag_ = false; // true if the value is stored outside LSM-tree
    uint32_t sequenceNumber_ = 0; // global sequence number
    uint32_t rawValueSize_ = 0; // store the raw value size, in case some delta are not separated.
    internalValueType() {} 
    internalValueType(bool mergeFlag, bool valueSeparatedFlag, uint32_t sequenceNumber, uint32_t rawValueSize) : mergeFlag_(mergeFlag), valueSeparatedFlag_(valueSeparatedFlag), sequenceNumber_(sequenceNumber), rawValueSize_(rawValueSize) {} 
};

struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
    externalIndexInfo()
    {
    }
    externalIndexInfo(uint32_t externalFileID,
        uint32_t externalFileOffset,
        uint32_t externalContentSize)
        : externalFileID_(externalFileID)
        , externalFileOffset_(externalFileOffset)
        , externalContentSize_(externalContentSize)
    {
    }
};

enum DBOperationType { kPutOp = 0,
    kMergeOp = 1 };

enum hashStoreFileCreateReason { kNewFile = 0,
    kInternalGCFile = 1,
    kSplitFile = 2,
    kMergeFile = 3,
    kRewritedObjectFile = 4 };

enum hashStoreFileOperationType { kPut = 0,
    kGet = 1,
    kMultiPut = 2 };

enum hashStoreFileGCType { kNew = 0, // newly created files (or only gc internal files)
    kMayGC = 1, // tried gc by start threshold, but could not done internal gc or split right now， waiting for force threshold
    kNoGC = 2, // tried gc by force threshold, but could not done internal gc or split, mark as not gc forever
    kNeverGC = 3, // if GC, the file will exceed trie bit number limit
    kShouldDelete = 4 }; // gc done, split/merge to new file, this file should be delete

struct hashStoreFileMetaDataHandler {
    uint64_t target_file_id_ = 0;
    uint64_t previous_file_id_first_; // for merge, should contain two different previous file id
    uint64_t previous_file_id_second_; // for merge, should contain two different previous file id
    uint64_t current_prefix_used_bit_ = 0;
    hashStoreFileCreateReason file_create_reason_ = kNewFile;
    uint64_t total_object_count_ = 0;
    uint64_t total_object_bytes_ = 0;
    uint64_t total_on_disk_bytes_ = 0;
    uint64_t no_gc_wait_operation_number_ = 0;
    hashStoreFileGCType gc_result_status_flag_ = kNew;
    bool markedByMultiPut_ = false;
    int8_t file_ownership_flag_ = 0; // 0-> file not in use, 1->file belongs to write, -1->file belongs to GC
    FileOperation* file_operation_func_ptr_;
    std::shared_mutex fileOperationMutex_;
//    std::unordered_set<string> storedKeysSet_;
    BucketKeyFilter* filter = nullptr;
};

typedef struct hashStoreWriteOperationHandler {
    mempoolHandler_t* mempoolHandler_ptr_;
} hashStoreWriteOperationHandler;

typedef struct hashStoreBatchedWriteOperationHandler {
    vector<mempoolHandler_t>* mempool_handler_vec_ptr_;
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
    hashStoreOperationHandler(hashStoreFileMetaDataHandler* file_handler)
        : file_handler_(file_handler) {};
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

typedef struct writeBackObjectStruct {
    string key;
    string value;
    uint32_t sequenceNumber;
    writeBackObjectStruct(string keyIn, string valueIn, uint32_t sequenceNumberIn)
    {
        key = keyIn;
        value = valueIn;
        sequenceNumber = sequenceNumberIn;
    };
    writeBackObjectStruct() {};
} writeBackObjectStruct; // key to value pair fpr write back

// following enums are used for indexStore only
enum DataType {
    KEY,
    VALUE,
    META
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
