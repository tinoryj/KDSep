#pragma once

#include "boost/thread.hpp"
#include "utils/utils.hpp"
#include "utils/fileOperation.hpp"
#include "utils/mempool.hpp"
#include "utils/xxhash.h"
#include <boost/atomic.hpp>
#include <condition_variable>
#include <shared_mutex>
#include <string_view>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/bind/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>
#include "common/rocksdbHeaders.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

class BucketIndexBlock;
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

static unsigned int charBasedHashFunc(char* data, uint32_t n)
{
    unsigned int hash = 388650013;
    unsigned int scale = 388650179;
    unsigned int hardener = 1176845762;
    uint32_t i;
    for (i = 0; i < n / 4 * 4; i+=4) {
        hash *= scale;
        hash += *((uint32_t*)(data + i));
    }
    for (; i < n; i++) {
        hash *= scale;
        hash += data[i];
    }
    return hash ^ hardener;
}

static unsigned int charBasedHashFuncConst(const char* data, uint32_t n)
{
    unsigned int hash = 388650013;
    unsigned int scale = 388650179;
    unsigned int hardener = 1176845762;
    uint32_t i;
    for (i = 0; i < n / 4 * 4; i+=4) {
        hash *= scale;
        hash += *((uint32_t*)(data + i));
    }
    for (; i < n; i++) {
        hash *= scale;
        hash += data[i];
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

struct mapEqualKeyForSlice {
    bool operator()(rocksdb::Slice const& a, rocksdb::Slice const& b) const
    {
        if (a.size() == b.size()) {
            return (memcmp(a.data(), b.data(), a.size()) == 0);
        }
        return false;
    }
};

struct mapHashKeyForSlice {
    size_t operator()(rocksdb::Slice const& s) const
    {
        return charBasedHashFuncConst(s.data(), s.size());
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
// header size: 12 bytes
struct KvHeader {
    bool mergeFlag_ = false; // true if the value request merge.
    bool valueSeparatedFlag_ = false; // true if the value is stored outside LSM-tree
    uint32_t sequenceNumber_ = 0; // global sequence number
    uint32_t rawValueSize_ = 0; // store the raw value size, in case some delta are not separated.
    KvHeader() {} 
    KvHeader(bool mergeFlag, bool valueSeparatedFlag, uint32_t sequenceNumber,
            uint32_t rawValueSize) : mergeFlag_(mergeFlag),
    valueSeparatedFlag_(valueSeparatedFlag), sequenceNumber_(sequenceNumber),
    rawValueSize_(rawValueSize) {} 
};

// index size: 12 bytes
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
    kMultiGet = 2,
    kMultiPut = 3,
    kFlush = 4,
    kFind = 5};

enum hashStoreFileGCType { kNew = 0, // newly created files (or only gc internal files)
    kMayGC = 1, // tried gc by start threshold, but could not done internal gc or split right now， waiting for force threshold
    kNoGC = 2, // tried gc by force threshold, but could not done internal gc or split, mark as not gc forever
    kNeverGC = 3, // if GC, the file will exceed trie bit number limit
    kShouldDelete = 4 }; // gc done, split/merge to new file, this file should be delete

struct hashStoreFileMetaDataHandler {
    uint64_t file_id = 0;
    uint64_t previous_file_id_first_; // for merge, should contain two different previous file id
    uint64_t previous_file_id_second_; // for merge, should contain two different previous file id
    uint64_t prefix_bit = 0;
    hashStoreFileCreateReason file_create_reason_ = kNewFile;
    uint64_t total_object_cnt = 0;
    uint64_t total_object_bytes = 0;
    uint64_t total_on_disk_bytes = 0;
    uint64_t no_gc_wait_operation_number_ = 0;
    hashStoreFileGCType gc_status = kNew;
    bool markedByMultiPut = false;
    bool markedByMultiGet = false;
    uint64_t num_anchors = 0;

    int8_t file_ownership = 0; // 0-> file not in use, 1->file belongs to write, -1->file belongs to GC
    FileOperation* file_op_ptr;
    std::shared_mutex fileOperationMutex_;
//    std::unordered_set<string> storedKeysSet_;
    BucketKeyFilter* filter = nullptr;
    BucketKeyFilter* sorted_filter = nullptr;
    BucketIndexBlock* index_block = nullptr;
    uint64_t unsorted_part_offset = 0;

    bool DiskAndBufferSizeExceeds(uint64_t threshold) {
        return total_on_disk_bytes + file_op_ptr->getFileBufferedSize() >
            threshold;
    }
    bool UnsortedPartExceeds(uint64_t threshold) {
        return total_on_disk_bytes + file_op_ptr->getFileBufferedSize() 
           - unsorted_part_offset > threshold;
    }
};

struct hashStoreWriteOperationHandler {
    mempoolHandler_t* object;
}; 

struct hashStoreMultiPutOperationHandler {
    mempoolHandler_t* objects;
    unsigned int size;
};

struct hashStoreMultiGetOperationHandler {
    vector<string*>* keys;
    vector<string*>* values;
    vector<int> key_indices;
};

enum operationStatus {
    kDone = 1,
    kNotDone = 2,
    kError = 3
};

struct hashStoreOperationHandler {
    hashStoreFileOperationType op_type;
    hashStoreFileMetaDataHandler* file_hdl;

    // kPut
    hashStoreWriteOperationHandler write_op;

    // kMultiGet
    hashStoreMultiGetOperationHandler multiget_op;

    // kMultiput
    hashStoreMultiPutOperationHandler multiput_op;
    bool need_flush = false;

    // kFind
    mempoolHandler_t* object;
    operationStatus job_done = kNotDone;

    hashStoreOperationHandler(hashStoreFileMetaDataHandler* file_hdl)
        : file_hdl(file_hdl) {};
    hashStoreOperationHandler() : file_hdl(nullptr) {};
};

// header size: 24 bytes 
typedef struct hashStoreFileHeader {
    uint64_t file_id;
    uint64_t previous_file_id_first_ = 0xffffffffffffffff; // used for file create reason == kInternalGCFile || kSplitFile || kMergeFile
    uint64_t previous_file_id_second_ = 0xffffffffffffffff; // only used for file create reason == kMergeFile
    uint64_t prefix_bit;
    uint32_t index_block_size = 0;
    uint32_t unsorted_part_offset = 0;
    hashStoreFileCreateReason file_create_reason_;
} hashStoreFileHeader;

// header size: 16 bytes
typedef struct hashStoreRecordHeader {
    uint32_t key_size_;
    uint32_t value_size_ = 0;
    uint32_t sequence_number_;
    bool is_anchor_;
    bool is_gc_done_ = false; // to mark gc job done
} hashStoreRecordHeader;

typedef struct writeBackObject {
    string key;
    string value;
    uint32_t sequenceNumber;
    writeBackObject(string keyIn, string valueIn, uint32_t sequenceNumberIn)
    {
        key = keyIn;
        value = valueIn;
        sequenceNumber = sequenceNumberIn;
    };
    writeBackObject() {};
} writeBackObject; // key to value pair fpr write back

struct lsmInterfaceOperationStruct {
    string key;
    string* value;
    rocksdb::WriteBatch* mergeBatch;
    vector<mempoolHandler_t>* handlerToValueStoreVecPtr;
    bool is_write;
    operationStatus job_done = kNotDone;
};

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
