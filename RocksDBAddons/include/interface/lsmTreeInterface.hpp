#pragma once

#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "hashBasedStore/hashStoreInterface.hpp"
#include "indexBasedStore/indexStoreInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/debug.hpp"
#include "utils/messageQueue.hpp"
#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <boost/bind/bind.hpp>
#include <boost/thread/thread.hpp>
#include <shared_mutex>

using namespace std;

namespace DELTAKV_NAMESPACE {

class LsmTreeInterface {
public:
    LsmTreeInterface();
    ~LsmTreeInterface();

    // No copying allowed
    LsmTreeInterface(const LsmTreeInterface&) = delete;
    void operator=(const LsmTreeInterface&) = delete;

    bool Open(DeltaKVOptions& options, const string& name);
    bool Close();

    bool Put(const mempoolHandler_t& mempoolHandler);
    bool Merge(const mempoolHandler_t& memPoolHandler);
    bool Merge(const char* key, uint32_t keySize, const char* value, uint32_t valueSize);
    bool Get(const string& key, string* value);
    bool MultiWriteWithBatch(const vector<mempoolHandler_t>& memPoolHandlersPut, rocksdb::WriteBatch* mergeBatch);

    void GetRocksDBProperty(const string& property, string* str);
//    bool RangeScan(const string& startKey, uint64_t targetScanNumber, vector<string*> valueVec);
//    bool SingleDelete(const string& key);

private:
    rocksdb::DB* pointerToRawRocksDB_;
    // batched write

    enum LsmTreeRunningMode { kValueLog = 0, kNoValueLog = 1};
    LsmTreeRunningMode lsmTreeRunningMode_ = kValueLog;

    // operations

    rocksdb::WriteOptions internalWriteOption_;
    rocksdb::WriteOptions internalMergeOption_;
    // Storage component for value store
    IndexStoreInterface* IndexStoreInterfaceObjPtr_ = nullptr;
    MergeOperator* mergeOperator_ = nullptr; 
    uint64_t valueExtractSize_ = 0;
    bool isValueStoreInUseFlag_ = false;
};

} // namespace DELTAKV_NAMESPACE
