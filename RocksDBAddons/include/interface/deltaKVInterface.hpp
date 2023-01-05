#pragma once

#include "common/rocksdbHeaders.hpp"
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

class RocksDBInternalMergeOperator : public MergeOperator {
public:
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override;

    static const char* kClassName() { return "RocksDBInternalMergeOperator"; }
    const char* Name() const override { return kClassName(); }

private:
    bool FullMergeFieldUpdates(string rawValue, vector<string>& operandList, string* finalValue) const;
    bool PartialMergeFieldUpdates(vector<pair<internalValueType, string>> batchedOperandVec, string& finalDeltaListStr) const;
};

class DeltaKV {
public:
    rocksdb::DB* pointerToRawRocksDB_;
    // Abstract class ctor
    DeltaKV();
    DeltaKV(DeltaKVOptions& options, const string& name);
    // No copying allowed
    DeltaKV(const DeltaKV&) = delete;
    void operator=(const DeltaKV&) = delete;
    // Abstract class dector
    ~DeltaKV();

    bool Open(DeltaKVOptions& options, const string& name);
    bool Close();

    bool Put(const string& key, const string& value);
    bool Merge(const string& key, const string& value);
    bool Get(const string& key, string* value);
    bool RangeScan(const string& startKey, uint64_t targetScanNumber, vector<string*> valueVec);
    bool SingleDelete(const string& key);

private:
    KeyValueMemPool* objectPairMemPool_ = nullptr;
    // batched write
    unordered_map<str_t, deque<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* writeBatchMapForSearch_[2]; // key to <operation type, value>
    uint64_t currentWriteBatchDequeInUse = 0;
    uint64_t maxBatchOperationBeforeCommitNumber_ = 3;
    messageQueue<unordered_map<str_t, deque<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>* notifyWriteBatchMQ_ = nullptr;
    uint64_t batchedOperationsCounter[2] = 0ULL;
    boost::atomic<bool> oneBufferDuringProcessFlag_ = false;
    boost::atomic<bool> writeBatchOperationWorkExitFlag = false;

    enum DBRunningMode { kPlainRocksDB = 0,
        kOnlyValueLog = 1,
        kOnlyDeltaLog = 2,
        kBothValueAndDeltaLog = 3,
        kBatchedWithBothValueAndDeltaLog = 4,
        kBatchedWithOnlyValueLog = 5,
        kBatchedWithOnlyDeltaLog = 6,
        kBatchedWithPlainRocksDB = 7 };

    DBRunningMode deltaKVRunningMode_ = kBothValueAndDeltaLog;

    // operations
    bool PutWithWriteBatch(mempoolHandler_t& objectPairMemPoolHandler);
    bool MergeWithWriteBatch(mempoolHandler_t& objectPairMemPoolHandler);
    bool GetWithWriteBatch(const string& key, string* value);

    bool PutWithPlainRocksDB(mempoolHandler_t& objectPairMemPoolHandler);
    bool MergeWithPlainRocksDB(mempoolHandler_t& objectPairMemPoolHandler);
    bool GetWithPlainRocksDB(const string& key, string* value);

    bool PutWithOnlyValueStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool MergeWithOnlyValueStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool GetWithOnlyValueStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag);

    bool PutWithOnlyDeltaStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool MergeWithOnlyDeltaStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool GetWithOnlyDeltaStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag);

    bool PutWithValueAndDeltaStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool MergeWithValueAndDeltaStore(mempoolHandler_t& objectPairMemPoolHandler);
    bool GetWithValueAndDeltaStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag);

    bool GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag);
    bool GetCurrentValueThenWriteBack(const string& key);
    bool performInBatchedBufferDeduplication(deque<tuple<DBOperationType, mempoolHandler_t>>*& operationsQueue);

    vector<bool> MultiGetWithBothValueAndDeltaStore(const vector<string>& keys, vector<string>& values);
    vector<bool> MultiGetWithOnlyValueStore(const vector<string>& keys, vector<string>& values);
    vector<bool> MultiGetWithOnlyDeltaStore(const vector<string>& keys, vector<string>& values);
    vector<bool> GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values);

    bool performInBatchedBufferDeduplication(unordered_map<str_t, deque<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*& operationsMap);

    void processBatchedOperationsWorker();
    void processWriteBackOperationsWorker();

    bool isDeltaStoreInUseFlag_ = false;
    bool isValueStoreInUseFlag_ = false;
    bool useInternalRocksDBBatchOperationsFlag_ = false;
    bool isBatchedOperationsWithBufferInUse_ = false;
    bool enableDeltaStoreWithBackgroundGCFlag_ = false;
    int writeBackWhenReadDeltaNumerThreshold_ = 4;
    uint64_t deltaExtractSize_ = 0;
    uint64_t valueExtractSize_ = 0;
    std::shared_mutex DeltaKVOperationsMtx_;

    uint32_t globalSequenceNumber_ = 0;
    std::shared_mutex globalSequenceNumberGeneratorMtx_;

    rocksdb::WriteOptions internalWriteOption_;
    rocksdb::WriteOptions internalMergeOption_;
    std::shared_mutex batchedBufferOperationMtx_;

    messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue_ = nullptr;
    bool enableWriteBackOperationsFlag_ = false;
    std::shared_mutex writeBackOperationsMtx_;
    bool enableKeyValueCache_ = false;
    AppendAbleLRUCache<string, string>* keyToValueListCache_ = nullptr;

    // thread management
    vector<boost::thread*> thList_;
    bool deleteExistingThreads();
    // for separated operations
    bool processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, bool& findNewValueIndex, externalIndexInfo& newExternalIndexInfo, uint32_t& maxSequenceNumber); // mergeOperatorsVec contains is_separted flag and related values if it is not separated.
    // Storage component for delta store
    HashStoreInterface* HashStoreInterfaceObjPtr_ = nullptr;
    HashStoreFileManager* hashStoreFileManagerPtr_ = nullptr;
    HashStoreFileOperator* hashStoreFileOperatorPtr_ = nullptr;
    shared_ptr<DeltaKVMergeOperator> deltaKVMergeOperatorPtr_;
    // Storage component for value store
    IndexStoreInterface* IndexStoreInterfaceObjPtr_ = nullptr;
};

} // namespace DELTAKV_NAMESPACE
