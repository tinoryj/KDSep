#pragma once

#include "interface/lsmTreeInterface.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

class DeltaKV {
public:
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

    void GetRocksDBProperty(const string& property, string* str);
//    bool RangeScan(const string& startKey, uint64_t targetScanNumber, vector<string*> valueVec);
//    bool SingleDelete(const string& key);

private:
    KeyValueMemPool* objectPairMemPool_ = nullptr;
    // batched write
    unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* writeBatchMapForSearch_[2]; // key to <operation type, value>
    uint64_t currentWriteBatchDequeInUse = 0;
    uint64_t maxBatchOperationBeforeCommitNumber_ = 3;
    messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>* notifyWriteBatchMQ_ = nullptr;
    uint64_t batchedOperationsCounter[2] = { 0UL, 0UL };
    boost::atomic<bool> oneBufferDuringProcessFlag_ = false;
    boost::atomic<bool> writeBatchOperationWorkExitFlag = false;

    enum DBRunningMode {
        kWithDeltaStore = 0,
        kWithNoDeltaStore = 1,
        kBatchedWithDeltaStore = 2,
        kBatchedWithNoDeltaStore = 3
    };

    DBRunningMode deltaKVRunningMode_ = kBatchedWithNoDeltaStore;

    // operations
    bool PutWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);
    bool MergeWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);

    bool PutImpl(const string& key, const string& value);
    bool PutInternal(const mempoolHandler_t& mempoolHandler); 
    bool MergeInternal(const mempoolHandler_t& mempoolHandler);
    bool GetInternal(const string& key, string* value, uint32_t maxSequenceNumber, bool getByWriteBackFlag);

//    bool GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag);
//    vector<bool> GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values);

    bool GetCurrentValueThenWriteBack(const string& key);

    bool performInBatchedBufferDeduplication(unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*& operationsMap);

    void processBatchedOperationsWorker();
    void processWriteBackOperationsWorker();

    bool isDeltaStoreInUseFlag_ = false;
    bool useInternalRocksDBBatchOperationsFlag_ = false;
    bool isBatchedOperationsWithBufferInUse_ = false;
    bool enableDeltaStoreWithBackgroundGCFlag_ = false;
    bool enableLsmTreeDeltaMeta_ = true;

    int writeBackWhenReadDeltaNumerThreshold_ = 4;
    uint64_t deltaExtractSize_ = 0;
    uint64_t valueExtractSize_ = 0;
    std::shared_mutex DeltaKVOperationsMtx_;

    uint32_t globalSequenceNumber_ = 0;
    std::shared_mutex globalSequenceNumberGeneratorMtx_;

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
    bool processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, uint32_t& maxSequenceNumber);
    bool processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, vector<internalValueType>& mergeOperatorsRecordVec, uint32_t& maxSequenceNumber);
    // Storage component for delta store

    HashStoreInterface* HashStoreInterfaceObjPtr_ = nullptr;
    HashStoreFileManager* hashStoreFileManagerPtr_ = nullptr;
    HashStoreFileOperator* hashStoreFileOperatorPtr_ = nullptr;
    shared_ptr<DeltaKVMergeOperator> deltaKVMergeOperatorPtr_;
    LsmTreeInterface lsmTreeInterface_;
};

} // namespace DELTAKV_NAMESPACE
