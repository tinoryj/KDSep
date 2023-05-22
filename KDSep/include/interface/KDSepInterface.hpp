#pragma once

#include "interface/lsmTreeInterface.hpp"

using namespace std;

namespace KDSEP_NAMESPACE {

class KDSep {
public:
    KDSep();
    KDSep(KDSepOptions& options, const string& name);
    // No copying allowed
    KDSep(const KDSep&) = delete;
    void operator=(const KDSep&) = delete;
    // Abstract class dector
    ~KDSep();

    bool Open(KDSepOptions& options, const string& name);
    bool Close();

    bool Put(const string& key, const string& value);
    bool Merge(const string& key, const string& value);
    bool Get(const string& key, string* value);

    void GetRocksDBProperty(const string& property, string* str);
    bool Scan(const string& startKey, int len, vector<string>& keys, vector<string>& values);
    //    bool SingleDelete(const string& key);

private:
    KeyValueMemPoolBase* objectPairMemPool_ = nullptr;
    // batched write
    unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* batch_map_[2]; // key to <operation type, value>
    uint64_t batch_in_use_ = 0;
    uint64_t maxBatchOperationBeforeCommitNumber_ = 3;
    uint64_t maxBatchOperationBeforeCommitSize_ = 2 * 1024 * 1024;
    messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>* notifyWriteBatchMQ_ = nullptr;
    uint64_t batch_nums_[2] = { 0UL, 0UL };
    uint64_t batch_sizes_[2] = { 0UL, 0UL };
    //    boost::atomic<bool>* write_stall_ = nullptr;
    bool* write_stall_ = nullptr;
    std::queue<string>* wb_keys = nullptr;
    std::mutex* wb_keys_mutex = nullptr;

    boost::atomic<bool> oneBufferDuringProcessFlag_ = false;
    boost::atomic<bool> writeBatchOperationWorkExitFlag = false;

    enum DBRunningMode {
        kWithDeltaStore = 0,
        kWithNoDeltaStore = 1,
        kBatchedWithDeltaStore = 2,
        kBatchedWithNoDeltaStore = 3
    };

    DBRunningMode KDSepRunningMode_ = kBatchedWithNoDeltaStore;

    // operations
    bool PutWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);
    bool MergeWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler);

    bool PutImpl(const string& key, const string& value);
    bool SinglePutInternal(const mempoolHandler_t& mempoolHandler);
    bool SingleMergeInternal(const mempoolHandler_t& mempoolHandler);
    bool GetInternal(const string& key, string* value, uint32_t maxSequenceNumber, bool writing_back);
    bool MultiGetFullMergeInternal(const vector<string>& keys,
        const vector<string>& lsm_values,
        const vector<vector<string>>& key_deltas,
        vector<string>& values);

    bool MultiGetInternal(const vector<string>& keys, vector<string>& values);
    //    bool GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool writing_back);
    bool GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values);

    bool GetCurrentValueThenWriteBack(const string& key);
    bool GetCurrentValuesThenWriteBack(const vector<string>& keys);
    //    bool GetFromBuffer(const string& key, vector<string>& values);

    bool performInBatchedBufferDeduplication(unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*& operationsMap);

    void processBatchedOperationsWorker();
    void processWriteBackOperationsWorker();
    void processLsmInterfaceOperationsWorker();
    void processLsmInterfaceThreadPoolOperationsWorker(lsmInterfaceOperationStruct* op);

    bool isDeltaStoreInUseFlag_ = false;
    bool useInternalRocksDBBatchOperationsFlag_ = false;
    bool isBatchedOperationsWithBufferInUse_ = false;
    bool enableDeltaStoreWithBackgroundGCFlag_ = false;
    bool enableLsmTreeDeltaMeta_ = true;
    bool enableParallelLsmInterface = true;
    bool enable_crash_consistency = false;
    bool enable_bucket_merge_ = true;

    int writeBackWhenReadDeltaNumerThreshold_ = 4;
    int writeBackWhenReadDeltaSizeThreshold_ = 4;
    uint64_t deltaExtractSize_ = 0;
    uint64_t valueExtractSize_ = 0;
    std::shared_mutex KDSepOperationsMtx_;

    uint32_t globalSequenceNumber_ = 0;
    std::shared_mutex globalSequenceNumberGeneratorMtx_;

    std::shared_mutex batchedBufferOperationMtx_;

    messageQueue<writeBackObject*>* writeBackOperationsQueue_ = nullptr;
    // useless
    messageQueue<lsmInterfaceOperationStruct*>* lsmInterfaceOperationsQueue_ = nullptr;
    std::mutex lsm_interface_mutex;
    std::condition_variable lsm_interface_cv;
    bool enable_write_back_ = false;
    std::shared_mutex writeBackOperationsMtx_;
    bool enableKeyValueCache_ = false;
    AppendAbleLRUCache<string, string>* keyToValueListCache_ = nullptr;

    // boost thread
    boost::asio::thread_pool* lsm_thread_ = nullptr;

    // thread management
    vector<boost::thread*> thList_;
    bool deleteExistingThreads();
    // for separated operations
    bool extractDeltas(string internalValue, uint64_t skipSize,
        vector<pair<bool, string>>& mergeOperatorsVec,
        uint32_t& maxSequenceNumber);
    bool extractDeltas(string internalValue, uint64_t skipSize,
        vector<pair<bool, string>>& mergeOperatorsVec,
        vector<KvHeader>& mergeOperatorsRecordVec,
        uint32_t& maxSequenceNumber);
    // Storage component for delta store

    HashStoreInterface* HashStoreInterfaceObjPtr_ = nullptr;
    HashStoreFileManager* hashStoreFileManagerPtr_ = nullptr;
    HashStoreFileOperator* hashStoreFileOperatorPtr_ = nullptr;
    shared_ptr<KDSepMergeOperator> KDSepMergeOperatorPtr_;
    LsmTreeInterface lsmTreeInterface_;
};

} // namespace KDSEP_NAMESPACE
