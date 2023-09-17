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
    uint64_t write_buffer_size_ = 2 * 1024 * 1024;
    messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>* notifyWriteBatchMQ_ = nullptr;
    uint64_t batch_nums_[2] = { 0UL, 0UL };
    uint64_t batch_sizes_[2] = { 0UL, 0UL };
//    boost::atomic<bool>* write_stall_ = nullptr;
    bool* write_stall_ = nullptr;
    shared_ptr<queue<string>> wb_keys;
    shared_ptr<mutex> wb_keys_mutex;

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
    bool GetInternal(const string& key, string* value, bool writing_back);
    bool MultiGetFullMergeInternal(const vector<string>& keys,
	const vector<string>& lsm_values,
	const vector<vector<string>>& key_deltas,
	vector<string>& values); 

    bool MultiGetInternalForWriteBack(const vector<string>& keys,
            vector<string>& values); 
    bool GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values);

    bool GetCurrentValueThenWriteBack(const string& key);
    bool GetCurrentValuesThenWriteBack(const vector<string>& keys);
//    bool GetFromBuffer(const string& key, vector<string>& values);

    bool writeBufferDedup(unordered_map<str_t, vector<pair<DBOperationType,
            mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*&
            operationsMap);

    void processBatchedOperationsWorker();
    void processWriteBackOperationsWorker();
    void processLsmInterfaceOperationsWorker();

    void Recovery();

    bool isDeltaStoreInUseFlag_ = false;
    bool useInternalRocksDBBatchOperationsFlag_ = false;
    bool isBatchedOperationsWithBufferInUse_ = false;
    bool enableDeltaStoreWithBackgroundGCFlag_ = false;
    bool enableLsmTreeDeltaMeta_ = true;
    bool enableParallelLsmInterface = true;
    bool enable_crash_consistency_ = false;
    bool enable_bucket_merge_ = true;

    int writeBackWhenReadDeltaNumerThreshold_ = 4;
    int writeBackWhenReadDeltaSizeThreshold_ = 4;
    uint64_t deltaExtractSize_ = 0;
    uint64_t valueExtractSize_ = 0;
    std::shared_mutex KDSepOperationsMtx_;

    uint32_t globalSequenceNumber_ = 0;
    std::shared_mutex globalSequenceNumberGeneratorMtx_;

    std::shared_mutex write_buffer_mtx_;

    //
    std::shared_ptr<messageQueue<writeBackObject*>> write_back_queue_;
    std::shared_ptr<std::condition_variable> write_back_cv_;
    std::shared_ptr<std::mutex> write_back_mutex_;

    // useless
    messageQueue<lsmInterfaceOperationStruct*>* lsmInterfaceOperationsQueue_ = nullptr;
    std::mutex lsm_interface_mutex;
    std::condition_variable lsm_interface_cv;
    bool enable_write_back_ = false;
    std::shared_mutex writeBackOperationsMtx_;

    // thread management
    vector<boost::thread*> thList_;
    bool deleteExistingThreads();
    // for separated operations
    bool extractDeltas(string internalValue, uint64_t skipSize,
            vector<pair<bool, string>>& mergeOperatorsVec);
    bool extractDeltas(string internalValue, uint64_t skipSize,
            vector<pair<bool, string>>& mergeOperatorsVec, 
            vector<KvHeader>& mergeOperatorsRecordVec);
    void pushWriteBuffer();
    // Storage component for delta store

    // tune the block cache size
    std::shared_ptr<rocksdb::Cache> rocks_block_cache_;
    struct timeval tv_tune_cache_;
    void tryTuneCache();
    uint64_t extra_mem_step_ = 16 * 1024 * 1024;
    uint64_t extra_mem_threshold_ = extra_mem_step_;
    uint64_t max_kd_cache_size_ = 0;
    uint64_t min_block_cache_size_ = 0;
    uint64_t memory_budget_ = 4ull * 1024 * 1024 * 1024;
    std::shared_ptr<KDLRUCache> kd_cache_;

    HashStoreInterface* delta_store_ = nullptr;
    BucketManager* bucket_manager_ = nullptr;
    BucketOperator* bucket_operator_ = nullptr;
    shared_ptr<KDSepMergeOperator> KDSepMergeOperatorPtr_;
    LsmTreeInterface lsmTreeInterface_;

};

} // namespace KDSEP_NAMESPACE
