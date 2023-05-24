#pragma once

#include "common/dataStructure.hpp"
#include "interface/KDSepOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <string_view>

namespace KDSEP_NAMESPACE {

class BucketManager;

class BucketOperator {
public:
    BucketOperator(KDSepOptions* options, string workingDirStr, BucketManager* bucketManager /*messageQueue<BucketHandler*>* fileManagerNotifyGCMQ*/);
    ~BucketOperator();
    // file operations with job queue support
    bool putWriteOperationIntoJobQueue(BucketHandler* bucket, mempoolHandler_t* memPoolHandler);
    bool putIntoJobQueue(hashStoreOperationHandler* op_hdl); 
    bool startJob(hashStoreOperationHandler* op_hdl);

    // file operations without job queue support-> only support single operation
    bool directlyWriteOperation(BucketHandler* bucket, mempoolHandler_t* memPoolHandler);
    bool directlyMultiWriteOperation(unordered_map<BucketHandler*, vector<mempoolHandler_t>> batchedWriteOperationsMap);
    bool directlyReadOperation(BucketHandler* bucket, string key, vector<string>& valueVec);
    bool waitOperationHandlerDone(hashStoreOperationHandler* op_hdl, bool need_delete = true);
    // threads with job queue support
    void operationWorker(int threadID);
    bool setJobDone();
    void notifyOperationWorkerThread();

private:
    // settings
    string working_dir_;
    uint64_t perFileFlushBufferSizeLimit_;
    uint64_t perFileGCSizeLimit_;
    uint64_t singleFileSizeLimit_;
    uint64_t operationNumberThresholdForForcedSingleFileGC_;
    shared_ptr<KDSepMergeOperator> KDSepMergeOperatorPtr_;
    bool enableGCFlag_ = false;
    bool enableLsmTreeDeltaMeta_ = true;
    bool enable_index_block_ = true;
    bool operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerGetFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerMultiGetFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerFlush(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerFind(hashStoreOperationHandler* currentHandlerPtr);

    uint64_t readWholeFile(BucketHandler* bucket, char** buf);
    uint64_t readUnsortedPart(BucketHandler* bucket, char** buf);
    uint64_t readSortedPart(BucketHandler* bucket, const string_view& key_view, char** buf, bool& key_exists);
    uint64_t readBothParts(BucketHandler* bucket, const string_view& key_view, char** buf);

    bool writeToFile(BucketHandler* bucket, 
            char* contentBuffer, uint64_t contentSize, 
            uint64_t contentObjectNumber, bool need_flush = false);
    bool readAndProcessSortedPart(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessWholeFile(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessUnsortedPart(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessBothParts(BucketHandler* bucket,
            string& key, vector<string_view>& kd_list, char** buf);
    
    // for multiget
    bool readAndProcessWholeFileKeyList(
	    BucketHandler* bucket, vector<string*>* key,
	    vector<vector<string_view>>& kd_list, char** buf);

    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMapInternal);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string_view, vector<string_view>>& resultMapInternal, const string_view& currentKey);
    uint64_t processReadContentToValueLists(char* contentBuffer, 
	    uint64_t contentSize, vector<string_view>& kd_list, 
	    const string_view& currentKey);
    uint64_t processReadContentToValueListsWithKeyList(char* read_buf, 
	    uint64_t read_buf_size, vector<vector<string_view>>& kd_list,
	    const vector<string_view>& keys);
    void putKeyValueToAppendableCacheIfExist(char* keyPtr, size_t keySize, char* valuePtr, size_t valueSize, bool isAnchor);
    void putKeyValueVectorToAppendableCacheIfNotExist(char* keyPtr, size_t keySize, vector<str_t>& values);
    void updateKDCacheIfExist(str_t key, str_t delta, bool isAnchor);
    void updateKDCache(char* keyPtr, size_t keySize, str_t delta); 
    bool putFileHandlerIntoGCJobQueueIfNeeded(BucketHandler* bucket);
    void operationBoostThreadWorker(hashStoreOperationHandler* op_hdl);
    // boost thread
    boost::asio::thread_pool*  workerThreads_ = nullptr;
    // message management
    messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ_ = nullptr;
    BucketManager* hashStoreFileManager_ = nullptr;
    AppendAbleLRUCacheStrVector* keyToValueListCacheStr_ = nullptr;
    KDLRUCache* kd_cache_ = nullptr;
    std::mutex operationNotifyMtx_;
    std::condition_variable operationNotifyCV_;
    boost::atomic<uint64_t> workingThreadExitFlagVec_;
    uint64_t workerThreadNumber_ = 0;
    bool syncStatistics_;
//    boost::atomic<bool>* write_stall_;
    bool* write_stall_;
};

} // namespace KDSEP_NAMESPACE
