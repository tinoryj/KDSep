#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/appendAbleLRUCacheStr.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <string_view>

namespace DELTAKV_NAMESPACE {

class HashStoreFileManager;

class HashStoreFileOperator {
public:
    HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, HashStoreFileManager* hashStoreFileManager /*messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ*/);
    ~HashStoreFileOperator();
    // file operations with job queue support
    bool putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* file_hdl, mempoolHandler_t* memPoolHandler);
    bool putWriteOperationsVectorIntoJobQueue(hashStoreOperationHandler* currentOperationHandler); 
    // file operations without job queue support-> only support single operation
    bool directlyWriteOperation(hashStoreFileMetaDataHandler* file_hdl, mempoolHandler_t* memPoolHandler);
    bool directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> batchedWriteOperationsMap);
    bool directlyReadOperation(hashStoreFileMetaDataHandler* file_hdl, string key, vector<string>& valueVec);
    bool waitOperationHandlerDone(hashStoreOperationHandler* currentOperationHandler);
    // threads with job queue support
    void operationWorker(int threadID);
    bool setJobDone();
    void notifyOperationWorkerThread();

private:
    // settings
    string workingDir_;
    uint64_t perFileFlushBufferSizeLimit_;
    uint64_t perFileGCSizeLimit_;
    uint64_t singleFileSizeLimit_;
    uint64_t operationNumberThresholdForForcedSingleFileGC_;
    shared_ptr<DeltaKVMergeOperator> deltaKVMergeOperatorPtr_;
    bool enableGCFlag_ = false;
    bool enableLsmTreeDeltaMeta_ = true;
    bool enable_index_block_ = true;
    bool operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    uint64_t readWholeFile(hashStoreFileMetaDataHandler* file_hdl, char** buf);
    uint64_t readUnsortedPart(hashStoreFileMetaDataHandler* file_hdl, char** buf);
    uint64_t readSortedPart(hashStoreFileMetaDataHandler* file_hdl, const string_view& key_view, char** buf, bool& key_exists);
    bool writeContentToFile(hashStoreFileMetaDataHandler* file_hdl, char* contentBuffer, uint64_t contentSize, uint64_t contentObjectNumber);
    bool readAndProcessSortedPart(hashStoreFileMetaDataHandler* file_hdl,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessWholeFile(hashStoreFileMetaDataHandler* file_hdl,
            string& key, vector<string_view>& kd_list, char** buf);
    bool readAndProcessUnsortedPart(hashStoreFileMetaDataHandler* file_hdl,
            string& key, vector<string_view>& kd_list, char** buf);
    
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMapInternal);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string_view, vector<string_view>>& resultMapInternal, const string_view& currentKey);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, vector<string_view>& kd_list, const string_view& currentKey);
    void putKeyValueToAppendableCacheIfExist(char* keyPtr, size_t keySize, char* valuePtr, size_t valueSize, bool isAnchor);
    void putKeyValueVectorToAppendableCacheIfNotExist(char* keyPtr, size_t keySize, vector<str_t>& values);
    bool putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* file_hdl);
    // message management
    messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ_ = nullptr;
    HashStoreFileManager* hashStoreFileManager_ = nullptr;
    AppendAbleLRUCacheStrT* keyToValueListCacheStr_ = nullptr;
    std::mutex operationNotifyMtx_;
    std::condition_variable operationNotifyCV_;
    boost::atomic<uint64_t> workingThreadExitFlagVec_;
    uint64_t workerThreadNumber_ = 0;
    bool syncStatistics_;
};

} // namespace DELTAKV_NAMESPACE
