#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
// #include "utils/trie.hpp"
#include <bits/stdc++.h>

namespace DELTAKV_NAMESPACE {

class HashStoreFileOperator {
public:
    HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ);
    ~HashStoreFileOperator();
    // file operations with job queue support
    bool putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, bool isAnchorStatus);
    bool putWriteOperationsVectorIntoJobQueue(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<bool>>> batchedWriteOperationsMap);
    bool putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec);
    bool putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec);
    // file operations without job queue support-> only support single operation
    bool directlyWriteOperation(hashStoreFileMetaDataHandler* fileHandler, string key, string value, bool isAnchorStatus);
    bool directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, string* value);
    // threads with job queue support
    void operationWorker();
    bool setJobDone();

private:
    // settings
    string workingDir_;
    uint64_t perFileFlushBufferSizeLimit_;
    uint64_t perFileGCSizeLimit_;
    uint64_t singleFileSizeLimit_;
    uint64_t operationNumberThresholdForForcedSingleFileGC_;
    bool enableGCFlag_ = false;
    bool operationWorkerPutWithCache(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerPutWithoutCahe(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerGetWithCache(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerGetWithoutCache(hashStoreOperationHandler* currentHandlerPtr);

    uint64_t readContentFromFile(hashStoreOperationHandler* opHandler, char* contentBuffer, uint64_t contentSize);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string, vector<string>>& resultMap);

    // message management
    messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ_ = nullptr;
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ_ = nullptr;
    AppendAbleLRUCache<string, vector<string>>* keyToValueListCache_ = nullptr;
};

} // namespace DELTAKV_NAMESPACE
