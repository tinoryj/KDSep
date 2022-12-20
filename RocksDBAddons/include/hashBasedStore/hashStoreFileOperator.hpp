#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include <bits/stdc++.h>
#include <shared_mutex>

namespace DELTAKV_NAMESPACE {

class HashStoreFileOperator {
public:
    HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ);
    ~HashStoreFileOperator();
    // file operations with job queue support
    bool putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, uint32_t sequenceNumber, bool isAnchorStatus);
    bool putWriteOperationsVectorIntoJobQueue(hashStoreOperationHandler* currentOperationHandler);
    bool putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec);
    bool putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec);
    // file operations without job queue support-> only support single operation
    bool directlyWriteOperation(hashStoreFileMetaDataHandler* fileHandler, string key, string value, uint32_t sequemceNumber, bool isAnchorStatus);
    bool directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> batchedWriteOperationsMap);
    bool directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec);
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
    bool operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerGetFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr);
    bool readContentFromFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer);
    bool writeContentToFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer, uint64_t contentSize, uint64_t contentObjectNumber);
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string, vector<string>>& resultMap);
    bool putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* fileHandler);
    // message management
    messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ_ = nullptr;
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ_ = nullptr;
    AppendAbleLRUCache<string, vector<string>>* keyToValueListCache_ = nullptr;
};

} // namespace DELTAKV_NAMESPACE
