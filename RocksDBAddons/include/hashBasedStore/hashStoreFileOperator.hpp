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

    // file operations
    bool putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, bool isAnchorStatus);
    bool putWriteOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<string> valueVec, vector<bool> isAnchorStatusVec);
    bool putWriteOperationsVectorIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, vector<string> keyVec, vector<string> valueVec, vector<bool> isAnchorStatusVec);
    bool putWriteOperationsVectorIntoJobQueue(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<bool>>> tempFileHandlerMap);
    bool putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec);
    bool putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec);
    void operationWorker();
    bool setJobDone();

private:
    uint64_t perFileFlushBufferSizeLimit_;
    uint64_t perFileGCSizeLimit_;
    uint64_t singleFileSizeLimit_;
    uint64_t operationNumberThresholdForForcedSingleFileGC_;
    bool enable_GC_flag_ = false;
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string, vector<string>>& resultMap);
    // message management
    messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ_ = nullptr;
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ_ = nullptr;
    AppendAbleLRUCache<string, vector<string>>* keyToValueListCache_ = nullptr;
    string workingDir_;
};

} // namespace DELTAKV_NAMESPACE
