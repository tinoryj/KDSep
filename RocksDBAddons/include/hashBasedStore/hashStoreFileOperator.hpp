#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/trie.hpp"
#include <bits/stdc++.h>

namespace DELTAKV_NAMESPACE {

class HashStoreFileOperator {
public:
    HashStoreFileOperator(messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ);
    ~HashStoreFileOperator();

    HashStoreFileOperator& operator=(const HashStoreFileOperator&) = delete;

    // file operations
    bool putOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string* key, string* value, bool isAnchorStatus);
    bool putOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string*> keyVec, vector<string*> valueVec, vector<bool> isAnchorStatusVec);

    void operationWorker();

private:
    // message management
    messageQueue<hashStoreOperationHandler*>*
        operationToWorkerMQ_;
};

} // namespace DELTAKV_NAMESPACE
