#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/boostLruCache.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/trie.hpp"
#include <bits/stdc++.h>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>

namespace DELTAKV_NAMESPACE {

class HashStoreFileOperator {
public:
    HashStoreFileOperator(DeltaKVOptions* options, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ);
    ~HashStoreFileOperator();

    // file operations
    bool putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, bool isAnchorStatus);
    bool putWriteOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<string> valueVec, vector<bool> isAnchorStatusVec);
    bool putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec);
    bool putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec);

    void operationWorker();

private:
    uint64_t processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string, vector<string>*>& resultMap);
    // message management
    messageQueue<hashStoreOperationHandler*>*
        operationToWorkerMQ_
        = nullptr;
    messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ_ = nullptr;
    BOOSTLRUCache<string, vector<string>*>* keyToValueListCache_ = nullptr;
};

} // namespace DELTAKV_NAMESPACE
