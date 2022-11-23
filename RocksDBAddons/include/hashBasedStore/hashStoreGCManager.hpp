#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/trie.hpp"
#include <bits/stdc++.h>

namespace DELTAKV_NAMESPACE {

class HashStoreGCManager {
public:
    HashStoreGCManager(string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ, messageQueue<hashStoreFileMetaDataHandler*>* GCNotifyFileMetaDataUpdateMQ);
    ~HashStoreGCManager();

    // file operations
    void gcWorker();

private:
    uint64_t deconstructAndGetValidContentsFromFile(char* fileContent, uint64_t fileSize, unordered_map<string, vector<string>>* resultMap); // return the processed object number
    // data structures
    string workingDirStr_;
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>*
        fileManagerNotifyGCMQ_;
    messageQueue<hashStoreFileMetaDataHandler*>*
        GCNotifyFileMetaDataUpdateMQ_;
};

} // namespace DELTAKV_NAMESPACE
