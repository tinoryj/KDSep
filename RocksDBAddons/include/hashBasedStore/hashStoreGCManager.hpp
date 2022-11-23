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
    HashStoreGCManager(messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ);
    ~HashStoreGCManager();
    HashStoreGCManager& operator=(const HashStoreGCManager&) = delete;

    // file operations
    void gcWorker();

private:
    bool deconstructFile(char* fileContent, uint64_t fileSize,vector<>);
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>*
        fileManagerNotifyGCMQ_;
};

} // namespace DELTAKV_NAMESPACE
