#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/trie.hpp"
#include <bits/stdc++.h>

namespace DELTAKV_NAMESPACE {

class HashStoreFileManager {
public:
    HashStoreFileManager(uint64_t initialBitNumber, uint64_t objectGCTriggerSize,
        uint64_t objectGlobalGCTriggerSize, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList(std::string workingDir);
    bool UpdateHashStoreFileMetaDataList(std::string workingDir);
    bool CreateHashStoreFileMetaDataListIfNotExist(std::string workingDir);

    bool RetriveHashStoreFileMetaDataList();
    bool UpdateHashStoreFileMetaDataList();
    bool CreateHashStoreFileMetaDataListIfNotExist();
    // file operations
    bool getHashStoreFileHandlerByInputKeyStr(string keyStr);

private:
    // settings
    uint64_t initialTrieBitNumber_;
    uint64_t singleFileGCTriggerSize_;
    uint64_t globalGCTriggerSize_;
    std::string workingDir_;
    // file metadata management
    Trie<hashStoreFileMetaDataHandler*>
        objectFileMetaDataTrie_; // prefix-hash to object file metadata.
    uint64_t currentTotalHashStoreFileSize_ = 0;
    uint64_t currentTotalHashStoreFileNumber_ = 0;
    uint64_t targetNewFileID_ = 0;
    bool getHashStoreFileHandlerStatusByPrefix(const string prefixStr);
    bool generateHashBasedPrefix(const string rawStr, string& prefixStr);
    bool getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler* fileHandlerPtr);
    bool createAnfGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler* fileHandlerPtr);
    uint64_t newFileIDGenerator();

    // message management
    messageQueue<hashStoreFileMetaDataHandler*>*
        fileManagerNotifyGCMQ_;
};

} // namespace DELTAKV_NAMESPACE
