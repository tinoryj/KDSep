#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
// #include "utils/trie.hpp"
#include <bits/stdc++.h>

namespace DELTAKV_NAMESPACE {

class HashStoreFileManager {
public:
    HashStoreFileManager(uint64_t initialBitNumber, uint64_t maxBitNumber, uint64_t objectGCTriggerSize,
        uint64_t objectGlobalGCTriggerSize, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList();
    bool UpdateHashStoreFileMetaDataList();
    bool CloseHashStoreFileMetaDataList();
    bool CreateHashStoreFileMetaDataListIfNotExist();

    // file operations
    bool getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr);

    // GC manager
    void processGCRequestWorker();
    void scheduleMetadataUpdateWorker();

private:
    // settings
    uint64_t initialTrieBitNumber_;
    uint64_t maxTrieBitNumber_;
    uint64_t singleFileGCTriggerSize_;
    uint64_t globalGCTriggerSize_;
    std::string workingDir_;
    // file metadata management
    // Trie<hashStoreFileMetaDataHandler*>
    //     objectFileMetaDataTrie_; // prefix-hash to object file metadata.
    std::unordered_map<string, hashStoreFileMetaDataHandler*>
        objectFileMetaDataTrie_; // prefix-hash to object file metadata.
    std::unordered_map<uint64_t, string>
        hashStoreFileIDToPrefixMap_; // hashStore file id -> prefix;
    uint64_t currentTotalHashStoreFileSize_ = 0;
    uint64_t currentTotalHashStoreFileNumber_ = 0;
    uint64_t targetNewFileID_ = 0;
    uint64_t getHashStoreFileHandlerStatusByPrefix(const string prefixStr);
    bool generateHashBasedPrefix(const string rawStr, string& prefixStr);
    bool getHashStoreFileHandlerByPrefix(const string prefixStr, uint64_t prefixUsageLength, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool createAndGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    uint64_t newFileIDGenerator();
    // recovery and GC
    bool recoveryFromFailuer(unordered_map<string, pair<bool, string>>*& targetListForRedo); // return map of key to all related values that need redo, bool flag used for is_anchor check
    pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>& resultMap);
    bool stopMessageQueueFlag_ = false;
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
};

} // namespace DELTAKV_NAMESPACE
