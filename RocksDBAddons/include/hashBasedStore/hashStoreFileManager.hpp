#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/prefixTree.hpp"
#include <bits/stdc++.h>
#include <filesystem>

using namespace std;

namespace DELTAKV_NAMESPACE {

class HashStoreFileManager {
public:
    HashStoreFileManager(uint64_t initialBitNumber, uint64_t maxBitNumber, uint64_t objectGCTriggerSize,
        uint64_t objectGlobalGCTriggerSize, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, fileOperationType fileOperationType);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList(); // will reopen all existing files
    bool UpdateHashStoreFileMetaDataList(); // online update metadata list to mainifest
    bool CloseHashStoreFileMetaDataList(); // will close all opened files
    bool CreateHashStoreFileMetaDataListIfNotExist();

    // file operations
    bool getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool setOperationNumberThresholdForMetadataUpdata(uint64_t threshold);

    // GC manager
    void processGCRequestWorker();
    void scheduleMetadataUpdateWorker();
    bool forcedManualGCAllFiles();

    // recovery
    bool recoveryFromFailure(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo); // return map of key to all related values that need redo, bool flag used for is_anchor check
    bool generateHashBasedPrefix(const string rawStr, string& prefixStr);

private:
    // settings
    uint64_t initialTrieBitNumber_;
    uint64_t maxTrieBitNumber_;
    uint64_t singleFileGCTriggerSize_;
    uint64_t globalGCTriggerSize_;
    std::string workingDir_;
    fileOperationType fileOperationMethod_ = kFstream;
    uint64_t operationCounterForMetadataCommit_ = 0;
    uint64_t operationNumberForMetadataCommitThreshold_ = 0;
    boost::shared_mutex operationCounterMtx_;
    // file metadata management
    PrefixTree<hashStoreFileMetaDataHandler*> objectFileMetaDataTrie_; // prefix-hash to object file metadata.
    uint64_t currentTotalHashStoreFileSize_ = 0;
    uint64_t currentTotalHashStoreFileNumber_ = 0;
    uint64_t targetNewFileID_ = 0;
    bool getHashStoreFileHandlerExistFlag(const string prefixStr);

    bool getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool createAndGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber, bool createByGCFlag, uint64_t previousFileID); // previousFileID only used when createByGCFlag == true
    uint64_t generateNewFileID();
    boost::mutex fileIDGeneratorMtx_;
    // GC
    pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>& resultMap);
    bool getOrCreateHashStoreFileHandlerByKeyStrForSplitGC(const string keyStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID);
    // recovery
    uint64_t deconstructTargetRecoveryContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone);
    bool stopMessageQueueFlag_ = false;
    bool shouldDoRecoveryFlag_ = false;
    bool gcThreadJobDoneFlag_ = false;
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
};

} // namespace DELTAKV_NAMESPACE
