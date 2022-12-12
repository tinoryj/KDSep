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
    HashStoreFileManager(DeltaKVOptions* options, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, messageQueue<string*>* notifyWriteBackMQ);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // file operations
    bool getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr, bool getForAnchorWriting = false);

    // GC manager
    void processGCRequestWorker();
    void scheduleMetadataUpdateWorker();
    bool forcedManualGCAllFiles();
    bool forcedManualDelteAllObsoleteFiles();

    // recovery
    bool recoveryFromFailure(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo); // return map of key to all related values that need redo, bool flag used for is_anchor check

private:
    // settings
    uint64_t initialTrieBitNumber_;
    uint64_t maxTrieBitNumber_;
    uint64_t singleFileGCTriggerSize_;
    uint64_t singleFileSplitGCTriggerSize_;
    uint64_t globalGCTriggerSize_;
    std::string workingDir_;
    fileOperationType fileOperationMethod_ = kFstream;
    uint64_t operationCounterForMetadataCommit_ = 0;
    uint64_t operationNumberForMetadataCommitThreshold_ = 0;
    uint64_t gcWriteBackDeltaNum_ = 5;
    bool enableGCFlag_ = true;

    // data structures
    PrefixTree<hashStoreFileMetaDataHandler*> objectFileMetaDataTrie_; // prefix-hash to object file metadata.
    deque<uint64_t> targetDelteFileQueue_; // collect need delete files during GC
    // file ID generator
    uint64_t targetNewFileID_ = 0;
    uint64_t generateNewFileID();
    std::shared_mutex fileIDGeneratorMtx_;
    // operation counter for metadata commit
    uint64_t currentTotalHashStoreFileSize_ = 0;
    uint64_t currentTotalHashStoreFileNumber_ = 0;
    std::shared_mutex operationCounterMtx_;
    // for threads sync
    bool shouldDoRecoveryFlag_ = false;
    bool gcThreadJobDoneFlag_ = false;

    // user-side operations
    bool generateHashBasedPrefix(const string rawStr, string& prefixStr);
    bool getHashStoreFileHandlerExistFlag(const string prefixStr);
    bool getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool createAndGetNewHashStoreFileHandlerByPrefixForUser(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber, bool createByGCFlag, uint64_t previousFileID); // previousFileID only used when createByGCFlag == true
    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList(); // will reopen all existing files
    bool UpdateHashStoreFileMetaDataList(); // online update metadata list to mainifest, and delete obsolete files
    bool CloseHashStoreFileMetaDataList(); // will close all opened files, and delete obsolete files
    bool CreateHashStoreFileMetaDataListIfNotExist();
    // recovery
    uint64_t deconstructAndGetAllContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone);
    // GC
    pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_set<string>& savedAnchors, unordered_map<string, vector<string>>& resultMap);
    bool createHashStoreFileHandlerByPrefixStrForGC(string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID);
    bool singleFileRewrite(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, vector<string>>& gcResultMap, uint64_t targetFileSize);
    bool singleFileSplit(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, vector<string>>& gcResultMap, uint64_t prefixBitNumber);
    bool singleFileMerge(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, vector<string>>& gcResultMap, uint64_t prefixBitNumber);
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
    messageQueue<string*>* notifyWriteBackMQ_;
};

} // namespace DELTAKV_NAMESPACE
