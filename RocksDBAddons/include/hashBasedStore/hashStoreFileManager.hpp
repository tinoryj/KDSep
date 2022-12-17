#pragma once

#include "common/dataStructure.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/messageQueue.hpp"
#include "utils/murmurHash.hpp"
#include "utils/prefixTreeForHashStore.hpp"
#include <bits/stdc++.h>
#include <boost/atomic.hpp>
#include <filesystem>

using namespace std;

namespace DELTAKV_NAMESPACE {

class HashStoreFileManager {
public:
    HashStoreFileManager(DeltaKVOptions* options, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue);
    HashStoreFileManager(DeltaKVOptions* options, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // file operations
    bool getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr, bool getForAnchorWriting = false);
    bool getHashStoreFileHandlerByInputKeyStrForMultiPut(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr, bool getForAnchorWriting);

    // GC manager
    void processGCRequestWorker();
    void scheduleMetadataUpdateWorker();
    bool forcedManualGCAllFiles();
    bool forcedManualDelteAllObsoleteFiles();
    bool setJobDone();

    // recovery
    bool recoveryFromFailure(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo); // return map of key to all related values that need redo, bool flag used for is_anchor check

private:
    // settings
    uint64_t initialTrieBitNumber_ = 0;
    uint64_t maxBucketNumber_ = 0;
    uint64_t singleFileGCTriggerSize_ = 0;
    uint64_t singleFileMergeGCUpperBoundSize_ = 0;
    uint64_t singleFileSplitGCTriggerSize_ = 0;
    uint64_t globalGCTriggerSize_ = 0;
    std::string workingDir_;
    fileOperationType fileOperationMethod_ = kFstream;
    uint64_t operationCounterForMetadataCommit_ = 0;
    uint64_t operationNumberForMetadataCommitThreshold_ = 0;
    uint64_t gcWriteBackDeltaNum_ = 5;
    bool enableGCFlag_ = false;
    bool enableWriteBackDuringGCFlag_ = false;
    vector<uint64_t> targetDeleteFileHandlerVec_;
    std::shared_mutex fileDeleteVecMtx_;
    boost::atomic<bool> metadataUpdateShouldExit_ = false;

    // data structures
    PrefixTreeForHashStore objectFileMetaDataTrie_; // prefix-hash to object file metadata.
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

    bool deleteObslateFileWithFileIDAsInput(uint64_t fileID);
    // user-side operations
    bool generateHashBasedPrefix(const string rawStr, string& prefixStr);
    bool getHashStoreFileHandlerExistFlag(const string prefixStr);
    bool getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool createAndGetNewHashStoreFileHandlerByPrefixForUser(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber); // previousFileID only used when createByGCFlag == true
    std::shared_mutex createNewBucketMtx_;

    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList(); // will reopen all existing files
    bool UpdateHashStoreFileMetaDataList(); // online update metadata list to mainifest, and delete obsolete files
    bool CloseHashStoreFileMetaDataList(); // will close all opened files, and delete obsolete files
    bool CreateHashStoreFileMetaDataListIfNotExist();
    // recovery
    uint64_t deconstructAndGetAllContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone);
    // GC
    pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, uint32_t>& savedAnchors, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& resultMap);
    bool createHashStoreFileHandlerByPrefixStrForGC(string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID1, uint64_t previousFileID2, hashStoreFileHeader& newFileHeader);

    bool singleFileRewrite(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& gcResultMap, uint64_t targetFileSize, bool fileContainsReWriteKeysFlag);
    bool singleFileSplit(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& gcResultMap, uint64_t prefixBitNumber, bool fileContainsReWriteKeysFlag);
    bool twoAdjacentFileMerge(hashStoreFileMetaDataHandler* currentHandlerPtr1, hashStoreFileMetaDataHandler* currentHandlerPtr2, string targetPrefixStr);
    bool selectFileForMerge(uint64_t targetFileIDForSplit, hashStoreFileMetaDataHandler*& currentHandlerPtr1, hashStoreFileMetaDataHandler*& currentHandlerPtr2, string& targetPrefixStr);
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
    messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue_;
};

} // namespace DELTAKV_NAMESPACE
