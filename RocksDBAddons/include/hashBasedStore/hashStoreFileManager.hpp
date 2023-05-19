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
    HashStoreFileManager(DeltaKVOptions* options, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, messageQueue<writeBackObject*>* writeBackOperationsQueue);
    ~HashStoreFileManager();
    HashStoreFileManager& operator=(const HashStoreFileManager&) = delete;

    // file operations
    bool getFileHandlerWithKey(const char* keyBuffer, uint32_t keySize,
	    hashStoreFileOperationType opType, 
	    hashStoreFileMetaDataHandler*& fileHandlerPtr, 
	    bool getForAnchorWriting = false);
    bool generateHashBasedPrefix(const char* rawStr, uint32_t strSize, uint64_t& prefixU64);

    // GC manager
    void processSingleFileGCRequestWorker(int threadID);
    void processMergeGCRequestWorker();
    void scheduleMetadataUpdateWorker();
    bool forcedManualGCAllFiles();
    bool forcedManualDelteAllObsoleteFiles();
    bool setJobDone();

    void pushToGCQueue(hashStoreFileMetaDataHandler* fileHandlerPtr);
    uint64_t getTrieAccessNum();

    // Consistency
    bool writeToCommitLog(vector<mempoolHandler_t> objects, bool& flag);
    bool cleanCommitLog();
//    bool flushAllBuffers();
    bool UpdateHashStoreFileMetaDataList(); // online update metadata list to mainifest, and delete obsolete files
    bool RemoveObsoleteFiles();
    bool prepareForUpdatingMetadata(vector<hashStoreFileMetaDataHandler*>& file_hdls);

    // recovery
    bool recoveryFromFailure(); // return map of key to all related values that need redo, bool flag used for is_anchor check
    bool recoveryFromFailureOld(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo); // return map of key to all related values that need redo, bool flag used for is_anchor check

private:
    // settings
    uint64_t initialTrieBitNumber_ = 0;
    uint64_t maxBucketNumber_ = 0;
    uint64_t singleFileGCTriggerSize_ = 0;
    uint64_t singleFileMergeGCUpperBoundSize_ = 0;
    uint64_t maxBucketSize_ = 0;
    uint64_t singleFileSplitGCTriggerSize_ = 0;
    uint64_t globalGCTriggerSize_ = 0;
    std::string workingDir_;
    fileOperationType fileOperationMethod_ = kFstream;
    uint64_t operationCounterForMetadataCommit_ = 0;
    uint64_t operationNumberForMetadataCommitThreshold_ = 0;
    uint64_t gcWriteBackDeltaNum_ = 5;
    uint64_t gcWriteBackDeltaSize_ = 0;
    bool enableGCFlag_ = false;
    bool enableWriteBackDuringGCFlag_ = false;
    bool enableBatchedOperations_ = false;
    bool enableLsmTreeDeltaMeta_ = true;
    bool enable_index_block_ = true;
    bool enable_crash_consistency_ = false;
    vector<uint64_t> targetDeleteFileHandlerVec_;
    std::shared_mutex fileDeleteVecMtx_;
    boost::atomic<bool> metadataUpdateShouldExit_ = false;
    boost::atomic<bool> oneThreadDuringSplitOrMergeGCFlag_ = false;
//    boost::atomic<bool>* write_stall_ = nullptr;
    bool* write_stall_ = nullptr;
    std::queue<string>* wb_keys = nullptr;
    std::mutex* wb_keys_mutex = nullptr;

    uint64_t singleFileGCWorkerThreadsNumebr_ = 1;
    uint64_t singleFileFlushSize_ = 4096;

    // data structures
    PrefixTreeForHashStore file_trie_; // prefix-hash to object file metadata.
    deque<uint64_t> targetDelteFileQueue_; // collect need delete files during GC
    shared_ptr<DeltaKVMergeOperator> deltaKVMergeOperatorPtr_;
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

    bool deleteObslateFileWithFileIDAsInput(uint64_t fileID);
    // user-side operations
    bool getHashStoreFileHandlerByPrefix(const uint64_t& prefixU64,
            hashStoreFileMetaDataHandler*& fileHandlerPtr);
    bool createAndGetNewHashStoreFileHandlerByPrefixForUser(const uint64_t&
            prefixU64, hashStoreFileMetaDataHandler*& fileHandlerPtr);
    std::shared_mutex createNewBucketMtx_;

    // Manager's metadata management
    bool RetriveHashStoreFileMetaDataList(); // will reopen all existing files
    bool CloseHashStoreFileMetaDataList(); // will close all opened files, and delete obsolete files
    bool CreateHashStoreFileMetaDataListIfNotExist();
    // recovery
    uint64_t deconstructAndGetAllContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone);
    // GC
    pair<uint64_t, uint64_t> deconstructAndGetValidContentsFromFile(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMap);
    // GC partial merge
    uint64_t partialMergeGcResultMap(unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMap, unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t>& shouldDelete); 
    void clearMemoryForTemporaryMergedDeltas(unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMap, unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t>& shouldDelete);

    bool createFileHandlerForGC(hashStoreFileMetaDataHandler*& fileHandlerPtr,
            uint64_t targetPrefixLen, uint64_t previousFileID1, uint64_t
            previousFileID2, hashStoreFileHeader& newFileHeader);

    void putKeyValueListToAppendableCache(const str_t& currentKeyStr, vector<str_t>& values); 
    void putKDToCache(const str_t& currentKeyStr, vector<str_t>& values); 
    bool singleFileRewrite(hashStoreFileMetaDataHandler* currentHandlerPtr,
            unordered_map<str_t, pair<vector<str_t>,
            vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
            mapEqualKeForStr_t>& gcResultMap, 
            uint64_t targetFileSize, bool fileContainsReWriteKeysFlag);
    bool singleFileSplit(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t>& gcResultMap, uint64_t prefixBitNumber, bool fileContainsReWriteKeysFlag);
    bool twoAdjacentFileMerge(hashStoreFileMetaDataHandler* currentHandlerPtr1,
            hashStoreFileMetaDataHandler* currentHandlerPtr2, 
            uint64_t target_prefix, uint64_t prefix_len);
    bool selectFileForMerge(uint64_t targetFileIDForSplit,
            hashStoreFileMetaDataHandler*& currentHandlerPtr1,
            hashStoreFileMetaDataHandler*& currentHandlerPtr2, 
            uint64_t& target_prefix, uint64_t& prefix_len);
    bool pushObjectsToWriteBackQueue(vector<writeBackObject*>& targetWriteBackVec);
    // message management
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
    messageQueue<writeBackObject*>* write_back_queue_;
    AppendAbleLRUCacheStrVector* keyToValueListCacheStr_ = nullptr;
    KDLRUCache* kd_cache_ = nullptr;
    std::mutex operationNotifyMtx_;
    std::mutex metaCommitMtx_;
    std::condition_variable operationNotifyCV_;
    std::condition_variable metaCommitCV_;
    boost::atomic<uint64_t> workingThreadExitFlagVec_;
    bool syncStatistics_;

    boost::atomic<uint64_t> num_buckets_;

    FileOperation* commit_log_fop_ = nullptr;
    uint64_t commit_log_maximum_size_ = 1024 * 1024 * 1024; // 1024 * 1024 * 1024;
    uint64_t commit_log_next_threshold_ = 1024 * 1024 * 1024; //1024 * 1024 * 1024;
};

} // namespace DELTAKV_NAMESPACE
