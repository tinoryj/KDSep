#pragma once

#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/mempool.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class HashStoreInterface {
public:
    HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
        HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue);
    ~HashStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(mempoolHandler_t objectPairMemPoolHandler);
    bool multiPut(vector<mempoolHandler_t> objectPairMemPoolHandlerVec);
    bool get(const string& keyStr, vector<string>& valueStrVecPtr);
    bool get(const string& keyStr, vector<string>& valueStrVec, vector<hashStoreRecordHeader>& recordVec);
    bool multiGet(vector<string> keyStrVec, vector<vector<string>>& valueStrVecVecPtr);
    bool forcedManualGarbageCollection();
    bool setJobDone();

private:
    bool anyBucketInitedFlag_ = false;
    DeltaKVOptions* internalOptionsPtr_ = nullptr;
    uint64_t fileFlushThreshold_ = 0;
    bool shouldUseDirectOperationsFlag_;
    bool enable_lsm_tree_delta_meta_ = true;
    bool enable_index_block_ = false;
    bool enable_crash_consistency_ = false;
    // size information
    uint64_t extractValueSizeThreshold_;
    // get function pointers
    HashStoreFileManager* hashStoreFileManagerPtr_ = nullptr;
    HashStoreFileOperator* hashStoreFileOperatorPtr_ = nullptr;
    // message queues for internal usage
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_ = nullptr;
};

}
