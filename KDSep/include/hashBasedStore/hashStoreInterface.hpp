#pragma once

#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "interface/KDSepOptions.hpp"
#include "utils/mempool.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace KDSEP_NAMESPACE {

class HashStoreInterface {
public:
    HashStoreInterface(KDSepOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
        HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObject*>* writeBackOperationsQueue);
    ~HashStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(mempoolHandler_t objectPairMemPoolHandler);
    bool multiPut(vector<mempoolHandler_t> objectPairMemPoolHandlerVec);
    bool get(const string& keyStr, vector<string>& valueStrVecPtr);
    bool get(const string& keyStr, vector<string>& valueStrVec, vector<hashStoreRecordHeader>& recordVec);
    bool multiGet(const vector<string>& keyStrVec, vector<vector<string>>& valueStrVecVecPtr);
    bool forcedManualGarbageCollection();
    bool setJobDone();

private:
    bool anyBucketInitedFlag_ = false;
    KDSepOptions* internalOptionsPtr_ = nullptr;
    uint64_t fileFlushThreshold_ = 0;
    bool shouldUseDirectOperationsFlag_;
    bool enable_lsm_tree_delta_meta_ = true;
    bool enable_crash_consistency_ = false;
    bool enable_parallel_get_hdl_ = true;
    bool enable_bucket_size_limit_ = false;
    // size information
    uint64_t extractValueSizeThreshold_;
    // get function pointers
    HashStoreFileManager* file_manager_ = nullptr;
    HashStoreFileOperator* file_operator_ = nullptr;
    KDLRUCache* kd_cache_ = nullptr;
    // message queues for internal usage
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_ = nullptr;
};

}
