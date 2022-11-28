#pragma once

#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "interface/deltaKVOptions.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class HashStoreInterface {
public:
    HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
        HashStoreFileOperator*& hashStoreFileOperator);
    ~HashStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(const string& keyStr, const string& valueStr, bool isAnchor);
    bool multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<bool> isAnchorVec);
    bool get(const string& keyStr, vector<string>*& valueStrVecPtr);
    bool multiGet(vector<string> keyStrVec, vector<vector<string>*>*& valueStrVecVecPtr);
    bool forcedManualGarbageCollection();

private:
    DeltaKVOptions* internalOptionsPtr_;
    // size information
    uint64_t extractValueSizeThreshold_;
    // get function pointers
    HashStoreFileManager* hashStoreFileManagerPtr_;
    HashStoreFileOperator* hashStoreFileOperatorPtr_;
    // message queues for internal usage
    messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ_;
};

}