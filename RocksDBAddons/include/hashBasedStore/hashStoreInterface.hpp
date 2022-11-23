#pragma once

#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "hashBasedStore/hashStoreGCManager.hpp"
#include "interface/deltaKVOptions.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class HashStoreInterface {
public:
    HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr);
    ~HashStoreInterface();
    bool put(const string& keyStr, const string& valueStr);
    vector<bool> multiPut(vector<string> keyStrVec, vector<string*> valueStrPtrVec);
    bool get(const string& keyStr, string* valueStrPtr);
    vector<bool> multiGet(vector<string> keyStrVec, vector<string*> valueStrPtrVec);
    bool forcedManualGarbageCollection();

    // get function pointers
    HashStoreFileManager* hashStoreFileManager_;
    HashStoreFileOperator* hashStoreFileOperator_;
    HashStoreGCManager* hashStoreGCManager_;

private:
    DeltaKVOptions* internalOptionsPtr_;
    messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ_;
    messageQueue<hashStoreFileMetaDataHandler*>* GCNotifyFileMetaDataUpdateMQ_;
};

}