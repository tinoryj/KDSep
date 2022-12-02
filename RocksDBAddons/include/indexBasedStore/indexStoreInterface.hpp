#include "indexStore.hh"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class IndexStoreInterface {
public:
    IndexStoreInterface(DeltaKVOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB);
    ~IndexStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(string keyStr, string valueStr, externalIndexInfo* storageInfoPtr);
    bool multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<externalIndexInfo*> storageInfoVecPtr);
    bool get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr);
    bool multiGet(vector<string> keyStrVec, vector<externalIndexInfo> storageInfoVec, vector<string*> valueStrPtrVec);
    bool forcedManualGarbageCollection();
    bool restoreVLog(std::map<std::string, externalIndexInfo>& keyValues);

private:
    uint64_t extractValueSizeThreshold_ = 0;
    string workingDir_;
    DeltaKVOptions* internalOptionsPtr_;
    rocksdb::DB* pointerToRawRocksDBForGC_;

    DeviceManager* devices_;
    KvServer* kvServer_;
};

}
