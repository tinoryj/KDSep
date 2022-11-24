#include "interface/deltaKVOptions.hpp"
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
    bool get(string keyStr, externalIndexInfo storageInfo, string* valueStrPtr);
    bool multiGet(vector<string> keyStrVec, vector<externalIndexInfo> storageInfoVec, vector<string*> valueStrPtrVec);
    bool forcedManualGarbageCollection();

private:
    uint64_t extractValueSizeThreshold_ = 0;
    string workingDir_;
    DeltaKVOptions* internalOptionsPtr_;
    rocksdb::DB* pointerToRawRocksDBForGC_;

    KvServer* kvServer_;
};

}
