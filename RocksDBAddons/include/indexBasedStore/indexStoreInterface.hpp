#include "indexStore.hh"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class IndexStoreInterface {
public:
    IndexStoreInterface(DeltaKVOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB);
    ~IndexStoreInterface();

    uint64_t getExtractSizeThreshold();
    bool put(mempoolHandler_t& objectPairMemPoolHandler, bool sync);
    bool multiPut(vector<mempoolHandler_t>& objectPairMemPoolHandlerVec);
    bool get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr, uint32_t* seqNumberPtr = nullptr);
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
