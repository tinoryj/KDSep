#include "indexBasedStore/indexStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

IndexStoreInterface::IndexStoreInterface(DeltaKVOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB)
{
    internalOptionsPtr_ = options;
    workingDir_ = workingDir;
    pointerToRawRocksDBForGC_ = pointerToRawRocksDB;
    extractValueSizeThreshold_ = options->extract_to_valueStore_size_lower_bound;

    kvServer_ = new KvServer();
}

IndexStoreInterface::~IndexStoreInterface()
{
    delete kvServer_;
}

uint64_t IndexStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool IndexStoreInterface::put(string keyStr, string valueStr, externalIndexInfo* storageInfoPtr)
{
    externalIndexInfo valueLoc;
    kvServer_->putValue(keyStr.c_str(), keyStr.length(), valueStr.c_str(), valueStr.length(), valueLoc);
    *storageInfoPtr = valueLoc;
    return true;
}

bool IndexStoreInterface::multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<externalIndexInfo*> storageInfoVecPtr)
{
    for (int i = 0; i < (int)keyStrVec.size(); i++) {
        put(keyStrVec[i], valueStrPtrVec[i], storageInfoVecPtr[i]);
    }
    return true;
}

bool IndexStoreInterface::get(string keyStr, externalIndexInfo storageInfo, string* valueStrPtr)
{
    char* key = new char[keyStr.length() + 2];
    char* value = nullptr;
    len_t valueSize = 0;

    strcpy(key, keyStr.c_str());
    kvServer_->getValue(key, keyStr.length(), value, valueSize, storageInfo);
    return true;
}
bool IndexStoreInterface::multiGet(vector<string> keyStrVec, vector<externalIndexInfo> storageInfoVec, vector<string*> valueStrPtrVec)
{
    for (int i = 0; i < (int)keyStrVec.size(); i++) {
        get(keyStrVec[i], storageInfoVec[i], valueStrPtrVec[i]);
    }
    return true;
}

bool IndexStoreInterface::forcedManualGarbageCollection()
{
    printf("forcedManualGarbageCollection()\n");
    return true;
}

}
