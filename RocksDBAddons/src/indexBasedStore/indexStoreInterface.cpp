#include "indexBasedStore/indexStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

IndexStoreInterface::IndexStoreInterface(DeltaKVOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB)
{
    internalOptionsPtr_ = options;
    workingDir_ = workingDir;
    pointerToRawRocksDBForGC_ = pointerToRawRocksDB;
    extractValueSizeThreshold_ = options->extract_to_valueStore_size_lower_bound;
}

IndexStoreInterface::~IndexStoreInterface()
{
}

uint64_t IndexStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool IndexStoreInterface::put(string keyStr, string valueStr, externalIndexInfo* storageInfoPtr)
{
    return true;
}

bool IndexStoreInterface::multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<externalIndexInfo>* storageInfoVecPtr)
{
    return true;
}

bool IndexStoreInterface::get(string keyStr, externalIndexInfo storageInfo, string* valueStrPtr)
{
    return true;
}
bool IndexStoreInterface::multiGet(vector<string> keyStrVec, vector<externalIndexInfo> storageInfoVec, vector<string*> valueStrPtrVec)
{
    return true;
}

bool IndexStoreInterface::forcedManualGarbageCollection()
{
    return true;
}

}