#include "indexBasedStore/indexStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

indexStoreInterface::indexStoreInterface(DeltaKVOptions* options, rocksdb::DB* pointerToRawRocksDB)
{
    internalOptionsPtr_ = options;
    pointerToRawRocksDBForGC_ = pointerToRawRocksDB;
}

indexStoreInterface::~indexStoreInterface()
{
}

bool indexStoreInterface::put(const string& keyStr, const string& valueStr)
{
    return true;
}

vector<bool> indexStoreInterface::multiPut(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool get(const string& keyStr, string* valueStrPtr)
{
    return true;
}

vector<bool> indexStoreInterface::multiGet(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool indexStoreInterface::forcedManualGarbageCollection()
{
    return true;
}

}