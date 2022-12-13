#include "indexBasedStore/indexStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

IndexStoreInterface::IndexStoreInterface(DeltaKVOptions* options, string workingDir, rocksdb::DB* pointerToRawRocksDB)
{

    internalOptionsPtr_ = options;
    workingDir_ = workingDir;
    pointerToRawRocksDBForGC_ = pointerToRawRocksDB;
    extractValueSizeThreshold_ = options->extract_to_valueStore_size_lower_bound;

    DiskInfo disk1(0, workingDir.c_str(), 1 * 1024 * 1024 * 1024);
    std::vector<DiskInfo> disks;
    disks.push_back(disk1);
    devices_ = new DeviceManager(disks);

    kvServer_ = new KvServer(devices_, pointerToRawRocksDBForGC_);
}

IndexStoreInterface::~IndexStoreInterface()
{
    delete kvServer_;
    delete devices_;
}

uint64_t IndexStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool IndexStoreInterface::put(string keyStr, string valueStr, externalIndexInfo* storageInfoPtr, bool sync)
{
    externalIndexInfo valueLoc;

    STAT_PROCESS(kvServer_->putValue(keyStr.c_str(), keyStr.length(), valueStr.c_str(), valueStr.length(), valueLoc, sync), StatsType::UPDATE);

    *storageInfoPtr = valueLoc;
    return true;
}

bool IndexStoreInterface::multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<externalIndexInfo*> storageInfoVecPtr)
{
    for (int i = 0; i < (int)keyStrVec.size(); i++) {
        put(keyStrVec[i], valueStrPtrVec[i], storageInfoVecPtr[i]);
    }
    kvServer_->flushBuffer();
    return true;
}

bool IndexStoreInterface::get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr)
{

    char* key = new char[keyStr.length() + 2];
    char* value = nullptr;
    len_t valueSize = 0;

    strcpy(key, keyStr.c_str());

    STAT_PROCESS(kvServer_->getValue(key, keyStr.length(), value, valueSize, storageInfo), StatsType::GET);

    *valueStrPtr = std::string(value, valueSize);
    debug_trace("get key [%.*s] valueSize %d\n", (int)keyStr.length(), keyStr.c_str(), (int)valueSize);
    if (value) {
        free(value);
    }
    delete[] key;
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
    kvServer_->gc(false);
    return true;
}

bool IndexStoreInterface::restoreVLog(std::map<std::string, externalIndexInfo>& keyValues)
{
    kvServer_->restoreVLog(keyValues);
    return true;
}

}
