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
    cerr << "Delete kv server ..." << endl;
    delete kvServer_;
    cerr << "Delete devices ..." << endl;
    delete devices_;
    cerr << "Delete IndexStoreInterface complete ..." << endl;
}

uint64_t IndexStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool IndexStoreInterface::put(mempoolHandler_t& objectPairMemPoolHandler, bool sync)
{
    externalIndexInfo valueLoc;
    char buffer[sizeof(uint32_t) + objectPairMemPoolHandler.valueSize_];
    memcpy(buffer, &objectPairMemPoolHandler.sequenceNumber_, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
    STAT_PROCESS(kvServer_->putValue(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_, buffer, objectPairMemPoolHandler.valueSize_ + sizeof(uint32_t), valueLoc, sync), StatsType::UPDATE);
    return true;
}

bool IndexStoreInterface::multiPut(vector<mempoolHandler_t>& objectPairMemPoolHandlerVec)
{
    for (auto i = 0; i < objectPairMemPoolHandlerVec.size(); i++) {
        put(objectPairMemPoolHandlerVec[i], false);
    }
    kvServer_->flushBuffer();
    return true;
}

bool IndexStoreInterface::get(const string keyStr, externalIndexInfo storageInfo, string* valueStrPtr, uint32_t* seqNumberPtr)
{

    char* key = new char[keyStr.length() + 2];
    char* value = nullptr;
    len_t valueSize = 0;

    strcpy(key, keyStr.c_str());

    debug_trace("get key [%.*s] offset %x%x valueSize %d\n", (int)keyStr.length(), keyStr.c_str(), storageInfo.externalFileID_, storageInfo.externalFileOffset_, storageInfo.externalContentSize_);

    STAT_PROCESS(kvServer_->getValue(key, keyStr.length(), value, valueSize, storageInfo), StatsType::GET);

    if (seqNumberPtr != nullptr) {
        memcpy(seqNumberPtr, value, sizeof(uint32_t));
    }

    *valueStrPtr = std::string(value + sizeof(uint32_t), valueSize - sizeof(uint32_t));
    debug_trace("get key [%.*s] valueSize %d seqnum %u\n", (int)keyStr.length(), keyStr.c_str(), (int)valueSize, (seqNumberPtr) ? *seqNumberPtr : 5678);
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
