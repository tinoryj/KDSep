#include "hashBasedStore/hashStoreFileManager.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(uint64_t initialBitNumber, uint64_t objectGCTriggerSize,
    uint64_t objectGlobalGCTriggerSize, std::string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ)
{
    initialTrieBitNumber_ = initialBitNumber;
    singleFileGCTriggerSize_ = objectGCTriggerSize;
    globalGCTriggerSize_ = objectGlobalGCTriggerSize;
    workingDir_ = workingDirStr;
    fileManagerNotifyGCMQ_ = fileManagerNotifyGCMQ;
}

HashStoreFileManager::~HashStoreFileManager()
{
}

// Manager's metadata management
bool HashStoreFileManager::RetriveHashStoreFileMetaDataList(std::string workingDir)
{
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList(std::string workingDir)
{
    return true;
}

bool HashStoreFileManager::CreateHashStoreFileMetaDataListIfNotExist(std::string workingDir)
{
    return true;
}

bool HashStoreFileManager::RetriveHashStoreFileMetaDataList()
{
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList()
{
    return true;
}

bool HashStoreFileManager::CreateHashStoreFileMetaDataListIfNotExist()
{
    return true;
}

// file operations
bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStr(string keyStr)
{
    return true;
}

bool HashStoreFileManager::getHashStoreFileHandlerStatusByPrefix(const string prefixStr)
{
    return true;
}

bool HashStoreFileManager::generateHashBasedPrefix(const string rawStr, string& prefixStr)
{
    return true;
}

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler* fileHandlerPtr)
{
    return true;
}

bool HashStoreFileManager::createAnfGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler* fileHandlerPtr)
{
    return true;
}

uint64_t HashStoreFileManager::newFileIDGenerator()
{
    targetNewFileID_ += 1;
    return targetNewFileID_;
}

}