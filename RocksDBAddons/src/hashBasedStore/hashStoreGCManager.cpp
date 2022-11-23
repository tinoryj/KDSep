#include "hashBasedStore/hashStoreGCManager.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreGCManager::HashStoreGCManager(string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ, messageQueue<hashStoreFileMetaDataHandler*>* GCNotifyFileMetaDataUpdateMQ)
{
    workingDirStr_ = workingDirStr;
    fileManagerNotifyGCMQ_ = fileManagerNotifyGCMQ;
    GCNotifyFileMetaDataUpdateMQ_ = GCNotifyFileMetaDataUpdateMQ;
}

HashStoreGCManager::~HashStoreGCManager()
{
}

uint64_t HashStoreGCManager::deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>* resultMap)
{
    uint64_t processedObjectNumber = 0;

    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    hashStoreFileHeader currentFileHeader;
    memcpy(&currentFileHeader, fileContentBuffer, sizeof(currentFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);

    while (currentProcessLocationIndex != fileSize) {
        processedObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMap->find(currentKeyStr) != resultMap->end()) {
                resultMap->at(currentKeyStr).clear();
                currentProcessLocationIndex += (currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                continue;
            } else {
                currentProcessLocationIndex += (currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                continue;
            }
        } else {
            if (resultMap->find(currentKeyStr) != resultMap->end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap->at(currentKeyStr).push_back(currentValueStr);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap->insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    return processedObjectNumber;
}

void HashStoreGCManager::gcWorker()
{
    while (true) {
        hashStoreFileMetaDataHandler* currentHandlerPtr;
        fileManagerNotifyGCMQ_->pop(currentHandlerPtr);
        {
            // rewrite current file for GC
            currentHandlerPtr->fileOperationMutex_.lock();
            currentHandlerPtr->fileOperationMutex_.unlock();
            GCNotifyFileMetaDataUpdateMQ_->push(currentHandlerPtr);
        }
    }
}

} // namespace DELTAKV_NAMESPACE
