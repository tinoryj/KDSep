#include "hashBasedStore/hashStoreFileOperator.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(messageQueue<hashStoreOperationHandler*>* operationToWorkerMQ)
{
    operationToWorkerMQ_ = operationToWorkerMQ;
}

HashStoreFileOperator::~HashStoreFileOperator()
{
}

// file operations
bool HashStoreFileOperator::putOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string* key, string* value, bool isAnchorStatus)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler;
    currentHandler->file_handler = fileHandler;
    currentHandler->jobDone = false;
    currentHandler->key_str_ = key;
    currentHandler->value_str_ = value;
    currentHandler->is_anchor = isAnchorStatus;
    operationToWorkerMQ_->push(currentHandler);
    while (currentHandler->jobDone == false) {
    }
    delete currentHandler;
    return true;
}

bool HashStoreFileOperator::putOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string*> keyVec, vector<string*> valueVec, vector<bool> isAnchorStatusVec)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto i = 0; i < fileHandlerVec.size(); i++) {
        hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler;
        currentHandler->file_handler = fileHandlerVec[i];
        currentHandler->jobDone = false;
        currentHandler->key_str_ = keyVec[i];
        currentHandler->value_str_ = valueVec[i];
        currentHandler->is_anchor = isAnchorStatusVec[i];
        operationToWorkerMQ_->push(currentHandler);
        currentOperationHandlerVec.push_back(currentHandler);
    }
    while (currentOperationHandlerVec.size() != 0) {
        for (vector<hashStoreOperationHandler*>::iterator currentIt = currentOperationHandlerVec.begin(); currentIt != currentOperationHandlerVec.end(); currentIt++) {
            if ((*currentIt)->jobDone == true) {
                delete (*currentIt);
                currentOperationHandlerVec.erase(currentIt);
            }
        }
    }
    return true;
}

void HashStoreFileOperator::operationWorker()
{
    while (true) {
        hashStoreOperationHandler* currentHandlerPtr;
        operationToWorkerMQ_->pop(currentHandlerPtr);
        {
            boost::unique_lock<boost::shared_mutex> t(currentHandlerPtr->file_handler->fileOperationMutex_);
            hashStoreRecordHeader newRecordHeader;
            newRecordHeader.is_anchor_ = currentHandlerPtr->is_anchor;
            newRecordHeader.key_size_ = currentHandlerPtr->key_str_->size();
            newRecordHeader.value_size_ = currentHandlerPtr->value_str_->size();
            char writeHeaderBuffer[sizeof(newRecordHeader)];
            memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
            currentHandlerPtr->file_handler->file_operation_stream_.write(writeHeaderBuffer, sizeof(newRecordHeader));
            currentHandlerPtr->file_handler->file_operation_stream_ << currentHandlerPtr->key_str_ << currentHandlerPtr->value_str_;
            currentHandlerPtr->file_handler->file_operation_stream_.flush();
            // Update metadata
            currentHandlerPtr->file_handler->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
            currentHandlerPtr->jobDone = true;
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
