#include "hashBasedStore/hashStoreFileOperator.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ)
{
    operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
    fileManagerNotifyGCMQ_ = fileManagerNotifyGCMQ;
    if (options->enable_deltaStore_KDLevel_cache == true) {
        keyToValueListCache_ = new BOOSTLRUCache<string, vector<string>>(options->deltaStore_KDLevel_cache_size);
    }
}

HashStoreFileOperator::~HashStoreFileOperator()
{
    if (keyToValueListCache_) {
        delete keyToValueListCache_;
    }
    operationToWorkerMQ_->done_ = true;
    delete operationToWorkerMQ_;
}

// file operations
bool HashStoreFileOperator::putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, bool isAnchorStatus)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone = false;
    currentHandler->write_operation_.key_str_ = &key;
    currentHandler->write_operation_.value_str_ = &value;
    currentHandler->write_operation_.is_anchor = isAnchorStatus;
    currentHandler->opType_ = kPut;
    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[putWriteOperationIntoJobQueue] put write operation to job queue success, fileHandler address = " << currentHandler->file_handler_ << RESET << endl;
    operationToWorkerMQ_->push(currentHandler);
    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[putWriteOperationIntoJobQueue] put write operation to job queue success, operation handler address = " << currentHandler << RESET << endl;
    while (!currentHandler->jobDone) {
        asm volatile("");
    }
    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[putWriteOperationIntoJobQueue] get write operation success flag" << RESET << endl;
    delete currentHandler;
    return true;
}

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<string> valueVec, vector<bool> isAnchorStatusVec)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto i = 0; i < fileHandlerVec.size(); i++) {
        hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandlerVec[i]);
        currentHandler->jobDone = false;
        currentHandler->write_operation_.key_str_ = &keyVec[i];
        currentHandler->write_operation_.value_str_ = &valueVec[i];
        currentHandler->write_operation_.is_anchor = isAnchorStatusVec[i];
        currentHandler->opType_ = kPut;
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

bool HashStoreFileOperator::putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>& valueVec)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone = false;
    currentHandler->read_operation_.key_str_ = &key;
    currentHandler->read_operation_.value_str_vec_ = &valueVec;
    currentHandler->opType_ = kGet;
    operationToWorkerMQ_->push(currentHandler);
    while (currentHandler->jobDone == false) {
        asm volatile("");
    }
    delete currentHandler;
    return true;
}

bool HashStoreFileOperator::putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>& valueVecVec)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto i = 0; i < fileHandlerVec.size(); i++) {
        hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandlerVec[i]);
        currentHandler->jobDone = false;
        currentHandler->read_operation_.key_str_ = &keyVec[i];
        currentHandler->read_operation_.value_str_vec_ = valueVecVec[i];
        currentHandler->opType_ = kGet;
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

uint64_t HashStoreFileOperator::processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string, vector<string>>& resultMap)
{
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    hashStoreFileHeader currentFileHeader;
    memcpy(&currentFileHeader, contentBuffer, sizeof(currentFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);
    uint64_t processedObjectNumber = 0;
    while (currentProcessLocationIndex != contentSize) {
        processedObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, contentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        string currentKeyStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[processReadContentToValueLists] current key = " << currentKeyStr << RESET << endl;
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                resultMap.at(currentKeyStr).clear();
                continue;
            } else {
                continue;
            }
        } else {
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[processReadContentToValueLists] current value = " << currentValueStr << RESET << endl;
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[processReadContentToValueLists] current value = " << currentValueStr << RESET << endl;
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    return processedObjectNumber;
}

void HashStoreFileOperator::operationWorker()
{
    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] start receive operations" << RESET << endl;
    if (operationToWorkerMQ_ == nullptr) {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] message queue not initial" << RESET << endl;
        return;
    }
    while (true) {
        if (operationToWorkerMQ_->done_ == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] receive operations, type = " << currentHandlerPtr->opType_ << RESET << endl;
            if (currentHandlerPtr->opType_ == kGet) {
                // // try extract from cache first
                if (keyToValueListCache_) {
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read operations from cache" << RESET << endl;
                    if (keyToValueListCache_->existsInCache(*currentHandlerPtr->read_operation_.key_str_)) {
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read operations from cache, cache hit, hit vec size = " << keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_).size() << RESET << endl;
                        vector<string> tempCopyVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_);
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(tempCopyVec.begin(), tempCopyVec.end());
                        // for (int i = 0; i < keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_).size(); i++) {
                        //     currentHandlerPtr->read_operation_.value_str_vec_->push_back(keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_)[i]);
                        // }
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read operations from cache result vec size = " << currentHandlerPtr->read_operation_.value_str_vec_->size() << RESET << endl;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read operations from cache, cache miss" << RESET << endl;
                        char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                        currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache enabled) size = " << currentHandlerPtr->file_handler_->total_object_bytes_ << ", current file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << ", current file write pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellp() << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_stream_.seekg(0, ios::beg);
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache enabled) after reset file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_stream_.read(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache enabled) after read file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << RESET << endl;
                        cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read file content buffer size (cache enabled) = " << sizeof(readBuffer) << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_stream_.seekp(0, ios::end);
                        currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                        unordered_map<string, vector<string>> currentFileProcessMap;
                        uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                        if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] read bucket get mismatched object number, number in metadata = " << currentHandlerPtr->file_handler_->total_object_count_ << ", number read from file = " << totalProcessedObjectNumber << RESET << endl;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                                cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] read bucket done, but could not found values for key = " << (*currentHandlerPtr->read_operation_.key_str_) << RESET << endl;
                                currentHandlerPtr->jobDone = true;
                                continue;
                            } else {
                                currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                                currentHandlerPtr->jobDone = true;
                                // insert to cache
                                for (auto mapIt : currentFileProcessMap) {
                                    string tempKeyForCacheInsert = mapIt.first;
                                    keyToValueListCache_->insertToCache(tempKeyForCacheInsert, mapIt.second);
                                }
                                continue;
                            }
                        }
                    }
                } else {
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read operations from file" << RESET << endl;
                    // no cache, only read content
                    char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache not enabled) size = " << currentHandlerPtr->file_handler_->total_object_bytes_ << ", current file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << ", current file write pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellp() << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_stream_.seekg(0, ios::beg);
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache not enabled) after reset file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_stream_.read(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] target read file content (cache not enabled) after read file read pointer = " << currentHandlerPtr->file_handler_->file_operation_stream_.tellg() << RESET << endl;
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] read file content buffer size (cache not enabled) = " << sizeof(readBuffer) << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_stream_.seekp(0, ios::end);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                    if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] read bucket get mismatched object number, number in metadata = " << currentHandlerPtr->file_handler_->total_object_count_ << ", number read from file = " << totalProcessedObjectNumber << RESET << endl;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] read bucket done, but could not found values for key = " << (*currentHandlerPtr->read_operation_.key_str_) << RESET << endl;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] get current key related values success, key = " << *currentHandlerPtr->read_operation_.key_str_ << ", value number = " << currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size() << RESET << endl;
                            currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                            currentHandlerPtr->jobDone = true;
                            continue;
                        }
                    }
                }
            } else if (currentHandlerPtr->opType_ == kPut) {
                cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations to file" << RESET << endl;
                hashStoreRecordHeader newRecordHeader;
                newRecordHeader.is_anchor_ = currentHandlerPtr->write_operation_.is_anchor;
                newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.key_str_->size();
                newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.value_str_->size();
                if (newRecordHeader.is_anchor_ == true) {
                    char writeHeaderBuffer[sizeof(newRecordHeader) + newRecordHeader.key_size_];
                    memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    // write content
                    currentHandlerPtr->file_handler_->file_operation_stream_.write(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_);
                    currentHandlerPtr->file_handler_->file_operation_stream_.flush();
                    // Update metadata
                    currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_);
                    currentHandlerPtr->file_handler_->total_object_count_++;
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations to file metadata updated" << RESET << endl;
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations to file flushed" << RESET << endl;

                } else {
                    char writeHeaderBuffer[sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_];
                    memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    // write content
                    currentHandlerPtr->file_handler_->file_operation_stream_.write(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->file_operation_stream_.flush();
                    // Update metadata
                    currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->total_object_count_++;
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations to file metadata updated" << RESET << endl;
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations to file flushed" << RESET << endl;
                }

                // insert to cache if need
                if (keyToValueListCache_ != nullptr) {
                    if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                        if (currentHandlerPtr->write_operation_.is_anchor == true) {
                            keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).clear();
                        } else {
                            vector<string> tempInsertCacheVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_);
                            tempInsertCacheVec.push_back(*currentHandlerPtr->write_operation_.value_str_);
                            keyToValueListCache_->insertToCache(*currentHandlerPtr->write_operation_.key_str_, tempInsertCacheVec);
                        }
                    } else {
                        if (currentHandlerPtr->write_operation_.is_anchor == true) {
                            cout << GREEN << "[INFO]:[Addons]-[HashStoreFileOperator]-[operationWorker] Skip put anchor into cache without deltas" << RESET << endl;
                        } else {
                            vector<string> tempValueVec;
                            tempValueVec.push_back(*currentHandlerPtr->write_operation_.value_str_);
                            keyToValueListCache_->insertToCache(*currentHandlerPtr->write_operation_.key_str_, tempValueVec);
                        }
                    }
                }
                // mark job done
                currentHandlerPtr->jobDone = true;
                cout << BLUE << "[DEBUG-LOG]:[Addons]-[HashStoreFileOperator]-[operationWorker] write operations done, operation handler address = " << currentHandlerPtr << RESET << endl;
                continue;
            } else {
                cerr << RED << "[ERROR]:[Addons]-[HashStoreFileOperator]-[operationWorker] Unknown operation type = " << currentHandlerPtr->opType_ << RESET << endl;
            }
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
