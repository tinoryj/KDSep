#include "hashBasedStore/hashStoreFileOperator.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_file_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
    notifyGCToManagerMQ_ = notifyGCToManagerMQ;
    if (options->enable_deltaStore_KDLevel_cache == true) {
        keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>>(options->deltaStore_KDLevel_cache_item_number);
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
    operationToWorkerMQ_->push(currentHandler);
    while (!currentHandler->jobDone) {
        asm volatile("");
    }
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get write operation success flag" << RESET << endl;
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

bool HashStoreFileOperator::putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone = false;
    currentHandler->read_operation_.key_str_ = &key;
    currentHandler->read_operation_.value_str_vec_ = valueVec;
    currentHandler->opType_ = kGet;
    operationToWorkerMQ_->push(currentHandler);
    while (currentHandler->jobDone == false) {
        asm volatile("");
    }
    delete currentHandler;
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get read operation success flag" << RESET << endl;
    return true;
}

bool HashStoreFileOperator::putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto i = 0; i < fileHandlerVec.size(); i++) {
        hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandlerVec[i]);
        currentHandler->jobDone = false;
        currentHandler->read_operation_.key_str_ = &keyVec[i];
        currentHandler->read_operation_.value_str_vec_ = valueVecVec->at(i);
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
        if (currentObjectRecordHeader.is_anchor_ == true) {
            string currentKeyStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find anchor, current key = " << currentKeyStr << RESET << endl;
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                resultMap.at(currentKeyStr).clear();
                continue;
            } else {
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find gc done flag" << RESET << endl;
            continue;
        } else {
            string currentKeyStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find content, current key = " << currentKeyStr << RESET << endl;
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): current value = " << currentValueStr << RESET << endl;
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): current value = " << currentValueStr << RESET << endl;
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    return processedObjectNumber;
}

void HashStoreFileOperator::operationWorker()
{
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): start receive operations" << RESET << endl;
    if (operationToWorkerMQ_ == nullptr) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): message queue not initial" << RESET << endl;
        return;
    }
    while (true) {
        if (operationToWorkerMQ_->done_ == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            if (currentHandlerPtr->opType_ == kGet) {
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): receive operations, type = kGet"
                     << ", key = " << (*currentHandlerPtr->read_operation_.key_str_) << ", target file ID = " << currentHandlerPtr->file_handler_->target_file_id_ << RESET << endl;
                // // try extract from cache first
                if (keyToValueListCache_) {
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): try read operations from cache" << RESET << endl;
                    if (keyToValueListCache_->existsInCache(*currentHandlerPtr->read_operation_.key_str_)) {
                        vector<string> tempResultVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read operations from cache, cache hit, hit vec size = " << tempResultVec.size() << RESET << endl;
                        for (auto it : tempResultVec) {
                            cout << BLUE << "\thit vec item = " << it << RESET << endl;
                        }
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(tempResultVec.begin(), tempResultVec.end());
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read operations from cache, cache hit, assigned vec size = " << currentHandlerPtr->read_operation_.value_str_vec_->size() << RESET << endl;
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read operations from cache, cache miss" << RESET << endl;
                        char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                        currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                        if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ > 0) {
                            currentHandlerPtr->file_handler_->file_operation_func_ptr_->flush();
                            currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                        }
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->seekg(0, ios::end);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache enabled) size = " << currentHandlerPtr->file_handler_->total_object_bytes_ << ", current file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << ", current file write pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellp() << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->seekg(0, ios::beg);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache enabled) after reset file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->read(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache enabled) after read file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << RESET << endl;
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file content buffer size (cache enabled) = " << sizeof(readBuffer) << RESET << endl;
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->resetPointer(0, ios::end);
                        currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                        unordered_map<string, vector<string>> currentFileProcessMap;
                        uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                        if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read bucket get mismatched object number, number in metadata = " << currentHandlerPtr->file_handler_->total_object_count_ << ", number read from file = " << totalProcessedObjectNumber << RESET << endl;
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read bucket done, but could not found values for key = " << (*currentHandlerPtr->read_operation_.key_str_) << RESET << endl;
                                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                                currentHandlerPtr->jobDone = true;
                                continue;
                            } else {
                                currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
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
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): direct read operations from file" << RESET << endl;
                    // no cache, only read content
                    char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ > 0) {
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->flush();
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                    }
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache not enabled) size = " << currentHandlerPtr->file_handler_->total_object_bytes_ << ", current file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << ", current file write pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellp() << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->seekg(0, ios::beg);
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache not enabled) after reset file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->read(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content (cache not enabled) after read file read pointer = " << currentHandlerPtr->file_handler_->file_operation_func_ptr_->tellg() << RESET << endl;
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file content buffer size (cache not enabled) = " << sizeof(readBuffer) << RESET << endl;
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->resetPointer(0, ios::end);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                    if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read bucket get mismatched object number, number in metadata = " << currentHandlerPtr->file_handler_->total_object_count_ << ", number read from file = " << totalProcessedObjectNumber << RESET << endl;
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read bucket done, but could not found values for key = " << (*currentHandlerPtr->read_operation_.key_str_) << RESET << endl;
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get current key related values success, key = " << *currentHandlerPtr->read_operation_.key_str_ << ", value number = " << currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size() << RESET << endl;
                            currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        }
                    }
                }
            } else if (currentHandlerPtr->opType_ == kPut) {
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): receive operations, type = kPut, key = " << (*currentHandlerPtr->write_operation_.key_str_) << ", target file ID = " << currentHandlerPtr->file_handler_->target_file_id_ << RESET << endl;
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
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->write(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_);
                    if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->flush();
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): flushed file id = " << currentHandlerPtr->file_handler_->target_file_id_ << ", flushed size = " << currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ << RESET << endl;
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                    } else {
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): buffered not flushed file id = " << currentHandlerPtr->file_handler_->target_file_id_ << ", buffered size = " << currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ << ", current header size = " << sizeof(newRecordHeader) << ", current key size = " << newRecordHeader.key_size_ << RESET << endl;
                    }
                    // Update metadata
                    currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_);
                    currentHandlerPtr->file_handler_->total_object_count_++;
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write operations to file metadata updated" << RESET << endl;
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                } else {
                    char writeHeaderBuffer[sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_];
                    memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    // write content
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->write(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->flush();
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): flushed file id = " << currentHandlerPtr->file_handler_->target_file_id_ << ", flushed size = " << currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ << RESET << endl;
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                    } else {
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): buffered not flushed file id = " << currentHandlerPtr->file_handler_->target_file_id_ << ", buffered size = " << currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ << ", current header size = " << sizeof(newRecordHeader) << ", current key size = " << newRecordHeader.key_size_ << ", current value size = " << newRecordHeader.value_size_ << RESET << endl;
                    }
                    // Update metadata
                    currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->total_object_count_++;
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write operations to file metadata updated" << RESET << endl;
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                }

                // insert to cache if need
                if (keyToValueListCache_ != nullptr) {
                    if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                        // insert into cache only if the key has been read
                        if (currentHandlerPtr->write_operation_.is_anchor == true) {
                            keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).clear();
                        } else {
                            keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
                        }
                    }
                }
                // mark job done
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone = true;
                // insert into GC job queue if exceed the threshold
                if (currentHandlerPtr->file_handler_->total_object_bytes_ >= perFileGCSizeLimit_ && currentHandlerPtr->file_handler_->gc_result_status_flag_ != kNoGC && currentHandlerPtr->file_handler_->gc_result_status_flag_ != kShouldDelete) {
                    notifyGCToManagerMQ_->push(currentHandlerPtr->file_handler_);
                    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Current file id = " << currentHandlerPtr->file_handler_->target_file_id_ << " exceed GC threshold = " << perFileGCSizeLimit_ << ", current size = " << currentHandlerPtr->file_handler_->total_object_bytes_ << ", put into GC job queue" << RESET << endl;
                }
                continue;
            } else {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Unknown operation type = " << currentHandlerPtr->opType_ << RESET << endl;
                break;
            }
        }
    }
    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): stop operation worker thread success" << RESET << endl;
    return;
}

} // namespace DELTAKV_NAMESPACE
