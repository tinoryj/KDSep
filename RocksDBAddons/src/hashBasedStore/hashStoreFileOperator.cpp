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
    delete operationToWorkerMQ_;
}

bool HashStoreFileOperator::setJobDone()
{
    operationToWorkerMQ_->done_ = true;
    return true;
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

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<bool>>> tempFileHandlerMap)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto fileHandlerIt : tempFileHandlerMap) {
        hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(fileHandlerIt.first);
        currentOperationHandler->jobDone = false;
        currentOperationHandler->batched_write_operation_.key_str_vec_ptr_ = &std::get<0>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.value_str_vec_ptr_ = &std::get<1>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.is_anchor_vec_ptr_ = &std::get<2>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->opType_ = kMultiPut;
        operationToWorkerMQ_->push(currentOperationHandler);
        currentOperationHandlerVec.push_back(currentOperationHandler);
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

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, vector<string> keyVec, vector<string> valueVec, vector<bool> isAnchorStatusVec)
{
    hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(fileHandler);
    currentOperationHandler->jobDone = false;
    currentOperationHandler->batched_write_operation_.key_str_vec_ptr_ = &keyVec;
    currentOperationHandler->batched_write_operation_.value_str_vec_ptr_ = &valueVec;
    currentOperationHandler->batched_write_operation_.is_anchor_vec_ptr_ = &isAnchorStatusVec;
    currentOperationHandler->opType_ = kMultiPut;
    operationToWorkerMQ_->push(currentOperationHandler);
    while (!currentOperationHandler->jobDone) {
        asm volatile("");
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
    if (valueVec->size() == 0) {
        return false;
    } else {
        return true;
    }
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
    for (auto it : *valueVecVec) {
        if (it->size() == 0) {
            return false;
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
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                resultMap.at(currentKeyStr).clear();
                continue;
            } else {
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            continue;
        } else {
            string currentKeyStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    return processedObjectNumber;
}

void HashStoreFileOperator::operationWorker()
{
    while (true) {
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            if (currentHandlerPtr->opType_ == kGet) {
                debug_trace("receive operations, type = kGet, key = %s, target file ID = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                // try extract from cache first
                if (keyToValueListCache_) {
                    if (keyToValueListCache_->existsInCache(*currentHandlerPtr->read_operation_.key_str_)) {
                        vector<string> tempResultVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_);
                        // debug_trace("read operations from cache, cache hit, hit vec size = %lu\n", tempResultVec.size());
                        // for (auto it : tempResultVec) {
                        //     debug_trace("\thit vec item =  %s\n", it.c_str());
                        // }
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(tempResultVec.begin(), tempResultVec.end());
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                        currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                        if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ > 0) {
                            currentHandlerPtr->file_handler_->file_operation_func_ptr_->flushFile();
                            currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                        }
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->readFile(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                        currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                        unordered_map<string, vector<string>> currentFileProcessMap;
                        uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                        if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                            debug_error("[ERROR] Read bucket get mismatched object number, number in metadata = %lu, number read from file = %lu\n", currentHandlerPtr->file_handler_->total_object_count_, totalProcessedObjectNumber);
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                                debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
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
                    // no cache, only read content
                    char readBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ > 0) {
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->flushFile();
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                    }
                    debug_trace("target read file content (cache not enabled) size = %lu\n", currentHandlerPtr->file_handler_->total_object_bytes_);
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->readFile(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                    if (totalProcessedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                        debug_error("[ERROR] Read bucket get mismatched object number, number in metadata = %lu, number read from file = %lu\n", currentHandlerPtr->file_handler_->total_object_count_, totalProcessedObjectNumber);
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } else {
                        if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                            debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        } else {
                            debug_trace("get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), (*currentHandlerPtr->read_operation_.key_str_).size());
                            currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            currentHandlerPtr->jobDone = true;
                            continue;
                        }
                    }
                }
            } else if (currentHandlerPtr->opType_ == kPut) {
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", (*currentHandlerPtr->write_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                hashStoreRecordHeader newRecordHeader;
                newRecordHeader.is_anchor_ = currentHandlerPtr->write_operation_.is_anchor;
                newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.key_str_->size();
                newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.value_str_->size();
                if (newRecordHeader.is_anchor_ == true) {
                    if (currentHandlerPtr->file_handler_->total_object_count_ != 0) {
                        char writeHeaderBuffer[sizeof(newRecordHeader) + newRecordHeader.key_size_];
                        memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
                        memcpy(writeHeaderBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
                        currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                        // write content
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->writeFile(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_);
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_);
                        if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                            currentHandlerPtr->file_handler_->file_operation_func_ptr_->flushFile();
                            debug_trace("lushed file id = %lu, flushed size = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_);
                            currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                        } else {
                            debug_trace("buffered not flushed file id = %lu, buffered size = %lu, current key size = %u\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_, newRecordHeader.key_size_);
                        }
                        // Update metadata
                        currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_);
                        currentHandlerPtr->file_handler_->total_object_count_++;
                        currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();
                    }
                } else {
                    char writeHeaderBuffer[sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_];
                    memcpy(writeHeaderBuffer, &newRecordHeader, sizeof(newRecordHeader));
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
                    memcpy(writeHeaderBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                    // write content
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->writeFile(writeHeaderBuffer, sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                        currentHandlerPtr->file_handler_->file_operation_func_ptr_->flushFile();
                        debug_trace("flushed file id = %lu, flushed size = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_);
                        currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                    } else {
                        debug_trace("buffered not flushed file id = %lu, buffered size = %lu, current key size = %u\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_, newRecordHeader.key_size_);
                    }
                    // Update metadata
                    currentHandlerPtr->file_handler_->total_object_bytes_ += (sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_);
                    currentHandlerPtr->file_handler_->total_object_count_++;
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
                // insert into GC job queue if exceed the threshold
                if (currentHandlerPtr->file_handler_->total_object_bytes_ >= perFileGCSizeLimit_ && (currentHandlerPtr->file_handler_->gc_result_status_flag_ == kNew || currentHandlerPtr->file_handler_->gc_result_status_flag_ == kMayGC)) {
                    debug_info("GC threshold = %lu\n", perFileGCSizeLimit_);
                    debug_info("Current file id = %lu, exceed GC threshold = %lu, current size = %lu, put into GC job queue\n", currentHandlerPtr->file_handler_->target_file_id_, perFileGCSizeLimit_, currentHandlerPtr->file_handler_->total_object_bytes_);
                    notifyGCToManagerMQ_->push(currentHandlerPtr->file_handler_);
                }
                // mark job done
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone = true;
                continue;
            } else if (currentHandlerPtr->opType_ == kMultiPut) {
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", (*currentHandlerPtr->write_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                // prepare write buffer;
                uint64_t targetWriteBufferSize = 0;
                for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
                    targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i).size());
                    if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i) == true) {
                        continue;
                    } else {
                        targetWriteBufferSize += currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i).size();
                    }
                }
                char writeContentBuffer[targetWriteBufferSize];
                uint64_t currentProcessedBufferIndex = 0;
                for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
                    hashStoreRecordHeader newRecordHeader;
                    newRecordHeader.is_anchor_ = currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i);
                    newRecordHeader.key_size_ = currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i).size();
                    newRecordHeader.value_size_ = currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i).size();
                    memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
                    currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
                    memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i).c_str(), newRecordHeader.key_size_);
                    currentProcessedBufferIndex += newRecordHeader.key_size_;
                    if (newRecordHeader.is_anchor_ == true) {
                        continue;
                    } else {
                        memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i).c_str(), newRecordHeader.value_size_);
                        currentProcessedBufferIndex += newRecordHeader.value_size_;
                    }
                }
                currentHandlerPtr->file_handler_->fileOperationMutex_.lock();
                // write content
                currentHandlerPtr->file_handler_->file_operation_func_ptr_->writeFile(writeContentBuffer, targetWriteBufferSize);
                currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ += targetWriteBufferSize;
                if (currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                    debug_trace("flushed file id = %lu, flushed size = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_);
                    currentHandlerPtr->file_handler_->file_operation_func_ptr_->flushFile();
                    currentHandlerPtr->file_handler_->temp_not_flushed_data_bytes_ = 0;
                }
                // Update metadata
                currentHandlerPtr->file_handler_->total_object_bytes_ += targetWriteBufferSize;
                currentHandlerPtr->file_handler_->total_object_count_ += currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size();
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->file_handler_->fileOperationMutex_.unlock();

                // insert to cache if need
                if (keyToValueListCache_ != nullptr) {
                    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
                        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i).size());
                        if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i) == true) {
                            keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).clear();
                        } else {
                            keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).push_back(currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i));
                        }
                    }
                }
                // mark job done
                currentHandlerPtr->jobDone = true;
                // insert into GC job queue if exceed the threshold
                if (currentHandlerPtr->file_handler_->total_object_bytes_ >= perFileGCSizeLimit_ && (currentHandlerPtr->file_handler_->gc_result_status_flag_ == kNew || currentHandlerPtr->file_handler_->gc_result_status_flag_ == kMayGC)) {
                    notifyGCToManagerMQ_->push(currentHandlerPtr->file_handler_);
                    debug_info("Current file id = %lu exceed GC threshold = %lu, current size = %lu, put into GC job queue\n", currentHandlerPtr->file_handler_->target_file_id_, perFileGCSizeLimit_, currentHandlerPtr->file_handler_->total_object_bytes_);
                }
                continue;
            } else {
                debug_error("[ERROR] Unknown operation type = %d\n", currentHandlerPtr->opType_);
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                break;
            }
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
