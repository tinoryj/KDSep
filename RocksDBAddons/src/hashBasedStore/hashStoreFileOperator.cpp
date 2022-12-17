#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_file_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileSizeLimit_ = options->deltaStore_single_file_maximum_size;
    uint64_t totalNumberOfThreadsAllowed = options->deltaStore_thread_number_limit - 1;
    if (options->enable_deltaStore_garbage_collection == true) {
        totalNumberOfThreadsAllowed--;
    }
    if (totalNumberOfThreadsAllowed > 2) {
        operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
        debug_info("Total thread number for operationWorker > 2, use multithread operation%s\n", "");
    }
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    notifyGCToManagerMQ_ = notifyGCToManagerMQ;
    if (options->enable_deltaStore_KDLevel_cache == true) {
        keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>>(options->deltaStore_KDLevel_cache_item_number);
    }
    workingDir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
}

HashStoreFileOperator::~HashStoreFileOperator()
{
    if (keyToValueListCache_) {
        delete keyToValueListCache_;
    }
    if (operationToWorkerMQ_ != nullptr) {
        delete operationToWorkerMQ_;
    }
}

bool HashStoreFileOperator::setJobDone()
{
    if (operationToWorkerMQ_ != nullptr) {
        operationToWorkerMQ_->done_ = true;
    }
    return true;
}

// file operations
bool HashStoreFileOperator::putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, string value, uint32_t sequenceNumber, bool isAnchorStatus)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone_ = kNotDone;
    currentHandler->write_operation_.key_str_ = &key;
    currentHandler->write_operation_.sequence_number_ = sequenceNumber;
    currentHandler->write_operation_.value_str_ = &value;
    currentHandler->write_operation_.is_anchor = isAnchorStatus;
    currentHandler->opType_ = kPut;
    operationToWorkerMQ_->push(currentHandler);
    while (currentHandler->jobDone_ == kNotDone) {
        asm volatile("");
    }
    if (currentHandler->jobDone_ == kError) {
        delete currentHandler;
        return false;
    } else {
        delete currentHandler;
        return true;
    }
}

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> tempFileHandlerMap)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto fileHandlerIt : tempFileHandlerMap) {
        hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(fileHandlerIt.first);
        currentOperationHandler->jobDone_ = kNotDone;
        currentOperationHandler->batched_write_operation_.key_str_vec_ptr_ = &std::get<0>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.value_str_vec_ptr_ = &std::get<1>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.sequence_number_vec_ptr_ = &std::get<2>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.is_anchor_vec_ptr_ = &std::get<3>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->opType_ = kMultiPut;
        operationToWorkerMQ_->push(currentOperationHandler);
        currentOperationHandlerVec.push_back(currentOperationHandler);
    }
    while (currentOperationHandlerVec.size() != 0) {
        for (vector<hashStoreOperationHandler*>::iterator currentIt = currentOperationHandlerVec.begin(); currentIt != currentOperationHandlerVec.end(); currentIt++) {
            if ((*currentIt)->jobDone_ == kDone) {
                delete (*currentIt);
                currentOperationHandlerVec.erase(currentIt);
            } else if ((*currentIt)->jobDone_ == kError) {
                for (vector<hashStoreOperationHandler*>::iterator currentDeleteIt = currentOperationHandlerVec.begin(); currentDeleteIt != currentOperationHandlerVec.end(); currentDeleteIt++) {
                    delete (*currentDeleteIt);
                }
                return false;
            }
        }
    }
    return true;
}

bool HashStoreFileOperator::putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone_ = kNotDone;
    currentHandler->read_operation_.key_str_ = &key;
    currentHandler->read_operation_.value_str_vec_ = valueVec;
    currentHandler->opType_ = kGet;
    operationToWorkerMQ_->push(currentHandler);
    while (currentHandler->jobDone_ == kNotDone) {
        asm volatile("");
    }
    if (valueVec->size() == 0 || currentHandler->jobDone_ == kError) {
        delete currentHandler;
        return false;
    } else {
        delete currentHandler;
        return true;
    }
}

bool HashStoreFileOperator::putReadOperationsVectorIntoJobQueue(vector<hashStoreFileMetaDataHandler*> fileHandlerVec, vector<string> keyVec, vector<vector<string>*>*& valueVecVec)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto i = 0; i < fileHandlerVec.size(); i++) {
        hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandlerVec[i]);
        currentHandler->jobDone_ = kNotDone;
        currentHandler->read_operation_.key_str_ = &keyVec[i];
        currentHandler->read_operation_.value_str_vec_ = valueVecVec->at(i);
        currentHandler->opType_ = kGet;
        operationToWorkerMQ_->push(currentHandler);
        currentOperationHandlerVec.push_back(currentHandler);
    }
    while (currentOperationHandlerVec.size() != 0) {
        for (vector<hashStoreOperationHandler*>::iterator currentIt = currentOperationHandlerVec.begin(); currentIt != currentOperationHandlerVec.end(); currentIt++) {
            if ((*currentIt)->jobDone_ == true) {
                delete (*currentIt);
                currentOperationHandlerVec.erase(currentIt);
            } else if ((*currentIt)->jobDone_ == kError) {
                for (vector<hashStoreOperationHandler*>::iterator currentDeleteIt = currentOperationHandlerVec.begin(); currentDeleteIt != currentOperationHandlerVec.end(); currentDeleteIt++) {
                    delete (*currentDeleteIt);
                }
                return false;
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

bool HashStoreFileOperator::operationWorkerGetFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    std::shared_lock<std::shared_mutex> r_lock(currentHandlerPtr->file_handler_->fileOperationMutex_);
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(*currentHandlerPtr->read_operation_.key_str_)) {
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_);
            debug_trace("read operations from cache, cache hit, key %s, hit vec size = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), tempResultVec.size());
            currentHandlerPtr->read_operation_.value_str_vec_->assign(tempResultVec.begin(), tempResultVec.end());
            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
            currentHandlerPtr->jobDone_ = kDone;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(currentHandlerPtr->file_handler_, readContentBuffer);
            if (readFromFileStatus == false) {
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone_ = kError;
                return false;
            } else {
                unordered_map<string, vector<string>> currentFileProcessMap;
                uint64_t processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
                if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    currentHandlerPtr->jobDone_ = kError;
                    return false;
                } else {
                    if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone_ = kError;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size());
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            string tempKeyStr = mapIt.first;
                            keyToValueListCache_->insertToCache(tempKeyStr, mapIt.second);
                            debug_trace("Insert to cache key %s delta num %d\n", tempKeyStr.c_str(), (int)mapIt.second.size());
                        }
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone_ = kDone;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(currentHandlerPtr->file_handler_, readContentBuffer);
        if (readFromFileStatus == false) {
            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
            currentHandlerPtr->jobDone_ = kError;
            return false;
        } else {
            unordered_map<string, vector<string>> currentFileProcessMap;
            uint64_t processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap);
            if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone_ = kError;
                return false;
            } else {
                if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    currentHandlerPtr->jobDone_ = kError;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size());
                    currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    currentHandlerPtr->jobDone_ = kDone;
                    return true;
                }
            }
        }
    }
}

bool HashStoreFileOperator::readContentFromFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer)
{
    debug_trace("Read content from file ID = %lu\n", fileHandler->target_file_id_);
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        debug_error("[ERROR] Should not read from a not opened file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    }
    if (fileHandler->temp_not_flushed_data_bytes_ > 0) {
        fileHandler->file_operation_func_ptr_->flushFile();
        fileHandler->temp_not_flushed_data_bytes_ = 0;
    }
    bool readFileStatus;
    STAT_PROCESS(readFileStatus = fileHandler->file_operation_func_ptr_->readFile(contentBuffer, fileHandler->total_object_bytes_), StatsType::DELTAKV_HASHSTORE_GET_IO_TRAFFIC);
    if (readFileStatus == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    } else {
        return true;
    }
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

bool HashStoreFileOperator::writeContentToFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer, uint64_t contentSize, uint64_t contentObjectNumber)
{
    debug_trace("Write content to file ID = %lu\n", fileHandler->target_file_id_);
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        debug_error("[ERROR] Could not write to a not opened file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    }
    uint64_t onDiskWriteSize = 0;
    STAT_PROCESS(onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(contentBuffer, contentSize), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
    if (onDiskWriteSize == 0) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    } else {
        fileHandler->temp_not_flushed_data_bytes_ += contentSize;
        if (fileHandler->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
            debug_trace("Flushed file ID = %lu, flushed size = %lu\n", fileHandler->target_file_id_, fileHandler->temp_not_flushed_data_bytes_);
            fileHandler->file_operation_func_ptr_->flushFile();
            fileHandler->temp_not_flushed_data_bytes_ = 0;
        }
        // update metadata
        fileHandler->total_object_bytes_ += contentSize;
        fileHandler->total_on_disk_bytes_ += onDiskWriteSize;
        fileHandler->total_object_count_ += contentObjectNumber;
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    std::scoped_lock<std::shared_mutex> w_lock(currentHandlerPtr->file_handler_->fileOperationMutex_);
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = false;
    newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.key_str_->size();
    newRecordHeader.sequence_number_ = currentHandlerPtr->write_operation_.sequence_number_;
    newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.value_str_->size();
    if (currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.current_prefix_used_bit_ = currentHandlerPtr->file_handler_->current_prefix_used_bit_;
        newFileHeader.previous_file_id_first_ = currentHandlerPtr->file_handler_->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = currentHandlerPtr->file_handler_->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = currentHandlerPtr->file_handler_->target_file_id_;
        // place file header and record header in write buffer
        uint64_t targetWriteSizeWithoutAnchors = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        char writeContentBufferWithoutAnchros[targetWriteSizeWithoutAnchors];
        memcpy(writeContentBufferWithoutAnchros, &newFileHeader, sizeof(newFileHeader));
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader) + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);
        // open target file
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->current_prefix_used_bit_);
        string targetFilePathStr = workingDir_ + "/" + to_string(currentHandlerPtr->file_handler_->target_file_id_) + ".delta";

        currentHandlerPtr->file_handler_->file_operation_func_ptr_->createFile(targetFilePathStr);
        if (currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == true) {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->closeFile();
        }
        currentHandlerPtr->file_handler_->file_operation_func_ptr_->openFile(targetFilePathStr);
        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBufferWithoutAnchros, targetWriteSizeWithoutAnchors, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler_->target_file_id_);
            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
            currentHandlerPtr->jobDone_ = kError;
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                    // insert into cache only if the key has been read
                    keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
                }
            }
            // try GC if enabled
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(currentHandlerPtr->file_handler_);
                if (putIntoGCJobQueueStatus == true) {
                    currentHandlerPtr->jobDone_ = kDone;
                    return true;
                } else {
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    currentHandlerPtr->jobDone_ = kDone;
                    return true;
                }
            } else {
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone_ = kDone;
                return true;
            }
        }
    } else {
        // since file exist, may contains unflushed anchors, check anchors first
        uint64_t totalNotFlushedAnchorNumber = 0;
        uint64_t totalNotFlushedAnchorSize = 0;
        if (currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.size() != 0) {
            for (auto keyStrIt : currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_) {
                totalNotFlushedAnchorNumber++;
                totalNotFlushedAnchorSize += keyStrIt.first.size();
            }
        }
        uint64_t targetWriteSizeWithAnchors = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_ + totalNotFlushedAnchorNumber * sizeof(hashStoreRecordHeader) + totalNotFlushedAnchorSize;
        char writeContentBufferWithAnchor[targetWriteSizeWithAnchors];
        uint64_t currentWriteBufferPtrPosition = 0;
        for (auto keyStrIt : currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_) {
            hashStoreRecordHeader anchorRecordHeader;
            anchorRecordHeader.is_anchor_ = true;
            anchorRecordHeader.key_size_ = keyStrIt.first.size();
            anchorRecordHeader.sequence_number_ = keyStrIt.second;
            anchorRecordHeader.value_size_ = 0; // not sure.
            memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition, &anchorRecordHeader, sizeof(anchorRecordHeader));
            memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(anchorRecordHeader), keyStrIt.first.c_str(), keyStrIt.first.size());
            currentWriteBufferPtrPosition += (sizeof(anchorRecordHeader) + keyStrIt.first.size());
        }
        // append current content into buffer
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition, &newRecordHeader, sizeof(newRecordHeader));
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);
        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBufferWithAnchor, targetWriteSizeWithAnchors, totalNotFlushedAnchorNumber + 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler_->target_file_id_);
            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
            currentHandlerPtr->jobDone_ = kError;
            return false;
        } else {
            currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.clear(); // clean up flushed anchors
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                    // insert into cache only if the key has been read
                    keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
                }
            }
            // try GC if enabled
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(currentHandlerPtr->file_handler_);
                if (putIntoGCJobQueueStatus == true) {
                    currentHandlerPtr->jobDone_ = kDone;
                    return true;
                } else {
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    currentHandlerPtr->jobDone_ = kDone;
                    return true;
                }
            } else {
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone_ = kDone;
                return true;
            }
        }
    }
}

bool HashStoreFileOperator::operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    std::scoped_lock<std::shared_mutex> w_lock(currentHandlerPtr->file_handler_->fileOperationMutex_);
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
    for (auto& keyIt : currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_) {
        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size());
    }
    char writeContentBuffer[targetWriteBufferSize];
    uint64_t currentProcessedBufferIndex = 0;
    hashStoreRecordHeader newRecordHeader;
    for (auto& keyIt : currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_) {
        newRecordHeader.is_anchor_ = true;
        newRecordHeader.key_size_ = keyIt.first.size();
        newRecordHeader.sequence_number_ = keyIt.second;
        newRecordHeader.value_size_ = 0;
        memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
        currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
        memcpy(writeContentBuffer + currentProcessedBufferIndex, keyIt.first.c_str(), keyIt.first.size());
        currentProcessedBufferIndex += keyIt.first.size();
    }

    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
        newRecordHeader.is_anchor_ = currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i);
        newRecordHeader.key_size_ = currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i).size();
        newRecordHeader.value_size_ = currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i).size();
        newRecordHeader.sequence_number_ = currentHandlerPtr->batched_write_operation_.sequence_number_vec_ptr_->at(i);
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
    uint64_t targetObjectNumber = currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size() + currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.size();
    // write content
    bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBuffer, targetWriteBufferSize, targetObjectNumber);
    if (writeContentStatus == false) {
        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
        currentHandlerPtr->jobDone_ = kError;
        return false;
    } else {
        currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.clear();
        // insert to cache if need
        if (keyToValueListCache_ != nullptr) {
            for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
                if (keyToValueListCache_->existsInCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i))) {
                    if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i) == true) {
                        keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).clear();
                    } else {
                        keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).push_back(currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i));
                    }
                }
            }
        }
        if (enableGCFlag_ == true) {
            bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(currentHandlerPtr->file_handler_);
            if (putIntoGCJobQueueStatus == true) {
                currentHandlerPtr->jobDone_ = kDone;
                return true;
            }
        }
        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
        currentHandlerPtr->jobDone_ = kDone;
        return true;
    }
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* fileHandler)
{
    // insert into GC job queue if exceed the threshold
    if (fileHandler->total_on_disk_bytes_ >= singleFileSizeLimit_ && fileHandler->gc_result_status_flag_ == kNoGC) {
        fileHandler->no_gc_wait_operation_number_++;
        if (fileHandler->no_gc_wait_operation_number_ % operationNumberThresholdForForcedSingleFileGC_ == 1) {
            fileHandler->file_ownership_flag_ = -1;
            fileHandler->gc_result_status_flag_ = kMayGC;
            notifyGCToManagerMQ_->push(fileHandler);
            debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, singleFileSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
            return false;
        }
    } else if (fileHandler->total_on_disk_bytes_ >= perFileGCSizeLimit_) {
        if (fileHandler->gc_result_status_flag_ == kNew || fileHandler->gc_result_status_flag_ == kMayGC) {
            fileHandler->file_ownership_flag_ = -1;
            notifyGCToManagerMQ_->push(fileHandler);
            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
            return false;
        }
    } else {
        return false;
    }
}

bool HashStoreFileOperator::directlyWriteOperation(hashStoreFileMetaDataHandler* fileHandler, string key, string value, uint32_t sequenceNumber, bool isAnchorStatus)
{
    std::scoped_lock<std::shared_mutex> w_lock(fileHandler->fileOperationMutex_);
    // process Anchor first
    if (isAnchorStatus == true) {
        fileHandler->bufferedUnFlushedAnchorsVec_.insert(make_pair(key, sequenceNumber));
        if (keyToValueListCache_ != nullptr) {
            if (keyToValueListCache_->existsInCache(key)) {
                // insert into cache only if the key has been read
                keyToValueListCache_->getFromCache(key).clear();
            }
        }
        fileHandler->file_ownership_flag_ = 0;
        return true;
    }
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = false;
    newRecordHeader.key_size_ = key.size();
    newRecordHeader.value_size_ = value.size();
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        // since file not created, shoud not flush anchors, but need to clean up buffered anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.current_prefix_used_bit_ = fileHandler->current_prefix_used_bit_;
        newFileHeader.previous_file_id_first_ = fileHandler->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = fileHandler->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = fileHandler->target_file_id_;
        // place file header and record header in write buffer
        uint64_t targetWriteSizeWithoutAnchors = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        char writeContentBufferWithoutAnchros[targetWriteSizeWithoutAnchors];
        memcpy(writeContentBufferWithoutAnchros, &newFileHeader, sizeof(newFileHeader));
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader) + sizeof(newRecordHeader), key.c_str(), newRecordHeader.key_size_);
        memcpy(writeContentBufferWithoutAnchros + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, value.c_str(), newRecordHeader.value_size_);
        // open target file
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", fileHandler->target_file_id_, fileHandler->current_prefix_used_bit_);
        string targetFilePathStr = workingDir_ + "/" + to_string(fileHandler->target_file_id_) + ".delta";

        fileHandler->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
        // write contents of file
        if (fileHandler->file_operation_func_ptr_->isFileOpen() == true) {
            uint64_t onDiskWriteSize;
            STAT_PROCESS(onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(writeContentBufferWithoutAnchros, targetWriteSizeWithoutAnchors), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
            if (onDiskWriteSize == 0) {
                debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                // Update metadata
                fileHandler->total_object_bytes_ += targetWriteSizeWithoutAnchors;
                fileHandler->total_on_disk_bytes_ += onDiskWriteSize;
                fileHandler->total_object_count_++;
                // insert to cache if current key exist in cache && cache is enabled
                if (keyToValueListCache_ != nullptr) {
                    if (keyToValueListCache_->existsInCache(key)) {
                        // insert into cache only if the key has been read
                        keyToValueListCache_->getFromCache(key).push_back(value);
                    }
                }
                fileHandler->bufferedUnFlushedAnchorsVec_.clear(); // clean up buffered anchors
                // try GC if enabled
                if (enableGCFlag_ == true) {
                    bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(fileHandler);
                    if (putIntoGCJobQueueStatus == true) {
                        return true;
                    } else {
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                } else {
                    fileHandler->file_ownership_flag_ = 0;
                    return true;
                }
            }
        } else {
            debug_error("[ERROR] after create, could not open file path = %s\n", targetFilePathStr.c_str());
            fileHandler->file_ownership_flag_ = 0;
            return false;
        }
    } else {
        // since file exist, may contains unflushed anchors, check anchors first
        uint64_t totalNotFlushedAnchorNumber = 0;
        uint64_t totalNotFlushedAnchorSize = 0;
        if (fileHandler->bufferedUnFlushedAnchorsVec_.size() != 0) {
            for (auto keyStrIt : fileHandler->bufferedUnFlushedAnchorsVec_) {
                totalNotFlushedAnchorNumber++;
                totalNotFlushedAnchorSize += keyStrIt.first.size();
            }
        }
        uint64_t targetWriteSizeWithAnchors = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_ + totalNotFlushedAnchorNumber * sizeof(hashStoreRecordHeader) + totalNotFlushedAnchorSize;
        char writeContentBufferWithAnchor[targetWriteSizeWithAnchors];
        uint64_t currentWriteBufferPtrPosition = 0;
        for (auto keyStrIt : fileHandler->bufferedUnFlushedAnchorsVec_) {
            hashStoreRecordHeader anchorRecordHeader;
            anchorRecordHeader.is_anchor_ = true;
            anchorRecordHeader.key_size_ = keyStrIt.first.size();
            anchorRecordHeader.sequence_number_ = keyStrIt.second;
            anchorRecordHeader.value_size_ = 0;
            memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition, &anchorRecordHeader, sizeof(anchorRecordHeader));
            memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(anchorRecordHeader), keyStrIt.first.c_str(), keyStrIt.first.size());
            currentWriteBufferPtrPosition += (sizeof(anchorRecordHeader) + keyStrIt.first.size());
        }
        // append current content into buffer
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition, &newRecordHeader, sizeof(newRecordHeader));
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(newRecordHeader), key.c_str(), newRecordHeader.key_size_);
        memcpy(writeContentBufferWithAnchor + currentWriteBufferPtrPosition + sizeof(newRecordHeader) + newRecordHeader.key_size_, value.c_str(), newRecordHeader.value_size_);
        // write contents of file
        if (fileHandler->file_operation_func_ptr_->isFileOpen() == true) {
            uint64_t onDiskWriteSize;
            STAT_PROCESS(onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(writeContentBufferWithAnchor, targetWriteSizeWithAnchors), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
            if (onDiskWriteSize == 0) {
                debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                // Update metadata
                fileHandler->total_object_bytes_ += targetWriteSizeWithAnchors;
                fileHandler->total_on_disk_bytes_ += onDiskWriteSize;
                fileHandler->total_object_count_ += (totalNotFlushedAnchorNumber + 1);
                // insert to cache if current key exist in cache && cache is enabled
                if (keyToValueListCache_ != nullptr) {
                    if (keyToValueListCache_->existsInCache(key)) {
                        // insert into cache only if the key has been read
                        keyToValueListCache_->getFromCache(key).push_back(value);
                    }
                }
                fileHandler->bufferedUnFlushedAnchorsVec_.clear(); // clean up flushed anchors
                // try GC if enabled
                if (enableGCFlag_ == true) {
                    bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(fileHandler);
                    if (putIntoGCJobQueueStatus == true) {
                        return true;
                    } else {
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                } else {
                    fileHandler->file_ownership_flag_ = 0;
                    return true;
                }
            }
        } else {
            debug_error("[ERROR] current file not opened, fil ID = %lu\n", fileHandler->target_file_id_);
            fileHandler->file_ownership_flag_ = 0;
            return false;
        }
    }
}

bool HashStoreFileOperator::directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> batchedWriteOperationsMap)
{
    for (auto batchIt : batchedWriteOperationsMap) {
        std::scoped_lock<std::shared_mutex> w_lock(batchIt.first->fileOperationMutex_);
        uint64_t targetWriteBufferSize = 0;
        for (auto i = 0; i < std::get<0>(batchIt.second).size(); i++) {
            targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + std::get<0>(batchIt.second).at(i).size());
            if (std::get<3>(batchIt.second).at(i) == true) {
                continue;
            } else {
                targetWriteBufferSize += std::get<1>(batchIt.second).at(i).size();
            }
        }
        for (auto& keyIt : batchIt.first->bufferedUnFlushedAnchorsVec_) {
            targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size());
        }
        char writeContentBuffer[targetWriteBufferSize];
        uint64_t currentProcessedBufferIndex = 0;
        hashStoreRecordHeader newRecordHeader;
        for (auto& keyIt : batchIt.first->bufferedUnFlushedAnchorsVec_) {
            newRecordHeader.is_anchor_ = true;
            newRecordHeader.key_size_ = keyIt.first.size();
            newRecordHeader.sequence_number_ = keyIt.second;
            newRecordHeader.value_size_ = 0;
            memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
            currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
            memcpy(writeContentBuffer + currentProcessedBufferIndex, keyIt.first.c_str(), keyIt.first.size());
            currentProcessedBufferIndex += keyIt.first.size();
        }

        for (auto i = 0; i < std::get<0>(batchIt.second).size(); i++) {
            newRecordHeader.key_size_ = std::get<0>(batchIt.second).at(i).size();
            newRecordHeader.value_size_ = std::get<1>(batchIt.second).at(i).size();
            newRecordHeader.sequence_number_ = std::get<2>(batchIt.second).at(i);
            newRecordHeader.is_anchor_ = std::get<3>(batchIt.second).at(i);
            memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
            currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
            memcpy(writeContentBuffer + currentProcessedBufferIndex, std::get<0>(batchIt.second).at(i).c_str(), newRecordHeader.key_size_);
            currentProcessedBufferIndex += newRecordHeader.key_size_;
            if (newRecordHeader.is_anchor_ == true) {
                continue;
            } else {
                memcpy(writeContentBuffer + currentProcessedBufferIndex, std::get<1>(batchIt.second).at(i).c_str(), newRecordHeader.value_size_);
                currentProcessedBufferIndex += newRecordHeader.value_size_;
            }
        }
        uint64_t targetObjectNumber = std::get<0>(batchIt.second).size() + batchIt.first->bufferedUnFlushedAnchorsVec_.size();
        // write content
        bool writeContentStatus = writeContentToFile(batchIt.first, writeContentBuffer, targetWriteBufferSize, targetObjectNumber);
        if (writeContentStatus == false) {
            batchIt.first->file_ownership_flag_ = 0;
            return false;
        } else {
            batchIt.first->bufferedUnFlushedAnchorsVec_.clear();
            // insert to cache if need
            if (keyToValueListCache_ != nullptr) {
                for (auto i = 0; i < std::get<0>(batchIt.second).size(); i++) {
                    if (keyToValueListCache_->existsInCache(std::get<0>(batchIt.second).at(i))) {
                        if (std::get<3>(batchIt.second).at(i) == true) {
                            keyToValueListCache_->getFromCache(std::get<0>(batchIt.second).at(i)).clear();
                        } else {
                            keyToValueListCache_->getFromCache(std::get<0>(batchIt.second).at(i)).push_back(std::get<1>(batchIt.second).at(i));
                        }
                    }
                }
            }
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(batchIt.first);
                if (putIntoGCJobQueueStatus != true) {
                    return false;
                }
            }
            batchIt.first->file_ownership_flag_ = 0;
        }
    }
    return true;
}

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec)
{
    std::shared_lock<std::shared_mutex> r_lock(fileHandler->fileOperationMutex_);
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(key);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec.size());
            valueVec->assign(tempResultVec.begin(), tempResultVec.end());
            fileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            // check if not flushed anchors exit, return directly.
            if (fileHandler->bufferedUnFlushedAnchorsVec_.find(key) != fileHandler->bufferedUnFlushedAnchorsVec_.end()) {
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
            // Not exist in cache, find the content in the file
            char readContentBuffer[fileHandler->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
            if (readFromFileStatus == false) {
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                unordered_map<string, vector<string>> currentFileProcessMap;
                uint64_t processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    if (currentFileProcessMap.find(key) == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(key).size());
                        valueVec->assign(currentFileProcessMap.at(key).begin(), currentFileProcessMap.at(key).end());
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            string tempInsertCacheKeyStr = mapIt.first;
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, mapIt.second);
                            debug_trace("Insert to cache key %s delta num %d\n", tempInsertCacheKeyStr.c_str(), (int)mapIt.second.size());
                        }
                    }
                }
            }
        }
    } else {
        char readContentBuffer[fileHandler->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
        if (readFromFileStatus == false) {
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            unordered_map<string, vector<string>> currentFileProcessMap;
            uint64_t processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                if (currentFileProcessMap.find(key) == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(key).size());
                    valueVec->assign(currentFileProcessMap.at(key).begin(), currentFileProcessMap.at(key).end());
                }
            }
        }
    }
    fileHandler->file_ownership_flag_ = 0;
    return true;
}

void HashStoreFileOperator::operationWorker()
{
    while (true) {
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            switch (currentHandlerPtr->opType_) {
            case kGet:
                debug_trace("receive operations, type = kGet, key = %s, target file ID = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                operationWorkerGetFunction(currentHandlerPtr);
                break;
            case kMultiPut:
                debug_trace("receive operations, type = kMultiPut, key number = %lu, target file ID = %lu\n", (*currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_).size(), currentHandlerPtr->file_handler_->target_file_id_);
                operationWorkerMultiPutFunction(currentHandlerPtr);
                break;
            case kPut:
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", (*currentHandlerPtr->write_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                operationWorkerPutFunction(currentHandlerPtr);
                break;
            default:
                debug_error("[ERROR] Unknown operation type = %d\n", currentHandlerPtr->opType_);
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                currentHandlerPtr->jobDone_ = kError;
                break;
            }
        }
    }
    debug_info("Thread of operation worker exit success %p\n", this);
    return;
}

} // namespace DELTAKV_NAMESPACE
