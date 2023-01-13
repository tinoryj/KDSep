#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_file_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileSizeLimit_ = options->deltaStore_single_file_maximum_size;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    }
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    notifyGCToManagerMQ_ = notifyGCToManagerMQ;
    if (options->enable_deltaStore_KDLevel_cache == true) {
        keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>>(options->deltaStore_KDLevel_cache_item_number);
    }
    workingDir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        syncStatistics_ = true;
        workerThreadNumber_ = options->deltaStore_op_worker_thread_number_limit_;
        workingThreadExitFlagVec_ = 0;
    }
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
        while (workingThreadExitFlagVec_ != workerThreadNumber_) {
            operationNotifyCV_.notify_all();
        }
    }
    return true;
}

// file operations
bool HashStoreFileOperator::putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, mempoolHandler_t* mempoolHandler)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone_ = kNotDone;
    currentHandler->write_operation_.mempoolHandler_ptr_ = mempoolHandler;
    currentHandler->opType_ = kPut;
    operationToWorkerMQ_->push(currentHandler);
    if (currentHandler->jobDone_ == kNotDone) {
        debug_trace("Wait for write job done%s\n", "");
        while (currentHandler->jobDone_ == kNotDone) {
            asm volatile("");
        }
        debug_trace("Wait for write job done%s over\n", "");
    }
    if (currentHandler->jobDone_ == kError) {
        delete currentHandler;
        return false;
    } else {
        delete currentHandler;
        return true;
    }
}

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(hashStoreOperationHandler* currentOperationHandler)
{
    operationToWorkerMQ_->push(currentOperationHandler);
    while (currentOperationHandler->jobDone_ == kNotDone) {
        asm volatile("");
    }
    if (currentOperationHandler->jobDone_ == kDone) {
        debug_trace("Process multiput operation for file ID = %lu, key number = %lu done\n", currentOperationHandler->file_handler_->target_file_id_, currentOperationHandler->batched_write_operation_.mempool_handler_vec_ptr_->size());
        delete currentOperationHandler;
        return true;
    } else {
        debug_error("[ERROR] Process multiput operation for file ID = %lu, key number = %lu error\n", currentOperationHandler->file_handler_->target_file_id_, currentOperationHandler->batched_write_operation_.mempool_handler_vec_ptr_->size());
        delete currentOperationHandler;
        return false;
    }
}

bool HashStoreFileOperator::putReadOperationIntoJobQueue(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(fileHandler);
    currentHandler->jobDone_ = kNotDone;
    currentHandler->read_operation_.key_str_ = &key;
    currentHandler->read_operation_.value_str_vec_ = valueVec;
    currentHandler->opType_ = kGet;
    operationToWorkerMQ_->push(currentHandler);
    if (currentHandler->jobDone_ == kNotDone) {
        debug_trace("Wait for read job done%s\n", "");
        while (currentHandler->jobDone_ == kNotDone) {
            asm volatile("");
        }
        debug_trace("Wait for read job done%s over\n", "");
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
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(*currentHandlerPtr->read_operation_.key_str_)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(*currentHandlerPtr->read_operation_.key_str_);
            debug_trace("read operations from cache, cache hit, key %s, hit vec size = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), tempResultVec.size());
            currentHandlerPtr->read_operation_.value_str_vec_->assign(tempResultVec.begin(), tempResultVec.end());
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(currentHandlerPtr->file_handler_, readContentBuffer);
            if (readFromFileStatus == false) {
                return false;
            } else {
                unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                    return false;
                } else {
                    str_t currentKey((char*)currentHandlerPtr->read_operation_.key_str_->c_str(), currentHandlerPtr->read_operation_.key_str_->size());
                    auto mapItForCurrentKey = currentFileProcessMap.find(currentKey);
                    if (mapItForCurrentKey == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), mapItForCurrentKey->second.size());
                        currentHandlerPtr->read_operation_.value_str_vec_->reserve(mapItForCurrentKey->second.size());
                        for (auto vecIt : mapItForCurrentKey->second) {
                            currentHandlerPtr->read_operation_.value_str_vec_->push_back(string(vecIt.data_, vecIt.size_));
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            string tempInsertCacheKeyStr(mapIt.first.data_, mapIt.first.size_);
                            vector<string> cachedValues;
                            cachedValues.reserve(mapIt.second.size());
                            for (auto vecIt : mapIt.second) {
                                cachedValues.push_back(string(vecIt.data_, vecIt.size_));
                            }
                            string tempInsertCacheValueStr(mapIt.first.data_, mapIt.first.size_);
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, cachedValues);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key %s delta num %lu\n", tempInsertCacheKeyStr.c_str(), cachedValues.size());
                        }
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[currentHandlerPtr->file_handler_->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(currentHandlerPtr->file_handler_, readContentBuffer);
        if (readFromFileStatus == false) {
            debug_error("[ERROR] Could not read file content, target file ID = %lu, size in metadata = %lu, on disk size in metadata = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->total_object_bytes_, currentHandlerPtr->file_handler_->total_on_disk_bytes_);
            return false;
        } else {
            unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                return false;
            } else {
                str_t currentKey((char*)currentHandlerPtr->read_operation_.key_str_->c_str(), currentHandlerPtr->read_operation_.key_str_->size());
                auto mapItForCurrentKey = currentFileProcessMap.find(currentKey);
                if (mapItForCurrentKey == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), mapItForCurrentKey->second.size());
                    currentHandlerPtr->read_operation_.value_str_vec_->reserve(mapItForCurrentKey->second.size());
                    for (auto vecIt : mapItForCurrentKey->second) {
                        currentHandlerPtr->read_operation_.value_str_vec_->push_back(string(vecIt.data_, vecIt.size_));
                    }
                    return true;
                }
            }
        }
    }
}

bool HashStoreFileOperator::readContentFromFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer)
{
    debug_trace("Read content from file ID = %lu\n", fileHandler->target_file_id_);
    fileOperationStatus_t readFileStatus;
    STAT_PROCESS(readFileStatus = fileHandler->file_operation_func_ptr_->readFile(contentBuffer, fileHandler->total_object_bytes_), StatsType::DELTAKV_HASHSTORE_GET_IO);
    StatsRecorder::getInstance()->DeltaOPBytesRead(fileHandler->total_on_disk_bytes_, fileHandler->total_object_bytes_, syncStatistics_);
    if (readFileStatus.success_ == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    } else {
        return true;
    }
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMapInternal)
{
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    currentProcessLocationIndex += sizeof(hashStoreFileHeader);
    uint64_t processedObjectNumber = 0;
    hashStoreRecordHeader currentObjectRecordHeader;
    while (currentProcessLocationIndex != contentSize) {
        processedObjectNumber++;
        memcpy(&currentObjectRecordHeader, contentBuffer + currentProcessLocationIndex, sizeof(hashStoreRecordHeader));
        currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
        if (currentObjectRecordHeader.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key str_t
        str_t currentKey(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            str_t currentValue(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<str_t> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
        }
    }
    return processedObjectNumber;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<str_t, vector<pair<str_t, hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t>& resultMapInternal)
{
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    currentProcessLocationIndex += sizeof(hashStoreFileHeader);
    uint64_t processedObjectNumber = 0;
    while (currentProcessLocationIndex != contentSize) {
        processedObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, contentBuffer + currentProcessLocationIndex, sizeof(hashStoreRecordHeader));
        currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
        if (currentObjectRecordHeader.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key str_t
        str_t currentKey(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            str_t currentValue(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(make_pair(currentValue, currentObjectRecordHeader));
            } else {
                vector<pair<str_t, hashStoreRecordHeader>> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(currentValue, currentObjectRecordHeader));
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
        }
    }
    return processedObjectNumber;
}

bool HashStoreFileOperator::writeContentToFile(hashStoreFileMetaDataHandler* fileHandler, char* contentBuffer, uint64_t contentSize, uint64_t contentObjectNumber)
{
    debug_trace("Write content to file ID = %lu\n", fileHandler->target_file_id_);
    fileOperationStatus_t onDiskWriteSizePair;
    STAT_PROCESS(onDiskWriteSizePair = fileHandler->file_operation_func_ptr_->writeFile(contentBuffer, contentSize), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
    StatsRecorder::getInstance()->DeltaOPBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
    if (onDiskWriteSizePair.success_ == false) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
        return false;
    } else {
        // update metadata
        fileHandler->total_object_bytes_ += contentSize;
        fileHandler->total_on_disk_bytes_ += onDiskWriteSizePair.physicalSize_;
        fileHandler->total_object_count_ += contentObjectNumber;
        debug_trace("Write content to file ID = %lu done, write to disk size = %lu\n", fileHandler->target_file_id_, onDiskWriteSizePair.physicalSize_);
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->isAnchorFlag_;
    newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keySize_;
    newRecordHeader.sequence_number_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->sequenceNumber_;
    newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valueSize_;
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
        uint64_t writeBufferSize = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        uint64_t targetWriteSize = 0;
        char writeBuffer[writeBufferSize];
        if (newRecordHeader.is_anchor_ == false) {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // open target file
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->file_handler_->current_prefix_used_bit_);
        string targetFilePathStr = workingDir_ + "/" + to_string(currentHandlerPtr->file_handler_->target_file_id_) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
        } else {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler_->target_file_id_);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                string currentKeyStr(currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keySize_);
                string currentValueStr(currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valuePtr_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valueSize_);
                if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                    // insert into cache only if the key has been read
                    if (newRecordHeader.is_anchor_ == true) {
                        keyToValueListCache_->getFromCache(currentKeyStr).clear();
                    } else {
                        keyToValueListCache_->getFromCache(currentKeyStr).push_back(currentValueStr);
                    }
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
            return true;
        }
    } else {
        // since file exist, may contains unflushed anchors, check anchors first
        uint64_t writeBufferSize = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        uint64_t targetWriteSize = 0;
        char writeBuffer[writeBufferSize];
        if (newRecordHeader.is_anchor_ == false) {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler_->target_file_id_);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                string currentKeyStr(currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keySize_);
                string currentValueStr(currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valuePtr_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valueSize_);
                if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                    // insert into cache only if the key has been read
                    if (newRecordHeader.is_anchor_ == true) {
                        keyToValueListCache_->getFromCache(currentKeyStr).clear();
                    } else {
                        keyToValueListCache_->getFromCache(currentKeyStr).push_back(currentValueStr);
                    }
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    if (currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size(); index++) {
            if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(index).isAnchorFlag_ == false) {
                onlyAnchorFlag = false;
                break;
            }
        }
        if (onlyAnchorFlag == true) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", currentHandlerPtr->file_handler_->target_file_id_);
            return true;
        }
    }

    uint64_t targetWriteBufferSize = 0;
    hashStoreFileHeader newFileHeader;
    bool needFlushFileHeader = false;
    if (currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
        string targetFilePath = workingDir_ + "/" + to_string(currentHandlerPtr->file_handler_->target_file_id_) + ".delta";
        if (std::filesystem::exists(targetFilePath) == false) {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->createThenOpenFile(targetFilePath);
            newFileHeader.current_prefix_used_bit_ = currentHandlerPtr->file_handler_->current_prefix_used_bit_;
            newFileHeader.file_create_reason_ = currentHandlerPtr->file_handler_->file_create_reason_;
            newFileHeader.file_id_ = currentHandlerPtr->file_handler_->target_file_id_;
            newFileHeader.previous_file_id_first_ = currentHandlerPtr->file_handler_->previous_file_id_first_;
            newFileHeader.previous_file_id_second_ = currentHandlerPtr->file_handler_->previous_file_id_second_;
            needFlushFileHeader = true;
            targetWriteBufferSize += sizeof(hashStoreFileHeader);
        } else {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->openFile(targetFilePath);
        }
    }
    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size(); i++) {
        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).keySize_);
        if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).isAnchorFlag_ == true) {
            continue;
        } else {
            targetWriteBufferSize += currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).valueSize_;
        }
    }
    char writeContentBuffer[targetWriteBufferSize];
    uint64_t currentProcessedBufferIndex = 0;
    if (needFlushFileHeader == true) {
        memcpy(writeContentBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
        currentProcessedBufferIndex += sizeof(hashStoreFileHeader);
    }
    hashStoreRecordHeader newRecordHeader;
    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size(); i++) {
        newRecordHeader.is_anchor_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).isAnchorFlag_;
        newRecordHeader.key_size_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).keySize_;
        newRecordHeader.value_size_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).valueSize_;
        newRecordHeader.sequence_number_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).sequenceNumber_;
        memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
        currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
        memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).keyPtr_, newRecordHeader.key_size_);
        currentProcessedBufferIndex += newRecordHeader.key_size_;
        if (newRecordHeader.is_anchor_ == true) {
            continue;
        } else {
            memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).valuePtr_, newRecordHeader.value_size_);
            currentProcessedBufferIndex += newRecordHeader.value_size_;
        }
    }
    uint64_t targetObjectNumber = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size();
    // write content
    bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBuffer, targetWriteBufferSize, targetObjectNumber);
    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID = %lu, content size = %lu, content bytes number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, targetObjectNumber, targetWriteBufferSize);
        return false;
    } else {
        // insert to cache if need
        if (keyToValueListCache_ != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size(); i++) {
                string currentKey(currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).keyPtr_, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).keySize_);
                if (keyToValueListCache_->existsInCache(currentKey)) {
                    if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).isAnchorFlag_ == true) {
                        keyToValueListCache_->getFromCache(currentKey).clear();
                    } else {
                        string currentValue(currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).valuePtr_, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->at(i).valueSize_);
                        keyToValueListCache_->getFromCache(currentKey).push_back(currentValue);
                    }
                }
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
        }
        return true;
    }
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* fileHandler)
{
    // insert into GC job queue if exceed the threshold
    if (fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize() >= singleFileSizeLimit_ && fileHandler->gc_result_status_flag_ == kNoGC) {
        fileHandler->no_gc_wait_operation_number_++;
        if (fileHandler->no_gc_wait_operation_number_ % operationNumberThresholdForForcedSingleFileGC_ == 1) {
            fileHandler->file_ownership_flag_ = -1;
            fileHandler->gc_result_status_flag_ = kMayGC;
            notifyGCToManagerMQ_->push(fileHandler);
            debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize());
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, singleFileSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize(), fileHandler->gc_result_status_flag_);
            return false;
        }
    } else if (fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize() >= perFileGCSizeLimit_) {
        if (fileHandler->gc_result_status_flag_ == kNew || fileHandler->gc_result_status_flag_ == kMayGC) {
            fileHandler->file_ownership_flag_ = -1;
            notifyGCToManagerMQ_->push(fileHandler);
            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize());
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_ + fileHandler->file_operation_func_ptr_->getFileBufferedSize(), fileHandler->gc_result_status_flag_);
            return false;
        }
    } else {
        debug_trace("Current file ID = %lu should not GC, skip\n", fileHandler->target_file_id_);
        return false;
    }
}

bool HashStoreFileOperator::directlyWriteOperation(hashStoreFileMetaDataHandler* fileHandler, mempoolHandler_t* mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> w_lock(fileHandler->fileOperationMutex_);
    // construct record header
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = mempoolHandler->isAnchorFlag_;
    newRecordHeader.key_size_ = mempoolHandler->keySize_;
    newRecordHeader.sequence_number_ = mempoolHandler->sequenceNumber_;
    newRecordHeader.value_size_ = mempoolHandler->valueSize_;
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.current_prefix_used_bit_ = fileHandler->current_prefix_used_bit_;
        newFileHeader.previous_file_id_first_ = fileHandler->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = fileHandler->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = fileHandler->target_file_id_;
        // place file header and record header in write buffer
        uint64_t writeBufferSize = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        uint64_t targetWriteSize = 0;
        char writeBuffer[writeBufferSize];
        if (newRecordHeader.is_anchor_ == false) {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), mempoolHandler->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, mempoolHandler->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), mempoolHandler->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // open target file
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", fileHandler->target_file_id_, fileHandler->current_prefix_used_bit_);
        string targetFilePathStr = workingDir_ + "/" + to_string(fileHandler->target_file_id_) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            fileHandler->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
        } else {
            fileHandler->file_operation_func_ptr_->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(fileHandler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                string currentKeyStr(mempoolHandler->keyPtr_, mempoolHandler->keySize_);
                if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                    // insert into cache only if the key has been read
                    if (newRecordHeader.is_anchor_ == true) {
                        keyToValueListCache_->getFromCache(currentKeyStr).clear();
                    } else {
                        string currentValueStr(mempoolHandler->valuePtr_, mempoolHandler->valueSize_);
                        keyToValueListCache_->getFromCache(currentKeyStr).push_back(currentValueStr);
                    }
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
            return true;
        }
    } else {
        // since file exist, may contains unflushed anchors, check anchors first
        uint64_t writeBufferSize = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        uint64_t targetWriteSize = 0;
        char writeBuffer[writeBufferSize];
        if (newRecordHeader.is_anchor_ == false) {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), mempoolHandler->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, mempoolHandler->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), mempoolHandler->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // write contents of file
        bool writeContentStatus = writeContentToFile(fileHandler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                string currentKeyStr(mempoolHandler->keyPtr_, mempoolHandler->keySize_);
                if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                    // insert into cache only if the key has been read
                    if (newRecordHeader.is_anchor_ == true) {
                        keyToValueListCache_->getFromCache(currentKeyStr).clear();
                    } else {
                        string currentValueStr(mempoolHandler->valuePtr_, mempoolHandler->valueSize_);
                        keyToValueListCache_->getFromCache(currentKeyStr).push_back(currentValueStr);
                    }
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> batchedWriteOperationsMap)
{
    vector<bool> jobeDoneStatus;
    for (auto batchIt : batchedWriteOperationsMap) {
        std::scoped_lock<std::shared_mutex> w_lock(batchIt.first->fileOperationMutex_);
        // check file existence, create if not exist
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < batchIt.second.size(); index++) {
            if (batchIt.second[index].isAnchorFlag_ == false) {
                onlyAnchorFlag = false;
            }
        }
        if (onlyAnchorFlag == true && batchIt.first->file_operation_func_ptr_->isFileOpen() == false) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", batchIt.first->target_file_id_);
            batchIt.first->file_ownership_flag_ = 0;
            jobeDoneStatus.push_back(true);
            continue;
        }
        uint64_t targetWriteBufferSize = 0;
        hashStoreFileHeader newFileHeader;
        bool needFlushFileHeader = false;
        if (batchIt.first->file_operation_func_ptr_->isFileOpen() == false) {
            string targetFilePath = workingDir_ + "/" + to_string(batchIt.first->target_file_id_) + ".delta";
            if (std::filesystem::exists(targetFilePath) == false) {
                batchIt.first->file_operation_func_ptr_->createThenOpenFile(targetFilePath);
                newFileHeader.current_prefix_used_bit_ = batchIt.first->current_prefix_used_bit_;
                newFileHeader.file_create_reason_ = batchIt.first->file_create_reason_;
                newFileHeader.file_id_ = batchIt.first->target_file_id_;
                newFileHeader.previous_file_id_first_ = batchIt.first->previous_file_id_first_;
                newFileHeader.previous_file_id_second_ = batchIt.first->previous_file_id_second_;
                needFlushFileHeader = true;
                targetWriteBufferSize += sizeof(hashStoreFileHeader);
            } else {
                batchIt.first->file_operation_func_ptr_->openFile(targetFilePath);
            }
        }
        for (auto i = 0; i < batchIt.second.size(); i++) {
            targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + batchIt.second[i].keySize_);
            if (batchIt.second[i].isAnchorFlag_ == true) {
                continue;
            } else {
                targetWriteBufferSize += batchIt.second[i].valueSize_;
            }
        }
        char writeContentBuffer[targetWriteBufferSize];
        uint64_t currentProcessedBufferIndex = 0;
        if (needFlushFileHeader == true) {
            memcpy(writeContentBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
            currentProcessedBufferIndex += sizeof(hashStoreFileHeader);
        }
        hashStoreRecordHeader newRecordHeader;
        for (auto i = 0; i < batchIt.second.size(); i++) {
            newRecordHeader.key_size_ = batchIt.second[i].keySize_;
            newRecordHeader.value_size_ = batchIt.second[i].valueSize_;
            newRecordHeader.sequence_number_ = batchIt.second[i].sequenceNumber_;
            newRecordHeader.is_anchor_ = batchIt.second[i].isAnchorFlag_;
            memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
            currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
            memcpy(writeContentBuffer + currentProcessedBufferIndex, batchIt.second[i].keyPtr_, newRecordHeader.key_size_);
            currentProcessedBufferIndex += newRecordHeader.key_size_;
            if (newRecordHeader.is_anchor_ == true) {
                continue;
            } else {
                memcpy(writeContentBuffer + currentProcessedBufferIndex, batchIt.second[i].valuePtr_, newRecordHeader.value_size_);
                currentProcessedBufferIndex += newRecordHeader.value_size_;
            }
        }
        uint64_t targetObjectNumber = batchIt.second.size();
        // write content
        bool writeContentStatus = writeContentToFile(batchIt.first, writeContentBuffer, targetWriteBufferSize, targetObjectNumber);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Could not write content to file ID = %lu, object number = %lu, object total size = %lu\n", batchIt.first->target_file_id_, targetObjectNumber, targetWriteBufferSize);
            batchIt.first->file_ownership_flag_ = 0;
            jobeDoneStatus.push_back(false);
        } else {
            // insert to cache if need
            if (keyToValueListCache_ != nullptr) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                for (auto i = 0; i < batchIt.second.size(); i++) {
                    string currentKeyStr(batchIt.second[i].keyPtr_, batchIt.second[i].keySize_);
                    if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                        if (batchIt.second[i].isAnchorFlag_ == true) {
                            keyToValueListCache_->getFromCache(currentKeyStr).clear();
                        } else {
                            string currentValueStr(batchIt.second[i].valuePtr_, batchIt.second[i].valueSize_);
                            keyToValueListCache_->getFromCache(currentKeyStr).push_back(currentValueStr);
                        }
                    }
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(batchIt.first);
                if (putIntoGCJobQueueStatus == false) {
                    batchIt.first->file_ownership_flag_ = 0;
                }
            } else {
                batchIt.first->file_ownership_flag_ = 0;
            }
            jobeDoneStatus.push_back(true);
        }
    }
    bool jobDoneSuccessFlag = true;
    if (jobeDoneStatus.size() != batchedWriteOperationsMap.size()) {
        debug_error("[ERROR] Job done flag in vec = %lu, not equal to request numebr = %lu\n", jobeDoneStatus.size(), batchedWriteOperationsMap.size());
        return false;
    } else {
        for (auto jobDoneIt : jobeDoneStatus) {
            if (jobDoneIt == false) {
                jobDoneSuccessFlag = false;
            }
        }
        if (jobDoneSuccessFlag == true) {
            debug_info("Batched operations processed done by DirectMultiPut, total file handler number = %lu\n", batchedWriteOperationsMap.size());
            return true;
        } else {
            debug_error("[ERROR] Batched operations processed done by DirectMultiPut, but some operations may not success, total file handler number = %lu\n", batchedWriteOperationsMap.size());
            return false;
        }
    }
}

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(fileHandler->fileOperationMutex_);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(key);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec.size());
            valueVec->assign(tempResultVec.begin(), tempResultVec.end());
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            fileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[fileHandler->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
            if (readFromFileStatus == false) {
                debug_error("[ERROR] Could not read from file for key = %s\n", key.c_str());
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    str_t currentKey((char*)key.c_str(), key.size());
                    auto mapIt = currentFileProcessMap.find(currentKey);
                    if (mapIt == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        valueVec->clear();
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                        valueVec->reserve(mapIt->second.size());
                        for (auto vecIt : mapIt->second) {
                            valueVec->push_back(string(vecIt.data_, vecIt.size_));
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            string tempInsertCacheKeyStr(mapIt.first.data_, mapIt.first.size_);
                            vector<string> cacheValuesVec;
                            for (auto vecIt : mapIt.second) {
                                cacheValuesVec.push_back(string(vecIt.data_, vecIt.size_));
                            }
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, cacheValuesVec);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key = %s delta num = %lu\n", tempInsertCacheKeyStr.c_str(), mapIt.second.size());
                        }
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[fileHandler->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
        if (readFromFileStatus == false) {
            valueVec->clear();
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                str_t currentKey((char*)key.c_str(), key.size());
                auto currentKeyIt = currentFileProcessMap.find(currentKey);
                if (currentKeyIt == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                    valueVec->reserve(currentKeyIt->second.size());
                    for (auto vecIt : currentKeyIt->second) {
                        valueVec->push_back(string(vecIt.data_, vecIt.size_));
                    }
                }
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
        }
    }
}

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<string>*& valueVec, vector<hashStoreRecordHeader>*& recordVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(fileHandler->fileOperationMutex_);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(key);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec.size());
            valueVec->assign(tempResultVec.begin(), tempResultVec.end());
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            fileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[fileHandler->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
            if (readFromFileStatus == false) {
                debug_error("[ERROR] Could not read from file for key = %s\n", key.c_str());
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                unordered_map<str_t, vector<pair<str_t, hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    str_t currentKey((char*)key.c_str(), key.size());
                    auto mapIt = currentFileProcessMap.find(currentKey);
                    if (mapIt == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        valueVec->clear();
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                        valueVec->reserve(mapIt->second.size());
                        for (auto vecIt : mapIt->second) {
                            valueVec->push_back(string(vecIt.first.data_, vecIt.first.size_));
                            recordVec->push_back(vecIt.second);
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            string tempInsertCacheKeyStr(mapIt.first.data_, mapIt.first.size_);
                            vector<string> cacheValuesVec;
                            for (auto vecIt : mapIt.second) {
                                cacheValuesVec.push_back(string(vecIt.first.data_, vecIt.first.size_));
                            }
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, cacheValuesVec);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key = %s delta num = %lu\n", tempInsertCacheKeyStr.c_str(), mapIt.second.size());
                        }
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[fileHandler->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
        if (readFromFileStatus == false) {
            valueVec->clear();
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            unordered_map<str_t, vector<pair<str_t, hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                str_t currentKey((char*)key.c_str(), key.size());
                auto currentKeyIt = currentFileProcessMap.find(currentKey);
                if (currentKeyIt == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                    valueVec->reserve(currentKeyIt->second.size());
                    for (auto vecIt : currentKeyIt->second) {
                        valueVec->push_back(string(vecIt.first.data_, vecIt.first.size_));
                        recordVec->push_back(vecIt.second);
                    }
                }
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
        }
    }
}

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<str_cpy_t>*& valueCpyVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(fileHandler->fileOperationMutex_);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(key);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec.size());
            for (auto it : tempResultVec) {
                valueCpyVec->push_back(str_cpy_t(const_cast<char*>(it.c_str()), it.size()));
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            fileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[fileHandler->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
            if (readFromFileStatus == false) {
                debug_error("[ERROR] Could not read from file for key = %s\n", key.c_str());
                valueCpyVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    valueCpyVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    str_t currentKey((char*)key.c_str(), key.size());
                    auto mapIt = currentFileProcessMap.find(currentKey);
                    if (mapIt == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        valueCpyVec->clear();
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                        valueCpyVec->reserve(mapIt->second.size());
                        for (auto vecIt : mapIt->second) {
                            valueCpyVec->push_back(str_cpy_t(vecIt.data_, vecIt.size_));
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            string tempInsertCacheKeyStr(mapIt.first.data_, mapIt.first.size_);
                            vector<string> cacheValuesVec;
                            for (auto vecIt : mapIt.second) {
                                cacheValuesVec.push_back(string(vecIt.data_, vecIt.size_));
                            }
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, cacheValuesVec);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key = %s delta num = %lu\n", tempInsertCacheKeyStr.c_str(), mapIt.second.size());
                        }
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[fileHandler->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
        if (readFromFileStatus == false) {
            valueCpyVec->clear();
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                valueCpyVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                str_t currentKey((char*)key.c_str(), key.size());
                auto currentKeyIt = currentFileProcessMap.find(currentKey);
                if (currentKeyIt == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    valueCpyVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                    valueCpyVec->reserve(currentKeyIt->second.size());
                    for (auto vecIt : currentKeyIt->second) {
                        valueCpyVec->push_back(str_cpy_t(vecIt.data_, vecIt.size_));
                    }
                }
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
        }
    }
}

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* fileHandler, string key, vector<str_cpy_t>*& valueCpyVec, vector<hashStoreRecordHeader>*& recordVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(fileHandler->fileOperationMutex_);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<string> tempResultVec = keyToValueListCache_->getFromCache(key);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec.size());
            for (auto it : tempResultVec) {
                valueCpyVec->push_back(str_cpy_t(const_cast<char*>(it.c_str()), it.size()));
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            fileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            char readContentBuffer[fileHandler->total_object_bytes_];
            bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
            if (readFromFileStatus == false) {
                debug_error("[ERROR] Could not read from file for key = %s\n", key.c_str());
                valueCpyVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                unordered_map<str_t, vector<pair<str_t, hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    valueCpyVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    str_t currentKey((char*)key.c_str(), key.size());
                    auto mapIt = currentFileProcessMap.find(currentKey);
                    if (mapIt == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        valueCpyVec->clear();
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                        valueCpyVec->reserve(mapIt->second.size());
                        for (auto vecIt : mapIt->second) {
                            valueCpyVec->push_back(str_cpy_t(vecIt.first.data_, vecIt.first.size_));
                            recordVec->push_back(vecIt.second);
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            string tempInsertCacheKeyStr(mapIt.first.data_, mapIt.first.size_);
                            vector<string> cacheValuesVec;
                            for (auto vecIt : mapIt.second) {
                                cacheValuesVec.push_back(string(vecIt.first.data_, vecIt.first.size_));
                            }
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, cacheValuesVec);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key = %s delta num = %lu\n", tempInsertCacheKeyStr.c_str(), mapIt.second.size());
                        }
                        fileHandler->file_ownership_flag_ = 0;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[fileHandler->total_object_bytes_];
        bool readFromFileStatus = readContentFromFile(fileHandler, readContentBuffer);
        if (readFromFileStatus == false) {
            valueCpyVec->clear();
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            unordered_map<str_t, vector<pair<str_t, hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> currentFileProcessMap;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                valueCpyVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                str_t currentKey((char*)key.c_str(), key.size());
                auto currentKeyIt = currentFileProcessMap.find(currentKey);
                if (currentKeyIt == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    valueCpyVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(currentKey).size());
                    valueCpyVec->reserve(currentKeyIt->second.size());
                    for (auto vecIt : currentKeyIt->second) {
                        valueCpyVec->push_back(str_cpy_t(vecIt.first.data_, vecIt.first.size_));
                        recordVec->push_back(vecIt.second);
                    }
                }
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
        }
    }
}

void HashStoreFileOperator::operationWorker(int threadID)
{
    while (true) {
        {
            std::unique_lock<std::mutex> lk(operationNotifyMtx_);
            operationNotifyCV_.wait(lk);
        }
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            bool operationsStatus;
            bool operationTypeCorrectFlag = false;
            std::scoped_lock<std::shared_mutex> w_lock(currentHandlerPtr->file_handler_->fileOperationMutex_);
            switch (currentHandlerPtr->opType_) {
            case kGet:
                operationTypeCorrectFlag = true;
                debug_trace("receive operations, type = kGet, key = %s, target file ID = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
                STAT_PROCESS(operationsStatus = operationWorkerGetFunction(currentHandlerPtr), StatsType::OP_GET);
                break;
            case kMultiPut:
                operationTypeCorrectFlag = true;
                debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size());
                STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(currentHandlerPtr), StatsType::OP_MULTIPUT);
                debug_trace("processed operations, type = kMultiPut, file ID = %lu, put deltas key number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_->size());
                break;
            case kPut:
                operationTypeCorrectFlag = true;
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, currentHandlerPtr->file_handler_->target_file_id_);
                STAT_PROCESS(operationsStatus = operationWorkerPutFunction(currentHandlerPtr), StatsType::OP_PUT);
                break;
            default:
                debug_error("[ERROR] Unknown operation type = %d\n", currentHandlerPtr->opType_);
                break;
            }
            if (operationTypeCorrectFlag == false) {
                currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                debug_trace("Process file ID = %lu error\n", currentHandlerPtr->file_handler_->target_file_id_);
                currentHandlerPtr->jobDone_ = kError;
            } else {
                if (operationsStatus == false) {
                    currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                    debug_trace("Process file ID = %lu error\n", currentHandlerPtr->file_handler_->target_file_id_);
                    currentHandlerPtr->jobDone_ = kError;
                } else {
                    if (currentHandlerPtr->opType_ != kGet && enableGCFlag_ == true) {
                        bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(currentHandlerPtr->file_handler_);
                        if (putIntoGCJobQueueStatus == false) {
                            currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                            debug_trace("Process file ID = %lu done\n", currentHandlerPtr->file_handler_->target_file_id_);
                            currentHandlerPtr->jobDone_ = kDone;
                        } else {
                            debug_trace("Process file ID = %lu done\n", currentHandlerPtr->file_handler_->target_file_id_);
                            currentHandlerPtr->jobDone_ = kDone;
                        }
                    } else {
                        currentHandlerPtr->file_handler_->file_ownership_flag_ = 0;
                        debug_trace("Process file ID = %lu done\n", currentHandlerPtr->file_handler_->target_file_id_);
                        currentHandlerPtr->jobDone_ = kDone;
                    }
                }
            }
        }
    }
    debug_info("Thread of operation worker exit success %p\n", this);
    workingThreadExitFlagVec_ += 1;
    return;
}

void HashStoreFileOperator::notifyOperationWorkerThread()
{
    while (true) {
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        if (operationToWorkerMQ_->isEmpty() == false) {
            operationNotifyCV_.notify_all();
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
