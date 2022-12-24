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
    if (options->deltaStore_op_worker_thread_number_limit_ > 2) {
        syncStatistics_ = true;
        for (int threadID = 0; threadID < options->deltaStore_op_worker_thread_number_limit_ - 1; threadID++) {
            workingThreadExitFlagVec_.push_back(false);
        }
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
        while (true) {
            bool threadExitFlag = true;
            for (auto it : workingThreadExitFlagVec_) {
                threadExitFlag = threadExitFlag && it;
            }
            if (threadExitFlag == false) {
                operationNotifyCV_.notify_all();
            } else {
                break;
            }
        }
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
        debug_trace("Process multiput operation for file ID = %lu, key number = %lu done\n", currentOperationHandler->file_handler_->target_file_id_, currentOperationHandler->batched_write_operation_.key_str_vec_ptr_->size());
        delete currentOperationHandler;
        return true;
    } else {
        debug_error("[ERROR] Process multiput operation for file ID = %lu, key number = %lu error\n", currentOperationHandler->file_handler_->target_file_id_, currentOperationHandler->batched_write_operation_.key_str_vec_ptr_->size());
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
    // check if not flushed anchors exit, return directly.
    string currentKeyStr = *currentHandlerPtr->read_operation_.key_str_;
    if (currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.find(currentKeyStr) != currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.end()) {
        debug_trace("Read operations from buffered anchors, key = %s\n", currentKeyStr.c_str());
        currentHandlerPtr->read_operation_.value_str_vec_->clear();
        return true;
    }
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
                unordered_map<string, vector<string>> currentFileProcessMap;
                currentFileProcessMap.reserve(currentHandlerPtr->file_handler_->total_object_count_);
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                    return false;
                } else {
                    if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size());
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            string tempInsertCacheKeyStr = mapIt.first;
                            if (currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.find(tempInsertCacheKeyStr) != currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.end()) {
                                if (keyToValueListCache_->existsInCache(tempInsertCacheKeyStr) == true) {
                                    struct timeval tv;
                                    gettimeofday(&tv, 0);
                                    keyToValueListCache_->getFromCache(tempInsertCacheKeyStr).clear();
                                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                                    continue;
                                } else {
                                    continue;
                                }
                            }
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, mapIt.second);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                            debug_trace("Insert to cache key %s delta num %d\n", tempInsertCacheKeyStr.c_str(), (int)mapIt.second.size());
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
            unordered_map<string, vector<string>> currentFileProcessMap;
            currentFileProcessMap.reserve(currentHandlerPtr->file_handler_->total_object_count_);
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, currentHandlerPtr->file_handler_->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != currentHandlerPtr->file_handler_->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, currentHandlerPtr->file_handler_->total_object_count_);
                return false;
            } else {
                if (currentFileProcessMap.find(*currentHandlerPtr->read_operation_.key_str_) == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", (*currentHandlerPtr->read_operation_.key_str_).c_str());
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).size());
                    currentHandlerPtr->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).begin(), currentFileProcessMap.at(*currentHandlerPtr->read_operation_.key_str_).end());
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
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                string currentValueStr(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
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
    string currentKeyStr = *currentHandlerPtr->write_operation_.key_str_;
    uint32_t currentSequenceNumber = currentHandlerPtr->write_operation_.sequence_number_;
    if (currentHandlerPtr->write_operation_.is_anchor == true) {
        if (currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.find(currentKeyStr) != currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.end()) {
            currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.at(currentKeyStr) = currentSequenceNumber;
        } else {
            currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.insert(make_pair(currentKeyStr, currentSequenceNumber));
        }
        if (keyToValueListCache_ != nullptr) {
            if (keyToValueListCache_->existsInCache(currentKeyStr)) {
                // insert into cache only if the key has been read
                struct timeval tv;
                gettimeofday(&tv, 0);
                keyToValueListCache_->getFromCache(currentKeyStr).clear();
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
            }
        }
        return true;
    }
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

        if (std::filesystem::exists(targetFilePathStr) != true) {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
        } else {
            currentHandlerPtr->file_handler_->file_operation_func_ptr_->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBufferWithoutAnchros, targetWriteSizeWithoutAnchors, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler_->target_file_id_);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                    // insert into cache only if the key has been read
                    struct timeval tv;
                    gettimeofday(&tv, 0);
                    keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                }
            }
            return true;
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
            return false;
        } else {
            currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.clear(); // clean up flushed anchors
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
                    // insert into cache only if the key has been read
                    struct timeval tv;
                    gettimeofday(&tv, 0);
                    keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                }
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    // if (currentHandlerPtr->file_handler_ == nullptr) {
    //     debug_error("[ERROR] Current file handler not exist%s\n", "");
    //     return false;
    // } else if (currentHandlerPtr->file_handler_->file_ownership_flag_ != 1) {
    //     debug_error("[ERROR] Current file handler ownership error, ownership = %d\n", currentHandlerPtr->file_handler_->file_ownership_flag_);
    //     return false;
    // }
    debug_trace("Test in thread: for file ID = %lu, put deltas key number = %lu, %lu, %lu, %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.sequence_number_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size());

    if (currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size(); index++) {
            if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(index) == false) {
                onlyAnchorFlag = false;
            }
        }
        if (onlyAnchorFlag == true && currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", currentHandlerPtr->file_handler_->target_file_id_);
            return true;
        }
    } else {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size(); index++) {
            if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(index) == false) {
                onlyAnchorFlag = false;
            }
        }
        if (onlyAnchorFlag == true && currentHandlerPtr->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
            debug_info("Only contains anchors for file ID = %lu, and file is opened, just load into anchor buffer\n", currentHandlerPtr->file_handler_->target_file_id_);
            for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size(); index++) {
                string keyStr = currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(index);
                uint32_t sequenceNumber = currentHandlerPtr->batched_write_operation_.sequence_number_vec_ptr_->at(index);
                currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.insert(make_pair(keyStr, sequenceNumber));
            }
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
    debug_trace("Test in thread (After create): for file ID = %lu, put deltas key number = %lu, %lu, %lu, %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.sequence_number_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size());
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
    if (needFlushFileHeader == true) {
        memcpy(writeContentBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
        currentProcessedBufferIndex += sizeof(hashStoreFileHeader);
    }
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
    debug_info("Target write object number = %lu, not flushed anchor buffer size = %lu\n", targetObjectNumber, currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.size());
    // write content
    bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler_, writeContentBuffer, targetWriteBufferSize, targetObjectNumber);
    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID = %lu, content size = %lu, content bytes number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, targetObjectNumber, targetWriteBufferSize);
        return false;
    } else {
        if (currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.size() != 0) {
            currentHandlerPtr->file_handler_->bufferedUnFlushedAnchorsVec_.clear();
        }
        // insert to cache if need
        if (keyToValueListCache_ != nullptr) {
            for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
                if (keyToValueListCache_->existsInCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i))) {
                    if (currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->at(i) == true) {
                        struct timeval tv;
                        gettimeofday(&tv, 0);
                        keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).clear();
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                    } else {
                        struct timeval tv;
                        gettimeofday(&tv, 0);
                        keyToValueListCache_->getFromCache(currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->at(i)).push_back(currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->at(i));
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                    }
                }
            }
        }
        return true;
    }
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* fileHandler)
{
    // debug_error("Current file ID = %lu, GC threshold = %lu, current size = %lu, total disk size = %lu\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
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

bool HashStoreFileOperator::directlyWriteOperation(hashStoreFileMetaDataHandler* fileHandler, string key, string value, uint32_t sequenceNumber, bool isAnchorStatus)
{
    std::scoped_lock<std::shared_mutex> w_lock(fileHandler->fileOperationMutex_);
    // process Anchor first
    if (isAnchorStatus == true) {
        if (fileHandler->bufferedUnFlushedAnchorsVec_.find(key) != fileHandler->bufferedUnFlushedAnchorsVec_.end()) {
            fileHandler->bufferedUnFlushedAnchorsVec_.at(key) = sequenceNumber;
        } else {
            fileHandler->bufferedUnFlushedAnchorsVec_.insert(make_pair(key, sequenceNumber));
        }
        if (keyToValueListCache_ != nullptr) {
            if (keyToValueListCache_->existsInCache(key)) {
                // insert into cache only if the key has been read
                struct timeval tv;
                gettimeofday(&tv, 0);
                keyToValueListCache_->getFromCache(key).clear();
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
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
    newRecordHeader.sequence_number_ = sequenceNumber;
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

        if (std::filesystem::exists(targetFilePathStr) != true) {
            fileHandler->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
        } else {
            fileHandler->file_operation_func_ptr_->openFile(targetFilePathStr);
        }
        // write contents of file

        fileOperationStatus_t onDiskWriteSizePair;
        STAT_PROCESS(onDiskWriteSizePair = fileHandler->file_operation_func_ptr_->writeFile(writeContentBufferWithoutAnchros, targetWriteSizeWithoutAnchors), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
        StatsRecorder::getInstance()->DeltaOPBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
        if (onDiskWriteSizePair.success_ == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            // Update metadata
            fileHandler->total_object_bytes_ += targetWriteSizeWithoutAnchors;
            fileHandler->total_on_disk_bytes_ += onDiskWriteSizePair.physicalSize_;
            fileHandler->total_object_count_++;
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(key)) {
                    // insert into cache only if the key has been read
                    struct timeval tv;
                    gettimeofday(&tv, 0);
                    keyToValueListCache_->getFromCache(key).push_back(value);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                }
            }
            fileHandler->bufferedUnFlushedAnchorsVec_.clear(); // clean up buffered anchors
            // try GC if enabled
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(fileHandler);
                if (putIntoGCJobQueueStatus == true) {
                    fileHandler->file_ownership_flag_ = -1;
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
        fileOperationStatus_t onDiskWriteSizePair;
        STAT_PROCESS(onDiskWriteSizePair = fileHandler->file_operation_func_ptr_->writeFile(writeContentBufferWithAnchor, targetWriteSizeWithAnchors), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
        StatsRecorder::getInstance()->DeltaOPBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
        if (onDiskWriteSizePair.success_ == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
            fileHandler->file_ownership_flag_ = 0;
            return false;
        } else {
            // Update metadata
            fileHandler->total_object_bytes_ += targetWriteSizeWithAnchors;
            fileHandler->total_on_disk_bytes_ += onDiskWriteSizePair.physicalSize_;
            fileHandler->total_object_count_ += (totalNotFlushedAnchorNumber + 1);
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCache_ != nullptr) {
                if (keyToValueListCache_->existsInCache(key)) {
                    // insert into cache only if the key has been read
                    struct timeval tv;
                    gettimeofday(&tv, 0);
                    keyToValueListCache_->getFromCache(key).push_back(value);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                }
            }
            fileHandler->bufferedUnFlushedAnchorsVec_.clear(); // clean up flushed anchors
            // try GC if enabled
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(fileHandler);
                if (putIntoGCJobQueueStatus == true) {
                    fileHandler->file_ownership_flag_ = -1;
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
    }
}

bool HashStoreFileOperator::directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> batchedWriteOperationsMap)
{
    vector<bool> jobeDoneStatus;
    for (auto batchIt : batchedWriteOperationsMap) {
        std::scoped_lock<std::shared_mutex> w_lock(batchIt.first->fileOperationMutex_);
        // check file existence, create if not exist
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < std::get<3>(batchIt.second).size(); index++) {
            if (std::get<3>(batchIt.second).at(index) == false) {
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
        if (needFlushFileHeader == true) {
            memcpy(writeContentBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
            currentProcessedBufferIndex += sizeof(hashStoreFileHeader);
        }
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
            debug_error("[ERROR] Could not write content to file ID = %lu, object number = %lu, object total size = %lu\n", batchIt.first->target_file_id_, targetObjectNumber, targetWriteBufferSize);
            batchIt.first->file_ownership_flag_ = 0;
            jobeDoneStatus.push_back(false);
        } else {
            batchIt.first->bufferedUnFlushedAnchorsVec_.clear();
            // insert to cache if need
            if (keyToValueListCache_ != nullptr) {
                for (auto i = 0; i < std::get<0>(batchIt.second).size(); i++) {
                    if (keyToValueListCache_->existsInCache(std::get<0>(batchIt.second).at(i))) {
                        if (std::get<3>(batchIt.second).at(i) == true) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            keyToValueListCache_->getFromCache(std::get<0>(batchIt.second).at(i)).clear();
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                        } else {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            keyToValueListCache_->getFromCache(std::get<0>(batchIt.second).at(i)).push_back(std::get<1>(batchIt.second).at(i));
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                        }
                    }
                }
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
    if (fileHandler->bufferedUnFlushedAnchorsVec_.find(key) != fileHandler->bufferedUnFlushedAnchorsVec_.end()) {
        debug_trace("Read operations from buffered anchors, key = %s\n", key.c_str());
        valueVec->clear();
        fileHandler->file_ownership_flag_ = 0;
        return true;
    }
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
                unordered_map<string, vector<string>> currentFileProcessMap;
                currentFileProcessMap.reserve(fileHandler->total_object_count_);
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != fileHandler->total_object_count_) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    if (currentFileProcessMap.find(key) == currentFileProcessMap.end()) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        valueVec->clear();
                        fileHandler->file_ownership_flag_ = 0;
                        return false;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(key).size());
                        valueVec->assign(currentFileProcessMap.at(key).begin(), currentFileProcessMap.at(key).end());
                        // Put the cache operation before job done, to avoid some synchronization issues
                        for (auto mapIt : currentFileProcessMap) {
                            string tempInsertCacheKeyStr = mapIt.first;
                            if (fileHandler->bufferedUnFlushedAnchorsVec_.find(tempInsertCacheKeyStr) != fileHandler->bufferedUnFlushedAnchorsVec_.end()) {
                                if (keyToValueListCache_->existsInCache(tempInsertCacheKeyStr) == true) {
                                    struct timeval tv;
                                    gettimeofday(&tv, 0);
                                    keyToValueListCache_->getFromCache(tempInsertCacheKeyStr).clear();
                                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
                                    continue;
                                } else {
                                    continue;
                                }
                            }
                            struct timeval tv;
                            gettimeofday(&tv, 0);
                            keyToValueListCache_->insertToCache(tempInsertCacheKeyStr, mapIt.second);
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
            unordered_map<string, vector<string>> currentFileProcessMap;
            currentFileProcessMap.reserve(fileHandler->total_object_count_);
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, fileHandler->total_object_bytes_, currentFileProcessMap), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != fileHandler->total_object_count_) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, fileHandler->total_object_count_);
                valueVec->clear();
                fileHandler->file_ownership_flag_ = 0;
                return false;
            } else {
                if (currentFileProcessMap.find(key) == currentFileProcessMap.end()) {
                    debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                    valueVec->clear();
                    fileHandler->file_ownership_flag_ = 0;
                    return false;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), currentFileProcessMap.at(key).size());
                    valueVec->assign(currentFileProcessMap.at(key).begin(), currentFileProcessMap.at(key).end());
                }
                fileHandler->file_ownership_flag_ = 0;
                return true;
            }
        }
    }
}

void HashStoreFileOperator::operationWorker(int threadID)
{
    workingThreadExitFlagVec_[threadID] = false;
    while (true) {
        {
            std::unique_lock<std::mutex> lk(operationNotifyMtx_);
            operationNotifyCV_.wait(lk);
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
                debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %lu, %lu, %lu, %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.value_str_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.sequence_number_vec_ptr_->size(), currentHandlerPtr->batched_write_operation_.is_anchor_vec_ptr_->size());
                STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(currentHandlerPtr), StatsType::OP_MULTIPUT);
                debug_trace("processed operations, type = kMultiPut, file ID = %lu, put deltas key number = %lu\n", currentHandlerPtr->file_handler_->target_file_id_, currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size());
                break;
            case kPut:
                operationTypeCorrectFlag = true;
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", (*currentHandlerPtr->write_operation_.key_str_).c_str(), currentHandlerPtr->file_handler_->target_file_id_);
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
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
    }
    debug_info("Thread of operation worker exit success %p\n", this);
    workingThreadExitFlagVec_[threadID] = true;
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
