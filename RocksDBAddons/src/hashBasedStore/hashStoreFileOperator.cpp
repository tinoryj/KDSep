#include "hashBasedStore/hashStoreFileOperator.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCToManagerMQ)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_file_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileSizeLimit_ = options->deltaStore_single_file_maximum_size;
    operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
    notifyGCToManagerMQ_ = notifyGCToManagerMQ;
    if (options->enable_deltaStore_KDLevel_cache == true) {
        keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>>(options->deltaStore_KDLevel_cache_item_number);
    }
    workingDir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
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

void HashStoreFileOperator::operationWorkerGetFromFile(hashStoreFileMetaDataHandler* fileHandler, hashStoreOperationHandler* opHandler, unordered_map<string, vector<string>>& currentFileProcessMap)
{
    debug_trace("get from file %s\n", "");
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        debug_error("[ERROR] Should not read from a not opened file ID = %lu\n", fileHandler->target_file_id_);
        exit(-1);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone = true;
        return;
    }
    char readBuffer[fileHandler->total_object_bytes_];
    fileHandler->fileOperationMutex_.lock();
    if (fileHandler->temp_not_flushed_data_bytes_ > 0) {
        fileHandler->file_operation_func_ptr_->flushFile();
        fileHandler->temp_not_flushed_data_bytes_ = 0;
    }
    bool readFileStatus = fileHandler->file_operation_func_ptr_->readFile(readBuffer, fileHandler->total_object_bytes_);
    fileHandler->fileOperationMutex_.unlock();
    if (readFileStatus == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", fileHandler->target_file_id_);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone = true;
        return;
    }
    uint64_t totalProcessedObjectNumber = processReadContentToValueLists(readBuffer, fileHandler->total_object_bytes_, currentFileProcessMap);
    auto keyStr = *opHandler->read_operation_.key_str_;

    for (auto& keyIt : fileHandler->savedAnchors_) {
        if (currentFileProcessMap.find(keyIt) != currentFileProcessMap.end()) {
            currentFileProcessMap.at(keyIt).clear();
            debug_trace("clear key %s in map\n", keyIt.c_str());
        }
    }

    if (totalProcessedObjectNumber != fileHandler->total_object_count_) {
        debug_error("[ERROR] Read bucket get mismatched object number, number in metadata = %lu, number read from file = %lu\n", fileHandler->total_object_count_, totalProcessedObjectNumber);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone = true;
        return;
    } 

    if (currentFileProcessMap.find(keyStr) == currentFileProcessMap.end()) {
        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", keyStr.c_str());
        exit(-1);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone = true;
        return;
    } 

    debug_trace("get current key related values success, key = %s, value number = %lu\n", keyStr.c_str(), currentFileProcessMap.at(keyStr).size());
    opHandler->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(keyStr).begin(), currentFileProcessMap.at(keyStr).end());
    fileHandler->file_ownership_flag_ = 0;
    opHandler->jobDone = true;
} 

// Assume that the file is already created and opened.
void HashStoreFileOperator::operationWorkerPutAnchorsAndWriteBuffer(hashStoreFileMetaDataHandler* fileHandler, char* data, uint64_t size, string openFileName) {
    debug_trace("push %d anchors\n", (int)fileHandler->savedAnchors_.size());
    uint64_t bufferSize = sizeof(hashStoreRecordHeader) * fileHandler->savedAnchors_.size() + size; 
    for (auto& keyIt : fileHandler->savedAnchors_) {
        bufferSize += keyIt.size();
    }

    char buffer[bufferSize];
    uint64_t bufferIndex = 0;
    hashStoreRecordHeader recordHeader;

    for (auto& keyIt : fileHandler->savedAnchors_) {
        recordHeader.is_anchor_ = true;
        recordHeader.key_size_ = keyIt.size(); 
        recordHeader.value_size_ = 0;  // not sure.
        memcpy(buffer + bufferIndex, &recordHeader, sizeof(recordHeader));
        memcpy(buffer + bufferIndex + sizeof(recordHeader), keyIt.c_str(), keyIt.size());
        bufferIndex += sizeof(recordHeader) + keyIt.size();
        debug_trace("buffer index %d key %s\n", (int)bufferIndex, keyIt.c_str()); 
    }

    if (data != nullptr) {
        memcpy(buffer + bufferIndex, data, size);
    }

    fileHandler->fileOperationMutex_.lock();

    // Check whether need to open file 
    if (!openFileName.empty()) {
        assert(fileHandler->file_operation_func_ptr_->isFileOpen() == false);
        assert(fileHandler->savedAnchors_.empty());
        debug_info("Newly created file ID = %lu, target prefix bit number = %lu\n", fileHandler->target_file_id_, fileHandler->current_prefix_used_bit_);
        fileHandler->file_operation_func_ptr_->createFile(openFileName);
        if (fileHandler->file_operation_func_ptr_->isFileOpen() == true) {
            fileHandler->file_operation_func_ptr_->closeFile();
        }
        fileHandler->file_operation_func_ptr_->openFile(openFileName);
    }

    // write content
    uint64_t onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(buffer, bufferSize);
    if (onDiskWriteSize == 0) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
        exit(-1);
//        fileHandler->file_ownership_flag_ = 0;
//        fileHandler->fileOperationMutex_.unlock();
//        currentHandlerPtr->jobDone = true;
        return;
    }
    fileHandler->temp_not_flushed_data_bytes_ += bufferSize;
    if (fileHandler->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
        fileHandler->file_operation_func_ptr_->flushFile();
        debug_trace("flushed file ID = %lu, flushed size = %lu\n", fileHandler->target_file_id_, fileHandler->temp_not_flushed_data_bytes_);
        fileHandler->temp_not_flushed_data_bytes_ = 0;
    } else {
        debug_trace("buffered not flushed file ID = %lu, buffered size = %lu\n", fileHandler->target_file_id_, fileHandler->temp_not_flushed_data_bytes_);
    }
    // Update metadata
    fileHandler->total_object_bytes_ += bufferSize;
    fileHandler->total_on_disk_bytes_ += onDiskWriteSize;
    fileHandler->total_object_count_ += fileHandler->savedAnchors_.size() + (data ? 1 : 0); // Not sure

    if (!openFileName.empty()) {  // Actually not necessary... just for safety
        fileHandler->total_object_bytes_ = bufferSize;
        fileHandler->total_on_disk_bytes_ = onDiskWriteSize;
        fileHandler->total_object_count_ = 1;
    }
    fileHandler->savedAnchors_.clear();
    fileHandler->fileOperationMutex_.unlock();
}

void HashStoreFileOperator::operationWorkerPut(hashStoreOperationHandler* currentHandlerPtr) {
    auto fileHandler = currentHandlerPtr->file_handler_;
    debug_trace("receive operations, type = kPut, key = %s, anchor %d, target file ID = %lu, current file size = %lu - %lu\n", 
            (*currentHandlerPtr->write_operation_.key_str_).c_str(), 
            (int)currentHandlerPtr->write_operation_.is_anchor,
            fileHandler->target_file_id_, 
            fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
    if (enableGCFlag_ && fileHandler->total_on_disk_bytes_ > (1ull << 21)) {
        debug_error("[ERROR] No... that's crazy. object bytes %lu v.s. disk bytes %lu\n", fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
        exit(-1);
    }
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = currentHandlerPtr->write_operation_.is_anchor;
    newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.key_str_->size();
    newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.value_str_->size();
    if (newRecordHeader.is_anchor_ == true) {
        // Anchors do not need value size.
        if (fileHandler->total_object_count_ != 0) { 
            // save some anchors.
            fileHandler->savedAnchors_.insert(*currentHandlerPtr->write_operation_.key_str_);
            if (fileHandler->savedAnchors_.size() < 20) { 
                // do not need to write anything for now
            } else {
                operationWorkerPutAnchorsAndWriteBuffer(fileHandler, /* buffer */ nullptr, /* size */ 0, /* openFileName */ "");
            }
            debug_trace("anchors saved %d\n", (int)fileHandler->savedAnchors_.size());
        }
    } else {
        if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
            hashStoreFileHeader newFileHeader;
            newFileHeader.current_prefix_used_bit_ = fileHandler->current_prefix_used_bit_;
            newFileHeader.previous_file_id_ = fileHandler->previous_file_id_;
            newFileHeader.file_create_reason_ = kNewFile;
            newFileHeader.file_id_ = fileHandler->target_file_id_;
            uint64_t size = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
            char buffer[size];
            memcpy(buffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(buffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
            memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);

            // write content
            operationWorkerPutAnchorsAndWriteBuffer(fileHandler, buffer, size, /* openFileName */ workingDir_ + "/" + to_string(fileHandler->target_file_id_) + ".delta");
        } else {
            uint64_t size = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
            char buffer[size];
            memcpy(buffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(buffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
            memcpy(buffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);

            // write content
            debug_trace("key %s directly write\n", (*currentHandlerPtr->write_operation_.key_str_).c_str());
            operationWorkerPutAnchorsAndWriteBuffer(fileHandler, buffer, size, "");
        }
    }
    // insert to cache if need
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(*currentHandlerPtr->write_operation_.key_str_)) {
            // insert into cache only if the key has been read
            if (currentHandlerPtr->write_operation_.is_anchor == true) {
                debug_trace("key %s clear in cache\n", (*currentHandlerPtr->write_operation_.key_str_).c_str());
                keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).clear();
            } else {
                keyToValueListCache_->getFromCache(*currentHandlerPtr->write_operation_.key_str_).push_back(*currentHandlerPtr->write_operation_.value_str_);
            }
        }
    }
    if (enableGCFlag_ == true) {
        // insert into GC job queue if exceed the threshold
        if (fileHandler->total_on_disk_bytes_ >= singleFileSizeLimit_ && fileHandler->gc_result_status_flag_ == kNoGC) {
            fileHandler->no_gc_wait_operation_number_++;
            if (fileHandler->no_gc_wait_operation_number_ % operationNumberThresholdForForcedSingleFileGC_ == 1) {
                fileHandler->file_ownership_flag_ = -1;
                fileHandler->gc_result_status_flag_ = kMayGC;
                notifyGCToManagerMQ_->push(fileHandler);
                debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
            } else {
                fileHandler->file_ownership_flag_ = 0;
            }
        } else if (fileHandler->total_on_disk_bytes_ >= perFileGCSizeLimit_) {
            if (fileHandler->gc_result_status_flag_ == kNew || fileHandler->gc_result_status_flag_ == kMayGC) {
                fileHandler->file_ownership_flag_ = -1;
                notifyGCToManagerMQ_->push(fileHandler);
                debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
            } else {
                debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
                fileHandler->file_ownership_flag_ = 0;
            }
        } else {
            fileHandler->file_ownership_flag_ = 0;
        }
    } else {
        fileHandler->file_ownership_flag_ = 0;
    }
    // mark job done
    currentHandlerPtr->jobDone = true;
}

void HashStoreFileOperator::operationWorker()
{
    while (true) {
        if (operationToWorkerMQ_->done_ == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            auto fileHandler = currentHandlerPtr->file_handler_;
            if (currentHandlerPtr->opType_ == kGet) {
                auto keyStr = *currentHandlerPtr->read_operation_.key_str_;
                debug_trace("receive operations, type = kGet, key = %s, target file ID = %lu\n", keyStr.c_str(), fileHandler->target_file_id_);
                // try extract from cache first
                if (keyToValueListCache_) {
                    if (keyToValueListCache_->existsInCache(keyStr)) {
                        vector<string> tempResultVec = keyToValueListCache_->getFromCache(keyStr);
                        debug_trace("read operations from cache, cache hit, key %s, hit vec size = %lu\n", keyStr.c_str(), tempResultVec.size());
                        // for (auto it : tempResultVec) {
                        //     debug_trace("\thit vec item =  %s\n", it.c_str());
                        // }
                        currentHandlerPtr->read_operation_.value_str_vec_->assign(tempResultVec.begin(), tempResultVec.end());
                        fileHandler->file_ownership_flag_ = 0;
                        currentHandlerPtr->jobDone = true;
                        continue;
                    } 

                    // Not exist in cache, find the content in the file
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    operationWorkerGetFromFile(fileHandler, currentHandlerPtr, currentFileProcessMap);

                    // insert to cache
                    for (auto mapIt : currentFileProcessMap) {
                        string tempKeyForCacheInsert = mapIt.first;
                        keyToValueListCache_->insertToCache(tempKeyForCacheInsert, mapIt.second);
                        debug_trace("Insert to cache key %s\n", mapIt.first.c_str());
                    }
                } else {
                    // no cache, only read content
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    operationWorkerGetFromFile(fileHandler, currentHandlerPtr, currentFileProcessMap);
                }
            } else if (currentHandlerPtr->opType_ == kPut) {
                operationWorkerPut(currentHandlerPtr);
            } else if (currentHandlerPtr->opType_ == kMultiPut) {
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", (*currentHandlerPtr->write_operation_.key_str_).c_str(), fileHandler->target_file_id_);
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

                for (auto& keyIt : fileHandler->savedAnchors_) {
                    targetWriteBufferSize += sizeof(hashStoreRecordHeader) + keyIt.size();
                }

                char writeContentBuffer[targetWriteBufferSize];
                uint64_t currentProcessedBufferIndex = 0;
                hashStoreRecordHeader newRecordHeader;
                for (auto& keyIt : fileHandler->savedAnchors_) {
                    newRecordHeader.is_anchor_ = true;
                    newRecordHeader.key_size_ = keyIt.size();
                    newRecordHeader.value_size_ = 0; // not sure
                    memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeader, sizeof(hashStoreRecordHeader));
                    currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
                    memcpy(writeContentBuffer + currentProcessedBufferIndex, keyIt.c_str(), keyIt.size());
                    currentProcessedBufferIndex += keyIt.size();
                }

                for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size(); i++) {
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
                fileHandler->fileOperationMutex_.lock();
                // write content
                uint64_t onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(writeContentBuffer, targetWriteBufferSize);
                fileHandler->temp_not_flushed_data_bytes_ += targetWriteBufferSize;
                if (fileHandler->temp_not_flushed_data_bytes_ >= perFileFlushBufferSizeLimit_) {
                    debug_trace("flushed file ID = %lu, flushed size = %lu\n", fileHandler->target_file_id_, fileHandler->temp_not_flushed_data_bytes_);
                    fileHandler->file_operation_func_ptr_->flushFile();
                    fileHandler->temp_not_flushed_data_bytes_ = 0;
                }
                // Update metadata
                fileHandler->total_object_bytes_ += targetWriteBufferSize;
                fileHandler->total_on_disk_bytes_ += onDiskWriteSize;
                fileHandler->total_object_count_ += currentHandlerPtr->batched_write_operation_.key_str_vec_ptr_->size() + fileHandler->savedAnchors_.size();
                fileHandler->savedAnchors_.clear();
                fileHandler->fileOperationMutex_.unlock();
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
                if (enableGCFlag_ == true) {
                    // insert into GC job queue if exceed the threshold
                    if (fileHandler->total_on_disk_bytes_ >= singleFileSizeLimit_ && fileHandler->gc_result_status_flag_ == kNoGC) {
                        fileHandler->no_gc_wait_operation_number_++;
                        if (fileHandler->no_gc_wait_operation_number_ % operationNumberThresholdForForcedSingleFileGC_ == 1) {
                            fileHandler->file_ownership_flag_ = -1;
                            fileHandler->gc_result_status_flag_ = kMayGC;
                            notifyGCToManagerMQ_->push(fileHandler);
                            debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
                        } else {
                            fileHandler->file_ownership_flag_ = 0;
                        }
                    } else if (fileHandler->total_on_disk_bytes_ >= perFileGCSizeLimit_) {
                        if (fileHandler->gc_result_status_flag_ == kNew || fileHandler->gc_result_status_flag_ == kMayGC) {
                            fileHandler->file_ownership_flag_ = -1;
                            notifyGCToManagerMQ_->push(fileHandler);
                            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
                        } else {
                            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", fileHandler->target_file_id_, perFileGCSizeLimit_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
                            fileHandler->file_ownership_flag_ = 0;
                        }
                    } else {
                        fileHandler->file_ownership_flag_ = 0;
                    }
                } else {
                    fileHandler->file_ownership_flag_ = 0;
                }
                // mark job done
                currentHandlerPtr->jobDone = true;
                continue;
            } else {
                debug_error("[ERROR] Unknown operation type = %d\n", currentHandlerPtr->opType_);
                fileHandler->file_ownership_flag_ = 0;
                break;
            }
        }
    }
    debug_info("Thread of operation worker exit success %p\n", this);
    return;
}

} // namespace DELTAKV_NAMESPACE
