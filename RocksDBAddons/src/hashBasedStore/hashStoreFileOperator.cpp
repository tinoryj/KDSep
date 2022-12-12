#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "utils/statsRecorder.hh"

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
    currentHandler->jobDone_ = kNotDone;
    currentHandler->write_operation_.key_str_ = &key;
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

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<bool>>> tempFileHandlerMap)
{
    vector<hashStoreOperationHandler*> currentOperationHandlerVec;
    for (auto fileHandlerIt : tempFileHandlerMap) {
        hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(fileHandlerIt.first);
        currentOperationHandler->jobDone_ = kNotDone;
        currentOperationHandler->batched_write_operation_.key_str_vec_ptr_ = &std::get<0>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.value_str_vec_ptr_ = &std::get<1>(tempFileHandlerMap.at(fileHandlerIt.first));
        currentOperationHandler->batched_write_operation_.is_anchor_vec_ptr_ = &std::get<2>(tempFileHandlerMap.at(fileHandlerIt.first));
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

bool operationWorkerPutWithCache(hashStoreOperationHandler* currentHandlerPtr)
{
}
bool operationWorkerPutWithoutCahe(hashStoreOperationHandler* currentHandlerPtr)
{
}
bool operationWorkerGetWithCache(hashStoreOperationHandler* currentHandlerPtr)
{
}
bool operationWorkerGetWithoutCache(hashStoreOperationHandler* currentHandlerPtr)
{
}

uint64_t readContentFromFile(hashStoreOperationHandler* opHandler, char* contentBuffer)
{
    debug_trace("Read content from file ID = %lu\n", opHandler->file_handler_->target_file_id_);
    if (opHandler->file_handler_->file_operation_func_ptr_->isFileOpen() == false) {
        debug_error("[ERROR] Should not read from a not opened file ID = %lu\n", fileHandler->target_file_id_);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone_ = kDone;
        return 0;
    }
    opHandler->file_handler_->fileOperationMutex_.lock();
    if (opHandler->file_handler_->temp_not_flushed_data_bytes_ > 0) {
        opHandler->file_handler_->file_operation_func_ptr_->flushFile();
        opHandler->file_handler_->temp_not_flushed_data_bytes_ = 0;
    }
    bool readFileStatus;
    STAT_TIME_PROCESS(readFileStatus = opHandler->file_handler_->file_operation_func_ptr_->readFile(contentBuffer, opHandler->file_handler_->total_object_bytes_), StatsType::DELTAKV_HASHSTORE_WORKER_GET_IO);
    opHandler->file_handler_->fileOperationMutex_.unlock();

    if (readFileStatus == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", opHandler->file_handler_->target_file_id_);
        opHandler->file_handler_->file_ownership_flag_ = 0;
        opHandler->jobDone_ = kError;
        return 0;
    } else {
        return opHandler->file_handler_->total_object_bytes_;
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

// Called after an operation is extracted from operationToWorkerMQ_.
// Read from file, when the cache does not have the object or there is no cache
void HashStoreFileOperator::operationWorkerGetFromFile(hashStoreFileMetaDataHandler* fileHandler, hashStoreOperationHandler* opHandler, unordered_map<string, vector<string>>& currentFileProcessMap)
{
    debug_trace("get from file %s\n", "");
    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        debug_error("[ERROR] Should not read from a not opened file ID = %lu\n", fileHandler->target_file_id_);
        exit(-1);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone_ = kDone;
        return;
    }
    char readBuffer[fileHandler->total_object_bytes_];
    fileHandler->fileOperationMutex_.lock();
    if (fileHandler->temp_not_flushed_data_bytes_ > 0) {
        fileHandler->file_operation_func_ptr_->flushFile();
        fileHandler->temp_not_flushed_data_bytes_ = 0;
    }
    bool readFileStatus;
    STAT_TIME_PROCESS(readFileStatus = fileHandler->file_operation_func_ptr_->readFile(readBuffer, fileHandler->total_object_bytes_), StatsType::DELTAKV_HASHSTORE_WORKER_GET_IO);
    fileHandler->fileOperationMutex_.unlock();

    if (readFileStatus == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", fileHandler->target_file_id_);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone_ = kDone;
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
        opHandler->jobDone_ = kDone;
        return;
    }

    if (currentFileProcessMap.find(keyStr) == currentFileProcessMap.end()) {
        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", keyStr.c_str());
        exit(-1);
        fileHandler->file_ownership_flag_ = 0;
        opHandler->jobDone_ = kDone;
        return;
    }

    debug_trace("get current key related values success, key = %s, value number = %lu\n", keyStr.c_str(), currentFileProcessMap.at(keyStr).size());
    opHandler->read_operation_.value_str_vec_->assign(currentFileProcessMap.at(keyStr).begin(), currentFileProcessMap.at(keyStr).end());

    // Put the cache operation before job done, to avoid some synchronization issues
    if (keyToValueListCache_) {
        // insert to cache
        // should not be in the cache before
        for (auto mapIt : currentFileProcessMap) {
            string tempKeyForCacheInsert = mapIt.first;
            keyToValueListCache_->insertToCache(tempKeyForCacheInsert, mapIt.second);
            debug_trace("Insert to cache key %s delta num %d\n", mapIt.first.c_str(), (int)mapIt.second.size());
        }
    }

    fileHandler->file_ownership_flag_ = 0;
    opHandler->jobDone_ = kDone;
}

// Assume that the file is already created and opened. It writes the buffered
// anchors first, then the other deltas.
void HashStoreFileOperator::putAnchorsAndWriteBuffer(hashStoreFileMetaDataHandler* fileHandler, char* data, uint64_t size, string openFileName)
{
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
        recordHeader.value_size_ = 0; // not sure.
        memcpy(buffer + bufferIndex, &recordHeader, sizeof(recordHeader));
        memcpy(buffer + bufferIndex + sizeof(recordHeader), keyIt.c_str(), keyIt.size());
        bufferIndex += sizeof(recordHeader) + keyIt.size();
        debug_trace("buffer index %d key %s\n", (int)bufferIndex, keyIt.c_str());
    }

    if (data != nullptr) {
        memcpy(buffer + bufferIndex, data, size);
    }

    struct timeval tv;
    gettimeofday(&tv, 0);

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
    uint64_t onDiskWriteSize;
    STAT_TIME_PROCESS(onDiskWriteSize = fileHandler->file_operation_func_ptr_->writeFile(buffer, bufferSize), StatsType::DELTAKV_HASHSTORE_WORKER_PUT_IO);
    if (onDiskWriteSize == 0) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", fileHandler->target_file_id_);
        exit(-1);
        //        fileHandler->file_ownership_flag_ = 0;
        //        fileHandler->fileOperationMutex_.unlock();
        //        currentHandlerPtr->jobDone_ = kDone;
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

    if (!openFileName.empty()) { // Actually not necessary... just for safety
        fileHandler->total_object_bytes_ = bufferSize;
        fileHandler->total_on_disk_bytes_ = onDiskWriteSize;
        fileHandler->total_object_count_ = 1;
    }
    fileHandler->savedAnchors_.clear();
    fileHandler->fileOperationMutex_.unlock();

    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_PUT_LOCK, tv);
}

//
// A simplified version of operationWorkerPut(), just for the anchors.  It saves
// lots of calculation. If it is executed directly in HashStoreInterface::put(),
// the writing of anchors can surprisingly save more than 3us for each operation,
// compared with putting into operationToWorkerMQ_ and running operationWorkerPut().
//
// It is a public function.
//
bool HashStoreFileOperator::bufferAnchor(hashStoreFileMetaDataHandler* fileHandler, string key)
{
    fileHandler->savedAnchors_.insert(key);
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            debug_trace("key %s clear in cache\n", key.c_str());
            keyToValueListCache_->getFromCache(key).clear();
        }
    }
    fileHandler->file_ownership_flag_ = 0;
    return true;
}

// A simplified version of operationWorkerPut(), just for the deltas.
// It is a public function.
void HashStoreFileOperator::put(hashStoreFileMetaDataHandler* fileHandler, string key, string value)
{
    debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu, current file size = %lu - %lu\n",
        key.c_str(), fileHandler->target_file_id_,
        fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);

    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = false;
    newRecordHeader.key_size_ = key.size();
    newRecordHeader.value_size_ = value.size();

    if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
        hashStoreFileHeader newFileHeader;
        newFileHeader.current_prefix_used_bit_ = fileHandler->current_prefix_used_bit_;
        newFileHeader.previous_file_id_first_ = fileHandler->previous_file_id_first_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = fileHandler->target_file_id_;
        uint64_t size = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        char buffer[size];
        memcpy(buffer, &newFileHeader, sizeof(newFileHeader));
        memcpy(buffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
        memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader), key.c_str(), newRecordHeader.key_size_);
        memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, value.c_str(), newRecordHeader.value_size_);

        // write content
        putAnchorsAndWriteBuffer(fileHandler, buffer, size, /* openFileName */ workingDir_ + "/" + to_string(fileHandler->target_file_id_) + ".delta");
    } else {
        uint64_t size = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        char buffer[size];
        memcpy(buffer, &newRecordHeader, sizeof(newRecordHeader));
        memcpy(buffer + sizeof(newRecordHeader), key.c_str(), newRecordHeader.key_size_);
        memcpy(buffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, value.c_str(), newRecordHeader.value_size_);

        // write content
        debug_trace("key %s directly write\n", key.c_str());
        putAnchorsAndWriteBuffer(fileHandler, buffer, size, "");
    }

    // insert to cache if need
    if (keyToValueListCache_ != nullptr) {
        if (keyToValueListCache_->existsInCache(key)) {
            // insert into cache only if the key has been read
            keyToValueListCache_->getFromCache(key).push_back(value);
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
}

// Called after an operation is extracted from operationToWorkerMQ_.
// The put operations, it can be used for both anchors and deltas.
void HashStoreFileOperator::operationWorkerPut(hashStoreOperationHandler* currentHandlerPtr)
{
    auto fileHandler = currentHandlerPtr->file_handler_;
    debug_trace("receive operations, type = kPut, key = %s, anchor %d, target file ID = %lu, current file size = %lu - %lu\n",
        (*currentHandlerPtr->write_operation_.key_str_).c_str(),
        (int)currentHandlerPtr->write_operation_.is_anchor,
        fileHandler->target_file_id_,
        fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
    if (enableGCFlag_ && fileHandler->total_on_disk_bytes_ > (1ull << 24)) {
        debug_error("[ERROR] disk bytes too large. object bytes %lu v.s. disk bytes %lu\n", fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_);
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
                putAnchorsAndWriteBuffer(fileHandler, /* buffer */ nullptr, /* size */ 0, /* openFileName */ "");
            }
            debug_trace("anchors saved %d\n", (int)fileHandler->savedAnchors_.size());
        }
    } else {
        if (fileHandler->file_operation_func_ptr_->isFileOpen() == false) {
            hashStoreFileHeader newFileHeader;
            newFileHeader.current_prefix_used_bit_ = fileHandler->current_prefix_used_bit_;
            newFileHeader.previous_file_id_first_ = fileHandler->previous_file_id_first_;
            newFileHeader.file_create_reason_ = kNewFile;
            newFileHeader.file_id_ = fileHandler->target_file_id_;
            uint64_t size = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
            char buffer[size];
            memcpy(buffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(buffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
            memcpy(buffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);

            // write content
            putAnchorsAndWriteBuffer(fileHandler, buffer, size, /* openFileName */ workingDir_ + "/" + to_string(fileHandler->target_file_id_) + ".delta");
        } else {
            uint64_t size = sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
            char buffer[size];
            memcpy(buffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(buffer + sizeof(newRecordHeader), currentHandlerPtr->write_operation_.key_str_->c_str(), newRecordHeader.key_size_);
            memcpy(buffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, currentHandlerPtr->write_operation_.value_str_->c_str(), newRecordHeader.value_size_);

            // write content
            debug_trace("key %s directly write\n", (*currentHandlerPtr->write_operation_.key_str_).c_str());
            putAnchorsAndWriteBuffer(fileHandler, buffer, size, "");
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
    currentHandlerPtr->jobDone_ = kDone;
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
                struct timeval tv;
                gettimeofday(&tv, 0);
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
                        currentHandlerPtr->jobDone_ = kDone;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GET_CACHE, tv);
                    } else {
                        // Not exist in cache, find the content in the file
                        unordered_map<string, vector<string>> currentFileProcessMap;
                        operationWorkerGetFromFile(fileHandler, currentHandlerPtr, currentFileProcessMap);
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GET_FILE, tv);
                    }
                } else {
                    // no cache, only read content
                    unordered_map<string, vector<string>> currentFileProcessMap;
                    operationWorkerGetFromFile(fileHandler, currentHandlerPtr, currentFileProcessMap);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GET_FILE, tv);
                }

                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GET, tv);
            } else if (currentHandlerPtr->opType_ == kPut) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                operationWorkerPut(currentHandlerPtr);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_PUT, tv);
                if (currentHandlerPtr->write_operation_.is_anchor == true) {
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_PUT_ANCHOR, tv);
                } else {
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_PUT_DELTA, tv);
                }
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
                currentHandlerPtr->jobDone_ = kDone;
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
