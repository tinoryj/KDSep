#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "hashBasedStore/hashStoreFileManager.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreFileOperator::HashStoreFileOperator(DeltaKVOptions* options, string workingDirStr, HashStoreFileManager* hashStoreFileManager)
{
    perFileFlushBufferSizeLimit_ = options->deltaStore_file_flush_buffer_size_limit_;
    perFileGCSizeLimit_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileSizeLimit_ = options->deltaStore_single_file_maximum_size;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        operationToWorkerMQ_ = new messageQueue<hashStoreOperationHandler*>;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    }
    if (options->keyToValueListCacheStr_ != nullptr) {
        keyToValueListCacheStr_ = options->keyToValueListCacheStr_;
    }
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    enableLsmTreeDeltaMeta_ = options->enable_lsm_tree_delta_meta;
//    notifyGCToManagerMQ_ = notifyGCToManagerMQ;
    hashStoreFileManager_ = hashStoreFileManager;
    workingDir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForForcedSingleFileGCThreshold_;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        syncStatistics_ = true;
        workerThreadNumber_ = options->deltaStore_op_worker_thread_number_limit_;
        workingThreadExitFlagVec_ = 0;
    }
    deltaKVMergeOperatorPtr_ = options->deltaKV_merge_operation_ptr;
}

HashStoreFileOperator::~HashStoreFileOperator()
{
    if (keyToValueListCacheStr_ != nullptr) {
        delete keyToValueListCacheStr_;
    }
    if (operationToWorkerMQ_ != nullptr) {
        delete operationToWorkerMQ_;
    }
}

bool HashStoreFileOperator::setJobDone()
{
    if (operationToWorkerMQ_ != nullptr) {
        operationToWorkerMQ_->done = true;
        operationNotifyCV_.notify_all();
        while (workingThreadExitFlagVec_ != workerThreadNumber_) {
            asm volatile("");
        }
    }
    return true;
}

// file operations
bool HashStoreFileOperator::putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* file_handler, mempoolHandler_t* mempoolHandler)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(file_handler);
    currentHandler->job_done = kNotDone;
    currentHandler->write_operation_.mempoolHandler_ptr_ = mempoolHandler;
    currentHandler->op_type = kPut;
    operationToWorkerMQ_->push(currentHandler);
    operationNotifyCV_.notify_all();
    if (currentHandler->job_done == kNotDone) {
        debug_trace("Wait for write job done%s\n", "");
        while (currentHandler->job_done == kNotDone) {
            asm volatile("");
        }
        debug_trace("Wait for write job done%s over\n", "");
    }
    if (currentHandler->job_done == kError) {
        delete currentHandler;
        return false;
    } else {
        delete currentHandler;
        return true;
    }
}

bool HashStoreFileOperator::putWriteOperationsVectorIntoJobQueue(hashStoreOperationHandler* currentOperationHandler)
{
    bool ret = operationToWorkerMQ_->push(currentOperationHandler);
    operationNotifyCV_.notify_all();
    return ret;
}

bool HashStoreFileOperator::waitOperationHandlerDone(hashStoreOperationHandler* currentOperationHandler) {
    while (currentOperationHandler->job_done == kNotDone) {
        asm volatile("");
    }
    if (currentOperationHandler->job_done == kDone) {
        debug_trace("Process operation %d for file ID = %lu, key number = %u\n", (int)currentOperationHandler->op_type, currentOperationHandler->file_handler->file_id, currentOperationHandler->batched_write_operation_.size);
        delete currentOperationHandler;
        return true;
    } else {
        debug_error("[ERROR] Process %d operation for file ID = %lu, key number = %u\n", (int)currentOperationHandler->op_type, currentOperationHandler->file_handler->file_id, currentOperationHandler->batched_write_operation_.size);
        delete currentOperationHandler;
        return false;
    }
}

bool HashStoreFileOperator::readContentFromFile(hashStoreFileMetaDataHandler* file_handler, char* contentBuffer)
{
    debug_trace("Read content from file ID = %lu\n", file_handler->file_id);
    fileOperationStatus_t readFileStatus;
    STAT_PROCESS(readFileStatus = file_handler->file_op_ptr->readFile(contentBuffer, file_handler->total_object_bytes), StatsType::DELTAKV_HASHSTORE_GET_IO);
    StatsRecorder::getInstance()->DeltaOPBytesRead(file_handler->total_on_disk_bytes, file_handler->total_object_bytes, syncStatistics_);
    if (readFileStatus.success_ == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault, could not read content from file ID = %lu\n", file_handler->file_id);
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

uint64_t HashStoreFileOperator::processReadContentToValueLists(char* contentBuffer, uint64_t contentSize, unordered_map<string_view, vector<string_view>>& resultMapInternal, const string_view& key)
{
    uint64_t currentProcessLocationIndex = 0;
    // skip file header
    currentProcessLocationIndex += sizeof(hashStoreFileHeader);
    uint64_t processedObjectNumber = 0;
    hashStoreRecordHeader* currentObjectRecordHeaderPtr;
    while (currentProcessLocationIndex != contentSize) {
        processedObjectNumber++;
        currentObjectRecordHeaderPtr = (hashStoreRecordHeader*)(contentBuffer + currentProcessLocationIndex);
        currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
        if (currentObjectRecordHeaderPtr->is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key 
        string_view currentKey(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeaderPtr->key_size_);
        if (key.size() != currentKey.size() || memcmp(key.data(), currentKey.data(), key.size()) != 0) {
            currentProcessLocationIndex += currentObjectRecordHeaderPtr->key_size_ + ((currentObjectRecordHeaderPtr->is_anchor_) ? 0 : currentObjectRecordHeaderPtr->value_size_);
            continue;
        }

        currentProcessLocationIndex += currentObjectRecordHeaderPtr->key_size_;
        if (currentObjectRecordHeaderPtr->is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            string_view currentValue(contentBuffer + currentProcessLocationIndex, currentObjectRecordHeaderPtr->value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<string_view> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            currentProcessLocationIndex += currentObjectRecordHeaderPtr->value_size_;
        }
    }
    return processedObjectNumber;
}

bool HashStoreFileOperator::writeContentToFile(hashStoreFileMetaDataHandler* file_handler, char* contentBuffer, uint64_t contentSize, uint64_t contentObjectNumber)
{
    debug_trace("Write content to file ID = %lu\n", file_handler->file_id);
    fileOperationStatus_t onDiskWriteSizePair;
    STAT_PROCESS(onDiskWriteSizePair = file_handler->file_op_ptr->writeFile(contentBuffer, contentSize), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
    StatsRecorder::getInstance()->DeltaOPBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
    if (onDiskWriteSizePair.success_ == false) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu, logical size = %lu, physical size = %lu\n", file_handler->file_id, file_handler->total_object_bytes, file_handler->total_on_disk_bytes);
        return false;
    } else {
        // update metadata
        file_handler->total_object_bytes += contentSize;
        file_handler->total_on_disk_bytes += onDiskWriteSizePair.physicalSize_;
        file_handler->total_object_cnt += contentObjectNumber;
        debug_trace("Write content to file ID = %lu done, write to disk size = %lu\n", file_handler->file_id, onDiskWriteSizePair.physicalSize_);
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    str_t currentKeyStr(currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keySize_);
    if (currentHandlerPtr->write_operation_.mempoolHandler_ptr_->isAnchorFlag_ == true) {
        if (currentHandlerPtr->file_handler->filter->MayExist(currentKeyStr)) {
            currentHandlerPtr->file_handler->filter->Erase(currentKeyStr);
        }
    } else {
        currentHandlerPtr->file_handler->filter->Insert(currentKeyStr);
    }
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->isAnchorFlag_;
    newRecordHeader.key_size_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keySize_;
    newRecordHeader.sequence_number_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->sequenceNumber_;
    newRecordHeader.value_size_ = currentHandlerPtr->write_operation_.mempoolHandler_ptr_->valueSize_;
    if (currentHandlerPtr->file_handler->file_op_ptr->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.prefix_bit = currentHandlerPtr->file_handler->prefix_bit;
        newFileHeader.previous_file_id_first_ = currentHandlerPtr->file_handler->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = currentHandlerPtr->file_handler->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = currentHandlerPtr->file_handler->file_id;
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
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", currentHandlerPtr->file_handler->file_id, currentHandlerPtr->file_handler->prefix_bit);
        string targetFilePathStr = workingDir_ + "/" + to_string(currentHandlerPtr->file_handler->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            currentHandlerPtr->file_handler->file_op_ptr->createThenOpenFile(targetFilePathStr);
        } else {
            currentHandlerPtr->file_handler->file_op_ptr->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler->file_id);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            auto mempoolHandler = currentHandlerPtr->write_operation_.mempoolHandler_ptr_;
            if (keyToValueListCacheStr_ != nullptr) {
                STAT_PROCESS(putKeyValueToAppendableCacheIfExist(
                            mempoolHandler->keyPtr_, mempoolHandler->keySize_, 
                            mempoolHandler->valuePtr_, mempoolHandler->valueSize_, newRecordHeader.is_anchor_),
                        StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE);
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
        bool writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", currentHandlerPtr->file_handler->file_id);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            auto mempoolHandler = currentHandlerPtr->write_operation_.mempoolHandler_ptr_;
            if (keyToValueListCacheStr_ != nullptr) {
                STAT_PROCESS(putKeyValueToAppendableCacheIfExist(
                            mempoolHandler->keyPtr_, mempoolHandler->keySize_, 
                            mempoolHandler->valuePtr_, mempoolHandler->valueSize_, newRecordHeader.is_anchor_),
                        StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE);
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::operationWorkerMultiPutFunction(hashStoreOperationHandler* currentHandlerPtr)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    if (currentHandlerPtr->file_handler->file_op_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.size; index++) {
            str_t currentKeyStr(currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].keyPtr_, 
                    currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].keySize_);
            if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].isAnchorFlag_ == false) {
                onlyAnchorFlag = false;
                currentHandlerPtr->file_handler->filter->Insert(currentKeyStr);
            } else {
                if (currentHandlerPtr->file_handler->filter->MayExist(currentKeyStr)) {
                    currentHandlerPtr->file_handler->filter->Erase(currentKeyStr);
                }
            }
        }
        if (onlyAnchorFlag == true) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", currentHandlerPtr->file_handler->file_id);
            return true;
        }
    } else {
        for (auto index = 0; index < currentHandlerPtr->batched_write_operation_.size; index++) {
            str_t currentKeyStr(currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].keyPtr_, 
                    currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].keySize_);
            if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[index].isAnchorFlag_ == false) {
                currentHandlerPtr->file_handler->filter->Insert(currentKeyStr);
            } else {
                if (currentHandlerPtr->file_handler->filter->MayExist(currentKeyStr)) {
                    currentHandlerPtr->file_handler->filter->Erase(currentKeyStr);
                }
            }
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_UPDATE_FILTER, tv);

    gettimeofday(&tv, 0);
    uint64_t targetWriteBufferSize = 0;
    hashStoreFileHeader newFileHeader;
    bool needFlushFileHeader = false;
    if (currentHandlerPtr->file_handler->file_op_ptr->isFileOpen() == false) {
        string targetFilePath = workingDir_ + "/" + to_string(currentHandlerPtr->file_handler->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePath) == false) {
            currentHandlerPtr->file_handler->file_op_ptr->createThenOpenFile(targetFilePath);
            newFileHeader.prefix_bit = currentHandlerPtr->file_handler->prefix_bit;
            newFileHeader.file_create_reason_ = currentHandlerPtr->file_handler->file_create_reason_;
            newFileHeader.file_id_ = currentHandlerPtr->file_handler->file_id;
            newFileHeader.previous_file_id_first_ = currentHandlerPtr->file_handler->previous_file_id_first_;
            newFileHeader.previous_file_id_second_ = currentHandlerPtr->file_handler->previous_file_id_second_;
            needFlushFileHeader = true;
            targetWriteBufferSize += sizeof(hashStoreFileHeader);
        } else {
            currentHandlerPtr->file_handler->file_op_ptr->openFile(targetFilePath);
        }
    }
    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.size; i++) {
        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + 
                currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].keySize_);
        if (currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].isAnchorFlag_ == true) {
            continue;
        } else {
            targetWriteBufferSize += currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].valueSize_;
        }
    }
    char writeContentBuffer[targetWriteBufferSize];
    uint64_t currentProcessedBufferIndex = 0;
    if (needFlushFileHeader == true) {
        memcpy(writeContentBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
        currentProcessedBufferIndex += sizeof(hashStoreFileHeader);
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_HEADER, tv);
    gettimeofday(&tv, 0);

    hashStoreRecordHeader* newRecordHeaderPtr;
    for (auto i = 0; i < currentHandlerPtr->batched_write_operation_.size; i++) {
        newRecordHeaderPtr = (hashStoreRecordHeader*)(writeContentBuffer + currentProcessedBufferIndex);
        newRecordHeaderPtr->is_anchor_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].isAnchorFlag_;
        newRecordHeaderPtr->is_gc_done_ = false;
        newRecordHeaderPtr->key_size_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].keySize_;
        newRecordHeaderPtr->value_size_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].valueSize_;
        newRecordHeaderPtr->sequence_number_ = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].sequenceNumber_;
//        memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeaderPtr, sizeof(hashStoreRecordHeader));
        currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
        memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].keyPtr_, newRecordHeaderPtr->key_size_);
        currentProcessedBufferIndex += newRecordHeaderPtr->key_size_;
        if (newRecordHeaderPtr->is_anchor_ == true) {
            continue;
        } else {
            memcpy(writeContentBuffer + currentProcessedBufferIndex, currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i].valuePtr_, newRecordHeaderPtr->value_size_);
            currentProcessedBufferIndex += newRecordHeaderPtr->value_size_;
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_CONTENT, tv);
    uint64_t targetObjectNumber = currentHandlerPtr->batched_write_operation_.size;
    // write content
    bool writeContentStatus;
    STAT_PROCESS(writeContentStatus = writeContentToFile(currentHandlerPtr->file_handler, writeContentBuffer, targetWriteBufferSize, targetObjectNumber), StatsType::DS_WRITE_FUNCTION);
    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID = %lu, content size = %lu, content bytes number = %lu\n", currentHandlerPtr->file_handler->file_id, targetObjectNumber, targetWriteBufferSize);
        exit(1);
        return false;
    } else {
        // insert to cache if need
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (keyToValueListCacheStr_ != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);

            for (uint32_t i = 0; i < currentHandlerPtr->batched_write_operation_.size; i++) {
                auto& it = currentHandlerPtr->batched_write_operation_.mempool_handler_vec_ptr_[i];
                putKeyValueToAppendableCacheIfExist(it.keyPtr_, it.keySize_, it.valuePtr_, it.valueSize_, it.isAnchorFlag_);
            }
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_INSERT_CACHE, tv);
        return true;
    }
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* file_handler)
{
    // insert into GC job queue if exceed the threshold
    if (file_handler->filter->ShouldRebuild() ||
            file_handler->DiskAndBufferSizeExceeds(perFileGCSizeLimit_)) {
        if (file_handler->gc_result_status_flag_ == kNoGC) {
            file_handler->no_gc_wait_operation_number_++;
            if (file_handler->no_gc_wait_operation_number_ >= operationNumberThresholdForForcedSingleFileGC_) {
                file_handler->file_ownership = -1;
                file_handler->gc_result_status_flag_ = kMayGC;
//                notifyGCToManagerMQ_->push(file_handler);
                hashStoreFileManager_->pushToGCQueue(file_handler);
                debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue, no gc wait count = %lu, threshold = %lu\n", file_handler->file_id, perFileGCSizeLimit_, file_handler->total_object_bytes, file_handler->total_on_disk_bytes + file_handler->file_op_ptr->getFileBufferedSize(), file_handler->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                file_handler->no_gc_wait_operation_number_ = 0;
                return true;
            } else {
                if (file_handler->no_gc_wait_operation_number_ % 10 == 1) {
                    debug_error("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d, no gc wait count = %lu, threshold = %lu\n", file_handler->file_id, perFileGCSizeLimit_, file_handler->total_object_bytes, file_handler->total_on_disk_bytes + file_handler->file_op_ptr->getFileBufferedSize(), file_handler->gc_result_status_flag_, file_handler->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                }
                debug_trace("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d, no gc wait count = %lu, threshold = %lu\n", file_handler->file_id, perFileGCSizeLimit_, file_handler->total_object_bytes, file_handler->total_on_disk_bytes + file_handler->file_op_ptr->getFileBufferedSize(), file_handler->gc_result_status_flag_, file_handler->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                return false;
            }
        } else if (file_handler->gc_result_status_flag_ == kNew || file_handler->gc_result_status_flag_ == kMayGC) {
            file_handler->file_ownership = -1;
//            notifyGCToManagerMQ_->push(file_handler);
            hashStoreFileManager_->pushToGCQueue(file_handler);
            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", file_handler->file_id, perFileGCSizeLimit_, file_handler->total_object_bytes, file_handler->total_on_disk_bytes + file_handler->file_op_ptr->getFileBufferedSize());
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", file_handler->file_id, perFileGCSizeLimit_, file_handler->total_object_bytes, file_handler->total_on_disk_bytes + file_handler->file_op_ptr->getFileBufferedSize(), file_handler->gc_result_status_flag_);
            return false;
        }
    } else {
        debug_trace("Current file ID = %lu should not GC, skip\n", file_handler->file_id);
        return false;
    }
}

// for put
inline void HashStoreFileOperator::putKeyValueToAppendableCacheIfExist(char* keyPtr, size_t keySize, char* valuePtr, size_t valueSize, bool isAnchor)
{
    str_t currentKeyStr(keyPtr, keySize);

    // insert into cache only if the key has been read
    if (isAnchor == true) {
        keyToValueListCacheStr_->cleanCacheIfExist(currentKeyStr);
    } else {
        str_t valueStr(valuePtr, valueSize);
        keyToValueListCacheStr_->appendToCacheIfExist(currentKeyStr, valueStr);
    }
}

// for get
inline void HashStoreFileOperator::putKeyValueVectorToAppendableCacheIfNotExist(char* keyPtr, size_t keySize, vector<str_t>& values) {
    str_t currentKeyStr(keyPtr, keySize);

    if (keyToValueListCacheStr_->existsInCache(currentKeyStr) == false) {
        str_t newKeyStr(new char[keySize], keySize);
        memcpy(newKeyStr.data_, keyPtr, keySize);

        vector<str_t>* valuesForInsertPtr = new vector<str_t>;
        for (auto& it : values) {
            str_t newValueStr(new char[it.size_], it.size_);
            memcpy(newValueStr.data_, it.data_, it.size_);
            valuesForInsertPtr->push_back(newValueStr);
        }

        keyToValueListCacheStr_->insertToCache(newKeyStr, valuesForInsertPtr);
    } 
}

bool HashStoreFileOperator::directlyWriteOperation(hashStoreFileMetaDataHandler* file_handler, mempoolHandler_t* mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> w_lock(file_handler->fileOperationMutex_);
    // update search set
    str_t currentKeyStr(mempoolHandler->keyPtr_, mempoolHandler->keySize_);
    if (mempoolHandler->isAnchorFlag_ == true) {
        if (file_handler->filter->MayExist(currentKeyStr)) {
            file_handler->filter->Erase(currentKeyStr);
        }
    } else {
        file_handler->filter->Insert(currentKeyStr);
    }
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = mempoolHandler->isAnchorFlag_;
    newRecordHeader.key_size_ = mempoolHandler->keySize_;
    newRecordHeader.sequence_number_ = mempoolHandler->sequenceNumber_;
    newRecordHeader.value_size_ = mempoolHandler->valueSize_;
    if (file_handler->file_op_ptr->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.prefix_bit = file_handler->prefix_bit;
        newFileHeader.previous_file_id_first_ = file_handler->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = file_handler->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id_ = file_handler->file_id;
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
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", file_handler->file_id, file_handler->prefix_bit);
        string targetFilePathStr = workingDir_ + "/" + to_string(file_handler->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            file_handler->file_op_ptr->createThenOpenFile(targetFilePathStr);
        } else {
            file_handler->file_op_ptr->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(file_handler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", file_handler->file_id);
            return false;
        } else {
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_handler);
                if (putIntoGCJobQueueStatus == false) {
                    file_handler->file_ownership = 0;
                }
            } else {
                file_handler->file_ownership = 0;
            }
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCacheStr_ != nullptr) {
                // do not implement
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
        bool writeContentStatus = writeContentToFile(file_handler, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", file_handler->file_id);
            return false;
        } else {
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_handler);
                if (putIntoGCJobQueueStatus == false) {
                    file_handler->file_ownership = 0;
                }
            } else {
                file_handler->file_ownership = 0;
            }
            // insert to cache if current key exist in cache && cache is enabled
            if (keyToValueListCacheStr_ != nullptr) {
                // do not implement
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> batchedWriteOperationsMap)
{
    vector<bool> jobeDoneStatus;
    for (auto& batchIt : batchedWriteOperationsMap) {
        std::scoped_lock<std::shared_mutex> w_lock(batchIt.first->fileOperationMutex_);
        // check file existence, create if not exist
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < batchIt.second.size(); index++) {
            str_t currentKeyStr(batchIt.second[index].keyPtr_, batchIt.second[index].keySize_);
            if (batchIt.second[index].isAnchorFlag_ == false) {
                onlyAnchorFlag = false;
                batchIt.first->filter->Insert(currentKeyStr);
            } else {
                if (batchIt.first->filter->MayExist(currentKeyStr)) {
                    batchIt.first->filter->Erase(currentKeyStr);
                }
            }
        }
        if (onlyAnchorFlag == true && batchIt.first->file_op_ptr->isFileOpen() == false) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", batchIt.first->file_id);
            batchIt.first->file_ownership = 0;
            jobeDoneStatus.push_back(true);
            continue;
        }
        uint64_t targetWriteBufferSize = 0;
        hashStoreFileHeader newFileHeader;
        bool needFlushFileHeader = false;
        if (batchIt.first->file_op_ptr->isFileOpen() == false) {
            string targetFilePath = workingDir_ + "/" + to_string(batchIt.first->file_id) + ".delta";
            if (std::filesystem::exists(targetFilePath) == false) {
                batchIt.first->file_op_ptr->createThenOpenFile(targetFilePath);
                newFileHeader.prefix_bit = batchIt.first->prefix_bit;
                newFileHeader.file_create_reason_ = batchIt.first->file_create_reason_;
                newFileHeader.file_id_ = batchIt.first->file_id;
                newFileHeader.previous_file_id_first_ = batchIt.first->previous_file_id_first_;
                newFileHeader.previous_file_id_second_ = batchIt.first->previous_file_id_second_;
                needFlushFileHeader = true;
                targetWriteBufferSize += sizeof(hashStoreFileHeader);
            } else {
                batchIt.first->file_op_ptr->openFile(targetFilePath);
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
            debug_error("[ERROR] Could not write content to file ID = %lu, object number = %lu, object total size = %lu\n", batchIt.first->file_id, targetObjectNumber, targetWriteBufferSize);
            batchIt.first->file_ownership = 0;
            jobeDoneStatus.push_back(false);
        } else {
            // insert to cache if need
            if (keyToValueListCacheStr_ != nullptr) {
                for (auto i = 0; i < batchIt.second.size(); i++) {
                    auto& it = batchIt.second[i];
                    putKeyValueToAppendableCacheIfExist(it.keyPtr_, it.keySize_, it.valuePtr_, it.valueSize_, it.isAnchorFlag_);
                }
            }
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(batchIt.first);
                if (putIntoGCJobQueueStatus == false) {
                    batchIt.first->file_ownership = 0;
                }
            } else {
                batchIt.first->file_ownership = 0;
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

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* file_handler, string key, vector<string>& valueVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(file_handler->fileOperationMutex_);
    // check if not flushed anchors exit, return directly.
    // try extract from cache first
    if (keyToValueListCacheStr_ != nullptr) {
        str_t currentKey(key.data(), key.size());
        vector<str_t>* tempResultVec = keyToValueListCacheStr_->getFromCache(currentKey);
        if (tempResultVec != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_trace("Read operations from cache, cache hit, key %s, hit vec size = %lu\n", key.c_str(), tempResultVec->size());
            valueVec.clear();
            for (auto& it : *tempResultVec) {
                valueVec.push_back(string(it.data_, it.size_));
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_CACHE, tv);
            file_handler->file_ownership = 0;
            return true;
        } else {
            // Not exist in cache, find the content in the file
            string_view key_view(key);
            char readContentBuffer[file_handler->total_object_bytes];
            bool readFromFileStatus = readContentFromFile(file_handler, readContentBuffer);
            if (readFromFileStatus == false) {
                debug_error("[ERROR] Could not read from file for key = %s\n", key.c_str());
                valueVec.clear();
                file_handler->file_ownership = 0;
                return false;
            } else {
                unordered_map<string_view, vector<string_view>> key_value_list;
                uint64_t processedObjectNumber = 0;
                STAT_PROCESS(processedObjectNumber = processReadContentToValueLists(readContentBuffer, file_handler->total_object_bytes, key_value_list, key_view), StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
                if (processedObjectNumber != file_handler->total_object_cnt) {
                    debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, file_handler->total_object_cnt);
                    valueVec.clear();
                    file_handler->file_ownership = 0;
                    return false;
                } else {
                    auto mapIt = key_value_list.find(key_view);
                    if (mapIt == key_value_list.end()) {
                        if (enableLsmTreeDeltaMeta_ == true) {
                            debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                            exit(1);
                        }
                        valueVec.clear();
                        file_handler->file_ownership = 0;
                        return true;
                    } else {
                        debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), key_value_list.at(key_view).size());
                        valueVec.reserve(mapIt->second.size());
                        for (auto& vecIt : mapIt->second) {
                            valueVec.push_back(string(vecIt.data(), vecIt.size()));
                        }
                        // Put the cache operation before job done, to avoid some synchronization issues
//                        for (auto& mapIt : key_value_list) {
                            struct timeval tv;
                            gettimeofday(&tv, 0);
//                            putKeyValueVectorToAppendableCacheIfNotExist(mapIt.first.data_, mapIt.first.size_, mapIt.second);
                            vector<str_t> tmpVec;
                            for (auto& it : mapIt->second) {
                                tmpVec.push_back(str_t(const_cast<char*>(it.data()), it.size()));
                            }
                            putKeyValueVectorToAppendableCacheIfNotExist(const_cast<char*>(mapIt->first.data()), mapIt->first.size(), tmpVec);
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);
//                        }
                        file_handler->file_ownership = 0;
                        return true;
                    }
                }
            }
        }
    } else {
        char readContentBuffer[file_handler->total_object_bytes];
        bool readFromFileStatus = readContentFromFile(file_handler, readContentBuffer);
        if (readFromFileStatus == false) {
            valueVec.clear();
            file_handler->file_ownership = 0;
            return false;
        } else {
            unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t, mapEqualKeForStr_t> key_value_list;
            uint64_t processedObjectNumber = 0;
            STAT_PROCESS(processedObjectNumber =
                    processReadContentToValueLists(readContentBuffer,
                        file_handler->total_object_bytes, key_value_list),
                    StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
            if (processedObjectNumber != file_handler->total_object_cnt) {
                debug_error("[ERROR] processed object number during read = %lu, not equal to object number in metadata = %lu\n", processedObjectNumber, file_handler->total_object_cnt);
                valueVec.clear();
                file_handler->file_ownership = 0;
                return false;
            } else {
                str_t currentKey((char*)key.c_str(), key.size());
                auto currentKeyIt = key_value_list.find(currentKey);
                if (currentKeyIt == key_value_list.end()) {
                    if (enableLsmTreeDeltaMeta_ == true) {
                        debug_error("[ERROR] Read bucket done, but could not found values for key = %s\n", key.c_str());
                        exit(1);
                    }
                    valueVec.clear();
                    file_handler->file_ownership = 0;
                    return true;
                } else {
                    debug_trace("Get current key related values success, key = %s, value number = %lu\n", key.c_str(), key_value_list.at(currentKey).size());
                    valueVec.reserve(currentKeyIt->second.size());
                    for (auto vecIt : currentKeyIt->second) {
                        valueVec.push_back(string(vecIt.data_, vecIt.size_));
                    }
                }
                file_handler->file_ownership = 0;
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
            while (operationToWorkerMQ_->isEmpty() && operationToWorkerMQ_->done == false) {
                operationNotifyCV_.wait(lk);
            }
        }
        if (operationToWorkerMQ_->done == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* currentHandlerPtr;
        if (operationToWorkerMQ_->pop(currentHandlerPtr)) {
            bool operationsStatus;
            std::scoped_lock<std::shared_mutex> w_lock(currentHandlerPtr->file_handler->fileOperationMutex_);
            switch (currentHandlerPtr->op_type) {
            case kGet:
                debug_error("receive operations, type = kGet, key = %s, target file ID = %lu\n", (*currentHandlerPtr->read_operation_.key_str_).c_str(), currentHandlerPtr->file_handler->file_id);
                operationsStatus = false;
                break;
            case kMultiPut:
                debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", currentHandlerPtr->file_handler->file_id, currentHandlerPtr->batched_write_operation_.size);
                STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(currentHandlerPtr), StatsType::OP_MULTIPUT);
                debug_trace("processed operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", currentHandlerPtr->file_handler->file_id, currentHandlerPtr->batched_write_operation_.size);
                break;
            case kPut:
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", currentHandlerPtr->write_operation_.mempoolHandler_ptr_->keyPtr_, currentHandlerPtr->file_handler->file_id);
                STAT_PROCESS(operationsStatus = operationWorkerPutFunction(currentHandlerPtr), StatsType::OP_PUT);
                break;
            default:
                debug_error("[ERROR] Unknown operation type = %d\n", currentHandlerPtr->op_type);
                break;
            }
            if (operationsStatus == false) {
                currentHandlerPtr->file_handler->file_ownership = 0;
                debug_trace("Process file ID = %lu error\n", currentHandlerPtr->file_handler->file_id);
                currentHandlerPtr->job_done = kError;
            } else {
                if (currentHandlerPtr->op_type != kGet && enableGCFlag_ == true) {
                    bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(currentHandlerPtr->file_handler);
                    if (putIntoGCJobQueueStatus == false) {
                        currentHandlerPtr->file_handler->file_ownership = 0;
                        debug_trace("Process file ID = %lu done, file should not GC, skip\n", currentHandlerPtr->file_handler->file_id);
                        currentHandlerPtr->job_done = kDone;
                    } else {
                        debug_trace("Process file ID = %lu done, file should GC\n", currentHandlerPtr->file_handler->file_id);
                        currentHandlerPtr->job_done = kDone;
                    }
                } else {
                    currentHandlerPtr->file_handler->file_ownership = 0;
                    debug_trace("Process file ID = %lu done, file should not GC, skip\n", currentHandlerPtr->file_handler->file_id);
                    currentHandlerPtr->job_done = kDone;
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
        if (operationToWorkerMQ_->done == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        if (operationToWorkerMQ_->isEmpty() == false) {
            operationNotifyCV_.notify_all();
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
