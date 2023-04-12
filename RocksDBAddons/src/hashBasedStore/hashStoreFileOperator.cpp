#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "hashBasedStore/hashStoreFileManager.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/bucketIndexBlock.hpp"
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
bool HashStoreFileOperator::putWriteOperationIntoJobQueue(hashStoreFileMetaDataHandler* file_hdl, mempoolHandler_t* mempoolHandler)
{
    hashStoreOperationHandler* currentHandler = new hashStoreOperationHandler(file_hdl);
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
        debug_trace("Process operation %d for file ID = %lu, key number = %u\n", (int)currentOperationHandler->op_type, currentOperationHandler->file_hdl->file_id, currentOperationHandler->batched_write_operation_.size);
        delete currentOperationHandler;
        return true;
    } else {
        debug_error("[ERROR] Process %d operation for file ID = %lu, key number = %u\n", (int)currentOperationHandler->op_type, currentOperationHandler->file_hdl->file_id, currentOperationHandler->batched_write_operation_.size);
        delete currentOperationHandler;
        return false;
    }
}

uint64_t HashStoreFileOperator::readWholeFile(
        hashStoreFileMetaDataHandler* file_hdl, char** read_buf)
{
    auto& file_size = file_hdl->total_object_bytes;
    debug_trace("Read content from file ID = %lu\n", file_hdl->file_id);
    *read_buf = new char[file_size];
    FileOpStatus read_s;
    STAT_PROCESS(
            read_s = file_hdl->file_op_ptr->readFile(*read_buf, file_size),
            StatsType::DELTAKV_HASHSTORE_GET_IO_ALL);
    StatsRecorder::getInstance()->DeltaOPBytesRead(file_hdl->total_on_disk_bytes,
            file_size, syncStatistics_);
    if (read_s.success_ == false) {
        debug_error("[ERROR] Read bucket error, internal file operation fault,"
                " could not read content from file ID = %lu\n",
                file_hdl->file_id);
        return 0;
    } else {
        return file_size;
    }
}

uint64_t HashStoreFileOperator::readSortedPart(
        hashStoreFileMetaDataHandler* file_hdl, const string_view& key_view, 
        char** read_buf, bool& key_exists) {
    if (file_hdl->index_block->GetSortedPartSize() == 0) {
        debug_error("No index block but read the sorted part %s\n", "");
        exit(1);
    }

    auto index_block_size = file_hdl->index_block->GetSize();
    pair<uint64_t, uint64_t> offlen = file_hdl->index_block->Search(key_view);
    if (offlen.first == 0) {
        key_exists = false;
        return 0;
    } else {
        key_exists = true;
    }
    FileOpStatus read_s;

    uint64_t read_file_offset = 
        sizeof(hashStoreFileHeader) + index_block_size + offlen.first; 
    *read_buf = new char[offlen.second];

    STAT_PROCESS(
            read_s = file_hdl->file_op_ptr->positionedReadFile(
                *read_buf, read_file_offset, offlen.second),
            StatsType::DELTAKV_HASHSTORE_GET_IO_SORTED);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_, syncStatistics_);
    return read_s.logicalSize_;
}

uint64_t HashStoreFileOperator::readUnsortedPart(
        hashStoreFileMetaDataHandler* file_hdl, char** read_buf)
{
    auto sorted_part_size = file_hdl->index_block->GetSortedPartSize(); 
    auto index_block_size = file_hdl->index_block->GetSize(); 
    auto file_size = file_hdl->total_object_bytes;
    auto unsorted_part_off = file_hdl->unsorted_part_offset;
        //sizeof(hashStoreFileHeader) + index_block_size +
        //sorted_part_size + sizeof(hashStoreRecordHeader);
    auto unsorted_part_size = file_size - unsorted_part_off;

    *read_buf = new char[unsorted_part_size];

    debug_trace("Read content from file ID = %lu\n", file_hdl->file_id);
    FileOpStatus read_s;
    STAT_PROCESS(read_s = file_hdl->file_op_ptr->positionedReadFile(
               *read_buf, unsorted_part_off, unsorted_part_size), 
            StatsType::DELTAKV_HASHSTORE_GET_IO_UNSORTED);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_,
            syncStatistics_);
    if (read_s.success_ == false) {
        debug_error("[ERROR] internal file operation fault,"
                " could not read content from file ID = %lu,"
                " file size %lu unsorted part off %lu,"
                " index_block %lu sorted %lu\n",
                file_hdl->file_id,
                file_size, unsorted_part_off, 
                index_block_size, sorted_part_size);
        return 0;
    } else {
        return read_s.logicalSize_;
    }

}

uint64_t HashStoreFileOperator::readBothParts(
        hashStoreFileMetaDataHandler* file_hdl, const string_view& key_view, 
        char** read_buf) {
    if (file_hdl->index_block->GetSortedPartSize() == 0) {
        debug_error("No index block but read the sorted part %s\n", "");
        exit(1);
    }

    auto index_block_size = file_hdl->index_block->GetSize();
    pair<uint64_t, uint64_t> offlen = file_hdl->index_block->Search(key_view);
    if (offlen.first == 0) {
        // not exist in the sorted part
        return readUnsortedPart(file_hdl, read_buf);
    }

    FileOpStatus read_s;

    uint64_t offset = 
        sizeof(hashStoreFileHeader) + index_block_size + offlen.first; 
    uint64_t len = file_hdl->total_object_bytes - offset; 

    *read_buf = new char[len];

    STAT_PROCESS(
            read_s = file_hdl->file_op_ptr->positionedReadFile(
                *read_buf, offset, len),
            StatsType::DELTAKV_HASHSTORE_GET_IO_BOTH);
    StatsRecorder::getInstance()->DeltaOPBytesRead(
            read_s.physicalSize_, read_s.logicalSize_, syncStatistics_);
    return read_s.logicalSize_;
}

bool HashStoreFileOperator::readAndProcessWholeFile(
        hashStoreFileMetaDataHandler* file_hdl, string& key,
        vector<string_view>& kd_list, char** buf)
{
    auto& file_size = file_hdl->total_object_bytes;
    bool readFromFileStatus = readWholeFile(file_hdl, buf);
    uint64_t skip_size = sizeof(hashStoreFileHeader) + 
       ((file_hdl->index_block) ? file_hdl->index_block->GetSize() : 0);

    kd_list.clear();
    if (readFromFileStatus == false) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        exit(1);
        return false;
    }

    string_view key_view(key);
    uint64_t process_delta_num = 0;

    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf + skip_size, file_size - skip_size, 
                kd_list, key_view),
            StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
    if (process_delta_num != file_hdl->total_object_cnt) {
        debug_error("[ERROR] processed object number during read = %lu, not"
                " equal to object number in metadata = %lu\n",
                process_delta_num, file_hdl->total_object_cnt);
        return false;
    }

    if (kd_list.empty() == false) {
        if (enableLsmTreeDeltaMeta_ == true) {
            debug_error("[ERROR] Read bucket done, but could not found values"
                    " for key = %s\n", key.c_str());
            exit(1);
        }
    }
    return true;
}

bool HashStoreFileOperator::readAndProcessSortedPart(
        hashStoreFileMetaDataHandler* file_hdl, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);
    bool key_exists;

    kd_list.clear();
    *buf = nullptr;
    uint64_t read_sz = readSortedPart(file_hdl, key_view, buf, key_exists);

    if (key_exists == false) {
        // The key does not exist in the index block, skip
        return true;
    }

    kd_list.clear();
    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::DELTAKV_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] processed object num = 0, read %lu fs %lu\n", 
                read_sz, file_hdl->total_object_bytes);
        exit(1);
    }

    if (kd_list.empty() == false) {
        if (enableLsmTreeDeltaMeta_ == true) {
            debug_error("[ERROR] Read bucket done, but could not found values"
                    " for key = %s\n", key.c_str());
            exit(1);
        }
    }
    return true;
}

bool HashStoreFileOperator::readAndProcessBothParts(
        hashStoreFileMetaDataHandler* file_hdl, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);

    kd_list.clear();
    *buf = nullptr;
    uint64_t read_sz = readBothParts(file_hdl, key_view, buf);

    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::DELTAKV_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] processed object num = 0, read %lu fs %lu\n", 
                read_sz, file_hdl->total_object_bytes);
        exit(1);
    }

    if (kd_list.empty() == false) {
        if (enableLsmTreeDeltaMeta_ == true) {
            debug_error("[ERROR] Read bucket done, but could not found values"
                    " for key = %s\n", key.c_str());
            exit(1);
        }
    }
    return true;
}

bool HashStoreFileOperator::readAndProcessUnsortedPart(
        hashStoreFileMetaDataHandler* file_hdl, string& key,
        vector<string_view>& kd_list, char** buf)
{
    string_view key_view(key);
    uint64_t read_sz = readUnsortedPart(file_hdl, buf);

    kd_list.clear();
    if (read_sz == 0) {
        debug_error("[ERROR] Could not read from file for key = %s\n",
                key.c_str());
        return false;
    }

    uint64_t process_delta_num = 0;
    STAT_PROCESS(process_delta_num = processReadContentToValueLists(
                *buf, read_sz, kd_list, key_view),
            StatsType::DELTAKV_HASHSTORE_GET_PROCESS);

    if (process_delta_num == 0) {
        debug_error("[ERROR] processed object num = 0, read %lu fs %lu\n", 
                read_sz, file_hdl->total_object_bytes);
        exit(1);
    }

    if (kd_list.empty() == false) {
        if (enableLsmTreeDeltaMeta_ == true) {
            debug_error("[ERROR] Read bucket done, but could not found values"
                    " for key = %s\n", key.c_str());
            exit(1);
        }
    }
    return true;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, 
        unordered_map<str_t, vector<str_t>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& resultMapInternal)
{
    // Do not consider header
    uint64_t read_buf_index = 0;
    uint64_t process_delta_num = 0;
    hashStoreRecordHeader currentObjectRecordHeader;
    while (read_buf_index != read_buf_size) {
        process_delta_num++;
        memcpy(&currentObjectRecordHeader, read_buf + read_buf_index, sizeof(hashStoreRecordHeader));
        read_buf_index += sizeof(hashStoreRecordHeader);
        if (currentObjectRecordHeader.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key str_t
        str_t currentKey(read_buf + read_buf_index, currentObjectRecordHeader.key_size_);
        read_buf_index += currentObjectRecordHeader.key_size_;
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            str_t currentValue(read_buf + read_buf_index, currentObjectRecordHeader.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<str_t> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            read_buf_index += currentObjectRecordHeader.value_size_;
        }
    }
    return process_delta_num;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, 
        unordered_map<string_view, vector<string_view>>& resultMapInternal,
        const string_view& key)
{
    uint64_t read_buf_index = 0;
    uint64_t process_delta_num = 0;
    hashStoreRecordHeader* currentObjectRecordHeaderPtr;
    while (read_buf_index != read_buf_size) {
        process_delta_num++;
        currentObjectRecordHeaderPtr = (hashStoreRecordHeader*)(read_buf + read_buf_index);
        read_buf_index += sizeof(hashStoreRecordHeader);
        if (currentObjectRecordHeaderPtr->is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key 
        string_view currentKey(read_buf + read_buf_index, currentObjectRecordHeaderPtr->key_size_);
        if (key.size() != currentKey.size() || memcmp(key.data(), currentKey.data(), key.size()) != 0) {
            read_buf_index += currentObjectRecordHeaderPtr->key_size_ + ((currentObjectRecordHeaderPtr->is_anchor_) ? 0 : currentObjectRecordHeaderPtr->value_size_);
            continue;
        }

        read_buf_index += currentObjectRecordHeaderPtr->key_size_;
        if (currentObjectRecordHeaderPtr->is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            string_view currentValue(read_buf + read_buf_index, currentObjectRecordHeaderPtr->value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<string_view> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            read_buf_index += currentObjectRecordHeaderPtr->value_size_;
        }
    }
    return process_delta_num;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, vector<string_view>& kd_list,
        const string_view& key)
{
    uint64_t read_buf_index = 0;
    uint64_t processed_delta_num = 0;
    hashStoreRecordHeader* record_header_ptr;
    while (read_buf_index < read_buf_size) {
        processed_delta_num++;
        record_header_ptr = (hashStoreRecordHeader*)(read_buf + read_buf_index);
        read_buf_index += sizeof(hashStoreRecordHeader);
        if (record_header_ptr->is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }

        // get key 
        string_view currentKey(read_buf + read_buf_index, record_header_ptr->key_size_);
        if (key != currentKey) {
            read_buf_index += record_header_ptr->key_size_ +
                ((record_header_ptr->is_anchor_) ? 0 :
                 record_header_ptr->value_size_);
            continue;
        }

        read_buf_index += record_header_ptr->key_size_;
        if (record_header_ptr->is_anchor_ == true) {
            kd_list.clear();
        } else {
            string_view currentValue(read_buf + read_buf_index, record_header_ptr->value_size_);
            kd_list.push_back(currentValue);
            read_buf_index += record_header_ptr->value_size_;
        }
    }
    if (read_buf_index > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu\n", 
                read_buf_index, read_buf_size);
        return 0;
    }
    return processed_delta_num;
}

bool HashStoreFileOperator::writeContentToFile(
        hashStoreFileMetaDataHandler* file_hdl, 
        char* write_buf, uint64_t write_buf_size, uint64_t contentObjectNumber)
{
    debug_trace("Write content to file ID = %lu\n", file_hdl->file_id);
    FileOpStatus onDiskWriteSizePair;
    STAT_PROCESS(onDiskWriteSizePair = file_hdl->file_op_ptr->writeFile(write_buf, write_buf_size), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
    StatsRecorder::getInstance()->DeltaOPBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
    if (onDiskWriteSizePair.success_ == false) {
        debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu, logical size = %lu, physical size = %lu\n", file_hdl->file_id, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes);
        return false;
    } else {
        // update metadata
        file_hdl->total_object_bytes += write_buf_size;
        file_hdl->total_on_disk_bytes += onDiskWriteSizePair.physicalSize_;
        file_hdl->total_object_cnt += contentObjectNumber;
        debug_trace("Write content to file ID = %lu done, write to disk size = %lu\n", file_hdl->file_id, onDiskWriteSizePair.physicalSize_);
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerPutFunction(hashStoreOperationHandler* op_hdl)
{
    str_t currentKeyStr(op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, op_hdl->write_operation_.mempoolHandler_ptr_->keySize_);
    if (op_hdl->write_operation_.mempoolHandler_ptr_->isAnchorFlag_ == true) {
        // Only when the key is not in the sorted filter, clean the filter.
        // Otherwise, it will read the deltas in the sorted part but ignore the
        // anchors in the unsorted part 
        if (op_hdl->file_hdl->sorted_filter->MayExist(currentKeyStr) == true) {
            op_hdl->file_hdl->filter->Insert(currentKeyStr);
        } else {
            if (op_hdl->file_hdl->filter->MayExist(currentKeyStr) == true) {
                op_hdl->file_hdl->filter->Erase(currentKeyStr);
            }
        }
    } else {
        op_hdl->file_hdl->filter->Insert(currentKeyStr);
    }
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = op_hdl->write_operation_.mempoolHandler_ptr_->isAnchorFlag_;
    newRecordHeader.key_size_ = op_hdl->write_operation_.mempoolHandler_ptr_->keySize_;
    newRecordHeader.sequence_number_ = op_hdl->write_operation_.mempoolHandler_ptr_->sequenceNumber_;
    newRecordHeader.value_size_ = op_hdl->write_operation_.mempoolHandler_ptr_->valueSize_;
    if (op_hdl->file_hdl->file_op_ptr->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.prefix_bit = op_hdl->file_hdl->prefix_bit;
        newFileHeader.previous_file_id_first_ = op_hdl->file_hdl->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = op_hdl->file_hdl->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id = op_hdl->file_hdl->file_id;
        // place file header and record header in write buffer
        uint64_t writeBufferSize = sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_ + newRecordHeader.value_size_;
        uint64_t targetWriteSize = 0;
        char writeBuffer[writeBufferSize];
        if (newRecordHeader.is_anchor_ == false) {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, op_hdl->write_operation_.mempoolHandler_ptr_->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // open target file
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", op_hdl->file_hdl->file_id, op_hdl->file_hdl->prefix_bit);
        string targetFilePathStr = workingDir_ + "/" + to_string(op_hdl->file_hdl->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            op_hdl->file_hdl->file_op_ptr->createThenOpenFile(targetFilePathStr);
        } else {
            op_hdl->file_hdl->file_op_ptr->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(op_hdl->file_hdl, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", op_hdl->file_hdl->file_id);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            auto mempoolHandler = op_hdl->write_operation_.mempoolHandler_ptr_;
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
            memcpy(writeBuffer + sizeof(newRecordHeader), op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, op_hdl->write_operation_.mempoolHandler_ptr_->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // write contents of file
        bool writeContentStatus = writeContentToFile(op_hdl->file_hdl, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", op_hdl->file_hdl->file_id);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            auto mempoolHandler = op_hdl->write_operation_.mempoolHandler_ptr_;
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

bool HashStoreFileOperator::operationWorkerMultiPutFunction(hashStoreOperationHandler* op_hdl)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    if (op_hdl->file_hdl->file_op_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < op_hdl->batched_write_operation_.size; index++) {
            str_t currentKeyStr(op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].keyPtr_, 
                    op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].keySize_);
            if (op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].isAnchorFlag_ == true) {
                if (op_hdl->file_hdl->sorted_filter->MayExist(currentKeyStr) == true) {
                    op_hdl->file_hdl->filter->Insert(currentKeyStr);
                    op_hdl->file_hdl->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (op_hdl->file_hdl->filter->MayExist(currentKeyStr) == true) {
                        op_hdl->file_hdl->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                onlyAnchorFlag = false;
                op_hdl->file_hdl->filter->Insert(currentKeyStr);
            }
        }
        if (onlyAnchorFlag == true) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", op_hdl->file_hdl->file_id);
            return true;
        }
    } else {
        for (auto index = 0; index < op_hdl->batched_write_operation_.size; index++) {
            str_t currentKeyStr(op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].keyPtr_, 
                    op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].keySize_);
            if (op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[index].isAnchorFlag_ == true) {
                if (op_hdl->file_hdl->sorted_filter->MayExist(currentKeyStr) == true) {
                    op_hdl->file_hdl->filter->Insert(currentKeyStr);
                    op_hdl->file_hdl->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (op_hdl->file_hdl->filter->MayExist(currentKeyStr) == true) {
                        op_hdl->file_hdl->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                op_hdl->file_hdl->filter->Insert(currentKeyStr);
            }
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_UPDATE_FILTER, tv);

    gettimeofday(&tv, 0);
    uint64_t targetWriteBufferSize = 0;
    hashStoreFileHeader newFileHeader;
    bool needFlushFileHeader = false;
    if (op_hdl->file_hdl->file_op_ptr->isFileOpen() == false) {
        string targetFilePath = workingDir_ + "/" + to_string(op_hdl->file_hdl->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePath) == false) {
            op_hdl->file_hdl->file_op_ptr->createThenOpenFile(targetFilePath);
            newFileHeader.prefix_bit = op_hdl->file_hdl->prefix_bit;
            newFileHeader.file_create_reason_ = op_hdl->file_hdl->file_create_reason_;
            newFileHeader.file_id = op_hdl->file_hdl->file_id;
            newFileHeader.previous_file_id_first_ = op_hdl->file_hdl->previous_file_id_first_;
            newFileHeader.previous_file_id_second_ = op_hdl->file_hdl->previous_file_id_second_;
            needFlushFileHeader = true;
            targetWriteBufferSize += sizeof(hashStoreFileHeader);
        } else {
            op_hdl->file_hdl->file_op_ptr->openFile(targetFilePath);
        }
    }
    for (auto i = 0; i < op_hdl->batched_write_operation_.size; i++) {
        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + 
                op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].keySize_);
        if (op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].isAnchorFlag_ == true) {
            continue;
        } else {
            targetWriteBufferSize += op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].valueSize_;
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
    for (auto i = 0; i < op_hdl->batched_write_operation_.size; i++) {
        newRecordHeaderPtr = (hashStoreRecordHeader*)(writeContentBuffer + currentProcessedBufferIndex);
        newRecordHeaderPtr->is_anchor_ = op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].isAnchorFlag_;
        newRecordHeaderPtr->is_gc_done_ = false;
        newRecordHeaderPtr->key_size_ = op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].keySize_;
        newRecordHeaderPtr->value_size_ = op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].valueSize_;
        newRecordHeaderPtr->sequence_number_ = op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].sequenceNumber_;
//        memcpy(writeContentBuffer + currentProcessedBufferIndex, &newRecordHeaderPtr, sizeof(hashStoreRecordHeader));
        currentProcessedBufferIndex += sizeof(hashStoreRecordHeader);
        memcpy(writeContentBuffer + currentProcessedBufferIndex, op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].keyPtr_, newRecordHeaderPtr->key_size_);
        currentProcessedBufferIndex += newRecordHeaderPtr->key_size_;
        if (newRecordHeaderPtr->is_anchor_ == true) {
            continue;
        } else {
            memcpy(writeContentBuffer + currentProcessedBufferIndex, op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i].valuePtr_, newRecordHeaderPtr->value_size_);
            currentProcessedBufferIndex += newRecordHeaderPtr->value_size_;
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_CONTENT, tv);
    uint64_t targetObjectNumber = op_hdl->batched_write_operation_.size;
    // write content
    bool writeContentStatus;
    STAT_PROCESS(writeContentStatus = writeContentToFile(op_hdl->file_hdl, writeContentBuffer, targetWriteBufferSize, targetObjectNumber), StatsType::DS_WRITE_FUNCTION);
    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID = %lu, content size = %lu, content bytes number = %lu\n", op_hdl->file_hdl->file_id, targetObjectNumber, targetWriteBufferSize);
        exit(1);
        return false;
    } else {
        // insert to cache if need
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (keyToValueListCacheStr_ != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);

            for (uint32_t i = 0; i < op_hdl->batched_write_operation_.size; i++) {
                auto& it = op_hdl->batched_write_operation_.mempool_handler_vec_ptr_[i];
                putKeyValueToAppendableCacheIfExist(it.keyPtr_, it.keySize_, it.valuePtr_, it.valueSize_, it.isAnchorFlag_);
            }
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_INSERT_CACHE, tv);
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerFlush(hashStoreOperationHandler* op_hdl)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    debug_error("flush size %p\n", op_hdl);

    if (op_hdl->file_hdl->file_op_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        return true;
    }

    // write content
    FileOpStatus status = op_hdl->file_hdl->file_op_ptr->flushFile();
    if (status.success_ == false) {
        debug_error("[ERROR] Could not flush to file, target file ID = %lu\n",
                op_hdl->file_hdl->file_id);
        exit(1);
    } 
    return true;
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* file_hdl)
{
    // insert into GC job queue if exceed the threshold
    if (file_hdl->filter->ShouldRebuild() ||
            file_hdl->DiskAndBufferSizeExceeds(perFileGCSizeLimit_) || 
            file_hdl->total_object_bytes - file_hdl->unsorted_part_offset >= 1024 * 1024) {
        if (file_hdl->gc_status == kNoGC) {
            file_hdl->no_gc_wait_operation_number_++;
            if (file_hdl->no_gc_wait_operation_number_ >= operationNumberThresholdForForcedSingleFileGC_) {
                file_hdl->file_ownership = -1;
                file_hdl->gc_status = kMayGC;
//                notifyGCToManagerMQ_->push(file_hdl);
                hashStoreFileManager_->pushToGCQueue(file_hdl);
                debug_info("Current file ID = %lu exceed GC threshold = %lu with kNoGC flag, current size = %lu, total disk size = %lu, put into GC job queue, no gc wait count = %lu, threshold = %lu\n", file_hdl->file_id, perFileGCSizeLimit_, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes + file_hdl->file_op_ptr->getFileBufferedSize(), file_hdl->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                file_hdl->no_gc_wait_operation_number_ = 0;
                return true;
            } else {
                if (file_hdl->no_gc_wait_operation_number_ % 25 == 1) {
                    debug_error("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d, no gc wait count = %lu, threshold = %lu\n", file_hdl->file_id, perFileGCSizeLimit_, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes + file_hdl->file_op_ptr->getFileBufferedSize(), file_hdl->gc_status, file_hdl->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                }
                debug_trace("Current file ID = %lu exceed file size threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d, no gc wait count = %lu, threshold = %lu\n", file_hdl->file_id, perFileGCSizeLimit_, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes + file_hdl->file_op_ptr->getFileBufferedSize(), file_hdl->gc_status, file_hdl->no_gc_wait_operation_number_, operationNumberThresholdForForcedSingleFileGC_);
                return false;
            }
        } else if (file_hdl->gc_status == kNew || file_hdl->gc_status == kMayGC) {
            file_hdl->file_ownership = -1;
//            notifyGCToManagerMQ_->push(file_hdl);
            hashStoreFileManager_->pushToGCQueue(file_hdl);
            debug_info("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, put into GC job queue\n", file_hdl->file_id, perFileGCSizeLimit_, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes + file_hdl->file_op_ptr->getFileBufferedSize());
            return true;
        } else {
            debug_trace("Current file ID = %lu exceed GC threshold = %lu, current size = %lu, total disk size = %lu, not put into GC job queue, since file type = %d\n", file_hdl->file_id, perFileGCSizeLimit_, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes + file_hdl->file_op_ptr->getFileBufferedSize(), file_hdl->gc_status);
            return false;
        }
    } else {
        debug_trace("Current file ID = %lu should not GC, skip\n", file_hdl->file_id);
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

bool HashStoreFileOperator::directlyWriteOperation(hashStoreFileMetaDataHandler* file_hdl, mempoolHandler_t* mempoolHandler)
{
    std::scoped_lock<std::shared_mutex> w_lock(file_hdl->fileOperationMutex_);
    // update search set
    str_t currentKeyStr(mempoolHandler->keyPtr_, mempoolHandler->keySize_);
    if (mempoolHandler->isAnchorFlag_ == true) {
        if (file_hdl->filter->MayExist(currentKeyStr)) {
            file_hdl->filter->Erase(currentKeyStr);
        }
    } else {
        file_hdl->filter->Insert(currentKeyStr);
    }
    // construct record header
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = mempoolHandler->isAnchorFlag_;
    newRecordHeader.key_size_ = mempoolHandler->keySize_;
    newRecordHeader.sequence_number_ = mempoolHandler->sequenceNumber_;
    newRecordHeader.value_size_ = mempoolHandler->valueSize_;
    if (file_hdl->file_op_ptr->isFileOpen() == false) {
        // since file not created, shoud not flush anchors
        // construct file header
        hashStoreFileHeader newFileHeader;
        newFileHeader.prefix_bit = file_hdl->prefix_bit;
        newFileHeader.previous_file_id_first_ = file_hdl->previous_file_id_first_;
        newFileHeader.previous_file_id_second_ = file_hdl->previous_file_id_second_;
        newFileHeader.file_create_reason_ = kNewFile;
        newFileHeader.file_id = file_hdl->file_id;
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
        debug_info("First open newly created file ID = %lu, target prefix bit number = %lu\n", file_hdl->file_id, file_hdl->prefix_bit);
        string targetFilePathStr = workingDir_ + "/" + to_string(file_hdl->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePathStr) != true) {
            file_hdl->file_op_ptr->createThenOpenFile(targetFilePathStr);
        } else {
            file_hdl->file_op_ptr->openFile(targetFilePathStr);
        }
        // write contents of file
        bool writeContentStatus = writeContentToFile(file_hdl, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", file_hdl->file_id);
            return false;
        } else {
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_hdl);
                if (putIntoGCJobQueueStatus == false) {
                    file_hdl->file_ownership = 0;
                }
            } else {
                file_hdl->file_ownership = 0;
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
        bool writeContentStatus = writeContentToFile(file_hdl, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", file_hdl->file_id);
            return false;
        } else {
            if (enableGCFlag_ == true) {
                bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_hdl);
                if (putIntoGCJobQueueStatus == false) {
                    file_hdl->file_ownership = 0;
                }
            } else {
                file_hdl->file_ownership = 0;
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
                newFileHeader.file_id = batchIt.first->file_id;
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

bool HashStoreFileOperator::directlyReadOperation(hashStoreFileMetaDataHandler* file_hdl, string key, vector<string>& valueVec)
{
    std::scoped_lock<std::shared_mutex> r_lock(file_hdl->fileOperationMutex_);
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
            file_hdl->file_ownership = 0;
            return true;
        } else if (enable_index_block_) { 
            // Do not enable index block, directly write 
            // Not exist in cache, find the content in the file
            str_t key_str_t(key.data(), key.size());

            vector<string_view> kd_list;

            char* buf = nullptr;
            bool success;

            if (file_hdl->index_block != nullptr) {
                if (file_hdl->sorted_filter->MayExist(key_str_t) == true) {
                    if (file_hdl->filter->MayExist(key_str_t) == false) {
                        // only in sorted part
                        success = readAndProcessSortedPart(file_hdl, key,
                                kd_list, &buf);
                    } else {
                        // both parts 
                        success = readAndProcessBothParts(file_hdl, key,
                                kd_list, &buf);
                    }
                } else {
                    if (file_hdl->filter->MayExist(key_str_t) == false) {
                        // does not exist, or not stored in the memory 
                        if (enableLsmTreeDeltaMeta_ == true) {
                            debug_error("[ERROR] Read bucket done, but could not"
                                    " found values for key = %s\n", key.c_str());
                            exit(1);
                        }
                        valueVec.clear();
                        file_hdl->file_ownership = 0;
                        return true;
                    } else {
                        // only in unsorted part
                        success = readAndProcessUnsortedPart(file_hdl, key,
                                kd_list, &buf);
                    }
                }
            } else {
                success = readAndProcessWholeFile(file_hdl, key, kd_list, &buf);
            }

            valueVec.clear();
            if (success == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                exit(1);
            } else if (buf == nullptr) {
                // Key miss because (partially) sorted part does not have key
                file_hdl->file_ownership = 0;
                return true;
            } 

            if (kd_list.size() > 0) {
                valueVec.reserve(kd_list.size());
                for (auto vecIt : kd_list) {
                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
                }
            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()),
                            it.size()));
            }
            putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
                    key.size(), deltas);
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            file_hdl->file_ownership = 0;
            return true;
        } else {
            // Do not enable index block, directly read the whole file 
            // Not exist in cache, find the content in the file
            vector<string_view> kd_list;
            char* buf;
            bool s = readAndProcessWholeFile(file_hdl, key, kd_list, &buf);
            valueVec.clear();

            if (s == false) {
                debug_error("[ERROR] read and process failed %s\n", key.c_str());
                file_hdl->file_ownership = 0;
                return false;
            }

            if (kd_list.size() > 0) {
                valueVec.reserve(kd_list.size());
                for (auto vecIt : kd_list) {
                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
                }
            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()), it.size()));
            }
            putKeyValueVectorToAppendableCacheIfNotExist(key.data(), key.size(), deltas);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            file_hdl->file_ownership = 0;
            return true;
        }
    } else {
        // no cache, directly read the whole file
        vector<string_view> kd_list;
        char* buf;
        bool s = readAndProcessWholeFile(file_hdl, key, kd_list, &buf);
        valueVec.clear();

        if (s == false) {
            debug_error("[ERROR] read and process failed %s\n", key.c_str());
            file_hdl->file_ownership = 0;
            return false;
        }

        if (kd_list.empty() == false) {
            valueVec.reserve(kd_list.size());
            for (auto vecIt : kd_list) {
                valueVec.push_back(string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;
        file_hdl->file_ownership = 0;
        return true;
    }
}

void HashStoreFileOperator::operationWorker(int threadID)
{
    struct timeval tvs, tve;
    gettimeofday(&tvs, 0);
    struct timeval empty_time;
    bool empty_started = false;
    while (true) {
        gettimeofday(&tve, 0);

        {
            std::unique_lock<std::mutex> lk(operationNotifyMtx_);
            if (operationToWorkerMQ_->isEmpty() && 
                    operationToWorkerMQ_->done == false && 
                    empty_started == true && 
                    tve.tv_sec - empty_time.tv_sec > 3) {
                operationNotifyCV_.wait(lk);
            }
        }
        if (operationToWorkerMQ_->done == true && operationToWorkerMQ_->isEmpty() == true) {
            break;
        }
        hashStoreOperationHandler* op_hdl;
        if (operationToWorkerMQ_->pop(op_hdl)) {
            empty_started = false;
            bool flag = false;
            bool operationsStatus;
            std::scoped_lock<std::shared_mutex> w_lock(op_hdl->file_hdl->fileOperationMutex_);

            auto file_hdl = op_hdl->file_hdl;

            switch (op_hdl->op_type) {
            case kGet:
                debug_error("receive operations, type = kGet, key = %s, target file ID = %lu\n", (*op_hdl->read_operation_.key_str_).c_str(), op_hdl->file_hdl->file_id);
                operationsStatus = false;
                break;
            case kMultiPut:
                debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", op_hdl->file_hdl->file_id, op_hdl->batched_write_operation_.size);
                STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(op_hdl), StatsType::OP_MULTIPUT);
                debug_trace("processed operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", op_hdl->file_hdl->file_id, op_hdl->batched_write_operation_.size);
                break;
            case kPut:
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", op_hdl->write_operation_.mempoolHandler_ptr_->keyPtr_, op_hdl->file_hdl->file_id);
                STAT_PROCESS(operationsStatus = operationWorkerPutFunction(op_hdl), StatsType::OP_PUT);
                break;
            case kFlush:
                STAT_PROCESS(operationsStatus = operationWorkerFlush(op_hdl), StatsType::OP_FLUSH);
                break;
            default:
                debug_error("[ERROR] Unknown operation type = %d\n", op_hdl->op_type);
                break;
            }
            if (operationsStatus == false) {
                op_hdl->file_hdl->file_ownership = 0;
                debug_trace("Process file ID = %lu error\n", op_hdl->file_hdl->file_id);
                op_hdl->job_done = kError;
            } else {
                if (op_hdl->op_type != kGet && enableGCFlag_ == true) {
                    bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(op_hdl->file_hdl);
                    if (putIntoGCJobQueueStatus == false) {
                        op_hdl->file_hdl->file_ownership = 0;
                        debug_trace("Process file ID = %lu done, file should not GC, skip\n", op_hdl->file_hdl->file_id);
                        op_hdl->job_done = kDone;
                    } else {
                        debug_trace("Process file ID = %lu done, file should GC\n", op_hdl->file_hdl->file_id);
                        op_hdl->job_done = kDone;
                    }
                } else {
                    op_hdl->file_hdl->file_ownership = 0;
                    debug_trace("Process file ID = %lu done, file should not GC, skip\n", op_hdl->file_hdl->file_id);
                    op_hdl->job_done = kDone;
                }
            }

            if (flag) {
                debug_error("file %d finished %p\n", 
                        (int)file_hdl->file_id, file_hdl);
            }
        } else {
            if (empty_started == false) {
                empty_started = true;
                gettimeofday(&empty_time, 0);
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
            usleep(10000);
        }
    }
    return;
}

} // namespace DELTAKV_NAMESPACE
