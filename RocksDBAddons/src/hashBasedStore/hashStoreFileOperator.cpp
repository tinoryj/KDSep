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
        workerThreads_ = new boost::asio::thread_pool(options->deltaStore_op_worker_thread_number_limit_);
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    }
    if (options->kd_cache != nullptr) {
        kd_cache_ = options->kd_cache;
    }
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    enableLsmTreeDeltaMeta_ = options->enable_lsm_tree_delta_meta;
    enable_index_block_ = options->enable_index_block;
    hashStoreFileManager_ = hashStoreFileManager;
    workingDir_ = workingDirStr;
    operationNumberThresholdForForcedSingleFileGC_ = options->deltaStore_operationNumberForForcedSingleFileGCThreshold_;
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        syncStatistics_ = true;
        workerThreadNumber_ = options->deltaStore_op_worker_thread_number_limit_;
        workingThreadExitFlagVec_ = 0;
    }
    deltaKVMergeOperatorPtr_ = options->deltaKV_merge_operation_ptr;
    unsorted_part_size_threshold_ = options->unsorted_part_size_threshold;
    write_stall_ = options->write_stall;
    fprintf(stdout, "read use partial merged delta in the KD cache!\n");
    fprintf(stdout, "put use partial merged delta in the KD cache!\n");
//    fprintf(stdout, "use all deltas in the KD cache!\n");
}

HashStoreFileOperator::~HashStoreFileOperator()
{
    if (kd_cache_ != nullptr) {
        delete kd_cache_;
    }
    if (operationToWorkerMQ_ != nullptr) {
        delete operationToWorkerMQ_;
    }
    if (workerThreads_ != nullptr) {
        delete workerThreads_;
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

bool HashStoreFileOperator::putIntoJobQueue(hashStoreOperationHandler* op_hdl)
{
    bool ret = operationToWorkerMQ_->push(op_hdl);
    operationNotifyCV_.notify_all();
    return ret;
}

bool HashStoreFileOperator::startJob(hashStoreOperationHandler* op_hdl)
{
    boost::asio::post(*workerThreads_, 
            boost::bind(
                &HashStoreFileOperator::operationBoostThreadWorker,
                this,
                op_hdl));
    return true;
}

bool HashStoreFileOperator::waitOperationHandlerDone(hashStoreOperationHandler* op_hdl, bool need_delete) {
    while (op_hdl->job_done == kNotDone) {
        asm volatile("");
    }
    if (op_hdl->job_done == kDone) {
        debug_trace("Process operation %d for file ID = %lu, key number = %u\n", (int)op_hdl->op_type, op_hdl->file_hdl->file_id, op_hdl->multiput_op.size);
        if (need_delete) {
            delete op_hdl;
        }
        return true;
    } else {
        debug_error("[ERROR] Process %d operation for file ID = %lu, key number = %u\n", (int)op_hdl->op_type, op_hdl->file_hdl->file_id, op_hdl->multiput_op.size);
        if (need_delete) {
            delete op_hdl;
        }
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

    pair<uint64_t, uint64_t> offlen = file_hdl->index_block->Search(key_view);
    if (offlen.second == 0) {
        key_exists = false;
        return 0;
    } else {
        key_exists = true;
    }
    FileOpStatus read_s;

    uint64_t read_file_offset = sizeof(hashStoreFileHeader) + offlen.first; 
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
    auto file_size = file_hdl->total_object_bytes;
    auto unsorted_part_off = file_hdl->unsorted_part_offset;
        // Fixed to:
        //sizeof(hashStoreFileHeader) + 
        //sorted_part_size + header_sz;
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
                " sorted %lu\n",
                file_hdl->file_id,
                file_size, unsorted_part_off, 
                sorted_part_size);
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

    pair<uint64_t, uint64_t> offlen = file_hdl->index_block->Search(key_view);

//    if (offlen.first == 0) 
    // Wrong. Fix later
    if (offlen.second == 0) {
        // not exist in the sorted part
        return readUnsortedPart(file_hdl, read_buf);
    }

    FileOpStatus read_s;

    uint64_t offset = sizeof(hashStoreFileHeader) + offlen.first; 
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
    uint64_t skip_size = sizeof(hashStoreFileHeader);

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

bool HashStoreFileOperator::readAndProcessWholeFileKeyList(
        hashStoreFileMetaDataHandler* file_hdl, vector<string*>* keys,
        vector<vector<string_view>>& kd_lists, char** buf)
{
    auto& file_size = file_hdl->total_object_bytes;
    bool readFromFileStatus = readWholeFile(file_hdl, buf);
    uint64_t skip_size = sizeof(hashStoreFileHeader);

    kd_lists.clear();
    if (readFromFileStatus == false) {
        debug_error("[ERROR] Could not read from file for key list %lu\n",
                keys->size());
        exit(1);
        return false;
    }

    vector<string_view> key_views;
    key_views.resize(keys->size());
    for (int i = 0; i < keys->size(); i++) {
	key_views[i] = string_view((*(*keys)[i]));
    }

    uint64_t process_delta_num = 0;

    STAT_PROCESS(process_delta_num = processReadContentToValueListsWithKeyList(
                *buf + skip_size, file_size - skip_size, 
                kd_lists, key_views),
            StatsType::DELTAKV_HASHSTORE_GET_PROCESS);
    if (process_delta_num != file_hdl->total_object_cnt) {
        debug_error("[ERROR] processed object number during read = %lu, not"
                " equal to object number in metadata = %lu\n",
                process_delta_num, file_hdl->total_object_cnt);
        return false;
    }

    if (kd_lists.empty() == false) {
        if (enableLsmTreeDeltaMeta_ == true) {
            debug_error("[ERROR] Read bucket done, but could not found values"
                    " for key %lu\n", keys->size());
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
        debug_error("[ERROR] no object, read %lu fs %lu, id %lu\n", 
                read_sz, file_hdl->total_object_bytes,
                file_hdl->file_id);
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
        debug_error("[ERROR] processed object num = 0, "
		"read %lu fs %lu fid %lu, unsorted part off %lu\n", 
		read_sz, file_hdl->total_object_bytes, file_hdl->file_id,
		file_hdl->unsorted_part_offset);
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
    uint64_t i = 0;
    uint64_t process_delta_num = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);;
    hashStoreRecordHeader header;
    while (i < read_buf_size) {
        process_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key str_t
        str_t currentKey(read_buf + i, header.key_size_);
        i += header.key_size_;
        if (header.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            str_t currentValue(read_buf + i, header.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<str_t> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            i += header.value_size_;
        }
    }

    if (i > read_buf_size) {
        debug_error("error i: %lu %lu\n", i, read_buf_size);
        exit(1);
    }
    return process_delta_num;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, 
        unordered_map<string_view, vector<string_view>>& resultMapInternal,
        const string_view& key)
{
    uint64_t i = 0;
    uint64_t process_delta_num = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    hashStoreRecordHeader header;
    while (i != read_buf_size) {
        process_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key 
        string_view currentKey(read_buf + i, header.key_size_);
        if (key.size() != currentKey.size() || memcmp(key.data(), currentKey.data(), key.size()) != 0) {
            i += header.key_size_ + ((header.is_anchor_) ? 0 : header.value_size_);
            continue;
        }

        i += header.key_size_;
        if (header.is_anchor_ == true) {
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).clear();
            }
        } else {
            string_view currentValue(read_buf + i, header.value_size_);
            if (resultMapInternal.find(currentKey) != resultMapInternal.end()) {
                resultMapInternal.at(currentKey).push_back(currentValue);
            } else {
                vector<string_view> newValuesRelatedToCurrentKeyVec;
                newValuesRelatedToCurrentKeyVec.push_back(currentValue);
                resultMapInternal.insert(make_pair(currentKey, newValuesRelatedToCurrentKeyVec));
            }
            i += header.value_size_;
        }
    }
    return process_delta_num;
}

uint64_t HashStoreFileOperator::processReadContentToValueLists(
        char* read_buf, uint64_t read_buf_size, vector<string_view>& kd_list,
        const string_view& key)
{
    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    hashStoreRecordHeader header;
    while (i < read_buf_size) {
        processed_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }

        // get key 
        string_view currentKey(read_buf + i, header.key_size_);
        if (key != currentKey) {
            i += header.key_size_ +
                ((header.is_anchor_) ? 0 :
                 header.value_size_);
            continue;
        }

        i += header.key_size_;
        if (header.is_anchor_ == true) {
            kd_list.clear();
        } else {
            string_view currentValue(read_buf + i, header.value_size_);
            kd_list.push_back(currentValue);
            i += header.value_size_;
        }
    }
    if (i > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu"
	       " already processed %lu\n", 
                i, read_buf_size, processed_delta_num);
        return 0;
    }
    return processed_delta_num;
}

uint64_t HashStoreFileOperator::processReadContentToValueListsWithKeyList(
        char* read_buf, uint64_t read_buf_size, 
	vector<vector<string_view>>& kd_lists,
        const vector<string_view>& keys)
{
    kd_lists.clear();
    kd_lists.resize(keys.size());

    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    hashStoreRecordHeader header;
    while (i < read_buf_size) {
        processed_delta_num++;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + i, header_sz);
        }
        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }

        // get key 
        string_view currentKey(read_buf + i, header.key_size_);

	int key_i = 0;
	bool has_key = false;
	for (key_i = 0; key_i < (int)keys.size(); key_i++) {
	    string_view key = keys[key_i];
	    if (key != currentKey) {
		continue;
	    }

	    has_key = true;
	    i += header.key_size_;
	    if (header.is_anchor_ == false) {
		string_view currentValue(read_buf + i, header.value_size_);
		kd_lists[key_i].push_back(currentValue);
		i += header.value_size_;
	    }
	}

	if (has_key == false) {
	    i += header.key_size_ +
		((header.is_anchor_) ? 0 :
		 header.value_size_);
	}
    }
    if (i > read_buf_size) {
        debug_error("[ERROR] read buf index error! %lu v.s. %lu\n", 
                i, read_buf_size);
        return 0;
    }
    return processed_delta_num;
}

bool HashStoreFileOperator::writeContentToFile(
        hashStoreFileMetaDataHandler* file_hdl, char* write_buf, uint64_t
        write_buf_size, uint64_t contentObjectNumber, bool need_flush)
{
    debug_trace("Write content to file ID = %lu\n", file_hdl->file_id);
    FileOpStatus status_w, status_f;
    STAT_PROCESS(status_w = file_hdl->file_op_ptr->writeFile(write_buf,
                write_buf_size), StatsType::DELTAKV_HASHSTORE_PUT_IO_TRAFFIC);
    StatsRecorder::getInstance()->DeltaOPBytesWrite(status_w.physicalSize_,
            status_w.logicalSize_, syncStatistics_);
    if (status_w.success_ == false) {
        debug_error("[ERROR] Write bucket error, internal file operation "
                "fault, could not write content to file ID = %lu, "
                " logical size = %lu, physical size = %lu\n", 
                file_hdl->file_id, file_hdl->total_object_bytes,
                file_hdl->total_on_disk_bytes);
        return false;
    } else {
        // update metadata
        file_hdl->total_object_bytes += write_buf_size;
        file_hdl->total_on_disk_bytes += status_w.physicalSize_;
        file_hdl->total_object_cnt += contentObjectNumber;

        if (need_flush) {
            status_f = file_hdl->file_op_ptr->flushFile();
            if (status_f.success_ == false) {
                debug_error("[ERROR] flush error, physical size = %lu\n",
                        file_hdl->total_on_disk_bytes);
                return false;
            }
            file_hdl->total_on_disk_bytes += status_f.physicalSize_;
        } 
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerPutFunction(hashStoreOperationHandler* op_hdl)
{
    str_t currentKeyStr(op_hdl->write_op.object->keyPtr_, op_hdl->write_op.object->keySize_);
    if (op_hdl->write_op.object->isAnchorFlag_ == true) {
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
    // leave it here 
    hashStoreRecordHeader newRecordHeader;
    newRecordHeader.is_anchor_ = op_hdl->write_op.object->isAnchorFlag_;
    newRecordHeader.key_size_ = op_hdl->write_op.object->keySize_;
    newRecordHeader.sequence_number_ = op_hdl->write_op.object->sequenceNumber_;
    newRecordHeader.value_size_ = op_hdl->write_op.object->valueSize_;
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
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), op_hdl->write_op.object->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader) + newRecordHeader.key_size_, op_hdl->write_op.object->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newFileHeader, sizeof(newFileHeader));
            memcpy(writeBuffer + sizeof(newFileHeader), &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newFileHeader) + sizeof(newRecordHeader), op_hdl->write_op.object->keyPtr_, newRecordHeader.key_size_);
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
            auto mempoolHandler = op_hdl->write_op.object;
            if (kd_cache_ != nullptr) {
                STAT_PROCESS(updateKDCacheIfExist(
                            str_t(mempoolHandler->keyPtr_, mempoolHandler->keySize_), 
                            str_t(mempoolHandler->valuePtr_, mempoolHandler->valueSize_), 
                            newRecordHeader.is_anchor_),
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
            memcpy(writeBuffer + sizeof(newRecordHeader), op_hdl->write_op.object->keyPtr_, newRecordHeader.key_size_);
            memcpy(writeBuffer + sizeof(newRecordHeader) + newRecordHeader.key_size_, op_hdl->write_op.object->valuePtr_, newRecordHeader.value_size_);
            targetWriteSize = writeBufferSize;
        } else {
            memcpy(writeBuffer, &newRecordHeader, sizeof(newRecordHeader));
            memcpy(writeBuffer + sizeof(newRecordHeader), op_hdl->write_op.object->keyPtr_, newRecordHeader.key_size_);
            targetWriteSize = writeBufferSize - newRecordHeader.value_size_;
        }

        // write contents of file
        bool writeContentStatus = writeContentToFile(op_hdl->file_hdl, writeBuffer, targetWriteSize, 1);
        if (writeContentStatus == false) {
            debug_error("[ERROR] Write bucket error, internal file operation fault, could not write content to file ID = %lu\n", op_hdl->file_hdl->file_id);
            return false;
        } else {
            // insert to cache if current key exist in cache && cache is enabled
            auto mempoolHandler = op_hdl->write_op.object;
            if (kd_cache_ != nullptr) {
                STAT_PROCESS(updateKDCacheIfExist(
                            str_t(mempoolHandler->keyPtr_, mempoolHandler->keySize_), 
                            str_t(mempoolHandler->valuePtr_, mempoolHandler->valueSize_), 
                            newRecordHeader.is_anchor_),
                        StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE);
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::operationWorkerMultiPutFunction(
	hashStoreOperationHandler* op_hdl)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    auto& file_hdl = op_hdl->file_hdl;

    if (file_hdl->file_op_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        bool onlyAnchorFlag = true;
        for (auto index = 0; index < op_hdl->multiput_op.size; index++) {
            str_t currentKeyStr(op_hdl->multiput_op.objects[index].keyPtr_, 
                    op_hdl->multiput_op.objects[index].keySize_);
            if (op_hdl->multiput_op.objects[index].isAnchorFlag_ == true) {
                if (file_hdl->sorted_filter->MayExist(currentKeyStr) == true) {
                    file_hdl->filter->Insert(currentKeyStr);
                    file_hdl->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (file_hdl->filter->MayExist(currentKeyStr) == true) {
                        file_hdl->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                onlyAnchorFlag = false;
                file_hdl->filter->Insert(currentKeyStr);
            }
        }
        if (onlyAnchorFlag == true) {
            debug_info("Only contains anchors for file ID = %lu, and file is not opened, skip\n", file_hdl->file_id);
            return true;
        }
    } else {
        for (auto index = 0; index < op_hdl->multiput_op.size; index++) {
            str_t currentKeyStr(op_hdl->multiput_op.objects[index].keyPtr_, 
                    op_hdl->multiput_op.objects[index].keySize_);
            if (op_hdl->multiput_op.objects[index].isAnchorFlag_ == true) {
                if (file_hdl->sorted_filter->MayExist(currentKeyStr) == true) {
                    file_hdl->filter->Insert(currentKeyStr);
                    file_hdl->filter->Erase(currentKeyStr); // Add the count
                } else {
                    if (file_hdl->filter->MayExist(currentKeyStr) == true) {
                        file_hdl->filter->Erase(currentKeyStr);
                    }
                }
            } else {
                file_hdl->filter->Insert(currentKeyStr);
            }
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_UPDATE_FILTER, tv);

    gettimeofday(&tv, 0);
    uint64_t targetWriteBufferSize = 0;
    hashStoreFileHeader newFileHeader;
    bool needFlushFileHeader = false;
    if (file_hdl->file_op_ptr->isFileOpen() == false) {
        string targetFilePath = workingDir_ + "/" + to_string(file_hdl->file_id) + ".delta";
        if (std::filesystem::exists(targetFilePath) == false) {
            file_hdl->file_op_ptr->createThenOpenFile(targetFilePath);
            newFileHeader.prefix_bit = file_hdl->prefix_bit;
            newFileHeader.file_create_reason_ = file_hdl->file_create_reason_;
            newFileHeader.file_id = file_hdl->file_id;
            newFileHeader.previous_file_id_first_ = file_hdl->previous_file_id_first_;
            newFileHeader.previous_file_id_second_ = file_hdl->previous_file_id_second_;
            needFlushFileHeader = true;
            targetWriteBufferSize += sizeof(hashStoreFileHeader);
        } else {
            file_hdl->file_op_ptr->openFile(targetFilePath);
        }
    }
    // leave more space for the buffer
    for (auto i = 0; i < op_hdl->multiput_op.size; i++) {
        targetWriteBufferSize += (sizeof(hashStoreRecordHeader) + 
                op_hdl->multiput_op.objects[i].keySize_);
        if (op_hdl->multiput_op.objects[i].isAnchorFlag_ == true) {
            file_hdl->num_anchors++;
            continue;
        } else {
            targetWriteBufferSize += op_hdl->multiput_op.objects[i].valueSize_;
        }
    }
    char write_buf[targetWriteBufferSize];
    uint64_t write_i = 0;
    if (needFlushFileHeader == true) {
        memcpy(write_buf, &newFileHeader, sizeof(hashStoreFileHeader));
        write_i += sizeof(hashStoreFileHeader);
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_HEADER, tv);
    gettimeofday(&tv, 0);

    hashStoreRecordHeader header;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    for (auto i = 0; i < op_hdl->multiput_op.size; i++) {
        auto& obj = op_hdl->multiput_op.objects[i];

        // write header
        header.is_anchor_ = obj.isAnchorFlag_;
        header.key_size_ = obj.keySize_;
        header.value_size_ = obj.valueSize_;
        header.sequence_number_ = obj.sequenceNumber_;
//	if (file_hdl->unsorted_part_offset > 0) {
//	    debug_error("fid %lu header key %d value %d anchor %d\n",
//		    file_hdl->file_id, header.key_size_,
//		    header.value_size_, (int)header.is_anchor_);
//	}
        if (use_varint_d_header == false) {
            copyInc(write_buf, write_i, &header, header_sz);
        } else {
            write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
        }

        // write key
        copyInc(write_buf, write_i, obj.keyPtr_, header.key_size_);

        // write value (if anchor)
        if (header.is_anchor_ == true) {
            continue;
        } else {
            copyInc(write_buf, write_i, obj.valuePtr_, header.value_size_);
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PREPARE_FILE_CONTENT, tv);
    uint64_t targetObjectNumber = op_hdl->multiput_op.size;
    // write content
    bool writeContentStatus;
    STAT_PROCESS(writeContentStatus = writeContentToFile(file_hdl,
                write_buf, write_i, targetObjectNumber,
                op_hdl->need_flush),
            StatsType::DS_WRITE_FUNCTION);
    if (writeContentStatus == false) {
        debug_error("[ERROR] Could not write content to file, target file ID"
                " = %lu, content size = %lu, content bytes number = %lu\n",
                file_hdl->file_id, targetObjectNumber, write_i);
        exit(1);
        return false;
    } else {
        // insert to cache if need
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (kd_cache_ != nullptr) {
            struct timeval tv;
            gettimeofday(&tv, 0);

            for (uint32_t i = 0; i < op_hdl->multiput_op.size; i++) {
                auto& it = op_hdl->multiput_op.objects[i];
                updateKDCacheIfExist(str_t(it.keyPtr_, it.keySize_),
                        str_t(it.valuePtr_, it.valueSize_),
                        it.isAnchorFlag_);
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

    if (op_hdl->file_hdl->file_op_ptr->isFileOpen() == false) {
        // prepare write buffer, file not open, may load, skip;
        return true;
    }

    // write content
    FileOpStatus status = op_hdl->file_hdl->file_op_ptr->flushFile();
//    debug_error("flush file %lu\n", op_hdl->file_hdl->file_id);
    if (status.success_ == false) {
        debug_error("[ERROR] Could not flush to file, target file ID = %lu\n",
                op_hdl->file_hdl->file_id);
        exit(1);
    } 
    op_hdl->file_hdl->total_on_disk_bytes += status.physicalSize_;
    return true;
}

bool HashStoreFileOperator::operationWorkerFind(hashStoreOperationHandler* op_hdl) {
    auto obj = op_hdl->object;
    bool status = hashStoreFileManager_->getFileHandlerWithKey(
            obj->keyPtr_, obj->keySize_, kMultiPut, op_hdl->file_hdl,
            obj->isAnchorFlag_);

    if (status == false) {
        debug_error("[ERROR] Get file handler for key = %s"
                " error during multiput\n", obj->keyPtr_);
    }

    return true;
}

bool HashStoreFileOperator::putFileHandlerIntoGCJobQueueIfNeeded(hashStoreFileMetaDataHandler* file_hdl)
{
    static int cnt = 0;
    if (write_stall_ != nullptr) {
        if (*write_stall_ == true) {
            // performing write-back, does not do GC now
            return false;
        }
    }
    // insert into GC job queue if exceed the threshold
    if (file_hdl->DiskAndBufferSizeExceeds(perFileGCSizeLimit_) || 
            file_hdl->UnsortedPartExceeds(unsorted_part_size_threshold_)) {
        if (file_hdl->UnsortedPartExceeds(unsorted_part_size_threshold_) &&
                file_hdl->unsorted_part_offset != 0) {
            debug_error("unsorted_part_size_threshold_ %lu %lu %lu %lu\n",
                    unsorted_part_size_threshold_, file_hdl->total_on_disk_bytes,
                    file_hdl->file_op_ptr->getFileBufferedSize(),
                    file_hdl->unsorted_part_offset);
            cnt++;
            if (cnt > 100) {
                exit(1);
            }
        }
        if (file_hdl->gc_status == kNoGC) {
            file_hdl->no_gc_wait_operation_number_++;
            if (file_hdl->no_gc_wait_operation_number_ >= operationNumberThresholdForForcedSingleFileGC_ ||
                    file_hdl->num_anchors >= operationNumberThresholdForForcedSingleFileGC_) {
                file_hdl->file_ownership = -1;
                file_hdl->gc_status = kMayGC;
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
inline void HashStoreFileOperator::putKeyValueToAppendableCacheIfExist(
        char* keyPtr, size_t keySize, char* valuePtr, size_t valueSize, 
        bool isAnchor)
{
    str_t key(keyPtr, keySize);
        
    // insert into cache only if the key has been read
    if (isAnchor == true) {
        keyToValueListCacheStr_->cleanCacheIfExist(key);
    } else {
        vector<str_t>* vec =
            keyToValueListCacheStr_->getFromCache(key);

        // TODO a bug here. vec may be evicted and deleted before the
        // update
        if (vec != nullptr) {
            str_t valueStr(valuePtr, valueSize);
            vector<str_t> temp_vec;
            for (auto& it : *vec) {
                temp_vec.push_back(it);
            }
            temp_vec.push_back(valueStr);

            str_t merged_delta;

            // allocate merged_delta. The deletion is now managed by cache
            deltaKVMergeOperatorPtr_->PartialMerge(temp_vec, merged_delta);

            vector<str_t>* merged_deltas = new vector<str_t>;
            merged_deltas->push_back(merged_delta);

            keyToValueListCacheStr_->updateCache(key, merged_deltas);
        }
    }
}

// for get
inline void HashStoreFileOperator::putKeyValueVectorToAppendableCacheIfNotExist(
        char* keyPtr, size_t keySize, vector<str_t>& values) {
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

// for put
inline void HashStoreFileOperator::updateKDCacheIfExist(
        str_t key, str_t delta, bool isAnchor)
{
    // insert into cache only if the key has been read
    if (isAnchor == true) {
        kd_cache_->cleanCacheIfExist(key);
    } else {
        str_t old_delta = kd_cache_->getFromCache(key);

        // TODO a bug here. vec may be evicted and deleted before the
        // update
        if (old_delta.data_ != nullptr && old_delta.size_ > 0) {
            // if there is a delta. Merge and then update 
            vector<str_t> temp_vec;
            str_t merged_delta;
            temp_vec.push_back(old_delta);
            temp_vec.push_back(delta);

            // allocated by partial merge
            deltaKVMergeOperatorPtr_->PartialMerge(temp_vec, merged_delta);
            kd_cache_->updateCache(key, merged_delta);
        } else if (old_delta.data_ == nullptr && old_delta.size_ == 0) {
            // if there is an anchor. directly update
            str_t inserted_delta(new char[delta.size_], delta.size_);
            memcpy(inserted_delta.data_, delta.data_, delta.size_);
            kd_cache_->updateCache(key, inserted_delta);
        }
    }
}

// for get
inline void HashStoreFileOperator::updateKDCache(
        char* keyPtr, size_t keySize, str_t delta) {
    str_t key(keyPtr, keySize);
    kd_cache_->updateCache(key, delta);
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
    // leave it here 
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
            if (kd_cache_ != nullptr) {
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
            if (kd_cache_ != nullptr) {
                // do not implement
            }
            return true;
        }
    }
}

bool HashStoreFileOperator::directlyMultiWriteOperation(unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> batchedWriteOperationsMap)
{
    // leave it here (header not fixed)
    debug_error("not implemented %s\n", "");
    exit(1);
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
            if (kd_cache_ != nullptr) {
                for (auto i = 0; i < batchIt.second.size(); i++) {
                    auto& it = batchIt.second[i];
                    updateKDCacheIfExist(str_t(it.keyPtr_, it.keySize_),
                            str_t(it.valuePtr_, it.valueSize_),
                            it.isAnchorFlag_);
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
    if (kd_cache_ != nullptr) {
        str_t currentKey(key.data(), key.size());
        str_t delta = kd_cache_->getFromCache(currentKey);
        if (delta.data_ != nullptr && delta.size_ > 0) {
            // get a delta from the cache 
            struct timeval tv;
            gettimeofday(&tv, 0);
            valueVec.clear();
            valueVec.push_back(string(delta.data_, delta.size_));
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DS_GET_CACHE_HIT_DELTA, tv);
            file_hdl->file_ownership = 0;
            return true;
        } else if (delta.data_ == nullptr && delta.size_ == 0) {
            // get an anchor from the cache
            struct timeval tv;
            gettimeofday(&tv, 0);
            valueVec.clear();
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DS_GET_CACHE_HIT_ANCHOR, tv);
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

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()),
                            it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                deltaKVMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(string(merged_delta.data_, merged_delta.size_));
            }

//            putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
//                    key.size(), deltas);
            updateKDCache(key.data(), key.size(), merged_delta);
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

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()), it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                deltaKVMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(string(merged_delta.data_,
                            merged_delta.size_));
            }

//            putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
//                    key.size(), deltas);
            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            file_hdl->file_ownership = 0;
            return true;
        }
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

        delete[] buf;

        file_hdl->file_ownership = 0;
        return true;
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

bool HashStoreFileOperator::operationWorkerGetFunction(hashStoreOperationHandler* op_hdl)
{
    auto& file_hdl = op_hdl->file_hdl; 
    // check if not flushed anchors exit, return directly.
    // try extract from cache first

    // only read the first key
    auto& multiget_op = op_hdl->multiget_op;
    auto& key = *((*multiget_op.keys)[0]);
    auto& valueVec = (*multiget_op.values);

    if (kd_cache_ != nullptr) {
        // does not check the cache. Already checked in the interface
         if (enable_index_block_) {
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

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()),
                            it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                deltaKVMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(new string(merged_delta.data_, merged_delta.size_));
            }

//            putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
//                    key.size(), deltas);
            updateKDCache(key.data(), key.size(), merged_delta);
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

//            if (kd_list.size() > 0) {
//                valueVec.reserve(kd_list.size());
//                for (auto vecIt : kd_list) {
//                    valueVec.push_back(string(vecIt.data(), vecIt.size()));
//                }
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            vector<str_t> deltas;
            for (auto& it : kd_list) {
                deltas.push_back(str_t(const_cast<char*>(it.data()), it.size()));
            }

            str_t merged_delta(nullptr, 0);
            if (deltas.size() > 0) {
                deltaKVMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
                valueVec.push_back(new string(merged_delta.data_,
                            merged_delta.size_));
            }

//            putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
//                    key.size(), deltas);
            updateKDCache(key.data(), key.size(), merged_delta);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);

            delete[] buf;

            file_hdl->file_ownership = 0;
            return true;
        }
        
        return true;
    } 
    
    if (enable_index_block_) {
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
                valueVec.push_back(new string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;

        file_hdl->file_ownership = 0;
        return true;
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
                valueVec.push_back(new string(vecIt.data(), vecIt.size()));
            }
        }

        delete[] buf;
        file_hdl->file_ownership = 0;
        return true;
    }
}

bool HashStoreFileOperator::operationWorkerMultiGetFunction(hashStoreOperationHandler* op_hdl)
{
    if (op_hdl->multiget_op.keys->size() == 1) {
	return operationWorkerGetFunction(op_hdl);
    }

    auto& file_hdl = op_hdl->file_hdl; 

    // check if not flushed anchors exit, return directly.
    // try extract from cache first

    // only read the first key
    auto& multiget_op = op_hdl->multiget_op;
    //    auto& key = *((*multiget_op.keys)[0]);
    auto& keys = multiget_op.keys;
    auto& valueVec = (*multiget_op.values);

    vector<vector<string_view>> kd_lists;

    char* buf = nullptr;
    bool success;

//    debug_error("read hdl %p keys %lu\n", file_hdl, keys->size());

    success = readAndProcessWholeFileKeyList(file_hdl, keys, kd_lists, &buf);

    multiget_op.values->clear();

    if (success == false) {
	debug_error("[ERROR] read and process failed: key num %lu\n",
		keys->size());
	exit(1);
    } else if (buf == nullptr) {
	// Key miss because (partially) sorted part does not have key
	file_hdl->file_ownership = 0;
	return true;
    } 

    struct timeval tv;
    gettimeofday(&tv, 0);

    valueVec.resize(keys->size());
    for (auto key_i = 0; key_i < keys->size(); key_i++) {
	valueVec[key_i] = nullptr;

	vector<str_t> deltas;
	for (auto& it : kd_lists[key_i]) {
	    deltas.push_back(str_t(const_cast<char*>(it.data()),
			it.size()));
	}

	str_t merged_delta(nullptr, 0);
	if (deltas.size() > 0) {
	    deltaKVMergeOperatorPtr_->PartialMerge(deltas, merged_delta);
	    valueVec[key_i] = new string(merged_delta.data_,
		    merged_delta.size_);
	}

	//putKeyValueVectorToAppendableCacheIfNotExist(key.data(),
	//    key.size(), deltas);
	if (kd_cache_ != nullptr) {
	    updateKDCache((*keys)[key_i]->data(), 
		    (*keys)[key_i]->size(), merged_delta);
	}
    }
    StatsRecorder::getInstance()->timeProcess(
	    StatsType::DELTAKV_HASHSTORE_GET_INSERT_CACHE, tv);

    delete[] buf;

    file_hdl->file_ownership = 0;
    return true;
}

void HashStoreFileOperator::operationBoostThreadWorker(hashStoreOperationHandler* op_hdl)
{
    bool operationsStatus = true;
    bool file_hdl_is_input = true;
    auto file_hdl = op_hdl->file_hdl;

    std::scoped_lock<std::shared_mutex>* w_lock = nullptr;
    if (file_hdl != nullptr) {
        w_lock = new
            std::scoped_lock<std::shared_mutex>(file_hdl->fileOperationMutex_);
    }

    switch (op_hdl->op_type) {
        case kMultiGet:
            STAT_PROCESS(operationsStatus = operationWorkerMultiGetFunction(op_hdl), StatsType::OP_MULTIGET);
            break;
        case kMultiPut:
            debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", file_hdl->file_id, op_hdl->multiput_op.size);
            STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(op_hdl), StatsType::OP_MULTIPUT);
            break;
        case kPut:
            debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", op_hdl->write_op.object->keyPtr_, file_hdl->file_id);
            STAT_PROCESS(operationsStatus = operationWorkerPutFunction(op_hdl), StatsType::OP_PUT);
            break;
        case kFlush:
            STAT_PROCESS(operationsStatus = operationWorkerFlush(op_hdl), StatsType::OP_FLUSH);
            break;
        case kFind:
            STAT_PROCESS(operationsStatus = operationWorkerFind(op_hdl), StatsType::OP_FIND);
            file_hdl_is_input = false;
            break;
        default:
            debug_error("[ERROR] Unknown operation type = %d\n", op_hdl->op_type);
            break;
    }

    if (operationsStatus == false) {
        file_hdl->file_ownership = 0;
        debug_trace("Process file ID = %lu error\n", file_hdl->file_id);
        op_hdl->job_done = kError;
    } else if (file_hdl_is_input == true) {
        if ((op_hdl->op_type == kPut || op_hdl->op_type == kMultiPut)
                && enableGCFlag_ == true) {
            bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_hdl);
            if (putIntoGCJobQueueStatus == false) {
                file_hdl->file_ownership = 0;
                op_hdl->job_done = kDone;
            } else {
                op_hdl->job_done = kDone;
            }
        } else {
            op_hdl->file_hdl->file_ownership = 0;
            op_hdl->job_done = kDone;
        }
    } else {
        op_hdl->job_done = kDone;
    }

    if (w_lock) {
        delete w_lock;
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
            bool operationsStatus = true;
            bool file_hdl_is_input = true;
            auto file_hdl = op_hdl->file_hdl;

            std::scoped_lock<std::shared_mutex>* w_lock = nullptr;
            if (file_hdl != nullptr) {
                w_lock = new
                    std::scoped_lock<std::shared_mutex>(file_hdl->fileOperationMutex_);
            }

            switch (op_hdl->op_type) {
            case kMultiGet:
                STAT_PROCESS(operationsStatus = operationWorkerMultiGetFunction(op_hdl), StatsType::OP_MULTIGET);
                break;
            case kMultiPut:
                debug_trace("receive operations, type = kMultiPut, file ID = %lu, put deltas key number = %u\n", file_hdl->file_id, op_hdl->multiput_op.size);
                STAT_PROCESS(operationsStatus = operationWorkerMultiPutFunction(op_hdl), StatsType::OP_MULTIPUT);
                break;
            case kPut:
                debug_trace("receive operations, type = kPut, key = %s, target file ID = %lu\n", op_hdl->write_op.object->keyPtr_, file_hdl->file_id);
                STAT_PROCESS(operationsStatus = operationWorkerPutFunction(op_hdl), StatsType::OP_PUT);
                break;
            case kFlush:
                STAT_PROCESS(operationsStatus = operationWorkerFlush(op_hdl), StatsType::OP_FLUSH);
                break;
            case kFind:
                STAT_PROCESS(operationsStatus = operationWorkerFind(op_hdl), StatsType::OP_FIND);
                file_hdl_is_input = false;
                break;
            default:
                debug_error("[ERROR] Unknown operation type = %d\n", op_hdl->op_type);
                break;
            }

            if (operationsStatus == false) {
                file_hdl->file_ownership = 0;
                debug_trace("Process file ID = %lu error\n", file_hdl->file_id);
                op_hdl->job_done = kError;
            } else if (file_hdl_is_input == true) {
                if ((op_hdl->op_type == kPut || op_hdl->op_type == kMultiPut)
                        && enableGCFlag_ == true) {
                    bool putIntoGCJobQueueStatus = putFileHandlerIntoGCJobQueueIfNeeded(file_hdl);
                    if (putIntoGCJobQueueStatus == false) {
                        file_hdl->file_ownership = 0;
                        op_hdl->job_done = kDone;
                    } else {
                        op_hdl->job_done = kDone;
                    }
                } else {
                    op_hdl->file_hdl->file_ownership = 0;
                    op_hdl->job_done = kDone;
                }
            } else {
                op_hdl->job_done = kDone;
            }

            if (w_lock) {
                delete w_lock;
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
