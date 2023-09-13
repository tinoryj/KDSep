#include "deltaStore/bucketManager.hpp"
#include "utils/bucketIndexBlock.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"
#include <map>

namespace KDSEP_NAMESPACE {

BucketManager::BucketManager(KDSepOptions* options, string workingDirStr, messageQueue<BucketHandler*>* notifyGCMQ, messageQueue<writeBackObject*>* writeBackOperationsQueue)
{
    maxBucketNumber_ = options->deltaStore_max_bucket_number_;
    bucket_bitmap_ = new BitMap(maxBucketNumber_ + 16);
    uint64_t k = 0;
    while (pow((double)2, (double)k) <= maxBucketNumber_) {
        k++;
    }
    k = k - 1;
    if (options->deltaStore_init_k_ > k) {
        initialTrieBitNumber_ = k - 1;
    } else {
        initialTrieBitNumber_ = options->deltaStore_init_k_;
    }
    if (options->kd_cache != nullptr) {
        kd_cache_ = options->kd_cache;
    } 
    singleFileGCTriggerSize_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_bucket_size_;
    maxBucketSize_ = options->deltaStore_bucket_size_;
    singleFileMergeGCUpperBoundSize_ = maxBucketSize_ * 0.5;
    enableBatchedOperations_ = options->enable_batched_operations_;
    enableLsmTreeDeltaMeta_ = options->enable_lsm_tree_delta_meta;
    debug_info("[Message]: singleFileGCTriggerSize_ = %lu, singleFileMergeGCUpperBoundSize_ = %lu, initialTrieBitNumber_ = %lu\n", singleFileGCTriggerSize_, singleFileMergeGCUpperBoundSize_, initialTrieBitNumber_);
    working_dir_ = workingDirStr;
    manifest_ = new ManifestManager(workingDirStr);
    notifyGCMQ_ = notifyGCMQ;
    enableWriteBackDuringGCFlag_ = (writeBackOperationsQueue != nullptr);
    write_back_queue_ = writeBackOperationsQueue;
    gcWriteBackDeltaNum_ = options->deltaStore_gc_write_back_delta_num;
    gcWriteBackDeltaSize_ = options->deltaStore_gc_write_back_delta_size;
    fileOperationMethod_ = options->fileOperationMethod_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    enable_crash_consistency_ = options->enable_crash_consistency;
    singleFileSplitGCTriggerSize_ =
	options->deltaStore_gc_split_threshold_
	* options->deltaStore_bucket_size_;
//    prefix_tree_ = new SkipListForBuckets(maxBucketNumber_);
    prefix_tree_.init(maxBucketNumber_);
    singleFileGCWorkerThreadsNumebr_ = options->deltaStore_gc_worker_thread_number_limit_;
    workingThreadExitFlagVec_ = 0;
    syncStatistics_ = true;
    singleFileFlushSize_ = options->deltaStore_bucket_flush_buffer_size_limit_;
    KDSepMergeOperatorPtr_ = options->KDSep_merge_operation_ptr;
    enable_index_block_ = options->enable_index_block;
    write_stall_ = options->write_stall;
    wb_keys = options->wb_keys;
    wb_keys_mutex = options->wb_keys_mutex;
    struct timeval tv, tv2;
    gettimeofday(&tv, 0);
    RetriveHashStoreFileMetaDataList();
    gettimeofday(&tv2, 0);
    printf("retrieve metadata list time: %.6lf\n", 
	    tv2.tv_sec + tv2.tv_usec / 1000000.0 - tv.tv_sec -
	    tv.tv_usec / 1000000.0);

    // for asio
    gc_threads_ = new boost::asio::thread_pool(options->deltaStore_gc_worker_thread_number_limit_);
    extra_threads_ = new boost::asio::thread_pool(4);  // for split/merge in parallel
    num_threads_ = 0;
}

BucketManager::~BucketManager()
{
    delete gc_threads_;
    delete extra_threads_;
    CloseHashStoreFileMetaDataList();
}

bool BucketManager::setJobDone()
{
    metadataUpdateShouldExit_ = true;
    metaCommitCV_.notify_all();
    if (enableGCFlag_ == true) {
        notifyGCMQ_->done = true;
        while (workingThreadExitFlagVec_ != singleFileGCWorkerThreadsNumebr_) {
            operationNotifyCV_.notify_all();
        }
    }
    return true;
}

void BucketManager::pushToGCQueue(BucketHandler* bucket) {
    boost::asio::post(*gc_threads_, [this, bucket]() {
            asioSingleFileGC(bucket);
            });

//    notifyGCMQ_->push(bucket);
//    operationNotifyCV_.notify_one();
}

BucketHandler* BucketManager::createFileHandler() {
    BucketHandler* bucket = new BucketHandler; 
    bucket->io_ptr = new FileOperation(fileOperationMethod_,
	    maxBucketSize_, singleFileFlushSize_);
    bucket->sorted_filter = new BucketKeyFilter();
    bucket->filter = new BucketKeyFilter();
    return bucket;
}

void BucketManager::deleteFileHandler(
	BucketHandler* bucket) {

    if (bucket->index_block) {
	delete bucket->index_block;
    }
    if (bucket->io_ptr) {
	delete bucket->io_ptr;
    }
    delete bucket->sorted_filter;
    delete bucket->filter;
}

bool BucketManager::writeToCommitLog(vector<mempoolHandler_t> objects,
        bool& need_flush, bool add_commit_message) {
    uint64_t write_buf_sz = sizeof(KDRecordHeader);
    for (auto i = 0; i < objects.size(); i++) {
        // reserve more space
        write_buf_sz += sizeof(KDRecordHeader) + 
            objects[i].keySize_ + objects[i].valueSize_;
    }

    char write_buf[write_buf_sz];
    uint64_t write_i = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader rec_header;
    for (auto i = 0; i < objects.size(); i++) {
        rec_header.is_anchor_ = objects[i].isAnchorFlag_;
        rec_header.is_gc_done_ = false;
        rec_header.key_size_ = objects[i].keySize_;
        rec_header.value_size_ = objects[i].valueSize_;
        rec_header.seq_num = objects[i].seq_num;
        if (use_varint_d_header == false) {
            copyInc(write_buf, write_i, &rec_header, header_sz);
        } else {
            write_i += PutDeltaHeaderVarint(write_buf + write_i, rec_header);
        }
        copyInc(write_buf, write_i, objects[i].keyPtr_, objects[i].keySize_);
        if (rec_header.is_anchor_ == false) {
            copyInc(write_buf, write_i, objects[i].valuePtr_, objects[i].valueSize_);
        }
    }

    // when there are no values to write
    if (add_commit_message == true) { 
        rec_header.is_anchor_ = false;
        rec_header.is_gc_done_ = true;
        rec_header.key_size_ = 0;
        rec_header.value_size_ = 0;
        rec_header.seq_num = 0;
        if (use_varint_d_header == false) {
            copyInc(write_buf, write_i, &rec_header, header_sz);
        } else {
            write_i += PutDeltaHeaderVarint(write_buf + write_i, rec_header);
        }
    }

    if (write_i == 0) {
        return true;
    }

    if (commit_log_fop_ == nullptr) {
	commit_log_fop_ = new FileOperation(kDirectIO,
		commit_log_maximum_size_, 0);
        commit_log_fop_->createThenOpenFile(working_dir_ + "/commit.log");
    }

    FileOpStatus status;
    STAT_PROCESS(status = commit_log_fop_->writeAndFlushFile(write_buf,
                write_i),
           StatsType::DS_PUT_COMMIT_LOG); 

    if (status.success_ == false) {
        debug_error("[ERROR] Write to commit log failed: buf size %lu\n", 
                write_i);
    }

    if (commit_log_fop_->getCachedFileSize() > commit_log_next_threshold_) {
        need_flush = true;
        debug_error("commit log next threshold %lu\n",
                commit_log_next_threshold_);
        commit_log_next_threshold_ += commit_log_maximum_size_;
    }

    return status.success_;
}

bool BucketManager::commitToCommitLog() {
    uint64_t write_buf_sz = sizeof(KDRecordHeader);
    char write_buf[write_buf_sz];
    uint64_t write_i = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader rec_header;
    rec_header.is_anchor_ = false;
    rec_header.is_gc_done_ = true;
    rec_header.key_size_ = 0;
    rec_header.value_size_ = 0;
    rec_header.seq_num = 0;
    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &rec_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i, rec_header);
    }

    if (commit_log_fop_ == nullptr) {
	commit_log_fop_ = new FileOperation(kDirectIO,
		commit_log_maximum_size_, 0);
        commit_log_fop_->createThenOpenFile(working_dir_ + "/commit.log");
    }

    FileOpStatus status;
    STAT_PROCESS(status = commit_log_fop_->writeAndFlushFile(write_buf,
                write_i),
           StatsType::DS_PUT_COMMIT_LOG); 

    if (status.success_ == false) {
        debug_error("[ERROR] Commit to commit log failed: buf size %lu\n", 
                write_i);
    }

    return status.success_;
}

bool BucketManager::cleanCommitLog() {
    bool ret;
    struct timeval tv;
    gettimeofday(&tv, 0);
    ret = commit_log_fop_->removeAndReopen();
    StatsRecorder::staticProcess(
	    StatsType::DS_REMOVE_COMMIT_LOG, tv);
    if (ret == false) {
	debug_error("remove failed commit log %s\n", "");
    }
    commit_log_next_threshold_ = commit_log_maximum_size_;
    return true;
}

//bool BucketManager::flushAllBuffers() {
////vector<BucketHandler*> BucketManager::flushAllBuffers() {
////    vector<pair<uint64_t, BucketHandler*>> validObjectVec;
////    prefix_tree_.getCurrentValidNodes(validObjectVec);
//
//    return true;
//}

// Recovery
/*
read_buf start after file header
resultMap include key - <is_anchor, value> map
*/
uint64_t BucketManager::deconstructAndGetAllContentsFromFile(char* read_buf, uint64_t fileSize, map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone)
{
    uint64_t processedTotalObjectNumber = 0;
    uint64_t read_i = 0;
    size_t header_sz = sizeof(KDRecordHeader);

    while (read_i != fileSize) {
        processedTotalObjectNumber++;
        KDRecordHeader header;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + read_i, header_sz);
        } else { 
            header = GetDeltaHeaderVarint(read_buf + read_i, header_sz); 
        }
        read_i += header_sz;
        if (header.is_anchor_ == true) {
            // is anchor, skip value only
            string currentKeyStr(read_buf + read_i, header.key_size_);
            debug_trace("deconstruct current record is anchor, key = %s\n", currentKeyStr.c_str());
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                read_i += header.key_size_;
                string currentValueStr = "";
                resultMap.at(currentKeyStr).push_back(make_pair(true, currentValueStr));
                continue;
            } else {
                vector<pair<bool, string>> newValuesRelatedToCurrentKeyVec;
                read_i += header.key_size_;
                string currentValueStr = "";
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(true, currentValueStr));
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                continue;
            }
        } else if (header.is_gc_done_ == true) {
            // is gc mark, skip key and value
            debug_trace("deconstruct current record is gc flushed flag = %d\n", header.is_gc_done_);
            isGCFlushDone = true;
            continue;
        } else {
            // is content, keep key and value
            string currentKeyStr(read_buf + read_i, header.key_size_);
            debug_trace("deconstruct current record is anchor, key = %s\n", currentKeyStr.c_str());
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                read_i += header.key_size_;
                string currentValueStr(read_buf + read_i, header.value_size_);
                resultMap.at(currentKeyStr).push_back(make_pair(false, currentValueStr));
                read_i += header.value_size_;
                continue;
            } else {
                vector<pair<bool, string>> newValuesRelatedToCurrentKeyVec;
                read_i += header.key_size_;
                string currentValueStr(read_buf + read_i, header.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(false, currentValueStr));
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                read_i += header.value_size_;
                continue;
            }
        }
    }
    return processedTotalObjectNumber;
}

bool BucketManager::deleteObslateFileWithFileIDAsInput(uint64_t fileID)
{
    string targetRemoveFilePath = working_dir_ + "/" + to_string(fileID) + ".delta";
    if (filesystem::exists(targetRemoveFilePath) != false) {
        auto removeOldManifestStatus = remove(targetRemoveFilePath.c_str());
        if (removeOldManifestStatus == -1) {
            debug_error("[ERROR] Could not delete the obsolete file ID = %lu\n", fileID);
            return false;
        } else {
            debug_info("Deleted obsolete file ID = %lu\n", fileID);
            return true;
        }
    } else {
        return true;
    }
}

void BucketManager::recoverFileMt(BucketHandler* bucket,
        boost::atomic<uint64_t>& data_sizes,
        boost::atomic<uint64_t>& disk_sizes,
        boost::atomic<uint64_t>& cnt) {
    FileOperation* fop = bucket->io_ptr;
    string filename = working_dir_ + "/" + to_string(bucket->file_id) +
	".delta";
    char* read_buf;
    uint64_t data_size;
    bool ret;

    STAT_PROCESS(
    ret = fop->openAndReadFile(filename, read_buf, data_size, true),
    StatsType::DS_RECOVERY_READ);

    if (ret == false) {
	printf("[ERROR] open failed: %s\n", filename.c_str()); 
	debug_error("[ERROR] open failed: %s\n", filename.c_str()); 
	exit(1);
    }

    uint64_t onDiskFileSize = fop->getCachedFileSize();
    if (onDiskFileSize == 0) {
	printf("[ERROR] read failed: %s\n", filename.c_str()); 
	debug_error("[ERROR] read failed: %s\n", filename.c_str()); 
	exit(1);
    }

    recoverIndexAndFilter(bucket, read_buf, data_size);

    disk_sizes += onDiskFileSize;
    data_sizes += data_size;

    cnt--;
}

uint64_t BucketManager::recoverIndexAndFilter(
	BucketHandler* bucket,
        char* read_buf, uint64_t read_buf_size)
{
    uint64_t i = 0;
    uint64_t processed_delta_num = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    KDRecordHeader header;
    string_view previous_key(read_buf, 0);

    // create the index block
    if (enable_index_block_) {
	if (bucket->index_block == nullptr) {
	    bucket->index_block = new BucketIndexBlock();
	} else {
	    bucket->index_block->Clear();
	}
    }

    bucket->max_seq_num = 0;

    // one key only belongs to one record
    vector<pair<str_t, KDRecordHeader>> may_sorted_records;
    bool sorted = true;

    bool read_header_success = false;
    uint64_t header_offset = 0;
    uint64_t rollback_offset = 0;

    struct timeval tv;
    gettimeofday(&tv, 0);

    while (i < read_buf_size) {
        processed_delta_num++;
	header_offset = i;
	// get header
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + i, header_sz);
        } else {
	    // TODO check whether header can be destructed
            header = GetDeltaHeaderVarintMayFail(read_buf + i, 
		    read_buf_size - i, header_sz, read_header_success);
	    if (read_header_success == false) {
		debug_error("because of header break: %lu %lu\n",
			i, read_buf_size);
		rollback_offset = header_offset;
		break;
	    }
        }

        i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
	    if (sorted == false) {
		debug_error("[ERROR] Have gc done flag but keys not"
		       " sorted, deltas %lu file id %lu size %lu "
		       "[%lu %lu] processed %lu\n", 
		       processed_delta_num, bucket->file_id,
		       may_sorted_records.size(),
		       i, read_buf_size, processed_delta_num);
		debug_error("file id %lu previous key %.*s\n",
			bucket->file_id,
			(int)previous_key.size(), 
			previous_key.data()); 
		exit(1);
	    }

	    sorted = false;

	    bucket->unsorted_part_offset = i;
	    bucket->io_ptr->markDirectDataAddress(i);

	    // build index and filter blocks
	    if (enable_index_block_) {
		for (auto i = 0; i < may_sorted_records.size(); i++) {
		    auto& key = may_sorted_records[i].first;
		    auto& tmp_header = may_sorted_records[i].second;
		    uint64_t total_kd_size =
			GetDeltaHeaderVarintSize(tmp_header) + 
			tmp_header.key_size_ + tmp_header.value_size_;
		    bucket->index_block->Insert(key, total_kd_size); 
		    bucket->sorted_filter->Insert(key);

		}

		bucket->index_block->Build();
		bucket->index_block->IndicesClear();
	    } else {
		for (auto& it : may_sorted_records) {
		    bucket->sorted_filter->Insert(it.first);
		}
	    }

	    may_sorted_records.clear();
            continue;
        }

	str_t key(read_buf + i, header.key_size_);
	string_view key_view(read_buf + i, header.key_size_);

//	if (key_view == "user13704398570070748503") {
//	    debug_error("user13704398570070748503 file %lu seqnum %u\n", 
//		    bucket->file_id, header.seq_num);
//	}

	// no sorted part if the key is smaller or equal to the previous key
	if (sorted && previous_key.size() > 0 && previous_key >= key_view) {
	    // no sorted part
	    sorted = false;
	} 

        i += header.key_size_;
	// no sorted part if there is an anchor 
        if (header.is_anchor_ == true) {
	    sorted = false;
        } else {
            i += header.value_size_;
        }

	if (i <= read_buf_size) {
	    // successfully read record. 
	    bucket->max_seq_num = max(
		    bucket->max_seq_num,
		    (uint64_t)header.seq_num); 
//	    if (bucket->file_id == 2295) {
//		//user13704398570070748503
//		debug_error("key %.*s file id %lu seq %lu maxseq %lu\n", 
//			(int)key.size_, key.data_,
//			header.seq_num,
//			bucket->file_id,
//			bucket->max_seq_num);
//	    }
	    if (sorted) {
		may_sorted_records.push_back(make_pair(key, header));
	    } else {
		// not the sorted part, release all the records and put into
		// the unsorted filter
		if (may_sorted_records.size() > 0) {
		    for (auto& it : may_sorted_records) {
			bucket->filter->Insert(it.first);
		    }
		    may_sorted_records.clear();
		}
		bucket->filter->Insert(key);
	    }

	    previous_key = key_view;
	} else {
	    // read record failed
	    rollback_offset = header_offset;
	    i = rollback_offset;
	    break;
	}
    }

//    if (i < read_buf_size) {
//        debug_error("file id %lu buf size %lu roll back to %lu\n", 
//		bucket->file_id,
//                read_buf_size, rollback_offset);
//    }

    StatsRecorder::staticProcess(StatsType::DS_RECOVERY_INDEX_FILTER, tv);

    if (i < read_buf_size) {
	STAT_PROCESS(
		bucket->io_ptr->rollbackFile(read_buf, rollback_offset),
		StatsType::DS_RECOVERY_ROLLBACK);
    }

    // update file handler
    bucket->total_object_cnt = processed_delta_num;
    bucket->total_object_bytes = rollback_offset;
    bucket->total_on_disk_bytes = bucket->io_ptr->getCachedFileSize();

//    debug_error("file id %lu seq num %lu\n", 
//	    bucket->file_id, bucket->max_seq_num);

    return processed_delta_num;
}

bool BucketManager::recoverBucketTable() {
    // read all 
    struct timeval tv, tv2;
    gettimeofday(&tv, 0);
    debug_error("start recovery %s\n", "");
    vector<uint64_t> scannedOnDiskFileIDList;
    int cnt_f = 0;
    uint64_t success_read_size = 0;

    boost::asio::thread_pool* recovery_thread_ = new
	boost::asio::thread_pool(8);
    boost::atomic<uint64_t> cnt_in_progress;
    boost::atomic<uint64_t> data_sizes;
    boost::atomic<uint64_t> disk_sizes;
    cnt_in_progress = 0;
    data_sizes = 0;
    disk_sizes = 0;

    // Step 2: Recover each bucket
    for (auto& it : id2buckets_) {
	cnt_in_progress++;

	boost::asio::post(*recovery_thread_,
		boost::bind(
		    &BucketManager::recoverFileMt,
		    this,
		    it.second,
		    boost::ref(data_sizes),
		    boost::ref(disk_sizes),
		    boost::ref(cnt_in_progress)));

	cnt_f++;
    }

    while (cnt_in_progress > 0) {
        usleep(10);
    }

    uint64_t t1, t2;
    t1 = data_sizes;
    t2 = disk_sizes;

    // get the minimum of the maximum sequence number
    min_seq_num_ = 0; 
    bool first = true;
    for (auto& it : id2buckets_) {
	if (first) {
	    min_seq_num_ = it.second->max_seq_num; 
	    first = false;
	} else {
	    min_seq_num_ = min(min_seq_num_, it.second->max_seq_num);
	}
    }

    printf("part 2 (%d files, data size %lu disk size %lu)\n", 
	    cnt_f, t1, t2);

    gettimeofday(&tv2, 0);
    printf("read all buckets time: %.6lf\n", 
	    tv2.tv_sec + tv2.tv_usec / 1000000.0 - tv.tv_sec -
	    tv.tv_usec / 1000000.0);
    return true;
}

uint64_t BucketManager::GetMinSequenceNumber() {
    return min_seq_num_;
}

bool BucketManager::readCommitLog(char*& read_buf, uint64_t& data_size)
{
    struct timeval tv, tv2;
    gettimeofday(&tv, 0);

    string commit_log_path = working_dir_ + "/commit.log";

    commit_log_fop_ = new FileOperation(kDirectIO, commit_log_maximum_size_,
	    0);
    bool ret;
    STAT_PROCESS(
    ret = commit_log_fop_->openAndReadFile(commit_log_path, read_buf,
	data_size, false),
    StatsType::DS_RECOVERY_COMMIT_LOG_READ);

    if (ret == false) {
        commit_log_fop_->createThenOpenFile(commit_log_path);
	read_buf = nullptr;
	data_size = 0;
	return true;
    }

    gettimeofday(&tv2, 0);
    printf("read commit log time: %.6lf\n", 
	    tv2.tv_sec + tv2.tv_usec / 1000000.0 - tv.tv_sec -
	    tv.tv_usec / 1000000.0);
    return true;
}

/*
 * File ID in metadata
    - file size > metadata size -> append new obj counter to metadata
    - file size == metadata size -> skip
    - file size < metadata size -> error
 * File ID not in metadata
    - file ID >= next ID:
        - kNew file -> add file to metadata
        - kGC file:
            - previous ID in metadata && prefix bit number equal:
                - find is_gc_done == true, flush success, keep kGC file, delete previous file
                - find is_gc_done == false, flush may error, remove kGC file, keep previous file
            - previous ID in metadata && prefix bit number not equal:
                - previous prefix len < current prefix len (split):
                    - Buffer splited files (should be 2)
                        - find two is_gc_done == true, remove previous file, add two new kGC file
                        - Otherwise, delete both kGC file, keep previous file
                - previous prefix len > current prefix len (merge):
                    - find is_gc_done == true, remove two previous files, keep current kGC file
                    - find is_gc_done == false, remove kGC file, keep previous files
            - previous ID not in metadata
                - find is_gc_done == true, add current kGC file
                - find is_gc_done == false, error.
    - file ID < next ID:
        - should be single file after gc or not deleted files after commit- > delete files
*/

//bool BucketManager::recoveryFromFailureOld(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo) // return key to isAnchor + value pair
//{
//    // buffer target delete file IDs
//    vector<uint64_t> targetDeleteFileIDVec;
//    // buffer no metadata kGC files generated by split
//    unordered_map<uint64_t, vector<uint64_t>> mapForBatchedkInternalGCFiles; // previous file ID to new file ID and file obj
//    vector<pair<uint64_t, BucketHandler*>> validPrefixToFileHandlerVec;
//    prefix_tree_.getCurrentValidNodes(validPrefixToFileHandlerVec);
//    unordered_map<uint64_t, pair<uint64_t, BucketHandler*>> hashStoreFileIDToPrefixMap;
//    for (auto validFileIt : validPrefixToFileHandlerVec) {
//        uint64_t currentFileID = validFileIt.second->file_id;
//        if (hashStoreFileIDToPrefixMap.find(currentFileID) != hashStoreFileIDToPrefixMap.end()) {
//            debug_error("[ERROR] Find duplicate file ID in prefixTree, file ID = %lu\n", currentFileID);
//            return false;
//        } else {
//            hashStoreFileIDToPrefixMap.insert(make_pair(currentFileID, make_pair(validFileIt.first, validFileIt.second)));
//        }
//    }
//    // process files
//    vector<uint64_t> scannedOnDiskFileIDList;
//    for (auto fileIDIt : scannedOnDiskFileIDList) {
//        if (hashStoreFileIDToPrefixMap.find(fileIDIt) == hashStoreFileIDToPrefixMap.end()) {
//            // file not exist in metadata, should scan and update into metadata
//            debug_trace("file ID = %lu not exist in metadata, try recovery\n", fileIDIt);
//            if (fileIDIt >= targetNewFileID_) {
//                // the file is newly created, should scan
//                FileOperation tempReadFileStream(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
//                string targetOpenFileName = working_dir_ + "/" + to_string(fileIDIt) + ".delta";
//                bool openCurrentFileStatus = tempReadFileStream.openFile(targetOpenFileName);
//                if (openCurrentFileStatus == false) {
//                    debug_error("[ERROR] could not open file for recovery, file path = %s\n", targetOpenFileName.c_str());
//                    return false;
//                } else {
//                    // read file header for check
//                    uint64_t targetFileSize = tempReadFileStream.getFileSize();
//                    debug_trace("target read file size = %lu\n", targetFileSize);
//                    uint64_t targetFileRemainReadSize = targetFileSize;
//                    char readContentBuffer[targetFileSize];
//                    tempReadFileStream.readFile(readContentBuffer, targetFileSize);
//                    tempReadFileStream.closeFile();
//                    // process file content
//                    bool isGCFlushedDoneFlag = false;
//                    unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMap;
//                    uint64_t currentFileObjectNumber = 
//			deconstructAndGetAllContentsFromFile(readContentBuffer,
//				targetFileRemainReadSize,
//				currentFileRecoveryMap, isGCFlushedDoneFlag);
//                }
//            } else {
//                // the file not in metadata, but ID smaller than committed ID, should delete
//                targetDeleteFileIDVec.push_back(fileIDIt);
//            }
//        } else {
//            // file exist in metadata
//            debug_trace("File ID = %lu exist in metadata, try skip or partial recovery\n", fileIDIt);
//            // get metadata file
//            BucketHandler* currentIDInMetadataFileHandlerPtr;
//            uint64_t k = hashStoreFileIDToPrefixMap.at(fileIDIt).first;
//            uint64_t prefix_u64 = prefixExtract(k);
//            prefix_tree_.get(prefix_u64, currentIDInMetadataFileHandlerPtr);
//            uint64_t onDiskFileSize = currentIDInMetadataFileHandlerPtr->io_ptr->getFileSize();
//            if (currentIDInMetadataFileHandlerPtr->total_object_bytes > onDiskFileSize) {
//                // metadata size > filesystem size, error
//                debug_error("[ERROR] file ID = %lu, file size in metadata = %lu larger than file size in file system = %lu\n", fileIDIt, currentIDInMetadataFileHandlerPtr->total_object_bytes, onDiskFileSize);
//            } else if (currentIDInMetadataFileHandlerPtr->total_object_bytes < onDiskFileSize) {
//                // file may append, should recovery
//                debug_trace("target file ID = %lu, file size (system) = %lu != file size (metadata) = %lu, try recovery\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes);
//
//                // start read
//                int targetReadSize = onDiskFileSize;
//                char readBuffer[targetReadSize];
//                debug_trace("target read file content for recovery size = %lu\n", currentIDInMetadataFileHandlerPtr->total_object_bytes);
//                currentIDInMetadataFileHandlerPtr->io_ptr->readFile(readBuffer, targetReadSize);
//                // read done, start process
//                bool isGCFlushedDoneFlag = false;
//                uint64_t recoveredObjectNumber = deconstructAndGetAllContentsFromFile(readBuffer + currentIDInMetadataFileHandlerPtr->total_object_bytes, targetReadSize - currentIDInMetadataFileHandlerPtr->total_object_bytes, targetListForRedo, isGCFlushedDoneFlag);
//                // update metadata
//                currentIDInMetadataFileHandlerPtr->total_object_cnt += recoveredObjectNumber;
//                currentIDInMetadataFileHandlerPtr->total_object_bytes += targetReadSize;
//            } else {
//                // file size match, skip current file
//                debug_trace("target file ID = %lu, file size (system) = %lu, file size (metadata) = %lu\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes);
//                continue;
//            }
//        }
//    }
//
//    // delete files
//    for (auto targetFileID : targetDeleteFileIDVec) {
//        debug_trace("Target delete file ID = %lu\n", targetFileID);
//        string targetRemoveFileName = working_dir_ + "/" + to_string(targetFileID) + ".delta";
//        auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
//        if (removeObsoleteFileStatus == -1) {
//            debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
//            return false;
//        } else {
//            debug_trace("delete the obsolete delta file, file path = %s\n", targetRemoveFileName.c_str());
//            continue;
//        }
//    }
////    bool updateMetadataStatus = UpdateHashStoreFileMetaDataList();
////    if (updateMetadataStatus == true) {
////        return true;
////    } else {
////        return false;
////    }
//    return true;
//}

// Manager's metadata management
bool BucketManager::RetriveHashStoreFileMetaDataList()
{
    bool should_recover;
    bool ret = manifest_->RetrieveFileMetadata(should_recover, id2prefixes_);
    if (ret == false) {
	debug_error("[ERROR] read metadata failed: ids %lu\n",
		id2prefixes_.size());
    }

    if (should_recover == false) {
	manifest_->CreateManifestIfNotExist();
	return true;
    }

    id2buckets_.clear();

    for (auto& it : id2prefixes_) {
	auto& file_id = it.first;
	auto& prefix = it.second;
	BucketHandler* old_hdl = nullptr;
	BucketHandler* bucket = createFileHandler();

	id2buckets_[file_id] = bucket;
        // TODO fix this
	bucket->key = "";
	bucket->file_id = file_id;

	uint64_t ret_lvl;
        // TODO for recovery
//	ret_lvl = prefix_tree_.insertWithFixedBitNumber(prefixExtract(prefix),
//		prefixLenExtract(prefix), bucket, old_hdl);

	if (ret_lvl == 0) {
	    debug_error("ret failed: old_hdl %p\n", old_hdl);
	}
    }

    return true;
}

//bool BucketManager::UpdateHashStoreFileMetaDataList()
//{
//    vector<pair<string, BucketHandler*>> validObjectVec;
//    vector<BucketHandler*> invalidObjectVec;
//    prefix_tree_.getCurrentValidNodes(validObjectVec);
//    debug_info("Start update metadata, current valid trie size = %lu\n", validObjectVec.size());
//    bool shouldUpdateFlag = false;
//    if (validObjectVec.size() != 0) {
//        for (auto it : validObjectVec) {
//            if (it.second->io_ptr->isFileOpen() == true) {
//                shouldUpdateFlag = true;
//                break;
//            }
//        }
//    }
//    if (shouldUpdateFlag == false) {
//        debug_info("Since no bucket open, should not perform metadata update, current valid file handler number = %lu\n", validObjectVec.size());
//        return true;
//    }
//    fstream pointer_fs;
//    pointer_fs.open(
//        working_dir_ + "/hashStoreFileManifest.pointer", ios::in);
//    uint64_t currentPointerInt = 0;
//    if (pointer_fs.is_open()) {
//        pointer_fs >> currentPointerInt;
//        currentPointerInt++;
//    } else {
//        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
//        return false;
//    }
//    pointer_fs.close();
//    ofstream manifest_fs;
//    manifest_fs.open(working_dir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt), ios::out);
//    manifest_fs << targetNewFileID_ << endl; // flush nextFileIDInfo
//    if (validObjectVec.size() != 0) {
//        for (auto it : validObjectVec) {
//            if (it.second->io_ptr->isFileOpen() == true) {
//                std::scoped_lock<std::shared_mutex> flush_lock(it.second->op_mtx);
////                FileOpStatus flushedSizePair = it.second->io_ptr->flushFile();
////                StatsRecorder::getInstance()->DeltaOPBytesWrite(flushedSizePair.physicalSize_, flushedSizePair.logicalSize_, syncStatistics_);
//                debug_trace("flushed file ID = %lu, file correspond prefix = %s\n", it.second->file_id, it.first.c_str());
////                it.second->total_on_disk_bytes += flushedSizePair.physicalSize_;
//                manifest_fs << it.first << endl;
//                manifest_fs << it.second->file_id << endl;
//                manifest_fs << it.second->prefix << endl;
//                manifest_fs << it.second->total_object_cnt << endl;
//                manifest_fs << it.second->total_object_bytes << endl;
//                manifest_fs << it.second->total_on_disk_bytes << endl;
//            }
//        }
//        manifest_fs.flush();
//        manifest_fs.close();
//    }
//    // Update manifest pointer
//    fstream hashStoreFileManifestPointerUpdateStream;
//    hashStoreFileManifestPointerUpdateStream.open(
//        working_dir_ + "/hashStoreFileManifest.pointer", ios::out);
//    if (hashStoreFileManifestPointerUpdateStream.is_open()) {
//        hashStoreFileManifestPointerUpdateStream << currentPointerInt;
//        hashStoreFileManifestPointerUpdateStream.flush();
//        hashStoreFileManifestPointerUpdateStream.close();
//        string targetRemoveFileName = working_dir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt - 1);
//        if (filesystem::exists(targetRemoveFileName) != false) {
//            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
//            if (removeOldManifestStatus == -1) {
//                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
//            }
//        }
//        prefix_tree_.getInvalidNodesNoKey(invalidObjectVec);
//        debug_info("Start delete obslate files, current invalid trie size = %lu\n", invalidObjectVec.size());
//        if (invalidObjectVec.size() != 0) {
//            for (auto it : invalidObjectVec) {
//                if (it) {
//                    if (it->io_ptr->isFileOpen() == true) {
//                        it->io_ptr->closeFile();
//                        debug_trace("Closed file ID = %lu\n", it->file_id);
//                    }
//                }
//            }
//        }
//        bucket_delete_mtx_.lock();
//        for (auto it : bucket_id_to_delete_) {
//            deleteObslateFileWithFileIDAsInput(it);
//        }
//        bucket_id_to_delete_.clear();
//        bucket_delete_mtx_.unlock();
//        return true;
//    } else {
//        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
//        return false;
//    }
//}

bool BucketManager::RemoveObsoleteFiles() {
    // Update manifest pointer
//    vector<BucketHandler*> invalidObjectVec;
//    prefix_tree_.getInvalidNodesNoKey(invalidObjectVec);
//    debug_info("Start delete obslate files, current invalid trie size = %lu\n", invalidObjectVec.size());
//    if (invalidObjectVec.size() != 0) {
//        for (auto it : invalidObjectVec) {
//            if (it) {
//                if (it->io_ptr->isFileOpen() == true) {
//                    it->io_ptr->closeFile();
//                    debug_trace("Closed file ID = %lu\n", it->file_id);
//                }
//            }
//        }
//    }
    bucket_delete_mtx_.lock();
    for (auto it : bucket_id_to_delete_) {
        deleteObslateFileWithFileIDAsInput(it);
    }
    bucket_id_to_delete_.clear();
    bucket_delete_mtx_.unlock();
    return true;
}

bool
BucketManager::prepareForUpdatingMetadata(
        vector<BucketHandler*>& vec)
{
    prefix_tree_.getCurrentValidNodesNoKey(vec);
    return true;
}

bool BucketManager::CloseHashStoreFileMetaDataList()
{
    delete manifest_;
//    fstream pointer_fs;
//    pointer_fs.open(
//        working_dir_ + "/hashStoreFileManifest.pointer", ios::in);
//    uint64_t currentPointerInt = 0;
//    if (pointer_fs.is_open()) {
//        pointer_fs >> currentPointerInt;
//        currentPointerInt++;
//    } else {
//        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
//        return false;
//    }
//    pointer_fs.close();
//    ofstream manifest_fs;
//    manifest_fs.open(working_dir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt), ios::out);
//    manifest_fs << targetNewFileID_ << endl; // flush nextFileIDInfo
//    vector<uint64_t> targetDeleteFileIDVec;
//    vector<pair<string, BucketHandler*>> validObjectVec;
//    prefix_tree_.getCurrentValidNodes(validObjectVec);
//    debug_info("Final commit metadata, current valid trie size = %lu\n", validObjectVec.size());
//    if (validObjectVec.size() != 0) {
//        for (auto it : validObjectVec) {
//            if (it.second->io_ptr->isFileOpen() == true) {
//                FileOpStatus flushedSizePair = it.second->io_ptr->flushFile();
//                StatsRecorder::getInstance()->DeltaOPBytesWrite(flushedSizePair.physicalSize_, flushedSizePair.logicalSize_, syncStatistics_);
//                it.second->total_on_disk_bytes += flushedSizePair.physicalSize_;
//                it.second->io_ptr->closeFile();
//                manifest_fs << it.first << endl;
//                manifest_fs << it.second->file_id << endl;
//                manifest_fs << it.second->prefix << endl;
//                manifest_fs << it.second->total_object_cnt << endl;
//                manifest_fs << it.second->total_object_bytes << endl;
//                manifest_fs << it.second->total_on_disk_bytes << endl;
//            }
//        }
//        manifest_fs.flush();
//        manifest_fs.close();
//    }
//    // Update manifest pointer
//    fstream hashStoreFileManifestPointerUpdateStream;
//    hashStoreFileManifestPointerUpdateStream.open(
//        working_dir_ + "/hashStoreFileManifest.pointer", ios::out);
//    if (hashStoreFileManifestPointerUpdateStream.is_open()) {
//        hashStoreFileManifestPointerUpdateStream << currentPointerInt << endl;
//        bool closedSuccessFlag = true;
//        hashStoreFileManifestPointerUpdateStream << closedSuccessFlag << endl;
//        hashStoreFileManifestPointerUpdateStream.flush();
//        hashStoreFileManifestPointerUpdateStream.close();
//        string targetRemoveFileName = working_dir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt - 1);
//        if (filesystem::exists(targetRemoveFileName) != false) {
//            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
//            if (removeOldManifestStatus == -1) {
//                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
//            }
//        }
//        vector<pair<string, BucketHandler*>> possibleValidObjectVec;
//        prefix_tree_.getPossibleValidNodes(possibleValidObjectVec);
//        for (auto it : possibleValidObjectVec) {
//            if (it.second) {
//                if (it.second->io_ptr->isFileOpen() == true) {
//                    it.second->io_ptr->closeFile();
//                }
//		deleteFileHandler(it.second);
//            }
//        }
//        bucket_delete_mtx_.lock();
//        for (auto it : bucket_id_to_delete_) {
//            deleteObslateFileWithFileIDAsInput(it);
//        }
//        bucket_id_to_delete_.clear();
//        bucket_delete_mtx_.unlock();
//        return true;
//    } else {
//        debug_error("[ERROR] could not update hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
//        vector<pair<string, BucketHandler*>> possibleValidObjectVec;
//        prefix_tree_.getPossibleValidNodes(possibleValidObjectVec);
//        for (auto it : possibleValidObjectVec) {
//            if (it.second) {
//                if (it.second->io_ptr->isFileOpen() == true) {
//                    it.second->io_ptr->closeFile();
//                }
//		deleteFileHandler(it.second);
//            }
//        }
//        return false;
//    }
    return true;
}

// file operations - public
// A modification: add "getForAnchorWriting". If true, and if the file handler
// does not exist, do not create the file and directly return.
bool BucketManager::getFileHandlerWithKey(const char* keyBuffer, 
        uint32_t keySize, deltaStoreOperationType op_type,
        BucketHandler*& bucket, bool getForAnchorWriting)
{
    struct timeval tv;
    string key(keyBuffer, keySize);

    // 1. Generate prefix
    gettimeofday(&tv, 0);
    if (op_type == kMultiPut) {
        StatsRecorder::staticProcess(
                StatsType::DSTORE_MULTIPUT_PREFIX, tv);
    } else {
        StatsRecorder::staticProcess(
                StatsType::DSTORE_PREFIX, tv);
    }

    // 2. Search the prefix tree 
    bool getFileHandlerStatus;
    gettimeofday(&tv, 0);
    getFileHandlerStatus = getHashStoreFileHandlerByPrefix(key, bucket);
    if (op_type == kMultiPut) {
        StatsRecorder::staticProcess(
                StatsType::DSTORE_MULTIPUT_GET_HANDLER, tv);
    }

    if (getFileHandlerStatus == false && 
            (op_type == kGet || op_type == kMultiGet)) {
        bucket = nullptr;
        return true;
    }

    if (getFileHandlerStatus == false && getForAnchorWriting == true) {
        bucket = nullptr;
        return true;
    }

    // 4. Create file if necessary 
    if (getFileHandlerStatus == false) {
        // not writing anchors, or reading the file. Need to create
        bool createNewFileHandlerStatus;
        // TODO there may be some sync problems
        // solution: For get, use "get"; For put, use "getOrCreate".
        STAT_PROCESS(createNewFileHandlerStatus =
                createAndGetNewHashStoreFileHandlerByPrefixForUser(bucket),
                StatsType::KDSep_HASHSTORE_CREATE_NEW_BUCKET);
        if (!createNewFileHandlerStatus || bucket == nullptr) {
            debug_error("[ERROR] create new bucket, key %.*s\n", 
                    (int)keySize, keyBuffer);
            return false;
        } else {
            debug_info("[Insert] Create new file ID = %lu, for key = %.*s, file"
                    " gc status flag = %d\n",
                    bucket->file_id, (int)keySize, keyBuffer,
                    bucket->gc_status);
            if (op_type == kMultiPut) {
                bucket->markedByMultiPut = true;
            } else if (op_type == kMultiGet) {
                bucket->markedByMultiGet = true;
            }
            bucket->ownership = 1;
            return true;
        }
    }
    
    // Get the handler
    struct timeval tv_loop;
    gettimeofday(&tv_loop, 0);
    bool first = true;
    while (true) {
        if (first == false) {
            gettimeofday(&tv, 0);
            getFileHandlerStatus =
                getHashStoreFileHandlerByPrefix(key, bucket);
            if (op_type == kMultiPut) {
                StatsRecorder::staticProcess(
                        StatsType::DSTORE_MULTIPUT_GET_HANDLER, tv);
            }
        }
        first = false;

        if (getFileHandlerStatus == false
                && (op_type == kPut || op_type == kMultiPut)) {
            debug_error("come here %d\n", 1);
            bool createNewFileHandlerStatus;
            STAT_PROCESS(createNewFileHandlerStatus =
                    createAndGetNewHashStoreFileHandlerByPrefixForUser(bucket),
                    StatsType::KDSep_HASHSTORE_CREATE_NEW_BUCKET);
            if (!createNewFileHandlerStatus) {
                debug_error("[ERROR] Previous file may deleted during GC,"
                        " and splited new files not contains current key"
                        " prefix, create new bucket for put operation"
                        " error, key = %s\n", keyBuffer);
                return false;
            } else {
                debug_warn("[Insert] Previous file may deleted during GC, and"
                        " splited new files not contains current key prefix,"
                        " create new file ID %lu, for key %s, file gc "
                        "status flag %d, prefix bit number used %lu\n",
                        bucket->file_id, keyBuffer, bucket->gc_status,
                        0ul//prefixLenExtract(bucket->prefix)
                        );
                if (op_type == kMultiPut) {
                    bucket->markedByMultiPut = true;
                } else {
                    bucket->markedByMultiGet = true;
                }
                bucket->ownership = 1;
                return true;
            }
        } else {
            if (bucket->ownership == 1 && 
                    ((op_type == kMultiPut && 
                     bucket->markedByMultiPut == true) ||
                    (op_type == kMultiGet &&
                      bucket->markedByMultiGet == true))) {
                StatsRecorder::staticProcess(
                        StatsType::DSTORE_GET_HANDLER_LOOP, tv_loop);
                return true;
            }
            // avoid get file handler which is in GC;
            if (bucket->ownership != 0) {
//                debug_error("Wait for file ownership, file ID = %lu, "
//                        " own = %d, gc status %d\n", bucket->file_id, 
//                        (int)bucket->ownership,
//                        (int)bucket->gc_status);
                debug_trace("Wait for file ownership, file ID = %lu, for"
                        " key = %s\n", bucket->file_id, keyBuffer);
                struct timeval tv, tv2, tv3;
                gettimeofday(&tv, 0);
                tv3 = tv;// for recording wait
                int own = bucket->ownership;
                while (bucket->ownership == -1 ||
                        (bucket->ownership == 1 && 
                         (!(op_type == kMultiPut && bucket->markedByMultiPut)
                          &&
                         !(op_type == kMultiGet &&
                             bucket->markedByMultiGet)))) {
                    gettimeofday(&tv2, 0);
                    if (tv2.tv_sec - tv.tv_sec > 10) {
                        debug_error("wait for 5 seconds; own %d, id %d, op %d\n",
                                (int)bucket->ownership,
                                (int)bucket->file_id,
                                (int)op_type);
                        tv = tv2;
                    }
//                    asm volatile("");
                    // wait if file is using in gc
                }
                debug_trace("Wait for file ownership, file ID = %lu, for"
                        " key = %s over\n", bucket->file_id,
                        keyBuffer);
                if (own == -1) {
                    StatsRecorder::staticProcess(StatsType::WAIT_GC, tv3);
                } else {
                    StatsRecorder::staticProcess(StatsType::WAIT_NORMAL, tv3);
                }
            }

            if (bucket->gc_status == kShouldDelete) {
                // retry if the file should delete;
                debug_warn("Get exist file ID = %lu, for key = %s, "
                        "this file is marked as kShouldDelete\n",
                        bucket->file_id, keyBuffer);
                continue;
            } else {
                debug_trace("Get exist file ID = %lu, for key = %s\n",
                        bucket->file_id, keyBuffer);
                if (op_type == kMultiPut) {
                    bucket->markedByMultiPut = true;
                } else if (op_type == kMultiGet) {
                    bucket->markedByMultiGet = true;
                }
                bucket->ownership = 1;
                StatsRecorder::staticProcess(
                        StatsType::DSTORE_GET_HANDLER_LOOP, tv_loop);
                return true;
            }
        }
    }
    return true;
}

bool BucketManager::getFileHandlerWithKeySimplified(const char* keyBuffer, 
        uint32_t keySize, deltaStoreOperationType op_type,
        BucketHandler*& bucket, bool getForAnchorWriting) {
    struct timeval tv;
    string key(keyBuffer, keySize);

    // 1. Generate prefix
    gettimeofday(&tv, 0);
    if (op_type == kMultiPut) {
        StatsRecorder::staticProcess(
                StatsType::DSTORE_MULTIPUT_PREFIX, tv);
    } else {
        StatsRecorder::staticProcess(
                StatsType::DSTORE_PREFIX, tv);
    }

    // 2. Search the prefix tree 
    bool s;
    gettimeofday(&tv, 0);
    if (op_type == kGet || op_type == kMultiGet || 
            ((op_type == kPut || op_type == kMultiPut) 
             && getForAnchorWriting)) {
         s = getBucketHandlerNoCreate(key, op_type, bucket);
         // What ever, return true 
//         if (s == false) {
//             return true;
//         }
//         return true;
    } else {
        // Need to create
        s = getBucketHandlerOrCreate(key, op_type, bucket);
        if (s == false) {
            debug_error("Cannot get or create buckets for key %.*s\n",
                    (int)keySize, keyBuffer);
            return false; // different from get!
        }
    }
    return true;
}

// file operations - private
bool BucketManager::generateHashBasedPrefix(const char* rawStr, 
        uint32_t strSize, uint64_t& prefixU64) {
//    u_char murmurHashResultBuffer[16];
//    MurmurHash3_x64_128((void*)rawStr, strSize, 0, murmurHashResultBuffer);
//    memcpy(&prefixU64, murmurHashResultBuffer, sizeof(uint64_t));
    prefixU64 = XXH64(rawStr, strSize, 10);
    return true;
}

bool BucketManager::getHashStoreFileHandlerByPrefix(
        const string& prefixU64, 
        BucketHandler*& bucket)
{
    bool handlerGetStatus = prefix_tree_.get(prefixU64, bucket);
    if (handlerGetStatus == true) {
        return true;
    } else {
        return false;
    }
}

bool BucketManager::getBucketHandlerNoCreate(const string& key, 
        deltaStoreOperationType op_type, BucketHandler*& bucket)
{
    bool s;
    s = prefix_tree_.get(key, bucket);
    if (s == false) {
        return s;
    }

    while (true) {
        if (s == false) {
            // impossible
            return s;
        }

        if (bucket->ownership == 1 &&
                ((op_type == kMultiPut && bucket->markedByMultiPut
                  && !bucket->markedByMultiGet) ||
                (op_type == kMultiGet && bucket->markedByMultiGet
                 && !bucket->markedByMultiPut))) {
            return true;
        }

        if (bucket->ownership != 0) {
            // wait if file is using in gc
            debug_trace("Wait for file ownership, file ID = %lu, for"
                    " key = %s\n", bucket->file_id, key.c_str());
            struct timeval tv, tv2, tv3;
            gettimeofday(&tv, 0);
            tv3 = tv;// for recording wait
            int own = bucket->ownership;
            while (bucket->ownership == -1 ||
                    (bucket->ownership == 1 && 
                     (!(op_type == kMultiPut && bucket->markedByMultiPut)
                      &&
                     !(op_type == kMultiGet &&
                         bucket->markedByMultiGet)))) {
                gettimeofday(&tv2, 0);
                if (tv2.tv_sec - tv.tv_sec > 5) {
                    debug_error("wait for 5 seconds; own %d, id %d, op %d\n",
                            (int)bucket->ownership,
                            (int)bucket->file_id,
                            (int)op_type);
                    tv = tv2;
                }
            }
            debug_trace("Wait for file ownership, file ID = %lu, for"
                    " key = %s over\n", bucket->file_id,
                    key.c_str());
            if (own == -1) {
                StatsRecorder::staticProcess(StatsType::WAIT_GC, tv3);
            } else {
                StatsRecorder::staticProcess(StatsType::WAIT_NORMAL, tv3);
            }
        }

        if (bucket->gc_status == kShouldDelete) {
            s = prefix_tree_.get(key, bucket);
            continue;
        } else {
            if (op_type == kMultiPut) {
                bucket->markedByMultiPut = true;
            } else if (op_type == kMultiGet) {
                bucket->markedByMultiGet = true;
            }
            bucket->ownership = 1;
            return true;
        }
    }
    return true;
}

bool BucketManager::getBucketHandlerOrCreate(const string& key,
        deltaStoreOperationType op_type, BucketHandler*& bucket) {
    bool s;
    {
        std::scoped_lock<std::shared_mutex> w_lock(createNewBucketMtx_);
        s = prefix_tree_.get(key, bucket);
        if (s == false) {
            s = createNewInitialBucket(bucket);
            if (s == false) {
                debug_error("create initial bucket failed, key %s\n",
                        key.c_str());
                return false;
            }
        }
    }

    while (true) {
        if (s == false) {
            // impossible
            return s;
        }

        if (bucket->ownership == 1 &&
                (op_type == kMultiPut && bucket->markedByMultiPut
                  && !bucket->markedByMultiGet)) {
            return true;
        }

        if (bucket->ownership != 0) {
            // wait if file is using in gc
            debug_trace("Wait for file ownership, file ID = %lu, for"
                    " key = %s\n", bucket->file_id, key.c_str());
            struct timeval tv, tv2, tv3;
            gettimeofday(&tv, 0);
            tv3 = tv;// for recording wait
            int own = bucket->ownership;
            while (bucket->ownership == -1 ||
                    (bucket->ownership == 1 && 
                     !(op_type == kMultiPut && bucket->markedByMultiPut))) {
                gettimeofday(&tv2, 0);
                if (tv2.tv_sec - tv.tv_sec > 5) {
                    debug_error("wait for 5 seconds; own %d, id %d, op %d\n",
                            (int)bucket->ownership,
                            (int)bucket->file_id,
                            (int)op_type);
                    tv = tv2;
                }
            }
            debug_trace("Wait for file ownership, file ID = %lu, for"
                    " key = %s over\n", bucket->file_id,
                    key.c_str());
            if (own == -1) {
                StatsRecorder::staticProcess(StatsType::WAIT_GC, tv3);
            } else {
                StatsRecorder::staticProcess(StatsType::WAIT_NORMAL, tv3);
            }
        }

        if (bucket->gc_status == kShouldDelete) {
            s = prefix_tree_.get(key, bucket);
            continue;
        } else {
            if (op_type == kMultiPut) {
                bucket->markedByMultiPut = true;
            } 
            bucket->ownership = 1;
            return true;
        }
    }
    return true;
}

bool
BucketManager::createNewInitialBucket(BucketHandler*& bucket)
{
    BucketHandler* tmp_bucket = createFileHandler();
    bool status = prefix_tree_.insert("", tmp_bucket);
    if (status == false) {
        debug_e("Insert failed!\n");
	return false;
    }

    // initialize the file handler
    tmp_bucket->file_id = generateNewFileID();
    tmp_bucket->ownership = 0;
    tmp_bucket->gc_status = kNew;
    tmp_bucket->total_object_bytes = 0;
    tmp_bucket->total_on_disk_bytes = 0;
    tmp_bucket->total_object_cnt = 0;
//    tmp_bucket->prefix = prefixConcat(prefix_u64, finalInsertLevel);
    tmp_bucket->key = "";

    bucket = tmp_bucket;
    if (enable_crash_consistency_) {
        // TODO modify manifest
	manifest_->InitialSnapshot(bucket);
    }
    return true;
}

bool
BucketManager::createAndGetNewHashStoreFileHandlerByPrefixForUser(BucketHandler*& bucket)
{
    BucketHandler* tmp_bucket = createFileHandler();
    // move pointer for return
    bool status = prefix_tree_.insert("", tmp_bucket);
    if (status == false) {
        debug_e("Insert failed!\n");
	return false;
    }

    // initialize the file handler
    tmp_bucket->file_id = generateNewFileID();
    tmp_bucket->ownership = 0;
    tmp_bucket->gc_status = kNew;
    tmp_bucket->total_object_bytes = 0;
    tmp_bucket->total_on_disk_bytes = 0;
    tmp_bucket->total_object_cnt = 0;
//    tmp_bucket->prefix = prefixConcat(prefix_u64, finalInsertLevel);
    tmp_bucket->key = "";

    bucket = tmp_bucket;
    if (enable_crash_consistency_) {
        // TODO modify manifest
	manifest_->InitialSnapshot(bucket);
    }
    return true;
}

bool BucketManager::createFileHandlerForGC(const string& key, 
        BucketHandler*& ret_bucket)
{
    auto bucket = createFileHandler();
//    bucket->prefix = prefixConcat(0, targetPrefixLen);
    bucket->key = key;
    bucket->file_id = generateNewFileID();
    bucket->ownership = -1;
    bucket->gc_status = kNew;
    bucket->total_object_bytes = 0;
    bucket->total_on_disk_bytes = 0;
    bucket->total_object_cnt = 0;
    // write header to current file
    string targetFilePathStr = working_dir_ + "/" +
        to_string(bucket->file_id) + ".delta";
    bool createAndOpenNewFileStatus =
        bucket->io_ptr->createThenOpenFile(targetFilePathStr);
    if (createAndOpenNewFileStatus == true) {
        // move pointer for return
        debug_info("Newly created file ID = %lu\n", bucket->file_id);
        ret_bucket = bucket;
        return true;
    } else {
        debug_error("[ERROR] Could not create file ID = %lu\n",
                bucket->file_id);
        ret_bucket = nullptr;
        return false;
    }
}

uint64_t BucketManager::generateNewFileID()
{
    fileIDGeneratorMtx_.lock();
    targetNewFileID_ += 1;
    uint64_t tempIDForReturn = targetNewFileID_;
//    uint64_t tempIDForReturn = bucket_bitmap_->getFirstZeroAndFlip(); 
//    if (tempIDForReturn == -1) {
//        debug_error("generate error: -1 %s\n", "")
//    }
    fileIDGeneratorMtx_.unlock();
    return tempIDForReturn;
}

pair<uint64_t, uint64_t>
BucketManager::deconstructAndGetValidContentsFromFile(
        char* read_buf, uint64_t buf_size, 
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t>& resultMap)
{
    uint64_t valid_obj_num = 0;
    uint64_t obj_num = 0;

    uint64_t read_i = 0;
    size_t header_sz = sizeof(KDRecordHeader);
    // skip file header

    while (read_i < buf_size) {
        obj_num++;
        KDRecordHeader header;
        if (use_varint_d_header == false) {
            memcpy(&header, read_buf + read_i, header_sz);
        } else {
            header = GetDeltaHeaderVarint(read_buf + read_i, header_sz);
        }
        read_i += header_sz;
        if (header.is_gc_done_ == true) {
            // skip since it is gc flag, no content.
            continue;
        }
        // get key str_t
        str_t currentKey(read_buf + read_i, header.key_size_);
        read_i += header.key_size_;
        if (header.is_anchor_ == true) {
            auto mapIndex = resultMap.find(currentKey);
            if (mapIndex != resultMap.end()) {
                valid_obj_num -= (mapIndex->second.first.size() + 1);
                mapIndex->second.first.clear();
                mapIndex->second.second.clear();
            }
        } else {
            valid_obj_num++;
            auto mapIndex = resultMap.find(currentKey);
            if (mapIndex != resultMap.end()) {
                str_t currentValueStr(read_buf + read_i, header.value_size_);
                mapIndex->second.first.push_back(currentValueStr);
                mapIndex->second.second.push_back(header);
            } else {
                vector<str_t> newValuesRelatedToCurrentKeyVec;
                vector<KDRecordHeader> newRecorderHeaderVec;
                str_t currentValueStr(read_buf + read_i, header.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                newRecorderHeaderVec.push_back(header);
                resultMap.insert(make_pair(currentKey, make_pair(newValuesRelatedToCurrentKeyVec, newRecorderHeaderVec)));
            }
            read_i += header.value_size_;
        }
    }

    if (read_i > buf_size) {
        debug_error("index error: %lu v.s. %lu\n", read_i, buf_size);
	return make_pair(0, 0);
    }
    debug_info("deconstruct current file done, find different key number = "
            "%lu, total processed object number = %lu, target keep object "
            "number = %lu\n", resultMap.size(), obj_num, valid_obj_num);
    return make_pair(valid_obj_num, obj_num);
}

uint64_t BucketManager::partialMergeGcResultMap(
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t>& gcResultMap, unordered_set<str_t,
        mapHashKeyForStr_t, mapEqualKeForStr_t>& shouldDelete) {

    shouldDelete.clear();
    uint64_t reducedObjectsNumber = 0;

    for (auto& keyIt : gcResultMap) {
        auto& key = keyIt.first;
        auto& values = keyIt.second.first;
        auto& headers = keyIt.second.second;
        if (values.size() >= 2) {
            reducedObjectsNumber += values.size() - 1;
            shouldDelete.insert(keyIt.first);
//            for (auto i = 0; i < keyIt.second.first.size(); i++) {
//                debug_error("value size %d %.*s\n", keyIt.second.first[i].size_, keyIt.second.first[i].size_, keyIt.second.first[i].data_); 
//            }

            str_t result;
            vector<KDRecordHeader> headerVec;
            KDRecordHeader newRecordHeader;

            KDSepMergeOperatorPtr_->PartialMerge(values, result);

            newRecordHeader.key_size_ = key.size_;
            newRecordHeader.value_size_ = result.size_; 
            // the largest sequence number
            newRecordHeader.seq_num =
                headers[headers.size()-1].seq_num;
            newRecordHeader.is_anchor_ = false;
            headerVec.push_back(newRecordHeader);

            vector<str_t> resultVec;
            resultVec.push_back(result);

            keyIt.second = make_pair(resultVec, headerVec); // directly update the map

//            debug_error("after partial merge %d %.*s\n", result.size_, result.size_, result.data_); 
        }
    }

    return reducedObjectsNumber;
}

inline void BucketManager::clearMemoryForTemporaryMergedDeltas(
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t>& resultMap, 
        unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t>&
        shouldDelete)
{
    for (auto& it : shouldDelete) {
        delete[] resultMap[it].first[0].data_;
    }
}

inline void BucketManager::putKeyValueListToAppendableCache(
        const str_t& currentKeyStr, vector<str_t>& values) {
    vector<str_t>* cacheVector = new vector<str_t>;
    for (auto& it : values) {
        str_t value_str(new char[it.size_], it.size_);
        memcpy(value_str.data_, it.data_, it.size_);
        cacheVector->push_back(value_str);
    }

    str_t keyStr = currentKeyStr;

    keyToValueListCacheStr_->updateCache(keyStr, cacheVector);
}

inline void BucketManager::putKDToCache(
        const str_t& currentKeyStr, vector<str_t>& values) {
    if (values.size() != 1) {
        debug_error("value number not 1: %lu\n", values.size()); 
        exit(1);
    }
    for (auto& it : values) {
        str_t keyStr = currentKeyStr;
        str_t value_str(new char[it.size_], it.size_);
        memcpy(value_str.data_, it.data_, it.size_);
        kd_cache_->updateCache(keyStr, value_str);
    }
}

bool BucketManager::singleFileRewrite(
        BucketHandler* bucket, 
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t>& gcResultMap, 
        uint64_t targetSizeWithHeader, bool fileContainsReWriteKeysFlag)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    // file before:
    // [[record_header] [key] [value]] ... [record_header]
    // file after:
    // [index block] [[record_header] [key] [value]] ... [record_header]

    // Write header 
//    uint64_t beforeRewriteSize = bucket->total_on_disk_bytes;
//    uint64_t beforeRewriteBytes = bucket->total_object_bytes;
    uint64_t newObjectNumber = 0;
    uint64_t write_i = 0;
    uint64_t new_id = generateNewFileID();
    uint64_t old_id = bucket->file_id;

    size_t header_sz = sizeof(KDRecordHeader);

    uint64_t targetFileSize = targetSizeWithHeader;
    if (enable_index_block_) {
        // create or clear the index block
        if (bucket->index_block == nullptr) {
            bucket->index_block = new BucketIndexBlock();
        } else {
            bucket->index_block->Clear();
        }

        // select keys for building index block
        for (auto keyIt : gcResultMap) {
            size_t total_kd_size = 0;
            auto& key = keyIt.first;

            for (auto i = 0; i < keyIt.second.first.size(); i++) {
                auto& value = keyIt.second.first[i];
                auto& header = keyIt.second.second[i];
                if (use_varint_d_header == true) {
                    header_sz = GetDeltaHeaderVarintSize(header);
                }
                total_kd_size += header_sz + key.size_ + value.size_;
            }

            if (total_kd_size > 0) {
                bucket->index_block->Insert(key, total_kd_size);
            }
        }

        bucket->index_block->Build();
        // do not write the index block to the file
    }

    char write_buf[targetFileSize];

    // copy the file header in the end
    //copyInc(write_buf, write_i, &file_header, sizeof(hashStoreFileHeader));
    StatsRecorder::staticProcess(StatsType::REWRITE_GET_FILE_ID, tv);

    // Write file
    // Now the keys should be written in a sorted way 
    gettimeofday(&tv, 0);
    bucket->sorted_filter->Clear();
    bucket->filter->Clear();

    if (enable_index_block_) {
        for (auto& sorted_it : bucket->index_block->indices) {
            // can optimize
            auto keyIt =
                gcResultMap.find(str_t(const_cast<char*>(sorted_it.first.data()),
                            sorted_it.first.size()));
            auto& key = keyIt->first;
            if (keyIt == gcResultMap.end()) {
                debug_error("data not found! key %.*s\n", 
                        (int)sorted_it.first.size(), sorted_it.first.data());
                exit(1);
            }

            for (auto vec_i = 0; vec_i < keyIt->second.first.size(); vec_i++) {
                auto& value = keyIt->second.first[vec_i];
                auto& header = keyIt->second.second[vec_i];
                newObjectNumber++;
                if (use_varint_d_header == false) {
                    copyInc(write_buf, write_i, &header, header_sz);
                } else {
                    write_i += PutDeltaHeaderVarint(write_buf + write_i,
                            header);
                }
                copyInc(write_buf, write_i, key.data_, key.size_);
                copyInc(write_buf, write_i, value.data_, value.size_);
            }
            if (keyIt->second.first.size() > 0) {
                bucket->sorted_filter->Insert(key);
            }
        }
        bucket->index_block->IndicesClear();
    } else {
        for (auto& keyIt : gcResultMap) {
            auto& key = keyIt.first;
            auto& values = keyIt.second.first;
            for (auto vec_i = 0; vec_i < values.size(); vec_i++) {
                newObjectNumber++;
                auto& value = keyIt.second.first[vec_i];
                auto& header = keyIt.second.second[vec_i];
                if (use_varint_d_header == false) {
                    copyInc(write_buf, write_i, &header, header_sz);
                } else {
                    write_i += PutDeltaHeaderVarint(write_buf + write_i,
                            header);
                }
                copyInc(write_buf, write_i, key.data_, key.size_);
                copyInc(write_buf, write_i, value.data_, value.size_);
            }
            if (keyIt.second.first.size() > 0) {
                bucket->filter->Insert(key);
            }
        }
    }

    // add gc done flag into bucket file
    KDRecordHeader gc_done_record_header;
    gc_done_record_header.is_anchor_ = false;
    gc_done_record_header.is_gc_done_ = true;
    gc_done_record_header.seq_num = 0;
    gc_done_record_header.key_size_ = 0;
    gc_done_record_header.value_size_ = 0;

    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &gc_done_record_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i,
                gc_done_record_header);
    }

    // copy the file header finally
    if (enable_index_block_) {
	bucket->unsorted_part_offset = write_i;
    } else {
	bucket->unsorted_part_offset = 0;
    }

    debug_trace("Rewrite done buffer size = %lu\n", write_i);

    string filename = working_dir_ + "/" + to_string(new_id) + ".delta";
    StatsRecorder::staticProcess(StatsType::REWRITE_ADD_HEADER, tv);
    gettimeofday(&tv, 0);
    // create since file not exist
    if (bucket->io_ptr->isFileOpen() == true) {
        bucket->io_ptr->closeFile();
    } // close old file
    bucket->io_ptr->createThenOpenFile(filename);

    if (bucket->io_ptr->isFileOpen() == false) {
	debug_error("[ERROR] Could not open new file ID = %lu, for old file "
		"ID = %lu for single file rewrite\n", new_id, old_id);
        bucket_delete_mtx_.lock();
        bucket_id_to_delete_.push_back(new_id);
        bucket_delete_mtx_.unlock();
        return false;
    }

    // write content and update current file stream to new one.
    FileOpStatus onDiskWriteSizePair;
    if (enable_crash_consistency_) {
	STAT_PROCESS(onDiskWriteSizePair =
		bucket->io_ptr->writeAndFlushFile(write_buf,
		    write_i), StatsType::KDSep_GC_WRITE);
    } else {
	STAT_PROCESS(onDiskWriteSizePair =
		bucket->io_ptr->writeFile(write_buf,
		    write_i), StatsType::KDSep_GC_WRITE);
    }

    bucket->io_ptr->markDirectDataAddress(write_i);
    StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
    debug_trace("Rewrite done file size = %lu, file path = %s\n", write_i,
	    filename.c_str());
    // update metadata
    bucket->file_id = new_id;
    bucket->total_object_cnt = newObjectNumber + 1;
    bucket->total_object_bytes = write_i;
    bucket->total_on_disk_bytes = onDiskWriteSizePair.physicalSize_;
    debug_trace("Rewrite file size in metadata = %lu, file ID = %lu\n",
	    bucket->total_object_bytes, bucket->file_id);
    // remove old file
    bucket_delete_mtx_.lock();
    bucket_id_to_delete_.push_back(old_id);
    bucket_delete_mtx_.unlock();
    // check if after rewrite, file size still exceed threshold, mark as no GC.
    if (bucket->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
	bucket->gc_status = kNoGC;

	if (write_stall_ != nullptr) {
	    //                debug_error("Start to rewrite, key number %lu\n",
	    //                        gcResultMap.size());
	    vector<writeBackObject*> objs;
	    objs.resize(gcResultMap.size());
	    int obji = 0;
	    bucket->num_anchors = 0;
	    for (auto& it : gcResultMap) {
		string k(it.first.data_, it.first.size_);
		writeBackObject* obj = new writeBackObject(k, "", 0);
		objs[obji++] = obj;
	    }
	    *write_stall_ = true; 
	    //                debug_error("Start to push %lu\n", gcResultMap.size());
	    pushObjectsToWriteBackQueue(objs);
	    //                debug_error("push end %lu\n", gcResultMap.size());
	}
    }

    // rewrite completed: flush the rewrite metadata
    if (enable_crash_consistency_) {
        // TODO update consistency
	STAT_PROCESS(
//	manifest_->UpdateGCMetadata(old_id, bucket->prefix,
//		new_id, bucket->prefix),
        manifest_->UpdateGCMetadata(old_id, 0, new_id, 0),
	StatsType::DS_MANIFEST_GC_REWRITE);
    }

    debug_info("flushed new file to filesystem since single file gc, the"
	    " new file ID = %lu, corresponding previous file ID = %lu,"
	    " target file size = %lu\n", 
	    new_id, old_id, write_i);
    return true;
}

void BucketManager::writeSingleSplitFile(BucketHandler* new_bucket, 
        vector<pair<map<str_t, uint64_t, mapSmallerKeyForStr_t>, uint64_t>>&
        tmpGcResult, 
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>, 
        mapSmallerKeyForStr_t>& gcResultMap, 
        int bi, boost::atomic<int>& write_fin_number)
{
    auto& keyToSizeMap = tmpGcResult[bi].first;
    new_bucket->filter->Clear();
    debug_info("Generate new file since split GC, target file ID = %lu\n",
            new_bucket->file_id);

    uint64_t targetFileSize = tmpGcResult[bi].second + sizeof(KDRecordHeader);

    if (enable_index_block_) {
        // create or clear the index block
        if (new_bucket->index_block == nullptr) {
            new_bucket->index_block = new BucketIndexBlock();
        } else {
            new_bucket->index_block->Clear();
        }

        // select keys for building index block
        for (auto keyToSizeIt : keyToSizeMap) {
            size_t total_kd_size = keyToSizeIt.second;

            if (total_kd_size > 0) {
                new_bucket->index_block->Insert(keyToSizeIt.first, total_kd_size);
            }
        }

        new_bucket->index_block->Build();
        //            new_bucket->index_block->IndicesClear();
    }

    char write_buf[targetFileSize];
    uint64_t write_i = 0;
    uint64_t tot_size = 0;

    // Iterate all the keys
    size_t header_sz = sizeof(KDRecordHeader);
    // Can further optimize. No need to iterate twice

    //        if (enable_index_block_) {
    //            // write KD pairs in sorted manner
    //            // TODO Can optimize. Don't need to find in the gcResultMap
    //            for (auto& sorted_it : new_bucket->index_block->indices) {
    //                auto keyIt = gcResultMap.find(str_t(const_cast<char*>(sorted_it.first.data()),
    //                            sorted_it.first.size()));
    //                if (keyIt == gcResultMap.end()) {
    //                    debug_error("data not found! key %.*s\n",
    //                            (int)sorted_it.first.size(), sorted_it.first.data());
    //                    exit(1);
    //                }
    //
    //                auto& key = keyIt->first;
    //                auto& values = keyIt->second.first;
    //                for (auto vec_i = 0; vec_i < values.size(); vec_i++) {
    //                    auto& value = keyIt->second.first[vec_i];
    //                    auto& header = keyIt->second.second[vec_i];
    //                    if (use_varint_d_header == false) {
    //                        copyInc(write_buf, write_i, &header, header_sz);
    //                    } else {
    //                        write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
    //                    }
    //                    copyInc(write_buf, write_i, key.data_, key.size_);
    //                    copyInc(write_buf, write_i, value.data_, value.size_);
    //                }
    //                new_bucket->total_object_cnt += values.size();
    //                if (keyIt->second.first.size() > 0) {
    //                    new_bucket->sorted_filter->Insert(key);
    //                }
    //            }
    //            new_bucket->index_block->IndicesClear();
    //        } else 
    {
        for (auto keyToSizeIt : keyToSizeMap) {
            auto keyIt = gcResultMap.find(keyToSizeIt.first);
            auto& key = keyToSizeIt.first;
            auto& values = keyIt->second.first;
            for (auto vec_i = 0; vec_i < keyIt->second.first.size();
                    vec_i++) {
                auto& value = keyIt->second.first[vec_i];
                auto& header = keyIt->second.second[vec_i];

                if (use_varint_d_header == false) {
                    copyInc(write_buf, write_i, &header, header_sz);
                } else {
                    write_i += PutDeltaHeaderVarint(write_buf + write_i,
                            header);
                }
                copyInc(write_buf, write_i, key.data_, key.size_);
                copyInc(write_buf, write_i, value.data_, value.size_);
            }
            new_bucket->total_object_cnt += values.size();
            if (keyIt->second.first.size() > 0) {
                if (enable_index_block_) {
                    new_bucket->sorted_filter->Insert(key);
                } else {
                    new_bucket->filter->Insert(key);
                }
            }
        }
    }
    //        for (auto keyToSizeIt : keyToSizeMap) {
    //            tot_size += keyToSizeIt.second;
    //            auto keyIt = gcResultMap.find(keyToSizeIt.first);
    //            auto& key = keyToSizeIt.first;
    //            auto& values = keyIt->second.first;
    //            for (auto vec_i = 0; vec_i < keyIt->second.first.size();
    //                    vec_i++) {
    //                auto& value = keyIt->second.first[vec_i]; 
    //                auto& header = keyIt->second.second[vec_i];
    //
    //                if (use_varint_d_header == false) {
    //                    copyInc(write_buf, write_i, &header, header_sz);
    //                } else {
    //                    write_i += PutDeltaHeaderVarint(write_buf + write_i,
    //                            header);
    //                }
    //                copyInc(write_buf, write_i, key.data_, key.size_);
    //                copyInc(write_buf, write_i, value.data_, value.size_);
    //            }
    //            new_bucket->total_object_cnt += values.size();
    //            if (keyIt->second.first.size() > 0) {
    //                new_bucket->filter->Insert(key);
    //            }
    //
    //            if (write_i > tot_size) {
    //                debug_error("[ERROR] write_i %lu > tot_size %lu\n", write_i, tot_size);
    //                exit(1);
    //            }
    //        }
    KDRecordHeader gc_fin_header;
    gc_fin_header.is_anchor_ = false;
    gc_fin_header.is_gc_done_ = true;
    gc_fin_header.seq_num = 0;
    gc_fin_header.key_size_ = 0;
    gc_fin_header.value_size_ = 0;
    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &gc_fin_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i, gc_fin_header);
    }
    new_bucket->unsorted_part_offset = write_i;

    // start write file
    FileOpStatus onDiskWriteSizePair;
    new_bucket->op_mtx.lock();

    // write the file generated by split
    if (enable_crash_consistency_) {
        STAT_PROCESS(onDiskWriteSizePair =
                new_bucket->io_ptr->writeAndFlushFile(write_buf,
                    write_i), StatsType::KDSep_GC_WRITE);
    } else {
        STAT_PROCESS(onDiskWriteSizePair =
                new_bucket->io_ptr->writeFile(write_buf,
                    write_i), StatsType::KDSep_GC_WRITE);
    }
    StatsRecorder::getInstance()->DeltaGcBytesWrite(
            onDiskWriteSizePair.physicalSize_,
            onDiskWriteSizePair.logicalSize_, syncStatistics_);
    new_bucket->io_ptr->markDirectDataAddress(write_i);
    new_bucket->total_object_bytes = write_i;
    new_bucket->total_on_disk_bytes = onDiskWriteSizePair.physicalSize_;
    new_bucket->total_object_cnt++;
    debug_trace("Flushed new file to filesystem since split gc, the new"
            " file ID = %lu\n", new_bucket->file_id);
    new_bucket->op_mtx.unlock();
    // update metadata
    write_fin_number++;
}

bool BucketManager::singleFileSplit(BucketHandler* bucket, 
        map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>, 
        mapSmallerKeyForStr_t>& gcResultMap, 
	bool fileContainsReWriteKeysFlag, uint64_t target_size)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    int bi = 0;
    vector<pair<map<str_t, uint64_t, mapSmallerKeyForStr_t>, uint64_t>> tmpGcResult;
    vector<string> startKeys; 

    StatsRecorder::staticProcess(StatsType::SPLIT_HANDLER, tv);
    gettimeofday(&tv, 0);
    size_t header_sz = sizeof(KDRecordHeader);

    tmpGcResult.push_back({{}, 0});
    auto it = gcResultMap.begin();
    startKeys.push_back(string(it->first.data_, it->first.size_));

    int size_ratio = (int)ceil((double)target_size / maxBucketSize_);
    if (size_ratio > 5) {
        debug_error("target_size %lu, maxBucketSize_ %lu, size_ratio %d\n", 
                target_size, maxBucketSize_, size_ratio);   
        size_ratio = 5;
    }

    int gc_result_map_size = gcResultMap.size();
    int key_cnt = 0;
    
    // fill the keys to two buckets
    for (auto keyIt : gcResultMap) {
        auto& key = keyIt.first;
        key_cnt++;

        // switch to the second bucket
        if (tmpGcResult[bi].second > maxBucketSize_ / 2 / size_ratio && 
                key_cnt < gc_result_map_size) {
            bi++;
            tmpGcResult.push_back({{}, 0});
            startKeys.push_back(string(key.data_, key.size_));
        }

        // calculate space
        uint64_t kd_pair_sz = 0;
        auto& values = keyIt.second.first;
        auto& headers = keyIt.second.second;
        for (auto i = 0; i < values.size(); i++) {
            if (use_varint_d_header == true) {
                header_sz = GetDeltaHeaderVarintSize(headers[i]);
            }
            kd_pair_sz += key.size_ +
                values[i].size_ + header_sz;
        }

        // update the space needed for this key
        tmpGcResult[bi].first.insert(make_pair(keyIt.first, kd_pair_sz));
        tmpGcResult[bi].second += kd_pair_sz;
    }

    StatsRecorder::staticProcess(StatsType::SPLIT_IN_MEMORY, tv);
    gettimeofday(&tv, 0);
    vector<pair<string, BucketHandler*>> new_prefix_and_hdls;

    if (tmpGcResult.size() > 2) {
        debug_error("Split to more than 2 files, current file number = %lu\n",
                tmpGcResult.size());
    }

    for (int bi = 0; bi < tmpGcResult.size(); bi++) {
        BucketHandler* new_bucket;
        bool getFileHandlerStatus = createFileHandlerForGC(startKeys[bi],
                new_bucket);
        if (getFileHandlerStatus == false) {
            debug_error("[ERROR] Failed to create hash store file handler by"
                    " key when split GC %s\n", startKeys[bi].c_str());
            return false;
        }
        new_prefix_and_hdls.push_back(make_pair(startKeys[bi], new_bucket));
    }

    boost::atomic<int> write_fin_number(0);

    for (int bi = 0; bi < tmpGcResult.size(); bi++) {
        BucketHandler* new_bucket = new_prefix_and_hdls[bi].second;

        if (bi == 0) {
            writeSingleSplitFile(new_bucket, tmpGcResult, gcResultMap, bi,
                    write_fin_number);
        } else {
            boost::asio::post(*extra_threads_, [this, new_bucket, &tmpGcResult,
                    &gcResultMap, bi, &write_fin_number]() {
                    writeSingleSplitFile(new_bucket, tmpGcResult, gcResultMap,
                            bi, write_fin_number);
            });
        }
    } 

    while (write_fin_number < tmpGcResult.size()) {
        usleep(10);
    }

    StatsRecorder::staticProcess(StatsType::SPLIT_WRITE_FILES, tv);
    gettimeofday(&tv, 0);
    if (new_prefix_and_hdls.size() <= 1) {
        // impossible
        debug_e("Impossible!");
        exit(1);
//	string& prefix_u64 = new_prefix_and_hdls[0].first;
//	auto& new_bucket = new_prefix_and_hdls[0].second;
//	uint64_t insert_lvl = prefix_tree_.insert(prefix_u64, new_bucket);
//        if (insert_lvl == 0) {
//	    debug_error("[ERROR] Error insert to prefix tree, prefix length"
//		    " used = %lu, inserted file ID = %lu\n",
//		    prefixLenExtract(new_bucket->prefix),
//		    new_bucket->file_id);
//            new_bucket->io_ptr->closeFile();
//            bucket_delete_mtx_.lock();
//            bucket_id_to_delete_.push_back(new_bucket->file_id);
//            bucket_delete_mtx_.unlock();
//	    deleteFileHandler(new_bucket);
//            return false;
//        } else {
//            if (prefixLenExtract(new_bucket->prefix) != insert_lvl) {
//                debug_info("After insert to prefix tree, get handler at level ="
//                        " %lu, but prefix length used = %lu, prefix = %lx,"
//                        " inserted file ID = %lu, update the current bit number"
//                        " used in the file handler\n", insert_lvl,
//                        new_bucket->prefix,
//                        new_prefix_and_hdls[0].first,
//                        new_bucket->file_id);
//                new_bucket->prefix = prefixConcat(prefix_u64, insert_lvl);
//            }
//            bucket->gc_status = kShouldDelete;
//            new_bucket->ownership = 0;
//            bucket_delete_mtx_.lock();
//            bucket_id_to_delete_.push_back(bucket->file_id);
//            bucket_delete_mtx_.unlock();
//            debug_info("Split file ID = %lu for gc success, mark as should"
//                    " delete done\n", bucket->file_id);
//            StatsRecorder::staticProcess(StatsType::SPLIT_METADATA, tv);
//
//	    if (enable_crash_consistency_) {
//		STAT_PROCESS(
//		manifest_->UpdateGCMetadata(
//			bucket->file_id, bucket->prefix,
//			new_bucket->file_id, new_bucket->prefix),
//		StatsType::DS_MANIFEST_GC_SPLIT);
//	    }
//
//            return true;
//        }
    } else {
	string& key1 = new_prefix_and_hdls[0].first;
	auto& bucket1 = new_prefix_and_hdls[0].second;

        // update the prefix tree
        vector<pair<string, BucketHandler*>> insertList;

        for (int i = 1, t = 0; i < new_prefix_and_hdls.size(); i++) {
            string& key = new_prefix_and_hdls[i].first;
            auto& bucket2 = new_prefix_and_hdls[i].second;
            insertList.push_back(make_pair(key, bucket2));

            // test
            int tree_size = maxBucketNumber_ - prefix_tree_.getRemainFileNumber();
            if (t == 0 && tree_size == (tree_size & (-tree_size))) {
                debug_error("bucket num %d\n", tree_size);
                t++;
                //            fprintf(stderr, "--- start ---\n");
                //            prefix_tree_.printNodeMap();
                //            fprintf(stderr, "--- end ---\n");
            }
        }

        bool s = prefix_tree_.batchInsertAndUpdate(insertList, key1, bucket1);
        if (s == false) {
            debug_e("batch insert failed");
            for (int i = 0; i < new_prefix_and_hdls.size(); i++) {
                auto& bucket = new_prefix_and_hdls[i].second;
                bucket->io_ptr->closeFile();
                bucket_delete_mtx_.lock();
                bucket_id_to_delete_.push_back(bucket->file_id);
                bucket_delete_mtx_.unlock();
                deleteFileHandler(bucket);
            }
            return false;
        }

        // successfully add one bucket
        bucket1->ownership = 0;
        bucket->gc_status = kShouldDelete;

        for (int i = 1; i < new_prefix_and_hdls.size(); i++) {
            new_prefix_and_hdls[i].second->ownership = 0;
        }
        StatsRecorder::staticProcess(StatsType::SPLIT_METADATA, tv);

        if (enable_crash_consistency_) {
            // TODO will update
            vector<BucketHandler*> old_hdls;
            vector<BucketHandler*> new_hdls;
            old_hdls.push_back(bucket);
            for (int i = 1; i < new_prefix_and_hdls.size(); i++) {
                new_hdls.push_back(new_prefix_and_hdls[i].second);
            }
            STAT_PROCESS(
                    manifest_->UpdateGCMetadata(old_hdls, new_hdls),
                    StatsType::DS_MANIFEST_GC_SPLIT);
        }

        bucket_delete_mtx_.lock();
        bucket_id_to_delete_.push_back(bucket->file_id);
        bucket_delete_mtx_.unlock();
        return true;
    }
}

bool BucketManager::twoAdjacentFileMerge(
        BucketHandler* bucket1, BucketHandler* bucket2)
{
    struct timeval tvAll, tv;
    gettimeofday(&tvAll, 0);
    std::scoped_lock<std::shared_mutex> w_lock1(bucket1->op_mtx);
    std::scoped_lock<std::shared_mutex> w_lock2(bucket2->op_mtx);
    StatsRecorder::staticProcess(StatsType::MERGE_WAIT_LOCK, tvAll);
    gettimeofday(&tv, 0);
    debug_info("Perform merge GC for file ID 1 = %lu, ID 2 = %lu\n",
            bucket1->file_id, bucket2->file_id);
    BucketHandler* bucket;

    bool generateFileHandlerStatus = createFileHandlerForGC(bucket1->key, bucket);
    StatsRecorder::staticProcess(StatsType::MERGE_CREATE_HANDLER, tv);
    gettimeofday(&tv, 0);
    if (generateFileHandlerStatus == false) {
        debug_error("[ERROR] Could not generate new file handler for merge GC,previous file ID 1 = %lu, ID 2 = %lu\n", bucket1->file_id, bucket2->file_id);
        bucket1->ownership = 0;
        bucket2->ownership = 0;
        return false;
    }

    if (bucket1->key >= bucket2->key) {
        debug_error("[ERROR] Bucket 1 key is larger than bucket 2 key, bucket"
                " 1 key = %s, bucket 2 key = %s\n", 
                bucket1->key.c_str(), bucket2->key.c_str());
        bucket1->ownership = 0;
        bucket2->ownership = 0;
        return false;
    }

    std::scoped_lock<std::shared_mutex> w_lock3(bucket->op_mtx);
    StatsRecorder::staticProcess(StatsType::MERGE_WAIT_LOCK3, tv);

    // process file 1
    gettimeofday(&tv, 0);
    char readWriteBuffer1[bucket1->total_object_bytes];
    char* readWriteBuffer1Ptr = readWriteBuffer1;
    bool finished = false;
    map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t> gcResultMap1;

    boost::asio::post(*extra_threads_, [this, readWriteBuffer1Ptr, bucket1,
            &finished, &gcResultMap1]() {
        FileOpStatus readStatus1;
        STAT_PROCESS(readStatus1 = bucket1->io_ptr->readFile(readWriteBuffer1Ptr,
                    bucket1->total_object_bytes), StatsType::KDSep_GC_READ);
        StatsRecorder::getInstance()->DeltaGcBytesRead(bucket1->total_on_disk_bytes,
                bucket1->total_object_bytes, syncStatistics_);
        // process GC contents
        pair<uint64_t, uint64_t> remainObjectNumberPair1 =
            deconstructAndGetValidContentsFromFile(readWriteBuffer1Ptr,
                    bucket1->total_object_bytes, gcResultMap1);
        if (remainObjectNumberPair1.first == 0 &&
                remainObjectNumberPair1.second == 0) {
            debug_error("Read error: file id %lu own %d\n", bucket1->file_id,
                    bucket1->ownership);
            exit(1);
        }
        finished = true;
    });

//    FileOpStatus readStatus1;
//    STAT_PROCESS(readStatus1 = bucket1->io_ptr->readFile(readWriteBuffer1, bucket1->total_object_bytes), StatsType::KDSep_GC_READ);
//    StatsRecorder::getInstance()->DeltaGcBytesRead(bucket1->total_on_disk_bytes, bucket1->total_object_bytes, syncStatistics_);
//    // process GC contents
//    pair<uint64_t, uint64_t> remainObjectNumberPair1 =
//	deconstructAndGetValidContentsFromFile(readWriteBuffer1,
//		bucket1->total_object_bytes, gcResultMap1);
//    if (remainObjectNumberPair1.first == 0 &&
//	    remainObjectNumberPair1.second == 0) {
//	debug_error("Read error: file id %lu own %d\n", bucket1->file_id,
//		bucket1->ownership);
//	exit(1);
//    }
//    finished = true;
//    StatsRecorder::staticProcess(StatsType::MERGE_FILE1, tv);
//    gettimeofday(&tv, 0);

    // process file2
    char readWriteBuffer2[bucket2->total_object_bytes];
    FileOpStatus readStatus2;
    STAT_PROCESS(readStatus2 = bucket2->io_ptr->readFile(readWriteBuffer2, bucket2->total_object_bytes), StatsType::KDSep_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(bucket2->total_on_disk_bytes, bucket2->total_object_bytes, syncStatistics_);
    // process GC contents
    map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>,
        mapSmallerKeyForStr_t> gcResultMap2;
    pair<uint64_t, uint64_t> remainObjectNumberPair2 = deconstructAndGetValidContentsFromFile(readWriteBuffer2, bucket2->total_object_bytes, gcResultMap2);

    StatsRecorder::staticProcess(StatsType::MERGE_FILE2, tv);
    gettimeofday(&tv, 0);

    // wait for file 1 to finish
    while (finished == false) {
        usleep(10);
    }

    StatsRecorder::staticProcess(StatsType::MERGE_FILE1, tv);
    gettimeofday(&tv, 0);

    uint64_t targetWriteSize = 0;
    for (auto& keyIt : gcResultMap1) {
        for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
            targetWriteSize += (sizeof(KDRecordHeader) + keyIt.first.size_ + keyIt.second.first[vec_i].size_);
        }
    }
    for (auto& keyIt : gcResultMap2) {
        for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
            targetWriteSize += (sizeof(KDRecordHeader) + keyIt.first.size_ + keyIt.second.first[vec_i].size_);
        }
    }
    // reserve more space, use sizeof()
    targetWriteSize += sizeof(KDRecordHeader);
    debug_info("Merge GC target write file size = %lu\n", targetWriteSize);
    char write_buf[targetWriteSize];

    bucket->filter->Clear();
    bucket->sorted_filter->Clear();
    uint64_t write_i = 0;
    size_t header_sz = sizeof(KDRecordHeader);

    // build the index block
    if (enable_index_block_) {
	if (bucket->index_block == nullptr) {
	    bucket->index_block = new BucketIndexBlock();
	} else {
	    bucket->index_block->Clear();
	}

	// select keys for building index block

	for (auto keyIt : gcResultMap1) {
            size_t total_kd_size = 0;
            auto& key = keyIt.first;

            for (auto i = 0; i < keyIt.second.first.size(); i++) {
                auto& value = keyIt.second.first[i];
                auto& header = keyIt.second.second[i];
                if (use_varint_d_header == true) {
                    header_sz = GetDeltaHeaderVarintSize(header);
                }
                total_kd_size += header_sz + key.size_ + value.size_;
            }

            if (total_kd_size > 0) {
                bucket->index_block->Insert(key, total_kd_size);
            }
	}

	for (auto keyIt : gcResultMap2) {
            size_t total_kd_size = 0;
            auto& key = keyIt.first;

            for (auto i = 0; i < keyIt.second.first.size(); i++) {
                auto& value = keyIt.second.first[i];
                auto& header = keyIt.second.second[i];
                if (use_varint_d_header == true) {
                    header_sz = GetDeltaHeaderVarintSize(header);
                }
                total_kd_size += header_sz + key.size_ + value.size_;
            }

            if (total_kd_size > 0) {
                bucket->index_block->Insert(key, total_kd_size);
            }
	}

	bucket->index_block->Build();
    }

    // write file buffer
    if (enable_index_block_) {
        for (auto& sorted_it : bucket->index_block->indices) {
	    auto* map_ptr = &gcResultMap1;
            auto keyIt =
                gcResultMap1.find(str_t(const_cast<char*>(sorted_it.first.data()),
                            sorted_it.first.size()));
	    if (keyIt == gcResultMap1.end()) {
		keyIt =
		    gcResultMap2.find(str_t(const_cast<char*>(sorted_it.first.data()),
				sorted_it.first.size()));
		map_ptr = &gcResultMap2;
	    }	

            auto& key = keyIt->first;
            if (keyIt == map_ptr->end()) {
                debug_error("data not found! key %.*s\n", 
                        (int)sorted_it.first.size(), sorted_it.first.data());
                exit(1);
            }

            for (auto vec_i = 0; vec_i < keyIt->second.first.size(); vec_i++) {
                auto& value = keyIt->second.first[vec_i];
                auto& header = keyIt->second.second[vec_i];
                if (use_varint_d_header == false) {
                    copyInc(write_buf, write_i, &header, header_sz);
                } else {
                    write_i += PutDeltaHeaderVarint(write_buf + write_i,
                            header);
                }
                copyInc(write_buf, write_i, key.data_, key.size_);
                copyInc(write_buf, write_i, value.data_, value.size_);
		bucket->total_object_cnt++;
            }
            if (keyIt->second.first.size() > 0) {
                bucket->sorted_filter->Insert(key);
            }
        }
        bucket->index_block->IndicesClear();
    } else {
	for (auto& keyIt : gcResultMap1) {
	    auto& key = keyIt.first;
	    for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
		auto& header = keyIt.second.second[vec_i];
		auto& value = keyIt.second.first[vec_i];

		// write header
		if (use_varint_d_header == false) {
		    copyInc(write_buf, write_i, &header, header_sz);
		} else {
		    write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
		}
		copyInc(write_buf, write_i, key.data_, key.size_);
		copyInc(write_buf, write_i, value.data_, value.size_);
		bucket->total_object_cnt++;
	    }
	    if (keyIt.second.first.size() > 0) {
		bucket->filter->Insert(key.data_, key.size_);
	    }
	}

	for (auto& keyIt : gcResultMap2) {
	    auto& key = keyIt.first;
	    for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
		auto& header = keyIt.second.second[vec_i];
		auto& value = keyIt.second.first[vec_i];

		if (use_varint_d_header == false) {
		    copyInc(write_buf, write_i, &header, header_sz);
		} else {
		    write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
		}
		copyInc(write_buf, write_i, key.data_, key.size_);
		copyInc(write_buf, write_i, value.data_, value.size_);
		bucket->total_object_cnt++;
	    }
	    if (keyIt.second.first.size() > 0) {
		bucket->filter->Insert(key.data_, key.size_);
	    }
	}
    }

    debug_info("Merge GC processed write file size = %lu\n", write_i);
    // write gc done flag into bucket file
    KDRecordHeader gc_fin_header;
    gc_fin_header.is_anchor_ = false;
    gc_fin_header.is_gc_done_ = true;
    gc_fin_header.seq_num = 0;
    gc_fin_header.key_size_ = 0;
    gc_fin_header.value_size_ = 0;
    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &gc_fin_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i, gc_fin_header);
    }

    if (enable_index_block_) {
	bucket->unsorted_part_offset = write_i;
    } else {
	bucket->unsorted_part_offset = 0;
    }

    debug_info("Merge GC processed total write file size = %lu\n", write_i);
    FileOpStatus onDiskWriteSizePair;

    // write the file generated by merge
    if (enable_crash_consistency_) {
	STAT_PROCESS(onDiskWriteSizePair =
		bucket->io_ptr->writeAndFlushFile(write_buf,
		    write_i), StatsType::KDSep_GC_WRITE);
    } else {
	STAT_PROCESS(onDiskWriteSizePair =
		bucket->io_ptr->writeFile(write_buf,
		    write_i), StatsType::KDSep_GC_WRITE);
    }
    bucket->io_ptr->markDirectDataAddress(write_i);

    StatsRecorder::getInstance()->DeltaGcBytesWrite(
	    onDiskWriteSizePair.physicalSize_,
	    onDiskWriteSizePair.logicalSize_, syncStatistics_);

    debug_info("Merge GC write file size = %lu done\n", write_i);
    bucket->total_object_bytes += write_i;
    bucket->total_on_disk_bytes += onDiskWriteSizePair.physicalSize_;
    bucket->total_object_cnt++;
    debug_info("Flushed new file to filesystem since merge gc, the new file ID = %lu, corresponding previous file ID 1 = %lu, ID 2 = %lu\n", bucket->file_id, bucket1->file_id, bucket2->file_id);

    // update metadata
    uint64_t newLeafNodeBitNumber = 0;
//    bool mergeNodeStatus = prefix_tree_.mergeNodesToNewLeafNode(target_prefix,
//            prefix_len, newLeafNodeBitNumber);
    bool mergeNodeStatus = prefix_tree_.remove(bucket2->key);

    if (mergeNodeStatus == false) {
        debug_error("[ERROR] Could not merge two existing node corresponding file ID 1 = %lu, ID 2 = %lu\n", bucket1->file_id, bucket2->file_id);
        if (bucket->io_ptr->isFileOpen() == true) {
            bucket->io_ptr->closeFile();
        }
        bucket_delete_mtx_.lock();
        bucket_id_to_delete_.push_back(bucket->file_id);
        bucket_delete_mtx_.unlock();
        bucket1->ownership = 0;
        bucket2->ownership = 0;
	deleteFileHandler(bucket);
        StatsRecorder::staticProcess(StatsType::MERGE_METADATA, tv);
        return false;
    }

    // check the existing file handler in the node
    BucketHandler* tempHandler = nullptr;
    // TODO remove 
    prefix_tree_.get(bucket1->key, tempHandler);
//    debug_error("bucket1 %p tempHandler %p key %s\n", bucket1, tempHandler,
//            bucket1->key.c_str());
    prefix_tree_.update(bucket1->key, bucket);
//    if (tempHandler != nullptr) {
//	// delete old handler;
//	debug_info("Find exist data handler = %p\n", tempHandler);
//	debug_error("Find exist data handler = %p\n", tempHandler);
//	if (tempHandler->io_ptr != nullptr) {
//	    if (tempHandler->io_ptr->isFileOpen() == true) {
//		tempHandler->io_ptr->closeFile();
//	    }
//	    bucket_delete_mtx_.lock();
//	    bucket_id_to_delete_.push_back(tempHandler->file_id);
//	    bucket_delete_mtx_.unlock();
//	}
//	deleteFileHandler(tempHandler);
//    }
    debug_info("Start update metadata for merged file ID = %lu\n", bucket->file_id);

    bucket_delete_mtx_.lock();
    bucket_id_to_delete_.push_back(bucket1->file_id);
    bucket_id_to_delete_.push_back(bucket2->file_id);
    bucket_delete_mtx_.unlock();
    bucket1->gc_status = kShouldDelete;
    bucket2->gc_status = kShouldDelete;
    bucket1->ownership = 0;
    bucket2->ownership = 0;

    if (enable_crash_consistency_) {
	vector<BucketHandler*> old_hdls;
	vector<BucketHandler*> new_hdls;
	old_hdls.push_back(bucket1);
	old_hdls.push_back(bucket2);
	new_hdls.push_back(bucket);
	STAT_PROCESS(
	manifest_->UpdateGCMetadata(old_hdls, new_hdls),
	StatsType::DS_MANIFEST_GC_MERGE);
    }

    if (bucket1->io_ptr->isFileOpen() == true) {
	bucket1->io_ptr->closeFile();
    }

    if (bucket2->io_ptr->isFileOpen() == true) {
	bucket2->io_ptr->closeFile();
    }
    bucket_delete_mtx_.lock();
    bucket_id_to_delete_.push_back(bucket1->file_id);
    bucket_id_to_delete_.push_back(bucket2->file_id);
    bucket_delete_mtx_.unlock();

    bucket->ownership = 0;
    StatsRecorder::staticProcess(StatsType::MERGE_METADATA, tv);

    return true;
}

bool BucketManager::selectFileForMerge(uint64_t targetFileIDForSplit,
        BucketHandler*& bucket1, BucketHandler*& bucket2)
{
    struct timeval tvAll;
    gettimeofday(&tvAll, 0);
    vector<pair<string, BucketHandler*>> validNodes;
    bool getValidNodesStatus = prefix_tree_.getCurrentValidNodes(validNodes);
    StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_GET_NODES, tvAll);
    if (getValidNodesStatus == false) {
        debug_error("[ERROR] Could not get valid tree nodes from prefixTree,"
                " current validNodes vector size = %lu\n", validNodes.size());
        return false;
    } 

    struct timeval tv;
    gettimeofday(&tv, 0);
    debug_trace("Current validNodes vector size = %lu\n", validNodes.size());
    vector<pair<BucketHandler*, BucketHandler*>> targetFilesForMerge;
    // sorted
    for (int i = 0; i < validNodes.size() - 1; i++) {
        auto& nodeIt = validNodes[i];
        auto& nextNodeIt = validNodes[i + 1];
        auto& bucket = nodeIt.second;
        auto& nextBucket = nextNodeIt.second;

        if (bucket->file_id == targetFileIDForSplit) {
            // skip one file
            continue;
        }
        if (nextBucket->file_id == targetFileIDForSplit) {
            // skip two files
            i++;
            continue;
        }
        if (bucket->total_object_bytes <= singleFileMergeGCUpperBoundSize_ &&
                nextBucket->total_object_bytes <= singleFileMergeGCUpperBoundSize_ &&
                bucket->ownership != -1 && nextBucket->ownership != -1) {
            targetFilesForMerge.push_back(make_pair(bucket, nextBucket));
        }
    }
    StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_SELECT_MERGE, tv);
    gettimeofday(&tv, 0);

    uint64_t sel_threshold = singleFileGCTriggerSize_;
    BucketHandler* sel_hdl1, *sel_hdl2;
    sel_hdl1 = sel_hdl2 = nullptr;

    vector<BucketHandler*> selected;
    vector<uint64_t> prefices_needed;

    if (targetFilesForMerge.size() == 0) {
        StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_R4, tvAll);
        return false;
    }

    for (auto pairIt : targetFilesForMerge) {
	BucketHandler* tmpBucket1 = pairIt.first;
        BucketHandler* tmpBucket2 = pairIt.second;

        uint64_t total_bytes = pairIt.first->total_object_bytes +
            pairIt.second->total_object_bytes;

        if (total_bytes < sel_threshold) {
            if (enableBatchedOperations_ == true) {
                if (tmpBucket1->ownership != 0 || tmpBucket2->ownership != 0) {
                    continue;
                    // skip wait if batched op
                }

                // skip the should delete files
                if (tmpBucket1->gc_status == kShouldDelete ||
                        tmpBucket2->gc_status == kShouldDelete) {
                    continue;
                }
            }
            if (tmpBucket1->ownership != 0) {
                debug_info("Stop this merge for file ID = %lu\n",
                        tmpBucket1->file_id);
                continue;
            }
            tmpBucket1->ownership = -1;
            // check again to see whether the assignment is correct
            if (tmpBucket1->ownership != -1) {
                continue;
            }

            bucket1 = tmpBucket1;
            if (tmpBucket2->ownership != 0) {
                tmpBucket1->ownership = 0;
                debug_info("Stop merge for file ID = %lu\n", tmpBucket2->file_id);
                continue;
            }
            tmpBucket2->ownership = -1;
            bucket2 = tmpBucket2;

            if (tmpBucket2->ownership != -1) {
                if (bucket1->ownership == -1) {
                    bucket1->ownership = 0;
                    continue;
                }
            }
            debug_info("Find two file for merge GC success,"
                    " bucket 1 ptr = %p,"
                    " bucket 2 ptr = %p\n",
                    bucket1, bucket2);
            StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_AFTER_SELECT, tv);
            StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_R2_SUCCESS, tvAll);

            // update threshold and release the previously selected
            // files
            sel_threshold = total_bytes;
            //			return true;

            //			bucket1->ownership = 0;
            //			bucket2->ownership = 0;
            //			selected.push_back(bucket1);
            //			selected.push_back(bucket2);
            //			prefices_needed.push_back(target_prefix);
            //			prefices_needed.push_back(prefix_len);

            BucketHandler* tmpBucket;
            prefix_tree_.getNext(bucket1->key, tmpBucket);
            if (tmpBucket != bucket2) {
                debug_error("Bucket is split during merge. Stop (key %s)\n",
                        bucket1->key.c_str());
                bucket1->ownership = 0;
                bucket2->ownership = 0;
                continue;
            }

            if (sel_hdl1 != nullptr &&
                    sel_hdl1 != bucket1 && sel_hdl2 != bucket2) {
                sel_hdl1->ownership = 0;
                sel_hdl2->ownership = 0;
            }

            sel_hdl1 = bucket1;
            sel_hdl2 = bucket2;
            continue;
        }
    }

    // have a selection. Return true
    if (sel_hdl1 != nullptr) {
	bucket1 = sel_hdl1;
	bucket2 = sel_hdl2;
	return true;
    }

    StatsRecorder::staticProcess(StatsType::GC_SELECT_MERGE_R3, tvAll);
    return false;
}

bool BucketManager::pushObjectsToWriteBackQueue(
        vector<writeBackObject*>& targetWriteBackVec) 
{
    if (enableWriteBackDuringGCFlag_ && !write_back_queue_->done) {
        for (auto writeBackIt : targetWriteBackVec) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (!write_back_queue_->tryPush(writeBackIt)) {
                wb_keys_mutex->lock();
                wb_keys->push(writeBackIt->key);
                wb_keys_mutex->unlock();
                delete writeBackIt;
            } 
            StatsRecorder::staticProcess(
                    StatsType::KDSep_GC_WRITE_BACK, tv);
        }
    }
    return true;
}

void BucketManager::TryMerge() {
    static int merge_cnt = 0;
    BucketHandler* bucket1;
    BucketHandler* bucket2;
    struct timeval tv;
    gettimeofday(&tv, 0);
    bool selectFileForMergeStatus = selectFileForMerge(0, bucket1,
            bucket2);
    StatsRecorder::staticProcess(StatsType::GC_MERGE_SELECT, tv);

    if (selectFileForMergeStatus == false) {
        return;
    } 

    debug_info("Select two file for merge GC success, "
            " bucket 1 key %s, bucket 2 key %s\n", 
            bucket1->key.c_str(), bucket2->key.c_str());
    if (merge_cnt % 10 == 0) {
        debug_error("Select two file for merge id1 %lu id2 %lu "
                "total size %lu + %lu = %lu\n", 
                bucket1->file_id, bucket2->file_id,
                bucket1->total_object_bytes,
                bucket2->total_object_bytes,
                bucket1->total_object_bytes +
                bucket2->total_object_bytes);
    }
    merge_cnt++;
    bool performFileMergeStatus;
    STAT_PROCESS(performFileMergeStatus = 
            twoAdjacentFileMerge(bucket1, bucket2), StatsType::DELTASTORE_MERGE);
    if (performFileMergeStatus != true) {
        debug_error("[ERROR] Could not merge two files for GC,"
                " bucket 1 key %s, bucket 2 key %s\n", 
                bucket1->key.c_str(), bucket2->key.c_str());
    }
    StatsRecorder::staticProcess(StatsType::GC_MERGE_SUCCESS, tv);
}
    
void BucketManager::processMergeGCRequestWorker()
{
    while (true) {
        if (notifyGCMQ_->done == true && notifyGCMQ_->isEmpty() == true) {
            break;
        }
        uint64_t remainEmptyBucketNumber = prefix_tree_.getRemainFileNumber();
        usleep(10000);
        if (remainEmptyBucketNumber >= singleFileGCWorkerThreadsNumebr_ + 2) {
            continue;
        }
        debug_info("May reached max file number, need to merge, current remain empty file numebr = %lu\n", remainEmptyBucketNumber);
        // perfrom merge before split, keep the total file number not changed
        TryMerge();
    }
    return;
}

void BucketManager::asioSingleFileGC(BucketHandler* bucket) {
    num_threads_++;
    singleFileGC(bucket);
    num_threads_--;
}

void BucketManager::singleFileGC(BucketHandler* bucket) {
    struct timeval tv;
    gettimeofday(&tv, 0);
    debug_warn("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, wait for lock\n", bucket->file_id, bucket->total_object_bytes, bucket->total_on_disk_bytes, bucket->gc_status);
    std::scoped_lock<std::shared_mutex> w_lock(bucket->op_mtx);
    debug_info("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, start process\n", bucket->file_id, bucket->total_object_bytes, bucket->total_on_disk_bytes, bucket->gc_status);
//    debug_error("total object bytes = %lu, total on disk bytes = %lu\n", bucket->total_object_bytes, bucket->total_on_disk_bytes);
    // read contents
    char readWriteBuffer[bucket->total_object_bytes];
    FileOpStatus readFileStatus;
    STAT_PROCESS(readFileStatus = bucket->io_ptr->readFile(readWriteBuffer, bucket->total_object_bytes), StatsType::KDSep_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(bucket->total_on_disk_bytes, bucket->total_object_bytes, syncStatistics_);
    if (readFileStatus.success_ == false || readFileStatus.logicalSize_ != bucket->total_object_bytes) {
        debug_error("[ERROR] Could not read contents of file for GC, fileID = %lu, target size = %lu, actual read size = %lu\n", bucket->file_id, bucket->total_object_bytes, readFileStatus.logicalSize_);
        bucket->gc_status = kNoGC;
        bucket->ownership = 0;
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
        return;
    }
    // process GC contents
    map<str_t, pair<vector<str_t>, vector<KDRecordHeader>>, mapSmallerKeyForStr_t>
        gcResultMap;
    pair<uint64_t, uint64_t> remainObjectNumberPair;
    STAT_PROCESS(remainObjectNumberPair =
            deconstructAndGetValidContentsFromFile(readWriteBuffer,
                bucket->total_object_bytes, gcResultMap),
            StatsType::KDSep_GC_PROCESS);
    unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t> shouldDelete;

    if (enableLsmTreeDeltaMeta_ == false) {
        STAT_PROCESS(
                remainObjectNumberPair.first -=
                partialMergeGcResultMap(gcResultMap, shouldDelete),
                StatsType::KDSep_GC_PARTIAL_MERGE);
    }

    bool fileContainsReWriteKeysFlag = false;
    // calculate target file size
    vector<writeBackObject*> targetWriteBackVec;
    uint64_t target_size = 0;
    size_t header_sz = sizeof(KDRecordHeader);

    // select keys for building index block
    for (auto keyIt : gcResultMap) {
        size_t total_kd_size = 0;
        auto& key = keyIt.first;

        for (auto i = 0; i < keyIt.second.first.size(); i++) {
            auto& value = keyIt.second.first[i];
            //                    auto& header = keyIt.second.second[i];
            //                    if (use_varint_d_header == true) {
            //                        header_sz = GetDeltaHeaderVarintSize(headerIt); 
            //                    }

            target_size += header_sz + key.size_ + value.size_;
            total_kd_size += header_sz + key.size_ + value.size_;
        }

        if (enableWriteBackDuringGCFlag_ == true) {
            debug_info("key = %s has %lu deltas\n", keyIt.first.data_, keyIt.second.first.size());
            if ((keyIt.second.first.size() > gcWriteBackDeltaNum_ && gcWriteBackDeltaNum_ != 0) ||
                    (total_kd_size > gcWriteBackDeltaSize_ && gcWriteBackDeltaSize_ != 0)) {
                fileContainsReWriteKeysFlag = true;
                if (kd_cache_ != nullptr) {
                    putKDToCache(keyIt.first, keyIt.second.first);
                }
                string currentKeyForWriteBack(keyIt.first.data_, keyIt.first.size_);
                writeBackObject* newWriteBackObject = new writeBackObject(currentKeyForWriteBack, "", 0);
                targetWriteBackVec.push_back(newWriteBackObject);
            } 
        }
    }

    uint64_t targetSizeWithHeader = target_size + sizeof(KDRecordHeader);
//    debug_error("totalObjectSize %lu targetSizeWithHeader %lu targetSize %lu\n",
//            bucket->total_object_bytes,
//            targetSizeWithHeader, target_size);
//
//    if (bucket->total_object_bytes < target_size) {
//        debug_error("[ERROR] File ID = %lu total object size %lu is smaller than target size %lu\n", bucket->file_id, bucket->total_object_bytes, target_size);
////        exit(1);
//    }


    // count valid object size to determine GC method;
    if (remainObjectNumberPair.second == 0) {
        debug_error("[ERROR] File ID = %lu has no object\n", bucket->file_id);
        singleFileRewrite(bucket, gcResultMap, targetSizeWithHeader, fileContainsReWriteKeysFlag);
        bucket->ownership = 0;
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
        pushObjectsToWriteBackQueue(targetWriteBackVec);

        return;
    }

    if (remainObjectNumberPair.first == 0 && gcResultMap.size() == 0) {
        debug_info("File ID = %lu total disk size %lu have no valid objects\n", bucket->file_id, bucket->total_on_disk_bytes);
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
        STAT_PROCESS(singleFileRewrite(bucket, gcResultMap, targetSizeWithHeader, fileContainsReWriteKeysFlag), StatsType::REWRITE);
        bucket->ownership = 0;
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
        pushObjectsToWriteBackQueue(targetWriteBackVec);
        return;
    }

    if (remainObjectNumberPair.first > 0 && gcResultMap.size() == 1) {
        // No invalid objects, cannot save space
        if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
            if (bucket->gc_status == kNew) {
                // keep tracking until forced gc threshold;
                bucket->gc_status = kMayGC;
                bucket->ownership = 0;
                debug_info("File ID = %lu contains only %lu different keys, marked as kMayGC\n", bucket->file_id, gcResultMap.size());
                StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
            } else if (bucket->gc_status == kMayGC) {
                // Mark this file as could not GC;
                bucket->gc_status = kNoGC;
                bucket->ownership = 0;
                debug_error("File ID = %lu contains only %lu different keys, marked as kNoGC\n", bucket->file_id, gcResultMap.size());
                //                        debug_info("File ID = %lu contains only %lu different keys, marked as kNoGC\n", bucket->file_id, gcResultMap.size());
                StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
            }
        } else {
            // single file rewrite
            debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write\n", bucket->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first);
            StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
            STAT_PROCESS(singleFileRewrite(bucket, gcResultMap, targetSizeWithHeader, fileContainsReWriteKeysFlag), StatsType::REWRITE);
            bucket->ownership = 0;
            StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
            pushObjectsToWriteBackQueue(targetWriteBackVec);
        }
        clearMemoryForTemporaryMergedDeltas(gcResultMap, shouldDelete);
        return;
    }

    // perform split into two buckets via extend prefix bit (+1)
    if (targetSizeWithHeader <= singleFileSplitGCTriggerSize_) {
        debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write, target file size = %lu, split threshold = %lu\n", bucket->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first, targetSizeWithHeader, singleFileSplitGCTriggerSize_);
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
        STAT_PROCESS(singleFileRewrite(bucket, gcResultMap, targetSizeWithHeader, fileContainsReWriteKeysFlag), StatsType::REWRITE);
        bucket->ownership = 0;
        StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
        pushObjectsToWriteBackQueue(targetWriteBackVec);
    } else {
        debug_info("try split for key number = %lu\n", gcResultMap.size());
        // use a longer prefix

        uint64_t remainEmptyFileNumber = 
            prefix_tree_.getRemainFileNumber();
        if (remainEmptyFileNumber >= singleFileGCWorkerThreadsNumebr_ + 2) {
            // cerr << "Perform split " << endl;
            debug_info("Still not reach max file number, split directly, current remain empty file numebr = %lu\n", remainEmptyFileNumber);
            debug_info("Perform split GC for file ID (without merge) = %lu\n", bucket->file_id);
            bool singleFileGCStatus;
            STAT_PROCESS(singleFileGCStatus = singleFileSplit(bucket, gcResultMap, fileContainsReWriteKeysFlag, target_size), StatsType::SPLIT);
            if (singleFileGCStatus == false) {
                debug_error("[ERROR] Could not perform split GC for file ID = %lu\n", bucket->file_id);
                bucket->gc_status = kNoGC;
                bucket->ownership = 0;
                exit(1);
                StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
            } else {
                debug_info("Perform split GC for file ID (without merge) = %lu done\n", bucket->file_id);
                bucket->gc_status = kShouldDelete;
                bucket->ownership = 0;
                StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
            }
        } else {
            // Case 3 in the paper: push all KD pairs in the bucket to the queue 
            if (remainObjectNumberPair.first < remainObjectNumberPair.second) {
                StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
                STAT_PROCESS(singleFileRewrite(bucket, gcResultMap, targetSizeWithHeader, fileContainsReWriteKeysFlag), StatsType::REWRITE);
            }
            bucket->ownership = 0;
            StatsRecorder::staticProcess(StatsType::KDSep_HASHSTORE_WORKER_GC, tv);
            pushObjectsToWriteBackQueue(targetWriteBackVec);
        }
    }
    clearMemoryForTemporaryMergedDeltas(gcResultMap, shouldDelete);
}

// threads workers
void BucketManager::processSingleFileGCRequestWorker(int threadID)
{
    int counter = 0;
    while (true) {
        {
            std::unique_lock<std::mutex> lk(operationNotifyMtx_);
            while (counter == 0 && notifyGCMQ_->done == false && notifyGCMQ_->isEmpty() == true) {
                operationNotifyCV_.wait(lk);
                counter++;
            }
        }
        if (notifyGCMQ_->done == true && notifyGCMQ_->isEmpty() == true) {
            break;
        }
        BucketHandler* bucket;
        if (notifyGCMQ_->pop(bucket)) {
            singleFileGC(bucket);
        }
    }
    workingThreadExitFlagVec_ += 1;
    return;
}

void BucketManager::scheduleMetadataUpdateWorker()
{
    while (true) {
	bool status; 
	usleep(100000);
//	    STAT_PROCESS(status = UpdateHashStoreFileMetaDataList(),
//		    StatsType::FM_UPDATE_META);
	STAT_PROCESS(status = RemoveObsoleteFiles(),
		StatsType::FM_UPDATE_META);
        if (metadataUpdateShouldExit_ == true) {
            break;
        }
    }
    return;
}

bool BucketManager::forcedManualGCAllFiles()
{
    vector<BucketHandler*> validFilesVec;
    prefix_tree_.getCurrentValidNodesNoKey(validFilesVec);
    for (auto bucket : validFilesVec) {
	if (bucket->ownership != 0) {
	    debug_error("file id %lu not zero %d\n", bucket->file_id, bucket->ownership);
	}
        while (bucket->ownership != 0) {
            usleep(10);
        }
        // cerr << "File ID = " << bucket->file_id << ", file size on disk = " << bucket->total_on_disk_bytes << endl;
        if (bucket->gc_status == kNoGC) {
            if (bucket->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
                debug_info("Current file ID = %lu, file size = %lu, has been"
                        " marked as kNoGC, but size overflow\n",
                        bucket->file_id,
                        bucket->total_on_disk_bytes);
                notifyGCMQ_->push(bucket);
                operationNotifyCV_.notify_one();
                // cerr << "Push file ID = " << bucket->file_id << endl;
                continue;
            } else {
                debug_info("Current file ID = %lu, file size = %lu, has been marked as kNoGC, skip\n", bucket->file_id, bucket->total_on_disk_bytes);
                continue;
            }
        } else if (bucket->gc_status == kShouldDelete) {
	    debug_error("[ERROR] During forced GC, should not find file "
		    " marked as kShouldDelete, file ID = %lu, "
		    " file size = %lu\n",
		    bucket->file_id, bucket->total_on_disk_bytes);
            bucket_delete_mtx_.lock();
            bucket_id_to_delete_.push_back(bucket->file_id);
            bucket_delete_mtx_.unlock();
            // cerr << "Delete file ID = " << bucket->file_id << endl;
            continue;
        } else {
            if (bucket->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
                // cerr << "Push file ID = " << bucket->file_id << endl;
                notifyGCMQ_->push(bucket);
                operationNotifyCV_.notify_one();
            }
        }
    }
    if (notifyGCMQ_->isEmpty() != true) {
        debug_trace("Wait for gc job done in forced GC%s\n", "");
        while (notifyGCMQ_->isEmpty() != true) {
            usleep(10);
            // wait for gc job done
        }
        debug_trace("Wait for gc job done in forced GC%s over\n", "");
    }
    return true;
}
}
