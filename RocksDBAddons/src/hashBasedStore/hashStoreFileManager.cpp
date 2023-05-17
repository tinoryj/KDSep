#include "hashBasedStore/hashStoreFileManager.hpp"
#include "utils/bucketIndexBlock.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"
#include <unordered_map>

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, messageQueue<writeBackObject*>* writeBackOperationsQueue)
{
    maxBucketNumber_ = options->hashStore_max_file_number_;
    uint64_t k = 0;
    while (pow((double)2, (double)k) <= maxBucketNumber_) {
        k++;
    }
    k = k - 1;
    if (options->deltaStore_prefix_tree_initial_bit_number_ > k) {
        initialTrieBitNumber_ = k - 1;
    } else {
        initialTrieBitNumber_ = options->deltaStore_prefix_tree_initial_bit_number_;
    }
    if (options->kd_cache != nullptr) {
        kd_cache_ = options->kd_cache;
    } 
    singleFileGCTriggerSize_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    maxBucketSize_ = options->deltaStore_single_file_maximum_size;
    singleFileMergeGCUpperBoundSize_ = maxBucketSize_ * 0.5;
    enableBatchedOperations_ = options->enable_batched_operations_;
    enableLsmTreeDeltaMeta_ = options->enable_lsm_tree_delta_meta;
    debug_info("[Message]: singleFileGCTriggerSize_ = %lu, singleFileMergeGCUpperBoundSize_ = %lu, initialTrieBitNumber_ = %lu\n", singleFileGCTriggerSize_, singleFileMergeGCUpperBoundSize_, initialTrieBitNumber_);
    globalGCTriggerSize_ = options->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * options->deltaStore_total_storage_maximum_size;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    enableWriteBackDuringGCFlag_ = (writeBackOperationsQueue != nullptr);
    write_back_queue_ = writeBackOperationsQueue;
    gcWriteBackDeltaNum_ = options->deltaStore_gc_write_back_delta_num;
    gcWriteBackDeltaSize_ = options->deltaStore_gc_write_back_delta_size;
    fileOperationMethod_ = options->fileOperationMethod_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    enable_crash_consistency_ = options->enable_crash_consistency;
    operationNumberForMetadataCommitThreshold_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    singleFileSplitGCTriggerSize_ = options->deltaStore_split_garbage_collection_start_single_file_minimum_occupancy_ * options->deltaStore_single_file_maximum_size;
    file_trie_.init(initialTrieBitNumber_, maxBucketNumber_);
    singleFileGCWorkerThreadsNumebr_ = options->deltaStore_gc_worker_thread_number_limit_;
    workingThreadExitFlagVec_ = 0;
    syncStatistics_ = true;
    singleFileFlushSize_ = options->deltaStore_file_flush_buffer_size_limit_;
    deltaKVMergeOperatorPtr_ = options->deltaKV_merge_operation_ptr;
    enable_index_block_ = options->enable_index_block;
    write_stall_ = options->write_stall;
    wb_keys = options->wb_keys;
    wb_keys_mutex = options->wb_keys_mutex;
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::~HashStoreFileManager()
{
    CloseHashStoreFileMetaDataList();
}

bool HashStoreFileManager::setJobDone()
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

void HashStoreFileManager::pushToGCQueue(hashStoreFileMetaDataHandler* file_hdl) {
    notifyGCMQ_->push(file_hdl);
    operationNotifyCV_.notify_one();
}

uint64_t HashStoreFileManager::getTrieAccessNum() {
    return file_trie_.getAccessNum();
}

bool HashStoreFileManager::writeToCommitLog(vector<mempoolHandler_t> objects,
        bool& need_flush) {
    if (objects.size() == 0) {
        return true;
    }
    uint64_t write_buf_sz = 0;
    for (auto i = 0; i < objects.size(); i++) {
        // reserve more space
        write_buf_sz += sizeof(hashStoreRecordHeader) + 
            objects[i].keySize_ + objects[i].valueSize_;
    }

    char write_buf[write_buf_sz];
    uint64_t write_i = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    hashStoreRecordHeader rec_header;
    for (auto i = 0; i < objects.size(); i++) {
        rec_header.is_anchor_ = objects[i].isAnchorFlag_;
        rec_header.is_gc_done_ = false;
        rec_header.key_size_ = objects[i].keySize_;
        rec_header.value_size_ = objects[i].valueSize_;
        rec_header.sequence_number_ = objects[i].sequenceNumber_;
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

    if (commit_log_fop_ == nullptr) {
	commit_log_fop_ = new FileOperation(kDirectIO,
		commit_log_maximum_size_, 0);
        commit_log_fop_->createThenOpenFile(workingDir_ + "/commit.log");
    }

    FileOpStatus status;
    STAT_PROCESS(status = commit_log_fop_->writeAndFlushFile(write_buf,
                write_i),
           StatsType::DELTAKV_HASHSTORE_PUT_COMMIT_LOG); 

    if (status.success_ == false) {
        debug_error("[ERROR] Write to commit log failed: buf size %lu\n", 
                write_i);
    }

    if (commit_log_fop_->getCachedFileSize() > commit_log_next_threshold_) {
        need_flush = true;
        commit_log_next_threshold_ += commit_log_maximum_size_;
    }

    return status.success_;
}

//bool HashStoreFileManager::flushAllBuffers() {
////vector<hashStoreFileMetaDataHandler*> HashStoreFileManager::flushAllBuffers() {
////    vector<pair<uint64_t, hashStoreFileMetaDataHandler*>> validObjectVec;
////    file_trie_.getCurrentValidNodes(validObjectVec);
//
//    return true;
//}

// Recovery
/*
read_buf start after file header
resultMap include key - <is_anchor, value> map
*/
uint64_t HashStoreFileManager::deconstructAndGetAllContentsFromFile(char* read_buf, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone)
{
    uint64_t processedTotalObjectNumber = 0;
    uint64_t read_i = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);

    while (read_i != fileSize) {
        processedTotalObjectNumber++;
        hashStoreRecordHeader header;
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

bool HashStoreFileManager::deleteObslateFileWithFileIDAsInput(uint64_t fileID)
{
    string targetRemoveFilePath = workingDir_ + "/" + to_string(fileID) + ".delta";
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

bool HashStoreFileManager::recoveryFromFailure(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo) // return key to isAnchor + value pair
{
    vector<uint64_t> scannedOnDiskFileIDList;
    // scan file list
    for (const auto& dirEntry : filesystem::recursive_directory_iterator(workingDir_)) {
        string currentFilePath = dirEntry.path();
        if (currentFilePath.find(".delta") != string::npos) {
            currentFilePath = currentFilePath.substr(currentFilePath.find("/") + 1);
            uint64_t currentFileID = stoull(currentFilePath);
            debug_trace("find file name = %s, file ID = %lu\n", currentFilePath.c_str(), currentFileID);
            scannedOnDiskFileIDList.push_back(currentFileID);
        }
    }
    if (shouldDoRecoveryFlag_ == false) {
        debug_trace("DB closed success, do not need recovery, flag = %d, just delete all not tracked files, number = %lu\n", shouldDoRecoveryFlag_, scannedOnDiskFileIDList.size());
        if (scannedOnDiskFileIDList.size() == 0) {
            return true;
        }
        for (auto targetFileID : scannedOnDiskFileIDList) {
            debug_trace("Target delete file ID = %lu\n", targetFileID);
            string targetRemoveFileName = workingDir_ + "/" + to_string(targetFileID) + ".delta";
            auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
            if (removeObsoleteFileStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
                return false;
            } else {
                debug_trace("delete the obsolete delta file, file path = %s\n", targetRemoveFileName.c_str());
                continue;
            }
        }
        debug_info("Deleted all not tracked files, number = %lu\n", scannedOnDiskFileIDList.size());
        return true;
    }
    // buffer target delete file IDs
    vector<uint64_t> targetDeleteFileIDVec;
    // buffer no metadata kGC files generated by split
    unordered_map<uint64_t, vector<uint64_t>> mapForBatchedkInternalGCFiles; // previous file ID to new file ID and file obj
    vector<pair<uint64_t, hashStoreFileMetaDataHandler*>> validPrefixToFileHandlerVec;
    file_trie_.getCurrentValidNodes(validPrefixToFileHandlerVec);
    unordered_map<uint64_t, pair<uint64_t, hashStoreFileMetaDataHandler*>> hashStoreFileIDToPrefixMap;
    for (auto validFileIt : validPrefixToFileHandlerVec) {
        uint64_t currentFileID = validFileIt.second->file_id;
        if (hashStoreFileIDToPrefixMap.find(currentFileID) != hashStoreFileIDToPrefixMap.end()) {
            debug_error("[ERROR] Find duplicate file ID in prefixTree, file ID = %lu\n", currentFileID);
            return false;
        } else {
            hashStoreFileIDToPrefixMap.insert(make_pair(currentFileID, make_pair(validFileIt.first, validFileIt.second)));
        }
    }
    // process files
    for (auto fileIDIt : scannedOnDiskFileIDList) {
        if (hashStoreFileIDToPrefixMap.find(fileIDIt) == hashStoreFileIDToPrefixMap.end()) {
            // file not exist in metadata, should scan and update into metadata
            debug_trace("file ID = %lu not exist in metadata, try recovery\n", fileIDIt);
            if (fileIDIt >= targetNewFileID_) {
                // the file is newly created, should scan
                FileOperation tempReadFileStream(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
                string targetOpenFileName = workingDir_ + "/" + to_string(fileIDIt) + ".delta";
                bool openCurrentFileStatus = tempReadFileStream.openFile(targetOpenFileName);
                if (openCurrentFileStatus == false) {
                    debug_error("[ERROR] could not open file for recovery, file path = %s\n", targetOpenFileName.c_str());
                    return false;
                } else {
                    // read file header for check
                    uint64_t targetFileSize = tempReadFileStream.getFileSize();
                    debug_trace("target read file size = %lu\n", targetFileSize);
                    uint64_t targetFileRemainReadSize = targetFileSize - sizeof(hashStoreFileHeader);
                    char readContentBuffer[targetFileSize];
                    tempReadFileStream.readFile(readContentBuffer, targetFileSize);
                    tempReadFileStream.closeFile();
                    hashStoreFileHeader file_header;
                    memcpy(&file_header, readContentBuffer, sizeof(hashStoreFileHeader));
                    // process file content
                    bool isGCFlushedDoneFlag = false;
                    unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMap;
                    uint64_t currentFileObjectNumber = deconstructAndGetAllContentsFromFile(readContentBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMap, isGCFlushedDoneFlag);

                    if (file_header.file_create_reason_ == kInternalGCFile) {
                        // GC file with ID > targetNewFileID
                        // judge previous file ID
                        if (hashStoreFileIDToPrefixMap.find(file_header.previous_file_id_first_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == false) {
                            // previous ID not in metadata && gc file is not correctly flushed;
                            debug_error("[ERROR] find kGC file that previous file ID not in metadata, seems error, previous file ID = %lu\n", file_header.previous_file_id_first_);
                            return false;
                        } else if (hashStoreFileIDToPrefixMap.find(file_header.previous_file_id_first_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == true) {
                            // gc flushed done, kGC file should add
                            // generate prefix
                            uint64_t prefix_u64;
                            generateHashBasedPrefix(
                                    (char*)currentFileRecoveryMap.begin()->first.c_str(),
                                    currentFileRecoveryMap.begin()->first.size(),
                                    prefix_u64);
                            prefix_u64 = prefixSubstr(prefix_u64, file_header.prefix_bit);
                            // generate file handler
                            hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                            currentRecoveryFileHandler->file_op_ptr = new FileOperation(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
                            currentRecoveryFileHandler->file_id = fileIDIt;
                            currentRecoveryFileHandler->prefix_bit = file_header.prefix_bit;
                            currentRecoveryFileHandler->total_object_cnt = currentFileObjectNumber;
                            currentRecoveryFileHandler->total_object_bytes = targetFileSize;
                            // open current file for further usage
                            currentRecoveryFileHandler->file_op_ptr->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                            // update metadata
                            file_trie_.insertWithFixedBitNumber(prefix_u64,
                                    file_header.prefix_bit,
                                    currentRecoveryFileHandler);
                            currentRecoveryFileHandler->sorted_filter = new BucketKeyFilter();
                            currentRecoveryFileHandler->filter = new BucketKeyFilter();
                            // update recovery data list
                            for (auto recoveryIt : currentFileRecoveryMap) {
                                if (targetListForRedo.find(recoveryIt.first) != targetListForRedo.end()) {
                                    for (auto contentsIt : recoveryIt.second) {
                                        targetListForRedo.at(recoveryIt.first).push_back(contentsIt);
                                    }
                                } else {
                                    targetListForRedo.insert(make_pair(recoveryIt.first, recoveryIt.second));
                                }
                            }
                            continue;
                        } else if (hashStoreFileIDToPrefixMap.find(file_header.previous_file_id_first_) != hashStoreFileIDToPrefixMap.end()) {
                            // previous file metadata exist
                            uint64_t prefixBitNumberUsedInPreviousFile = hashStoreFileIDToPrefixMap.at(file_header.previous_file_id_first_).second->prefix_bit;
                            if (prefixBitNumberUsedInPreviousFile == file_header.prefix_bit) {
                                // single file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    // get file handler
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = hashStoreFileIDToPrefixMap.at(file_header.previous_file_id_first_).second;
                                    currentRecoveryFileHandler->file_id = fileIDIt;
                                    currentRecoveryFileHandler->prefix_bit = file_header.prefix_bit;
                                    currentRecoveryFileHandler->total_object_cnt = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes = targetFileSize;
                                    // open current file for further usage
                                    if (currentRecoveryFileHandler->file_op_ptr->isFileOpen() == true) {
                                        currentRecoveryFileHandler->file_op_ptr->closeFile();
                                    }
                                    currentRecoveryFileHandler->file_op_ptr->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    // update metadata
                                    hashStoreFileIDToPrefixMap.at(file_header.previous_file_id_first_).second->file_id = fileIDIt;
                                    // update recovery data list
                                    for (auto recoveryIt : currentFileRecoveryMap) {
                                        if (targetListForRedo.find(recoveryIt.first) != targetListForRedo.end()) {
                                            for (auto contentsIt : recoveryIt.second) {
                                                targetListForRedo.at(recoveryIt.first).push_back(contentsIt);
                                            }
                                        } else {
                                            targetListForRedo.insert(make_pair(recoveryIt.first, recoveryIt.second));
                                        }
                                    }
                                    targetDeleteFileIDVec.push_back(file_header.previous_file_id_first_);
                                    continue;
                                } else {
                                    // gc not success, keep old file, and delete current one
                                    targetDeleteFileIDVec.push_back(fileIDIt);
                                    continue;
                                }
                            } else if (prefixBitNumberUsedInPreviousFile < file_header.prefix_bit) {
                                // file created by split, cache to find the other one
                                if (mapForBatchedkInternalGCFiles.find(file_header.previous_file_id_first_) == mapForBatchedkInternalGCFiles.end()) {
                                    vector<uint64_t> tempVec;
                                    tempVec.push_back(fileIDIt);
                                    mapForBatchedkInternalGCFiles.insert(make_pair(file_header.previous_file_id_first_, tempVec));
                                } else {
                                    mapForBatchedkInternalGCFiles.at(file_header.previous_file_id_first_).push_back(fileIDIt);
                                }
                            } else if (prefixBitNumberUsedInPreviousFile > file_header.prefix_bit) {
                                // merge file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    uint64_t prefix1 =
                                        hashStoreFileIDToPrefixMap.at(file_header.previous_file_id_first_).first;
                                    uint64_t prefix1_len = prefixLenExtract(prefix1); 
                                    prefix1 = prefixExtract(prefix1);
                                    // TODO check whether prefix1_len equals to file_header.prefix_bit?

                                    uint64_t prefix2 = prefixSubstr(prefix1, file_header.prefix_bit);
                                    uint64_t prefix2_len = file_header.prefix_bit + 1;
                                    prefix2 |= (1ull << file_header.prefix_bit);

                                    // we store left file as previous file ID
                                    // (last bit in use = 0)
                                    uint64_t leftFatherFileID = file_header.previous_file_id_first_;
                                    hashStoreFileMetaDataHandler* tempHandlerForRightFileID;
                                    file_trie_.get(prefix2, tempHandlerForRightFileID);
                                    uint64_t rightFatherFileID = tempHandlerForRightFileID->file_id;
                                    // delete left father
                                    uint64_t findFileAtLevel = 0;
                                    file_trie_.remove(prefix1, prefix1_len,
                                            findFileAtLevel);
                                    if (findFileAtLevel != prefix1_len) {
                                        debug_error("[ERROR] In merged gc, find previous left file prefix mismatch, in prefix tree, bit number = %lu, but the file used %lu in header\n", findFileAtLevel, prefix1_len);
                                        return false;
                                    }
                                    targetDeleteFileIDVec.push_back(leftFatherFileID);
                                    // delete right father
                                    file_trie_.remove(prefix2, prefix2_len,
                                            findFileAtLevel);
                                    if (findFileAtLevel != prefix2_len) {
                                        debug_error("[ERROR] In merged gc, find previous right file prefix mismatch, in prefix tree, bit number = %lu, but the file used %lu in header\n", findFileAtLevel, prefix2_len);
                                        return false;
                                    }
                                    targetDeleteFileIDVec.push_back(rightFatherFileID);
                                    // insert new file into metadata
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                                    currentRecoveryFileHandler->file_op_ptr = new FileOperation(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
                                    currentRecoveryFileHandler->file_id = fileIDIt;
                                    currentRecoveryFileHandler->prefix_bit = file_header.prefix_bit;
                                    currentRecoveryFileHandler->total_object_cnt = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes = targetFileSize;
                                    currentRecoveryFileHandler->sorted_filter = new BucketKeyFilter();
                                    currentRecoveryFileHandler->filter = new BucketKeyFilter();
                                    // open current file for further usage
                                    currentRecoveryFileHandler->file_op_ptr->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");

                                    uint64_t prefixU64 = prefixSubstr(prefix1, file_header.prefix_bit);
                                    // update metadata
                                    file_trie_.insertWithFixedBitNumber(prefixU64,
                                            file_header.prefix_bit,
                                            currentRecoveryFileHandler);
                                    // update recovery data list
                                    for (auto recoveryIt : currentFileRecoveryMap) {
                                        if (targetListForRedo.find(recoveryIt.first) != targetListForRedo.end()) {
                                            for (auto contentsIt : recoveryIt.second) {
                                                targetListForRedo.at(recoveryIt.first).push_back(contentsIt);
                                            }
                                        } else {
                                            targetListForRedo.insert(make_pair(recoveryIt.first, recoveryIt.second));
                                        }
                                    }
                                    continue;
                                } else {
                                    // gc not success, keep old file, and delete current one
                                    targetDeleteFileIDVec.push_back(fileIDIt);
                                    continue;
                                }
                            }
                        }
                    } else if (file_header.file_create_reason_ == kNewFile) {
                        // new file with ID > targetNewFileID, should add into metadata
                        hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                        currentRecoveryFileHandler->file_op_ptr = new FileOperation(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
                        currentRecoveryFileHandler->file_id = fileIDIt;
                        currentRecoveryFileHandler->prefix_bit = file_header.prefix_bit;
                        currentRecoveryFileHandler->total_object_cnt = currentFileObjectNumber;
                        currentRecoveryFileHandler->total_object_bytes = targetFileSize;
                        currentRecoveryFileHandler->sorted_filter = new BucketKeyFilter();
                        currentRecoveryFileHandler->filter = new BucketKeyFilter();
                        // open current file for further usage
                        currentRecoveryFileHandler->file_op_ptr->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                        // update metadata
                        uint64_t prefixU64 = 0;
                        generateHashBasedPrefix((char*)currentFileRecoveryMap.begin()->first.c_str(),
                                currentFileRecoveryMap.begin()->first.size(),
                                prefixU64);
                        file_trie_.insertWithFixedBitNumber(prefixU64, file_header.prefix_bit, currentRecoveryFileHandler);
                        // update recovery data list
                        for (auto recoveryIt : currentFileRecoveryMap) {
                            if (targetListForRedo.find(recoveryIt.first) != targetListForRedo.end()) {
                                for (auto contentsIt : recoveryIt.second) {
                                    targetListForRedo.at(recoveryIt.first).push_back(contentsIt);
                                }
                            } else {
                                targetListForRedo.insert(make_pair(recoveryIt.first, recoveryIt.second));
                            }
                        }
                    } else {
                        debug_error("[ERROR] Read file header with unknown create reason, file path = %s\n", targetOpenFileName.c_str());
                        return false;
                    }
                }
            } else {
                // the file not in metadata, but ID smaller than committed ID, should delete
                targetDeleteFileIDVec.push_back(fileIDIt);
            }
        } else {
            // file exist in metadata
            debug_trace("File ID = %lu exist in metadata, try skip or partial recovery\n", fileIDIt);
            // get metadata file
            hashStoreFileMetaDataHandler* currentIDInMetadataFileHandlerPtr;
            uint64_t k = hashStoreFileIDToPrefixMap.at(fileIDIt).first;
            uint64_t prefix_u64 = prefixExtract(k);
            file_trie_.get(prefix_u64, currentIDInMetadataFileHandlerPtr);
            uint64_t onDiskFileSize = currentIDInMetadataFileHandlerPtr->file_op_ptr->getFileSize();
            if (currentIDInMetadataFileHandlerPtr->total_object_bytes > onDiskFileSize) {
                // metadata size > filesystem size, error
                debug_error("[ERROR] file ID = %lu, file size in metadata = %lu larger than file size in file system = %lu\n", fileIDIt, currentIDInMetadataFileHandlerPtr->total_object_bytes, onDiskFileSize);
            } else if (currentIDInMetadataFileHandlerPtr->total_object_bytes < onDiskFileSize) {
                // file may append, should recovery
                debug_trace("target file ID = %lu, file size (system) = %lu != file size (metadata) = %lu, try recovery\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes);

                // start read
                int targetReadSize = onDiskFileSize;
                char readBuffer[targetReadSize];
                debug_trace("target read file content for recovery size = %lu\n", currentIDInMetadataFileHandlerPtr->total_object_bytes);
                currentIDInMetadataFileHandlerPtr->file_op_ptr->readFile(readBuffer, targetReadSize);
                // read done, start process
                bool isGCFlushedDoneFlag = false;
                uint64_t recoveredObjectNumber = deconstructAndGetAllContentsFromFile(readBuffer + currentIDInMetadataFileHandlerPtr->total_object_bytes, targetReadSize - currentIDInMetadataFileHandlerPtr->total_object_bytes, targetListForRedo, isGCFlushedDoneFlag);
                // update metadata
                currentIDInMetadataFileHandlerPtr->total_object_cnt += recoveredObjectNumber;
                currentIDInMetadataFileHandlerPtr->total_object_bytes += targetReadSize;
            } else {
                // file size match, skip current file
                debug_trace("target file ID = %lu, file size (system) = %lu, file size (metadata) = %lu\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes);
                continue;
            }
        }
    }
    // process not in metadata files created by split old file
    for (auto splitFileIt : mapForBatchedkInternalGCFiles) {
        // check split status;
        uint64_t exactFileNumber = 0;
        bool gcFlagStatus[2];
        uint64_t objectNumberCount[2];
        uint64_t targetFileRealSize[2];
        uint64_t prefixBitNumber = 0;
        unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMapTemp[2];
        for (int i = 0; i < 2; i++) {
            hashStoreFileHeader file_header;
            FileOperation tempReadFileStream(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
            uint64_t currentFileSize = tempReadFileStream.getFileSize();
            targetFileRealSize[i] = currentFileSize;
            string targetOpenFileName = workingDir_ + "/" + to_string(splitFileIt.second[i]) + ".delta";
            bool fileOpenStatus = tempReadFileStream.openFile(targetOpenFileName);
            if (fileOpenStatus == false) {
                break;
            } else {
                exactFileNumber++;
            }
            char readBuffer[currentFileSize];
            tempReadFileStream.readFile(readBuffer, currentFileSize);
            memcpy(&file_header, readBuffer, sizeof(hashStoreFileHeader));
            prefixBitNumber = file_header.prefix_bit;
            uint64_t targetFileRemainReadSize = currentFileSize - sizeof(hashStoreFileHeader);
            tempReadFileStream.closeFile();
            // process file content
            objectNumberCount[i] = deconstructAndGetAllContentsFromFile(readBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMapTemp[i], gcFlagStatus[i]);
        }
        if ((exactFileNumber == 1 && gcFlagStatus[0]) == true || (exactFileNumber == 2 && gcFlagStatus[0] && gcFlagStatus[1]) == true) {
            // keep two new files
            for (int i = 0; i < exactFileNumber; i++) {
                hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                currentRecoveryFileHandler->file_op_ptr = new FileOperation(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
                currentRecoveryFileHandler->file_id = splitFileIt.second[i];
                currentRecoveryFileHandler->prefix_bit = prefixBitNumber;
                currentRecoveryFileHandler->total_object_cnt = objectNumberCount[i];
                currentRecoveryFileHandler->total_object_bytes = targetFileRealSize[i];
                currentRecoveryFileHandler->sorted_filter = new BucketKeyFilter();
                currentRecoveryFileHandler->filter = new BucketKeyFilter();
                // open current file for further usage
                currentRecoveryFileHandler->file_op_ptr->openFile(workingDir_ + "/" + to_string(splitFileIt.second[i]) + ".delta");
                // update metadata
                uint64_t prefix_u64;
                generateHashBasedPrefix((char*)currentFileRecoveryMapTemp[i].begin()->first.c_str(),
                        currentFileRecoveryMapTemp[i].begin()->first.size(),
                        prefix_u64);
                file_trie_.insertWithFixedBitNumber(prefix_u64, prefixBitNumber, currentRecoveryFileHandler);
                // update recovery data list
                for (auto recoveryIt : currentFileRecoveryMapTemp[i]) {
                    if (targetListForRedo.find(recoveryIt.first) != targetListForRedo.end()) {
                        for (auto contentsIt : recoveryIt.second) {
                            targetListForRedo.at(recoveryIt.first).push_back(contentsIt);
                        }
                    } else {
                        targetListForRedo.insert(make_pair(recoveryIt.first, recoveryIt.second));
                    }
                }
            }
            hashStoreFileMetaDataHandler* tempRemoveHandler;
            uint64_t k = hashStoreFileIDToPrefixMap.at(splitFileIt.first).first; 
            uint64_t prefix_u64 = prefixExtract(k); 
            uint64_t prefix_len_tmp = prefixLenExtract(k);
            file_trie_.get(prefix_u64, tempRemoveHandler);
            if (tempRemoveHandler->file_op_ptr->isFileOpen()) {
                tempRemoveHandler->file_op_ptr->closeFile();
            }
            uint64_t findPrefixInTreeAtLevelID;
            file_trie_.remove(prefix_u64, prefix_len_tmp, findPrefixInTreeAtLevelID);
            if (prefix_len_tmp != findPrefixInTreeAtLevelID) {
                debug_error("[ERROR] Remove object in prefix tree error, the"
                        " prefix length mismatch, in tree length = %lu, in file"
                        " length = %lu\n", findPrefixInTreeAtLevelID,
                        prefix_len_tmp);
                return false;
            }
            targetDeleteFileIDVec.push_back(splitFileIt.first);
        } else {
            // keep old file
            targetDeleteFileIDVec.push_back(splitFileIt.second[0]);
            targetDeleteFileIDVec.push_back(splitFileIt.second[1]);
        }
    }
    // before delete, check max file ID, and update next file ID;
    uint64_t maxFileIDExist = 0;
    for (auto fileIDIt : scannedOnDiskFileIDList) {
        if (fileIDIt > maxFileIDExist) {
            maxFileIDExist = fileIDIt;
        }
    }
    targetNewFileID_ = maxFileIDExist++;
    // delete files
    for (auto targetFileID : targetDeleteFileIDVec) {
        debug_trace("Target delete file ID = %lu\n", targetFileID);
        string targetRemoveFileName = workingDir_ + "/" + to_string(targetFileID) + ".delta";
        auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
        if (removeObsoleteFileStatus == -1) {
            debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
            return false;
        } else {
            debug_trace("delete the obsolete delta file, file path = %s\n", targetRemoveFileName.c_str());
            continue;
        }
    }
    bool updateMetadataStatus = UpdateHashStoreFileMetaDataList();
    if (updateMetadataStatus == true) {
        return true;
    } else {
        return false;
    }
}

// Manager's metadata management
bool HashStoreFileManager::RetriveHashStoreFileMetaDataList()
{
    fstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::in);
    string currentPointerStr;
    if (hashStoreFileManifestPointerStream.is_open()) {
        getline(hashStoreFileManifestPointerStream, currentPointerStr);
        string closeFlagStr;
        getline(hashStoreFileManifestPointerStream, closeFlagStr);
        if (closeFlagStr.size() == 0) {
            shouldDoRecoveryFlag_ = true;
        } else {
            shouldDoRecoveryFlag_ = false;
        }
        hashStoreFileManifestPointerStream.close();
    } else {
        if (CreateHashStoreFileMetaDataListIfNotExist()) {
            // first load, not need to recovery
            shouldDoRecoveryFlag_ = false;
            return true;
        } else {
            return false;
        }
    }
    ifstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifestFile." + currentPointerStr, ios::in);
    string currentLineStr;
    if (hashStoreFileManifestStream.is_open()) {
        getline(hashStoreFileManifestStream, currentLineStr);
        targetNewFileID_ = stoull(currentLineStr) + 1; // update next file ID from metadata
        while (getline(hashStoreFileManifestStream, currentLineStr)) {
            string prefixHashStr = currentLineStr;
            getline(hashStoreFileManifestStream, currentLineStr);
            uint64_t hashStoreFileID = stoull(currentLineStr);
            getline(hashStoreFileManifestStream, currentLineStr);
            uint64_t currentFileUsedPrefixLength = stoull(currentLineStr);
            getline(hashStoreFileManifestStream, currentLineStr);
            uint64_t currentFileStoredObjectCount = stoull(currentLineStr);
            getline(hashStoreFileManifestStream, currentLineStr);
            uint64_t currentFileStoredBytes = stoull(currentLineStr);
            getline(hashStoreFileManifestStream, currentLineStr);
            uint64_t currentFileStoredPhysicalBytes = stoull(currentLineStr);
            hashStoreFileMetaDataHandler* file_hdl = new hashStoreFileMetaDataHandler;
            file_hdl->file_op_ptr = new FileOperation(fileOperationMethod_, maxBucketSize_, singleFileFlushSize_);
            file_hdl->file_id = hashStoreFileID;
            file_hdl->prefix_bit = currentFileUsedPrefixLength;
            file_hdl->total_object_cnt = currentFileStoredObjectCount;
            file_hdl->total_object_bytes = currentFileStoredBytes;
            file_hdl->total_on_disk_bytes = currentFileStoredPhysicalBytes;
            file_hdl->sorted_filter = new BucketKeyFilter();
            file_hdl->filter = new BucketKeyFilter();
            // open current file for further usage
            file_hdl->file_op_ptr->openFile(workingDir_ + "/" + to_string(file_hdl->file_id) + ".delta");
            uint64_t onDiskFileSize = file_hdl->file_op_ptr->getFileSize();
            if (onDiskFileSize > file_hdl->total_object_bytes && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, file_hdl->total_object_bytes);
            } else if (onDiskFileSize < file_hdl->total_object_bytes && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, file_hdl->total_object_bytes);
            }
            // re-insert into trie and map for build index
            if (currentFileUsedPrefixLength != prefixHashStr.size()) {
                debug_error("[ERROR] prefix len error: %lx v.s. %lx\n",
                        currentFileUsedPrefixLength, prefixHashStr.size());
                exit(1);
            }
            uint64_t prefix_u64 = prefixStrToU64(prefixHashStr);
            file_trie_.insertWithFixedBitNumber(prefix_u64,
                    currentFileUsedPrefixLength, file_hdl);
        }
        
        if (DEBUG_LEVEL >= DebugOutPutLevel::INFO) {
            vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec;
            file_trie_.getCurrentValidNodes(validObjectVec);
            for (auto it : validObjectVec) {
                debug_info("Read prefix = %s, file ID = %lu from metadata\n", it.first.c_str(), it.second->file_id);
            }
        }
    } else {
        return false;
    }
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList()
{
    vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec;
    vector<hashStoreFileMetaDataHandler*> invalidObjectVec;
    file_trie_.getCurrentValidNodes(validObjectVec);
    debug_info("Start update metadata, current valid trie size = %lu\n", validObjectVec.size());
    bool shouldUpdateFlag = false;
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_op_ptr->isFileOpen() == true) {
                shouldUpdateFlag = true;
                break;
            }
        }
    }
    if (shouldUpdateFlag == false) {
        debug_info("Since no bucket open, should not perform metadata update, current valid file handler number = %lu\n", validObjectVec.size());
        return true;
    }
    fstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::in);
    uint64_t currentPointerInt = 0;
    if (hashStoreFileManifestPointerStream.is_open()) {
        hashStoreFileManifestPointerStream >> currentPointerInt;
        currentPointerInt++;
    } else {
        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
        return false;
    }
    hashStoreFileManifestPointerStream.close();
    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_op_ptr->isFileOpen() == true) {
                std::scoped_lock<std::shared_mutex> flush_lock(it.second->fileOperationMutex_);
//                FileOpStatus flushedSizePair = it.second->file_op_ptr->flushFile();
//                StatsRecorder::getInstance()->DeltaOPBytesWrite(flushedSizePair.physicalSize_, flushedSizePair.logicalSize_, syncStatistics_);
                debug_trace("flushed file ID = %lu, file correspond prefix = %s\n", it.second->file_id, it.first.c_str());
//                it.second->total_on_disk_bytes += flushedSizePair.physicalSize_;
                hashStoreFileManifestStream << it.first << endl;
                hashStoreFileManifestStream << it.second->file_id << endl;
                hashStoreFileManifestStream << it.second->prefix_bit << endl;
                hashStoreFileManifestStream << it.second->total_object_cnt << endl;
                hashStoreFileManifestStream << it.second->total_object_bytes << endl;
                hashStoreFileManifestStream << it.second->total_on_disk_bytes << endl;
            }
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
    }
    // Update manifest pointer
    fstream hashStoreFileManifestPointerUpdateStream;
    hashStoreFileManifestPointerUpdateStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::out);
    if (hashStoreFileManifestPointerUpdateStream.is_open()) {
        hashStoreFileManifestPointerUpdateStream << currentPointerInt;
        hashStoreFileManifestPointerUpdateStream.flush();
        hashStoreFileManifestPointerUpdateStream.close();
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt - 1);
        if (filesystem::exists(targetRemoveFileName) != false) {
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
            }
        }
        file_trie_.getInvalidNodesNoKey(invalidObjectVec);
        debug_info("Start delete obslate files, current invalid trie size = %lu\n", invalidObjectVec.size());
        if (invalidObjectVec.size() != 0) {
            for (auto it : invalidObjectVec) {
                if (it) {
                    if (it->file_op_ptr->isFileOpen() == true) {
                        it->file_op_ptr->closeFile();
                        debug_trace("Closed file ID = %lu\n", it->file_id);
                    }
                }
            }
        }
        fileDeleteVecMtx_.lock();
        for (auto it : targetDeleteFileHandlerVec_) {
            deleteObslateFileWithFileIDAsInput(it);
        }
        targetDeleteFileHandlerVec_.clear();
        fileDeleteVecMtx_.unlock();
        return true;
    } else {
        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
        return false;
    }
}

bool HashStoreFileManager::RemoveObsoleteFiles() {
    // Update manifest pointer
    vector<hashStoreFileMetaDataHandler*> invalidObjectVec;
    file_trie_.getInvalidNodesNoKey(invalidObjectVec);
    debug_info("Start delete obslate files, current invalid trie size = %lu\n", invalidObjectVec.size());
    if (invalidObjectVec.size() != 0) {
        for (auto it : invalidObjectVec) {
            if (it) {
                if (it->file_op_ptr->isFileOpen() == true) {
                    it->file_op_ptr->closeFile();
                    debug_trace("Closed file ID = %lu\n", it->file_id);
                }
            }
        }
    }
    fileDeleteVecMtx_.lock();
    for (auto it : targetDeleteFileHandlerVec_) {
        deleteObslateFileWithFileIDAsInput(it);
    }
    targetDeleteFileHandlerVec_.clear();
    fileDeleteVecMtx_.unlock();
    return true;
}

bool
HashStoreFileManager::prepareForUpdatingMetadata(
        vector<hashStoreFileMetaDataHandler*>& vec)
{
    file_trie_.getCurrentValidNodesNoKey(vec);
    return true;
}

bool HashStoreFileManager::CloseHashStoreFileMetaDataList()
{
    fstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::in);
    uint64_t currentPointerInt = 0;
    if (hashStoreFileManifestPointerStream.is_open()) {
        hashStoreFileManifestPointerStream >> currentPointerInt;
        currentPointerInt++;
    } else {
        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
        return false;
    }
    hashStoreFileManifestPointerStream.close();
    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    vector<uint64_t> targetDeleteFileIDVec;
    vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec;
    file_trie_.getCurrentValidNodes(validObjectVec);
    debug_info("Final commit metadata, current valid trie size = %lu\n", validObjectVec.size());
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_op_ptr->isFileOpen() == true) {
                FileOpStatus flushedSizePair = it.second->file_op_ptr->flushFile();
                StatsRecorder::getInstance()->DeltaOPBytesWrite(flushedSizePair.physicalSize_, flushedSizePair.logicalSize_, syncStatistics_);
                it.second->total_on_disk_bytes += flushedSizePair.physicalSize_;
                it.second->file_op_ptr->closeFile();
                hashStoreFileManifestStream << it.first << endl;
                hashStoreFileManifestStream << it.second->file_id << endl;
                hashStoreFileManifestStream << it.second->prefix_bit << endl;
                hashStoreFileManifestStream << it.second->total_object_cnt << endl;
                hashStoreFileManifestStream << it.second->total_object_bytes << endl;
                hashStoreFileManifestStream << it.second->total_on_disk_bytes << endl;
            }
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
    }
    // Update manifest pointer
    fstream hashStoreFileManifestPointerUpdateStream;
    hashStoreFileManifestPointerUpdateStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::out);
    if (hashStoreFileManifestPointerUpdateStream.is_open()) {
        hashStoreFileManifestPointerUpdateStream << currentPointerInt << endl;
        bool closedSuccessFlag = true;
        hashStoreFileManifestPointerUpdateStream << closedSuccessFlag << endl;
        hashStoreFileManifestPointerUpdateStream.flush();
        hashStoreFileManifestPointerUpdateStream.close();
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt - 1);
        if (filesystem::exists(targetRemoveFileName) != false) {
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
            }
        }
        vector<pair<string, hashStoreFileMetaDataHandler*>> possibleValidObjectVec;
        file_trie_.getPossibleValidNodes(possibleValidObjectVec);
        for (auto it : possibleValidObjectVec) {
            if (it.second) {
                if (it.second->file_op_ptr->isFileOpen() == true) {
                    it.second->file_op_ptr->closeFile();
                }
                if (it.second->index_block) {
                    delete it.second->index_block;
                }
                delete it.second->file_op_ptr;
                delete it.second->sorted_filter;
                delete it.second->filter;
                delete it.second;
            }
        }
        fileDeleteVecMtx_.lock();
        for (auto it : targetDeleteFileHandlerVec_) {
            deleteObslateFileWithFileIDAsInput(it);
        }
        targetDeleteFileHandlerVec_.clear();
        fileDeleteVecMtx_.unlock();
        return true;
    } else {
        debug_error("[ERROR] could not update hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
        vector<pair<string, hashStoreFileMetaDataHandler*>> possibleValidObjectVec;
        file_trie_.getPossibleValidNodes(possibleValidObjectVec);
        for (auto it : possibleValidObjectVec) {
            if (it.second) {
                if (it.second->file_op_ptr->isFileOpen() == true) {
                    it.second->file_op_ptr->closeFile();
                }
                if (it.second->index_block) {
                    delete it.second->index_block;
                }
                delete it.second->file_op_ptr;
                delete it.second->sorted_filter;
                delete it.second->filter;
                delete it.second;
            }
        }
        return false;
    }
}

bool HashStoreFileManager::CreateHashStoreFileMetaDataListIfNotExist()
{
    fstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::out);
    uint64_t currentPointerInt = 0;
    if (hashStoreFileManifestPointerStream.is_open()) {
        hashStoreFileManifestPointerStream << currentPointerInt << endl;
        hashStoreFileManifestPointerStream.flush();
        hashStoreFileManifestPointerStream.close();
        ofstream hashStoreFileManifestStream;
        hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifestFile." + to_string(currentPointerInt), ios::out);
        hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        return true;
    } else {
        debug_error("[ERROR] Could not open hashStore file metadata list pointer file currentDeltaPointer = %lu\n", currentPointerInt);
        return false;
    }
}

// file operations - public
// A modification: add "getForAnchorWriting". If true, and if the file handler
// does not exist, do not create the file and directly return.
bool HashStoreFileManager::getFileHandlerWithKey(char* keyBuffer, 
        uint32_t keySize, hashStoreFileOperationType op_type,
        hashStoreFileMetaDataHandler*& file_hdl, bool getForAnchorWriting)
{
    if (op_type == kPut || op_type == kMultiPut) {
        operationCounterMtx_.lock();
        operationCounterForMetadataCommit_++;
        if (operationCounterForMetadataCommit_ >=
                operationNumberForMetadataCommitThreshold_) {
            metaCommitCV_.notify_one();
        }
        operationCounterMtx_.unlock();
    }

    struct timeval tv;
    uint64_t prefix_u64;
    bool genPrefixStatus;

    // 1. Generate prefix
    gettimeofday(&tv, 0);
    genPrefixStatus = generateHashBasedPrefix(keyBuffer, keySize, prefix_u64);
    if (op_type == kMultiPut) {
        StatsRecorder::getInstance()->timeProcess(
                StatsType::DSTORE_MULTIPUT_PREFIX, tv);
    } else {
        StatsRecorder::getInstance()->timeProcess(
                StatsType::DSTORE_PREFIX, tv);
    }

    if (genPrefixStatus == false) {
        debug_error("[ERROR]  generate prefix hash for current key error, key = %s\n", keyBuffer);
        return false;
    }

    // 2. Search the prefix tree 
    bool getFileHandlerStatus;
    gettimeofday(&tv, 0);
    getFileHandlerStatus =
            getHashStoreFileHandlerByPrefix(prefix_u64, file_hdl);
    if (op_type == kMultiPut) {
        StatsRecorder::getInstance()->timeProcess(
                StatsType::DSTORE_MULTIPUT_GET_HANDLER, tv);
    }

    if (getFileHandlerStatus == false && 
            (op_type == kGet || op_type == kMultiGet)) {
        file_hdl = nullptr;
        return true;
    }

    if (getFileHandlerStatus == false && getForAnchorWriting == true) {
        file_hdl = nullptr;
        return true;
    }

    // 4. Create file if necessary 
    if (getFileHandlerStatus == false) {
        // not writing anchors, or reading the file. Need to create
        bool createNewFileHandlerStatus;
        STAT_PROCESS(createNewFileHandlerStatus =
                createAndGetNewHashStoreFileHandlerByPrefixForUser(prefix_u64,
                    file_hdl),
                StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
        if (!createNewFileHandlerStatus || file_hdl == nullptr) {
            debug_error("[ERROR] create new bucket, key %.*s\n", 
                    (int)keySize, keyBuffer);
            return false;
        } else {
            debug_info("[Insert] Create new file ID = %lu, for key = %.*s, file"
                    " gc status flag = %d, prefix bit number used = %lu\n",
                    file_hdl->file_id, (int)keySize, keyBuffer,
                    file_hdl->gc_status,
                    file_hdl->prefix_bit);
            if (op_type == kMultiPut) {
                file_hdl->markedByMultiPut = true;
            } else if (op_type == kMultiGet) {
                file_hdl->markedByMultiGet = true;
            }
            file_hdl->file_ownership = 1;
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
                getHashStoreFileHandlerByPrefix(prefix_u64, file_hdl);
            if (op_type == kMultiPut) {
                StatsRecorder::getInstance()->timeProcess(
                        StatsType::DSTORE_MULTIPUT_GET_HANDLER, tv);
            }
        }
        first = false;

        if (getFileHandlerStatus == false
                && (op_type == kPut || op_type == kMultiPut)) {
            debug_error("come here %d\n", 1);
            bool createNewFileHandlerStatus;
            STAT_PROCESS(createNewFileHandlerStatus =
                    createAndGetNewHashStoreFileHandlerByPrefixForUser(
                        prefix_u64, file_hdl),
                    StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
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
                        file_hdl->file_id, keyBuffer, file_hdl->gc_status,
                        file_hdl->prefix_bit);
                if (op_type == kMultiPut) {
                    file_hdl->markedByMultiPut = true;
                } else {
                    file_hdl->markedByMultiGet = true;
                }
                file_hdl->file_ownership = 1;
                return true;
            }
        } else {
            if (file_hdl->file_ownership == 1 && 
                    ((op_type == kMultiPut && 
                     file_hdl->markedByMultiPut == true) ||
                    (op_type == kMultiGet &&
                      file_hdl->markedByMultiGet == true))) {
                StatsRecorder::getInstance()->timeProcess(
                        StatsType::DSTORE_GET_HANDLER_LOOP, tv_loop);
                return true;
            }
            // avoid get file handler which is in GC;
            if (file_hdl->file_ownership != 0) {
//                debug_error("Wait for file ownership, file ID = %lu, "
//                        " own = %d, gc status %d\n", file_hdl->file_id, 
//                        (int)file_hdl->file_ownership,
//                        (int)file_hdl->gc_status);
                debug_trace("Wait for file ownership, file ID = %lu, for"
                        " key = %s\n", file_hdl->file_id, keyBuffer);
                struct timeval tv, tv2;
                gettimeofday(&tv, 0);
                while (file_hdl->file_ownership == -1 ||
                        (file_hdl->file_ownership == 1 && 
                         (!(op_type == kMultiPut && file_hdl->markedByMultiPut)
                          &&
                         !(op_type == kMultiGet &&
                             file_hdl->markedByMultiGet)))) {
                    gettimeofday(&tv2, 0);
                    if (tv2.tv_sec - tv.tv_sec > 10) {
                        debug_error("wait for 5 seconds; own %d, id %d, op %d\n",
                                (int)file_hdl->file_ownership,
                                (int)file_hdl->file_id,
                                (int)op_type);
                        tv = tv2;
                    }
//                    asm volatile("");
                    // wait if file is using in gc
                }
                debug_trace("Wait for file ownership, file ID = %lu, for"
                        " key = %s over\n", file_hdl->file_id,
                        keyBuffer);
            }

            if (file_hdl->gc_status == kShouldDelete) {
                // retry if the file should delete;
                debug_warn("Get exist file ID = %lu, for key = %s, "
                        "this file is marked as kShouldDelete\n",
                        file_hdl->file_id, keyBuffer);
                continue;
            } else {
                debug_trace("Get exist file ID = %lu, for key = %s\n",
                        file_hdl->file_id, keyBuffer);
                if (op_type == kMultiPut) {
                    file_hdl->markedByMultiPut = true;
                } else if (op_type == kMultiGet) {
                    file_hdl->markedByMultiGet = true;
                }
                file_hdl->file_ownership = 1;
                StatsRecorder::getInstance()->timeProcess(
                        StatsType::DSTORE_GET_HANDLER_LOOP, tv_loop);
                return true;
            }
        }
    }
    return true;
}

// file operations - private
bool HashStoreFileManager::generateHashBasedPrefix(char* rawStr, 
        uint32_t strSize, uint64_t& prefixU64) {
//    u_char murmurHashResultBuffer[16];
//    MurmurHash3_x64_128((void*)rawStr, strSize, 0, murmurHashResultBuffer);
//    memcpy(&prefixU64, murmurHashResultBuffer, sizeof(uint64_t));
    prefixU64 = XXH64(rawStr, strSize, 10);
    return true;
}

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(
        const uint64_t& prefixU64, 
        hashStoreFileMetaDataHandler*& file_hdl)
{
    bool handlerGetStatus = file_trie_.get(prefixU64, file_hdl);
    if (handlerGetStatus == true) {
        return true;
    } else {
        debug_trace("Could not find prefix = %lx for any length in trie, need "
                "to create\n", prefixU64);
        return false;
    }
}

bool
HashStoreFileManager::createAndGetNewHashStoreFileHandlerByPrefixForUser(const
        uint64_t& prefixU64, hashStoreFileMetaDataHandler*& file_hdl)
{
    hashStoreFileMetaDataHandler* tmp_file_hdl = new hashStoreFileMetaDataHandler;
    tmp_file_hdl->file_op_ptr = new
        FileOperation(fileOperationMethod_, maxBucketSize_,
                singleFileFlushSize_);
    tmp_file_hdl->file_id = generateNewFileID();
    tmp_file_hdl->file_ownership = 0;
    tmp_file_hdl->gc_status = kNew;
    tmp_file_hdl->total_object_bytes = 0;
    tmp_file_hdl->total_on_disk_bytes = 0;
    tmp_file_hdl->total_object_cnt = 0;
    tmp_file_hdl->previous_file_id_first_ = 0xffffffffffffffff;
    tmp_file_hdl->file_create_reason_ = kNewFile;
    tmp_file_hdl->sorted_filter = new BucketKeyFilter();
    tmp_file_hdl->filter = new BucketKeyFilter();
    // move pointer for return
    uint64_t finalInsertLevel = file_trie_.insert(prefixU64, tmp_file_hdl);
    if (finalInsertLevel == 0) {
        debug_error("[ERROR] Error insert to prefix tree, prefix length used ="
                " %lu, inserted file ID = %lu\n",
                tmp_file_hdl->prefix_bit,
                tmp_file_hdl->file_id);
        file_hdl = nullptr;
        return false;
    } else {
        tmp_file_hdl->prefix_bit = finalInsertLevel;
        file_hdl = tmp_file_hdl;
        return true;
    }
}

bool HashStoreFileManager::createFileHandlerForGC(
        hashStoreFileMetaDataHandler*& ret_file_hdl, uint64_t
        targetPrefixLen, uint64_t previousFileID1, uint64_t previousFileID2,
        hashStoreFileHeader& newFileHeader)
{
    hashStoreFileMetaDataHandler* file_hdl = new hashStoreFileMetaDataHandler;
    file_hdl->file_op_ptr = new
        FileOperation(fileOperationMethod_, maxBucketSize_,
                singleFileFlushSize_);
    file_hdl->prefix_bit = targetPrefixLen;
    file_hdl->file_id = generateNewFileID();
    file_hdl->file_ownership = -1;
    file_hdl->gc_status = kNew;
    file_hdl->total_object_bytes = 0;
    file_hdl->total_on_disk_bytes = 0;
    file_hdl->total_object_cnt = 0;
    file_hdl->previous_file_id_first_ = previousFileID1;
    file_hdl->previous_file_id_second_ = previousFileID2;
    file_hdl->file_create_reason_ = kInternalGCFile;
    file_hdl->sorted_filter = new BucketKeyFilter();
    file_hdl->filter = new BucketKeyFilter();
    // set up new file header for write
    newFileHeader.prefix_bit = targetPrefixLen;
    newFileHeader.previous_file_id_first_ = previousFileID1;
    newFileHeader.previous_file_id_second_ = previousFileID2;
    newFileHeader.file_create_reason_ = kInternalGCFile;
    newFileHeader.file_id = file_hdl->file_id;
    newFileHeader.index_block_size = 0;
    // write header to current file
    string targetFilePathStr = workingDir_ + "/" +
        to_string(file_hdl->file_id) + ".delta";
    bool createAndOpenNewFileStatus =
        file_hdl->file_op_ptr->createThenOpenFile(targetFilePathStr);
    if (createAndOpenNewFileStatus == true) {
        // move pointer for return
        debug_info("Newly created file ID = %lu, target prefix bit number = "
                "%lu, corresponding previous file ID = %lu and %lu\n",
                file_hdl->file_id, targetPrefixLen,
                previousFileID1, previousFileID2);
        ret_file_hdl = file_hdl;
        return true;
    } else {
        debug_error("[ERROR] Could not create file ID = %lu, target prefix bit "
                "number = %lu, corresponding previous file ID = %lu and %lu\n",
                file_hdl->file_id, targetPrefixLen,
                previousFileID1, previousFileID2);
        ret_file_hdl = nullptr;
        return false;
    }
}

uint64_t HashStoreFileManager::generateNewFileID()
{
    fileIDGeneratorMtx_.lock();
    targetNewFileID_ += 1;
    uint64_t tempIDForReturn = targetNewFileID_;
    fileIDGeneratorMtx_.unlock();
    return tempIDForReturn;
}

pair<uint64_t, uint64_t>
HashStoreFileManager::deconstructAndGetValidContentsFromFile(
        char* read_buf, uint64_t buf_size, unordered_map<str_t,
        pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& resultMap)
{
    uint64_t valid_obj_num = 0;
    uint64_t obj_num = 0;

    uint64_t read_i = 0;
    size_t header_sz = sizeof(hashStoreRecordHeader);
    // skip file header
    read_i += sizeof(hashStoreFileHeader);

    while (read_i < buf_size) {
        obj_num++;
        hashStoreRecordHeader header;
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
                vector<hashStoreRecordHeader> newRecorderHeaderVec;
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
    }
    debug_info("deconstruct current file done, find different key number = "
            "%lu, total processed object number = %lu, target keep object "
            "number = %lu\n", resultMap.size(), obj_num, valid_obj_num);
    return make_pair(valid_obj_num, obj_num);
}

uint64_t HashStoreFileManager::partialMergeGcResultMap(
        unordered_map<str_t, pair<vector<str_t>,
        vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& gcResultMap, unordered_set<str_t,
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
            vector<hashStoreRecordHeader> headerVec;
            hashStoreRecordHeader newRecordHeader;

            deltaKVMergeOperatorPtr_->PartialMerge(values, result);

            newRecordHeader.key_size_ = key.size_;
            newRecordHeader.value_size_ = result.size_; 
            // the largest sequence number
            newRecordHeader.sequence_number_ =
                headers[headers.size()-1].sequence_number_;
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

inline void HashStoreFileManager::clearMemoryForTemporaryMergedDeltas(
        unordered_map<str_t, pair<vector<str_t>,
        vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& resultMap, 
        unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t>&
        shouldDelete)
{
    for (auto& it : shouldDelete) {
        delete[] resultMap[it].first[0].data_;
    }
}

inline void HashStoreFileManager::putKeyValueListToAppendableCache(
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

inline void HashStoreFileManager::putKDToCache(
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

bool HashStoreFileManager::singleFileRewrite(
        hashStoreFileMetaDataHandler* file_hdl, 
        unordered_map<str_t, pair<vector<str_t>,
        vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& gcResultMap, 
        uint64_t targetFileSizeWoIndexBlock, bool fileContainsReWriteKeysFlag)
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    // file before:
    // [file_header] [[record_header] [key] [value]] ... [record_header]
    // file after:
    // [file_header] [index block] [[record_header] [key] [value]] ... [record_header]

    // Write header 
//    uint64_t beforeRewriteSize = file_hdl->total_on_disk_bytes;
//    uint64_t beforeRewriteBytes = file_hdl->total_object_bytes;
    uint64_t newObjectNumber = 0;
    uint64_t write_i = sizeof(hashStoreFileHeader);
    hashStoreFileHeader file_header;
    file_header.prefix_bit = file_hdl->prefix_bit;
    if (fileContainsReWriteKeysFlag == true) {
        file_header.file_create_reason_ = kRewritedObjectFile;
    } else {
        file_header.file_create_reason_ = kInternalGCFile;
    }

    file_header.index_block_size = 0; 
    file_header.file_id = generateNewFileID();
    file_header.previous_file_id_first_ = file_hdl->file_id;

    size_t header_sz = sizeof(hashStoreRecordHeader);

    uint64_t targetFileSize = targetFileSizeWoIndexBlock;
    if (enable_index_block_) {
        // create or clear the index block
        if (file_hdl->index_block == nullptr) {
            file_hdl->index_block = new BucketIndexBlock();
        } else {
            file_hdl->index_block->Clear();
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
                file_hdl->index_block->Insert(key, total_kd_size);
            }
        }

        file_hdl->index_block->Build();
        file_header.index_block_size = file_hdl->index_block->GetSize();
//        targetFileSize += file_header.index_block_size;
        // do not write the index block to the file
    }

    char write_buf[targetFileSize];

    // copy the file header in the end
    //copyInc(write_buf, write_i, &file_header, sizeof(hashStoreFileHeader));
    StatsRecorder::getInstance()->timeProcess(StatsType::REWRITE_GET_FILE_ID, tv);

    // Write index block
//    if (enable_index_block_) {
//        file_hdl->index_block->Serialize(write_buf + write_i);
//        write_i += file_header.index_block_size;
//    }

    // Write file
    // Now the keys should be written in a sorted way 
    gettimeofday(&tv, 0);
    file_hdl->sorted_filter->Clear();
    file_hdl->filter->Clear();

    if (enable_index_block_) {
        for (auto& sorted_it : file_hdl->index_block->indices) {
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
                file_hdl->sorted_filter->Insert(key);
            }
        }
        file_hdl->index_block->IndicesClear();
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
                file_hdl->filter->Insert(key);
            }
        }
    }

    // add gc done flag into bucket file
    hashStoreRecordHeader gc_done_record_header;
    gc_done_record_header.is_anchor_ = false;
    gc_done_record_header.is_gc_done_ = true;
    gc_done_record_header.sequence_number_ = 0;
    gc_done_record_header.key_size_ = 0;
    gc_done_record_header.value_size_ = 0;

    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &gc_done_record_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i,
                gc_done_record_header);
    }

    // copy the file header finally
    file_header.unsorted_part_offset = write_i;
    file_hdl->unsorted_part_offset = write_i;
    memcpy(write_buf, &file_header, sizeof(file_header));

    debug_trace("Rewrite done buffer size = %lu\n", write_i);

    string filename = workingDir_ + "/" + to_string(file_header.file_id) + ".delta";
    StatsRecorder::getInstance()->timeProcess(StatsType::REWRITE_ADD_HEADER, tv);
    gettimeofday(&tv, 0);
    // create since file not exist
    if (file_hdl->file_op_ptr->isFileOpen() == true) {
        file_hdl->file_op_ptr->closeFile();
    } // close old file
    file_hdl->file_op_ptr->createThenOpenFile(filename);
    if (file_hdl->file_op_ptr->isFileOpen() == true) {
        // write content and update current file stream to new one.
        FileOpStatus onDiskWriteSizePair;
        if (enable_crash_consistency_) {
            STAT_PROCESS(onDiskWriteSizePair =
                    file_hdl->file_op_ptr->writeAndFlushFile(write_buf,
                        write_i), StatsType::DELTAKV_GC_WRITE);
        } else {
            STAT_PROCESS(onDiskWriteSizePair =
                    file_hdl->file_op_ptr->writeFile(write_buf,
                        write_i), StatsType::DELTAKV_GC_WRITE);
        }
        file_hdl->file_op_ptr->markDirectDataAddress(write_i);
        StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSizePair.physicalSize_, onDiskWriteSizePair.logicalSize_, syncStatistics_);
        debug_trace("Rewrite done file size = %lu, file path = %s\n", write_i,
                filename.c_str());
        // update metadata
        file_hdl->file_id = file_header.file_id;
        file_hdl->total_object_cnt = newObjectNumber + 1;
        file_hdl->total_object_bytes = write_i;
        file_hdl->total_on_disk_bytes = onDiskWriteSizePair.physicalSize_;
        debug_trace("Rewrite file size in metadata = %lu, file ID = %lu\n", file_hdl->total_object_bytes, file_hdl->file_id);
        // remove old file
        fileDeleteVecMtx_.lock();
        targetDeleteFileHandlerVec_.push_back(file_header.previous_file_id_first_);
        fileDeleteVecMtx_.unlock();
        // check if after rewrite, file size still exceed threshold, mark as no GC.
        if (file_hdl->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
//            debug_error("flushed new file with file ID = %lu marked as no GC from %lu (%lu) to %lu, object count %lu\n", 
//                    file_header.file_id, beforeRewriteSize, beforeRewriteBytes,
//                    file_hdl->total_on_disk_bytes, file_hdl->total_object_cnt);
            file_hdl->gc_status = kNoGC;

            if (write_stall_ != nullptr) {
//                debug_error("Start to rewrite, key number %lu\n",
//                        gcResultMap.size());
                vector<writeBackObject*> objs;
                objs.resize(gcResultMap.size());
                int obji = 0;
                file_hdl->num_anchors = 0;
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
        debug_info("flushed new file to filesystem since single file gc, the"
                " new file ID = %lu, corresponding previous file ID = %lu,"
                " target file size = %lu\n", 
                file_header.file_id, file_header.previous_file_id_first_,
                write_i);
        return true;
    } else {
        debug_error("[ERROR] Could not open new file ID = %lu, for old file ID = %lu for single file rewrite\n", file_header.file_id, file_header.previous_file_id_first_);
        fileDeleteVecMtx_.lock();
        targetDeleteFileHandlerVec_.push_back(file_header.file_id);
        fileDeleteVecMtx_.unlock();
        return false;
    }
}

bool HashStoreFileManager::singleFileSplit(hashStoreFileMetaDataHandler*
        file_hdl, unordered_map<str_t, pair<vector<str_t>,
        vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
        mapEqualKeForStr_t>& gcResultMap, uint64_t prefix_len, bool
        fileContainsReWriteKeysFlag)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    uint64_t previous_prefix;
    generateHashBasedPrefix((char*)gcResultMap.begin()->first.data_,
            gcResultMap.begin()->first.size_, previous_prefix);
    previous_prefix = prefixSubstr(previous_prefix, prefix_len - 1);
    unordered_map<uint64_t, pair<unordered_map<str_t, uint64_t,
        mapHashKeyForStr_t, mapEqualKeForStr_t>, uint64_t>>
            tempPrefixToKeysVecAndTotalSizeMap;
    StatsRecorder::getInstance()->timeProcess(StatsType::SPLIT_HANDLER, tv);
    gettimeofday(&tv, 0);
    size_t header_sz = sizeof(hashStoreRecordHeader);
    for (auto keyIt : gcResultMap) {
        uint64_t prefix;
        generateHashBasedPrefix(keyIt.first.data_, keyIt.first.size_, prefix);
        prefix = prefixSubstr(prefix, prefix_len); 
        auto& key = keyIt.first;
        if (tempPrefixToKeysVecAndTotalSizeMap.find(prefix) !=
                tempPrefixToKeysVecAndTotalSizeMap.end()) {
            // current prefix exist, update
            uint64_t currentKeyTargetAllocateSpace = 0;
            auto& values = keyIt.second.first;
            auto& headers = keyIt.second.second;
            for (auto i = 0; i < values.size(); i++) {
                if (use_varint_d_header == true) {
                    header_sz = GetDeltaHeaderVarintSize(headers[i]);
                }
                currentKeyTargetAllocateSpace += key.size_ +
                    values[i].size_ + header_sz;
            }
            // update the space needed for this key
            tempPrefixToKeysVecAndTotalSizeMap.at(prefix).first.insert(make_pair(keyIt.first, currentKeyTargetAllocateSpace));
            tempPrefixToKeysVecAndTotalSizeMap.at(prefix).second += currentKeyTargetAllocateSpace;
        } else {
            uint64_t currentKeyTargetAllocateSpace = 0;
            auto& values = keyIt.second.first;
            auto& headers = keyIt.second.second;
            for (auto i = 0; i < values.size(); i++) {
                if (use_varint_d_header == true) {
                    header_sz = GetDeltaHeaderVarintSize(headers[i]);
                }
                currentKeyTargetAllocateSpace += key.size_ +
                        values[i].size_ + header_sz;
            }
            pair<unordered_map<str_t, uint64_t, mapHashKeyForStr_t, mapEqualKeForStr_t>, uint64_t> tempNewKeyMap;
            tempNewKeyMap.first.insert(make_pair(keyIt.first, currentKeyTargetAllocateSpace));
            tempNewKeyMap.second += currentKeyTargetAllocateSpace;
            tempPrefixToKeysVecAndTotalSizeMap.insert(make_pair(prefix, tempNewKeyMap));
        }
    }
    if (tempPrefixToKeysVecAndTotalSizeMap.size() > 2) {
        debug_error("[ERROR] Need to generate more than 2 files during split"
                " GC, current target file number = %lu\n",
                tempPrefixToKeysVecAndTotalSizeMap.size());
        return false;
    } else {
        debug_info("Generate new files since split GC, target file number ="
                " %lu\n", tempPrefixToKeysVecAndTotalSizeMap.size());
        for (auto prefixIt : tempPrefixToKeysVecAndTotalSizeMap) {
            debug_info("During split GC, target prefix = %lx\n",
                    prefixIt.first);
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::SPLIT_IN_MEMORY, tv);
    gettimeofday(&tv, 0);
    vector<pair<uint64_t, hashStoreFileMetaDataHandler*>> needUpdateMetaDataHandlers;
    for (auto prefixIt : tempPrefixToKeysVecAndTotalSizeMap) {

        hashStoreFileMetaDataHandler* new_file_hdl;
        hashStoreFileHeader file_header;
        bool getFileHandlerStatus = createFileHandlerForGC(new_file_hdl,
                prefix_len, file_hdl->file_id, 0,
                file_header);
        if (getFileHandlerStatus == false) {
            debug_error("[ERROR] Failed to create hash store file handler by"
                    " prefix %lx when split GC\n", prefixIt.first);
            return false;
        }
        new_file_hdl->filter->Clear();
        debug_info("Generate new file since split GC, current prefix = %lx,"
                " target file ID = %lu\n",
                prefixIt.first, new_file_hdl->file_id);

        uint64_t targetFileSize = prefixIt.second.second +
            sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader);

        if (enable_index_block_) {
            // create or clear the index block
            if (new_file_hdl->index_block == nullptr) {
                new_file_hdl->index_block = new BucketIndexBlock();
            } else {
                new_file_hdl->index_block->Clear();
            }

            // select keys for building index block
            for (auto keyToSizeIt : prefixIt.second.first) {
                size_t total_kd_size = keyToSizeIt.second;

                if (total_kd_size > 0) {
                    new_file_hdl->index_block->Insert(keyToSizeIt.first, total_kd_size);
                }
            }

            new_file_hdl->index_block->Build();
            file_header.index_block_size = new_file_hdl->index_block->GetSize();
//            targetFileSize += file_header.index_block_size;
        }

        char write_buf[targetFileSize];
        uint64_t write_i = sizeof(file_header);

//        if (enable_index_block_) {
//            new_file_hdl->index_block->Serialize(write_buf + write_i);
//            write_i += file_header.index_block_size;
//        }

        // Iterate all the keys
        size_t header_sz = sizeof(hashStoreRecordHeader);
        if (enable_index_block_) {
            for (auto& sorted_it : new_file_hdl->index_block->indices) {
                auto keyIt = gcResultMap.find(str_t(const_cast<char*>(sorted_it.first.data()),
                            sorted_it.first.size()));
                if (keyIt == gcResultMap.end()) {
                    debug_error("data not found! key %.*s\n", 
                            (int)sorted_it.first.size(), sorted_it.first.data());
                    exit(1);
                }

                auto& key = keyIt->first;
                auto& values = keyIt->second.first;
                for (auto vec_i = 0; vec_i < values.size(); vec_i++) {
                    auto& value = keyIt->second.first[vec_i];
                    auto& header = keyIt->second.second[vec_i];
                    if (use_varint_d_header == false) {
                        copyInc(write_buf, write_i, &header, header_sz);
                    } else {
                        write_i += PutDeltaHeaderVarint(write_buf + write_i, header);
                    }
                    copyInc(write_buf, write_i, key.data_, key.size_);
                    copyInc(write_buf, write_i, value.data_, value.size_);
                }
                new_file_hdl->total_object_cnt += values.size();
                if (keyIt->second.first.size() > 0) {
                    new_file_hdl->sorted_filter->Insert(key);
                }
            }
            new_file_hdl->index_block->IndicesClear();
        } else {
            for (auto keyToSizeIt : prefixIt.second.first) {
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
                new_file_hdl->total_object_cnt += values.size();
                if (keyIt->second.first.size() > 0) {
                    new_file_hdl->filter->Insert(key);
                }
            }
        }
        hashStoreRecordHeader gc_fin_header;
        gc_fin_header.is_anchor_ = false;
        gc_fin_header.is_gc_done_ = true;
        gc_fin_header.sequence_number_ = 0;
        gc_fin_header.key_size_ = 0;
        gc_fin_header.value_size_ = 0;
        if (use_varint_d_header == false) {
            copyInc(write_buf, write_i, &gc_fin_header, header_sz);
        } else {
            write_i += PutDeltaHeaderVarint(write_buf + write_i, gc_fin_header);
        }
        file_header.unsorted_part_offset = write_i;
        new_file_hdl->unsorted_part_offset = write_i;
        memcpy(write_buf, &file_header, sizeof(file_header));

//        debug_error("file: %lu [%d] [%lu] ... [%lu] tot %lu\n",
//                new_file_hdl->file_id, sizeof(file_header),
//                file_header.index_block_size, 
//                sizeof(GCJobDoneRecord), write_i);

        // start write file
        FileOpStatus onDiskWriteSizePair;
        new_file_hdl->fileOperationMutex_.lock();
        STAT_PROCESS(onDiskWriteSizePair =
                new_file_hdl->file_op_ptr->writeFile(write_buf,
                    write_i), StatsType::DELTAKV_GC_WRITE);
        StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSizePair.physicalSize_,
                onDiskWriteSizePair.logicalSize_, syncStatistics_);
        new_file_hdl->file_op_ptr->markDirectDataAddress(write_i);
        new_file_hdl->total_object_bytes = write_i;
        new_file_hdl->total_on_disk_bytes = onDiskWriteSizePair.physicalSize_;
        new_file_hdl->total_object_cnt++;
        debug_trace("Flushed new file to filesystem since split gc, the new"
                " file ID = %lu, corresponding previous file ID = %lu\n",
                new_file_hdl->file_id, file_hdl->file_id);
        new_file_hdl->fileOperationMutex_.unlock();
        // update metadata
        needUpdateMetaDataHandlers.push_back(make_pair(prefixIt.first, new_file_hdl));
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::SPLIT_WRITE_FILES, tv);
    gettimeofday(&tv, 0);
    if (needUpdateMetaDataHandlers.size() == 1) {
        uint64_t insertAtLevel =
            file_trie_.insert(needUpdateMetaDataHandlers[0].first,
                    needUpdateMetaDataHandlers[0].second);
        if (insertAtLevel == 0) {
            debug_error("[ERROR] Error insert to prefix tree, prefix length used = %lu, inserted file ID = %lu\n", needUpdateMetaDataHandlers[0].second->prefix_bit, needUpdateMetaDataHandlers[0].second->file_id);
            needUpdateMetaDataHandlers[0].second->file_op_ptr->closeFile();
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(needUpdateMetaDataHandlers[0].second->file_id);
            fileDeleteVecMtx_.unlock();
            if (needUpdateMetaDataHandlers[0].second->index_block) {
                delete needUpdateMetaDataHandlers[0].second->index_block;
            }
            delete needUpdateMetaDataHandlers[0].second->file_op_ptr;
            delete needUpdateMetaDataHandlers[0].second->sorted_filter;
            delete needUpdateMetaDataHandlers[0].second->filter;
            delete needUpdateMetaDataHandlers[0].second;
            return false;
        } else {
            if (needUpdateMetaDataHandlers[0].second->prefix_bit != insertAtLevel) {
                debug_info("After insert to prefix tree, get handler at level ="
                        " %lu, but prefix length used = %lu, prefix = %lx,"
                        " inserted file ID = %lu, update the current bit number"
                        " used in the file handler\n", insertAtLevel,
                        needUpdateMetaDataHandlers[0].second->prefix_bit,
                        needUpdateMetaDataHandlers[0].first,
                        needUpdateMetaDataHandlers[0].second->file_id);
                needUpdateMetaDataHandlers[0].second->prefix_bit = insertAtLevel;
            }
            file_hdl->gc_status = kShouldDelete;
            needUpdateMetaDataHandlers[0].second->file_ownership = 0;
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_hdl->file_id);
            fileDeleteVecMtx_.unlock();
            debug_info("Split file ID = %lu for gc success, mark as should"
                    " delete done\n", file_hdl->file_id);
            StatsRecorder::getInstance()->timeProcess(StatsType::SPLIT_METADATA, tv);
            return true;
        }
    } else if (needUpdateMetaDataHandlers.size() == 2) {
        pair<uint64_t, uint64_t> insertPrefixTreeStatus = 
            file_trie_.insertPairOfNodes(needUpdateMetaDataHandlers[0].first,
                    needUpdateMetaDataHandlers[0].second,
                    needUpdateMetaDataHandlers[1].first,
                    needUpdateMetaDataHandlers[1].second);
        if (insertPrefixTreeStatus.first == 0 || insertPrefixTreeStatus.second == 0) {
            debug_error("[ERROR] Error insert to prefix tree: target prefix 1 ="
                    " %lx, insert at level = %lu, file ID = %lu; target prefix 2 ="
                    " %lx, insert at level = %lu, file ID = %lu\n",
                    needUpdateMetaDataHandlers[0].first,
                    insertPrefixTreeStatus.first,
                    needUpdateMetaDataHandlers[0].second->file_id,
                    needUpdateMetaDataHandlers[1].first,
                    insertPrefixTreeStatus.second,
                    needUpdateMetaDataHandlers[1].second->file_id);
            // clean up temporary info
            for (int i = 0; i < 2; i++) {
                needUpdateMetaDataHandlers[i].second->file_op_ptr->closeFile();
                fileDeleteVecMtx_.lock();
                targetDeleteFileHandlerVec_.push_back(needUpdateMetaDataHandlers[i].second->file_id);
                fileDeleteVecMtx_.unlock();
                if (needUpdateMetaDataHandlers[i].second->index_block) {
                    delete needUpdateMetaDataHandlers[i].second->index_block;
                }
                delete needUpdateMetaDataHandlers[i].second->file_op_ptr;
                delete needUpdateMetaDataHandlers[i].second->sorted_filter;
                delete needUpdateMetaDataHandlers[i].second->filter;
                delete needUpdateMetaDataHandlers[i].second;
            }
            return false;
        } else {
            if (needUpdateMetaDataHandlers[0].second->prefix_bit != insertPrefixTreeStatus.first) {
                debug_info("After insert to prefix tree, get handler at level ="
                        " %lu, but prefix length used = %lu, prefix = %lx,"
                        " inserted file ID = %lu, update the current bit number"
                        " used in the file handler\n",
                        insertPrefixTreeStatus.first,
                        needUpdateMetaDataHandlers[0].second->prefix_bit,
                        needUpdateMetaDataHandlers[0].first,
                        needUpdateMetaDataHandlers[0].second->file_id);
                needUpdateMetaDataHandlers[0].second->prefix_bit = insertPrefixTreeStatus.first;
            }
            needUpdateMetaDataHandlers[0].second->file_ownership = 0;
            if (needUpdateMetaDataHandlers[1].second->prefix_bit != insertPrefixTreeStatus.second) {
                debug_info("After insert to prefix tree, get handler at level ="
                        " %lu, but prefix length used = %lu, prefix = %lx,"
                        " inserted file ID = %lu, update the current bit number"
                        " used in the file handler\n",
                        insertPrefixTreeStatus.second,
                        needUpdateMetaDataHandlers[1].second->prefix_bit,
                        needUpdateMetaDataHandlers[1].first,
                        needUpdateMetaDataHandlers[1].second->file_id);
                needUpdateMetaDataHandlers[1].second->prefix_bit = insertPrefixTreeStatus.second;
            }
//            debug_error("mark as should delete %d\n", (int)file_hdl->file_id);
            file_hdl->gc_status = kShouldDelete;
            needUpdateMetaDataHandlers[1].second->file_ownership = 0;
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_hdl->file_id);
            fileDeleteVecMtx_.unlock();
            debug_info("Split file ID = %lu for gc success, mark as should"
                    " delete done\n", file_hdl->file_id);
            StatsRecorder::getInstance()->timeProcess(StatsType::SPLIT_METADATA, tv);
            return true;
        }
    } else {
        file_hdl->gc_status = kMayGC;
        debug_error("[ERROR] Split file ID = %lu for gc error, generate too"
                " many files, the file numebr = %lu\n",
                file_hdl->file_id, needUpdateMetaDataHandlers.size());
        return false;
    }
}

bool HashStoreFileManager::twoAdjacentFileMerge(
        hashStoreFileMetaDataHandler* file_hdl1,
        hashStoreFileMetaDataHandler* file_hdl2, 
        uint64_t target_prefix, uint64_t prefix_len)
{
    struct timeval tvAll, tv;
    gettimeofday(&tvAll, 0);
    std::scoped_lock<std::shared_mutex> w_lock1(file_hdl1->fileOperationMutex_);
    std::scoped_lock<std::shared_mutex> w_lock2(file_hdl2->fileOperationMutex_);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_WAIT_LOCK, tvAll);
    gettimeofday(&tv, 0);
    debug_info("Perform merge GC for file ID 1 = %lu, ID 2 = %lu\n",
            file_hdl1->file_id, file_hdl2->file_id);
    hashStoreFileMetaDataHandler* mergedFileHandler;
    hashStoreFileHeader newFileHeaderForMergedFile;
    bool generateFileHandlerStatus = createFileHandlerForGC(
            mergedFileHandler, prefix_len, 
            file_hdl1->file_id, file_hdl2->file_id,
            newFileHeaderForMergedFile);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_CREATE_HANDLER, tv);
    gettimeofday(&tv, 0);
    if (generateFileHandlerStatus == false) {
        debug_error("[ERROR] Could not generate new file handler for merge GC,previous file ID 1 = %lu, ID 2 = %lu\n", file_hdl1->file_id, file_hdl2->file_id);
        file_hdl1->file_ownership = 0;
        file_hdl2->file_ownership = 0;
        return false;
    }
    std::scoped_lock<std::shared_mutex> w_lock3(mergedFileHandler->fileOperationMutex_);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_WAIT_LOCK3, tv);
    gettimeofday(&tv, 0);
    // process file 1
    char readWriteBuffer1[file_hdl1->total_object_bytes];
    FileOpStatus readStatus1;
    STAT_PROCESS(readStatus1 = file_hdl1->file_op_ptr->readFile(readWriteBuffer1, file_hdl1->total_object_bytes), StatsType::DELTAKV_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(file_hdl1->total_on_disk_bytes, file_hdl1->total_object_bytes, syncStatistics_);
    // process GC contents
    unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> gcResultMap1;
    pair<uint64_t, uint64_t> remainObjectNumberPair1 = deconstructAndGetValidContentsFromFile(readWriteBuffer1, file_hdl1->total_object_bytes, gcResultMap1);
    debug_info("Merge GC read file ID 1 = %lu done, valid object number = %lu, total object number = %lu\n", file_hdl1->file_id, remainObjectNumberPair1.first, remainObjectNumberPair1.second);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_FILE1, tv);
    gettimeofday(&tv, 0);

    // process file2
    char readWriteBuffer2[file_hdl2->total_object_bytes];
    FileOpStatus readStatus2;
    STAT_PROCESS(readStatus2 = file_hdl2->file_op_ptr->readFile(readWriteBuffer2, file_hdl2->total_object_bytes), StatsType::DELTAKV_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(file_hdl2->total_on_disk_bytes, file_hdl2->total_object_bytes, syncStatistics_);
    // process GC contents
    unordered_map<str_t, pair<vector<str_t>, vector<hashStoreRecordHeader>>, mapHashKeyForStr_t, mapEqualKeForStr_t> gcResultMap2;
    pair<uint64_t, uint64_t> remainObjectNumberPair2 = deconstructAndGetValidContentsFromFile(readWriteBuffer2, file_hdl2->total_object_bytes, gcResultMap2);
    debug_info("Merge GC read file ID 2 = %lu done, valid object number = %lu, total object number = %lu\n", file_hdl2->file_id, remainObjectNumberPair2.first, remainObjectNumberPair2.second);

    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_FILE2, tv);
    gettimeofday(&tv, 0);

    uint64_t targetWriteSize = 0;
    for (auto& keyIt : gcResultMap1) {
        for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
            targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size_ + keyIt.second.first[vec_i].size_);
        }
    }
    for (auto& keyIt : gcResultMap2) {
        for (auto vec_i = 0; vec_i < keyIt.second.first.size(); vec_i++) {
            targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size_ + keyIt.second.first[vec_i].size_);
        }
    }
    // reserve more space, use sizeof()
    targetWriteSize += (sizeof(hashStoreRecordHeader) + sizeof(hashStoreFileHeader));
    debug_info("Merge GC target write file size = %lu\n", targetWriteSize);
    debug_error("Merge not implemented well: id %lu and %lu\n", 
            file_hdl1->file_id, file_hdl2->file_id);
    char write_buf[targetWriteSize];
    memcpy(write_buf, &newFileHeaderForMergedFile, sizeof(hashStoreFileHeader));
    mergedFileHandler->filter->Clear();
    uint64_t write_i = sizeof(hashStoreFileHeader);
    size_t header_sz = sizeof(hashStoreRecordHeader);
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
            mergedFileHandler->total_object_cnt++;
        }
        if (keyIt.second.first.size() > 0) {
            mergedFileHandler->filter->Insert(key.data_, key.size_);
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
            mergedFileHandler->total_object_cnt++;
        }
        if (keyIt.second.first.size() > 0) {
            mergedFileHandler->filter->Insert(key.data_, key.size_);
        }
    }
    debug_info("Merge GC processed write file size = %lu\n", write_i);
    // write gc done flag into bucket file
    hashStoreRecordHeader gc_fin_header;
    gc_fin_header.is_anchor_ = false;
    gc_fin_header.is_gc_done_ = true;
    gc_fin_header.sequence_number_ = 0;
    gc_fin_header.key_size_ = 0;
    gc_fin_header.value_size_ = 0;
    mergedFileHandler->unsorted_part_offset = sizeof(hashStoreFileHeader);
    if (use_varint_d_header == false) {
        copyInc(write_buf, write_i, &gc_fin_header, header_sz);
    } else {
        write_i += PutDeltaHeaderVarint(write_buf + write_i, gc_fin_header);
    }
    debug_info("Merge GC processed total write file size = %lu\n", write_i);
    FileOpStatus onDiskWriteSizePair;
    STAT_PROCESS(onDiskWriteSizePair =
            mergedFileHandler->file_op_ptr->writeFile(write_buf,
                write_i), StatsType::DELTAKV_GC_WRITE);
    StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSizePair.physicalSize_,
            onDiskWriteSizePair.logicalSize_, syncStatistics_);
    debug_info("Merge GC write file size = %lu done\n", write_i);
    mergedFileHandler->total_object_bytes += write_i;
    mergedFileHandler->total_on_disk_bytes += onDiskWriteSizePair.physicalSize_;
    mergedFileHandler->total_object_cnt++;
    debug_info("Flushed new file to filesystem since merge gc, the new file ID = %lu, corresponding previous file ID 1 = %lu, ID 2 = %lu\n", mergedFileHandler->file_id, file_hdl1->file_id, file_hdl2->file_id);

    // update metadata
    uint64_t newLeafNodeBitNumber = 0;
    bool mergeNodeStatus = file_trie_.mergeNodesToNewLeafNode(target_prefix,
            prefix_len, newLeafNodeBitNumber);
    if (mergeNodeStatus == false) {
        debug_error("[ERROR] Could not merge two existing node corresponding file ID 1 = %lu, ID 2 = %lu\n", file_hdl1->file_id, file_hdl2->file_id);
        if (mergedFileHandler->file_op_ptr->isFileOpen() == true) {
            mergedFileHandler->file_op_ptr->closeFile();
        }
        fileDeleteVecMtx_.lock();
        targetDeleteFileHandlerVec_.push_back(mergedFileHandler->file_id);
        fileDeleteVecMtx_.unlock();
        file_hdl1->file_ownership = 0;
        file_hdl2->file_ownership = 0;
        if (mergedFileHandler->index_block) {
            delete mergedFileHandler->index_block;
        }
        delete mergedFileHandler->file_op_ptr;
        delete mergedFileHandler->sorted_filter;
        delete mergedFileHandler->filter;
        delete mergedFileHandler;
        StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_METADATA, tv);
        return false;
    } else {
        hashStoreFileMetaDataHandler* tempHandler = nullptr;
        file_trie_.get(target_prefix, tempHandler);
        if (tempHandler != nullptr) {
            // delete old handler;
            debug_info("Find exist data handler = %p\n", tempHandler);
            if (tempHandler->file_op_ptr != nullptr) {
                if (tempHandler->file_op_ptr->isFileOpen() == true) {
                    tempHandler->file_op_ptr->closeFile();
                }
                fileDeleteVecMtx_.lock();
                targetDeleteFileHandlerVec_.push_back(tempHandler->file_id);
                fileDeleteVecMtx_.unlock();
                delete tempHandler->file_op_ptr;
            }
            if (tempHandler->index_block) {
                delete tempHandler->index_block;
            }
            delete tempHandler->sorted_filter;
            delete tempHandler->filter;
            delete tempHandler;
        }
        debug_info("Start update metadata for merged file ID = %lu\n", mergedFileHandler->file_id);
        bool updateFileHandlerToNewLeafNodeStatus =
            file_trie_.updateDataObjectForTargetLeafNode(target_prefix,
                    prefix_len, newLeafNodeBitNumber, mergedFileHandler);
        if (updateFileHandlerToNewLeafNodeStatus == true) {
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_hdl1->file_id);
            targetDeleteFileHandlerVec_.push_back(file_hdl2->file_id);
            fileDeleteVecMtx_.unlock();
            file_hdl1->gc_status = kShouldDelete;
            file_hdl2->gc_status = kShouldDelete;
            file_hdl1->file_ownership = 0;
            file_hdl2->file_ownership = 0;
            if (file_hdl1->file_op_ptr->isFileOpen() == true) {
                file_hdl1->file_op_ptr->closeFile();
            }
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_hdl1->file_id);
            fileDeleteVecMtx_.unlock();
            // delete file_hdl1->file_op_ptr;
            // delete file_hdl1;
            if (file_hdl2->file_op_ptr->isFileOpen() == true) {
                file_hdl2->file_op_ptr->closeFile();
            }
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_hdl2->file_id);
            fileDeleteVecMtx_.unlock();
            // delete file_hdl2->file_op_ptr;
            // delete file_hdl2;
            mergedFileHandler->file_ownership = 0;
            StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_METADATA, tv);
//            debug_error("finished merge GC for file ID 1 = %lu, ID 2 = %lu\n",
//                    file_hdl1->file_id, file_hdl2->file_id);
            return true;
        } else {
            debug_error("[ERROR] Could not update metadata for file ID = %lu\n", mergedFileHandler->file_id);
            if (mergedFileHandler->file_op_ptr->isFileOpen() == true) {
                mergedFileHandler->file_op_ptr->closeFile();
            }
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(mergedFileHandler->file_id);
            fileDeleteVecMtx_.unlock();
            if (mergedFileHandler->index_block) {
                delete mergedFileHandler->index_block;
            }
            delete mergedFileHandler->file_op_ptr;
            delete mergedFileHandler->sorted_filter;
            delete mergedFileHandler->filter;
            delete mergedFileHandler;
            file_hdl1->file_ownership = 0;
            file_hdl2->file_ownership = 0;
            StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_METADATA, tv);
            return false;
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_METADATA, tv);
    return true;
}

bool HashStoreFileManager::selectFileForMerge(uint64_t targetFileIDForSplit,
        hashStoreFileMetaDataHandler*& file_hdl1,
        hashStoreFileMetaDataHandler*& file_hdl2, 
        uint64_t& target_prefix, uint64_t& prefix_len)
{
    struct timeval tvAll;
    gettimeofday(&tvAll, 0);
    vector<pair<uint64_t, hashStoreFileMetaDataHandler*>> validNodes;
    bool getValidNodesStatus = file_trie_.getCurrentValidNodes(validNodes);
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE_GET_NODES, tvAll);
    if (getValidNodesStatus == false) {
        debug_error("[ERROR] Could not get valid tree nodes from prefixTree,"
                " current validNodes vector size = %lu\n", validNodes.size());
        return false;
    } 

    struct timeval tv;
    gettimeofday(&tv, 0);
    debug_trace("Current validNodes vector size = %lu\n", validNodes.size());
    unordered_map<uint64_t, hashStoreFileMetaDataHandler*>
        targetFileForMergeMap; // Key: [8B len] + [56B prefix]
    for (auto& nodeIt : validNodes) {
        auto& file_hdl = nodeIt.second;
        if (file_hdl->file_id == targetFileIDForSplit) {
            debug_trace("Skip file ID = %lu, prefix bit number = %lu," 
                    " size = %lu, which is currently during GC\n",
                    file_hdl->file_id, file_hdl->prefix_bit,
                    file_hdl->total_object_bytes);
            continue;
        }
        if (file_hdl->total_object_bytes <=
                singleFileMergeGCUpperBoundSize_ &&
                file_trie_.isMergableLength(prefixLenExtract(nodeIt.first)) &&
                file_hdl->file_ownership != -1) {
            targetFileForMergeMap.insert(nodeIt);
            debug_trace("Select file ID = %lu, prefix bit number = %lu,"
                    " size = %lu, which should not exceed threshould ="
                    " %lu\n", 
                    file_hdl->file_id, file_hdl->prefix_bit,
                    file_hdl->total_object_bytes,
                    singleFileMergeGCUpperBoundSize_);
        } else {
            debug_trace("Skip file ID = %lu, prefix bit number = %lu, size"
                    " = %lu, which may exceed threshould = %lu\n",
                    file_hdl->file_id, file_hdl->prefix_bit,
                    file_hdl->total_object_bytes,
                    singleFileMergeGCUpperBoundSize_);
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE_SELECT_MERGE, tv);
    gettimeofday(&tv, 0);
//    debug_error("Selected from file number = %lu for merge GC (valid %lu)\n",
//            targetFileForMergeMap.size(), validNodes.size());
    if (targetFileForMergeMap.size() != 0) {
        int maxTryNumber = 100;
        while (maxTryNumber--) {
            for (auto mapIt : targetFileForMergeMap) {
                uint64_t prefix1, prefix2;
                uint64_t prefix_len1, prefix_len2;
                prefix1 = prefixExtract(mapIt.first);
                prefix_len1 = prefixLenExtract(mapIt.first);
                prefix2 = prefixSubstr(prefix1, prefix_len1 - 1);
                prefix_len2 = prefix_len1;

                // Another should be '1'
                if ((prefix1 & (1 << (prefix_len1 - 1))) == 0) { 
                    prefix2 |= 1 << prefix_len1;
                } 
                debug_info("original prefix = %lx, pair prefix = %lx\n", 
                        prefixSubstr(prefix1, prefix_len1),
                        prefixSubstr(prefix2, prefix_len2));
                hashStoreFileMetaDataHandler* tempHandler;
                if (file_trie_.get(prefix2, tempHandler, prefix_len2) == true) {
                    if (tempHandler->file_id == targetFileIDForSplit) {
                        debug_trace("Skip file ID = %lu, "
                                " prefix bit number = %lu, size = %lu, "
                                " which is currently during GC\n", 
                                tempHandler->file_id,
                                tempHandler->prefix_bit,
                                tempHandler->total_object_bytes);
                        continue;
                    }
                    if (tempHandler->total_object_bytes + mapIt.second->total_object_bytes < singleFileGCTriggerSize_) {
                        if (enableBatchedOperations_ == true) {
                            if (mapIt.second->file_ownership != 0 || tempHandler->file_ownership != 0) {
                                continue;
                                // skip wait if batched op
                            }
                        }
                        if (mapIt.second->file_ownership != 0) {
                            debug_trace("Waiting for file ownership for select file ID = %lu\n", mapIt.second->file_id);
                            while (mapIt.second->file_ownership != 0) {
                                asm volatile("");
                            }
                        }
                        mapIt.second->file_ownership = -1;
                        target_prefix = prefix1; // don't care about substr
                        prefix_len = prefix_len1 - 1;

                        file_hdl1 = mapIt.second;
                        if (tempHandler->file_ownership != 0) {
                            mapIt.second->file_ownership = 0;
                            debug_info("Stop this merge for file ID = %lu\n", tempHandler->file_id);
                            return false;
//                                debug_trace("Waiting for file ownership for select file ID = %lu\n", tempHandler->file_id);
//                                while (tempHandler->file_ownership != 0) {
//                                    asm volatile("");
//                                }
                        }
                        tempHandler->file_ownership = -1;
                        file_hdl2 = tempHandler;
                        debug_info("Find two file for merge GC success,"
                                " file_hdl 1 ptr = %p,"
                                " file_hdl 2 ptr = %p,"
                                " target prefix = %lx\n",
                                file_hdl1, file_hdl2, target_prefix);
                        StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE_AFTER_SELECT, tv);
                        StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE, tvAll);
                        return true;
                    }
                } else {
                    debug_info("Could not find adjacent node for current"
                            " node, skip this node, current node prefix = %lx,"
                            " target node prefix = %lx\n",
                            prefix1, prefix2);
                    continue;
                }
            }
        }
        debug_info("Could not get could merge tree nodes from prefixTree, current targetFileForMergeMap size = %lu\n", targetFileForMergeMap.size());
        StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE, tvAll);
        return false;
    } else {
        debug_info("Could not get could merge tree nodes from prefixTree, current targetFileForMergeMap size = %lu\n", targetFileForMergeMap.size());
        StatsRecorder::getInstance()->timeProcess(StatsType::GC_SELECT_MERGE, tvAll);
        return false;
    }
}

bool HashStoreFileManager::pushObjectsToWriteBackQueue(
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
            StatsRecorder::getInstance()->timeProcess(
                    StatsType::DELTAKV_GC_WRITE_BACK, tv);
        }
    }
    return true;
}

void HashStoreFileManager::processMergeGCRequestWorker()
{
    while (true) {
        if (notifyGCMQ_->done == true && notifyGCMQ_->isEmpty() == true) {
            break;
        }
        uint64_t remainEmptyBucketNumber = file_trie_.getRemainFileNumber();
        usleep(100000);
        if (remainEmptyBucketNumber >= singleFileGCWorkerThreadsNumebr_ + 2) {
            continue;
        }
        debug_info("May reached max file number, need to merge, current remain empty file numebr = %lu\n", remainEmptyBucketNumber);
        // perfrom merge before split, keep the total file number not changed
        hashStoreFileMetaDataHandler* file_hdl1;
        hashStoreFileMetaDataHandler* file_hdl2;
        uint64_t merge_prefix;
        uint64_t prefix_len;
        bool selectFileForMergeStatus = selectFileForMerge(0,
                file_hdl1, file_hdl2, merge_prefix,
                prefix_len);
        if (selectFileForMergeStatus == true) {
            debug_info("Select two file for merge GC success, "
                    "file_hdl 1 ptr = %p, file_hdl 2 ptr = %p, "
                    "target prefix = %lx\n", 
                    file_hdl1, file_hdl2, merge_prefix);
            debug_error("Select two file for merge GC success, "
                    "file_hdl 1 ptr = %p, file_hdl 2 ptr = %p, "
                    "target prefix = %lx\n", 
                    file_hdl1, file_hdl2, merge_prefix);
            bool performFileMergeStatus =
                twoAdjacentFileMerge(file_hdl1, file_hdl2,
                        merge_prefix, prefix_len);
            if (performFileMergeStatus != true) {
                debug_error("[ERROR] Could not merge two files for GC,"
                        " file_hdl 1 ptr = %p, file_hdl 2 ptr = %p, "
                        "target prefix = %lx\n", 
                        file_hdl1, file_hdl2, merge_prefix);
            }
        }
    }
    return;
}

// threads workers
void HashStoreFileManager::processSingleFileGCRequestWorker(int threadID)
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
        hashStoreFileMetaDataHandler* file_hdl;
        if (notifyGCMQ_->pop(file_hdl)) {
            counter--;
//            bool flag = false;

//            if (file_hdl->file_id >= 0 && file_hdl->file_id <= 140) {
//                flag = true;
//                debug_error("start gc on file %u, owner %d, gc status %d\n", 
//                        (int)file_hdl->file_id,
//                        (int)file_hdl->file_ownership,
//                        (int)file_hdl->gc_status);
//            }

            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_warn("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, wait for lock\n", file_hdl->file_id, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes, file_hdl->gc_status);
            std::scoped_lock<std::shared_mutex> w_lock(file_hdl->fileOperationMutex_);
            debug_info("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, start process\n", file_hdl->file_id, file_hdl->total_object_bytes, file_hdl->total_on_disk_bytes, file_hdl->gc_status);
            // read contents
            char readWriteBuffer[file_hdl->total_object_bytes];
            FileOpStatus readFileStatus;
            STAT_PROCESS(readFileStatus = file_hdl->file_op_ptr->readFile(readWriteBuffer, file_hdl->total_object_bytes), StatsType::DELTAKV_GC_READ);
            StatsRecorder::getInstance()->DeltaGcBytesRead(file_hdl->total_on_disk_bytes, file_hdl->total_object_bytes, syncStatistics_);
            if (readFileStatus.success_ == false || readFileStatus.logicalSize_ != file_hdl->total_object_bytes) {
                debug_error("[ERROR] Could not read contents of file for GC, fileID = %lu, target size = %lu, actual read size = %lu\n", file_hdl->file_id, file_hdl->total_object_bytes, readFileStatus.logicalSize_);
                file_hdl->gc_status = kNoGC;
                file_hdl->file_ownership = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                continue;
            }
            // process GC contents
            unordered_map<str_t, pair<vector<str_t>,
                vector<hashStoreRecordHeader>>, mapHashKeyForStr_t,
                mapEqualKeForStr_t> gcResultMap;
            pair<uint64_t, uint64_t> remainObjectNumberPair;
            STAT_PROCESS(remainObjectNumberPair =
                    deconstructAndGetValidContentsFromFile(readWriteBuffer,
                        file_hdl->total_object_bytes, gcResultMap),
                    StatsType::DELTAKV_GC_PROCESS);
            unordered_set<str_t, mapHashKeyForStr_t, mapEqualKeForStr_t> shouldDelete;

            if (enableLsmTreeDeltaMeta_ == false) {
                STAT_PROCESS(
                        remainObjectNumberPair.first -=
                        partialMergeGcResultMap(gcResultMap, shouldDelete),
                        StatsType::DELTAKV_GC_PARTIAL_MERGE);
            }

            bool fileContainsReWriteKeysFlag = false;
            // calculate target file size
            vector<writeBackObject*> targetWriteBackVec;
            uint64_t target_size = 0;
            size_t header_sz = sizeof(hashStoreRecordHeader);

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

            uint64_t targetFileSizeWoIndexBlock = target_size +
                sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader);


            // count valid object size to determine GC method;
            if (remainObjectNumberPair.second == 0) {
                debug_error("[ERROR] File ID = %lu contains no object, should just delete, total contains object number = %lu, should keep object number = %lu\n", file_hdl->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first);
                singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag);
                file_hdl->file_ownership = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);

                continue;
            }

            if (remainObjectNumberPair.first != 0 && gcResultMap.size() == 0) {
                debug_error("[ERROR] File ID = %lu contains valid objects but result map is zero, processed object number = %lu, target keep object number = %lu\n", file_hdl->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first);
                singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag);
                file_hdl->file_ownership = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                continue;
            }

            if (remainObjectNumberPair.first == 0 && gcResultMap.size() == 0) {
                debug_info("File ID = %lu total disk size %lu have no valid objects\n", file_hdl->file_id, file_hdl->total_on_disk_bytes);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
                STAT_PROCESS(singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag), StatsType::REWRITE);
                file_hdl->file_ownership = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
                continue;
            }

            if (remainObjectNumberPair.first > 0 && gcResultMap.size() == 1) {
                // No invalid objects, cannot save space
                if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
                    if (file_hdl->gc_status == kNew) {
                        // keep tracking until forced gc threshold;
                        file_hdl->gc_status = kMayGC;
                        file_hdl->file_ownership = 0;
                        debug_info("File ID = %lu contains only %lu different keys, marked as kMayGC\n", file_hdl->file_id, gcResultMap.size());
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        pushObjectsToWriteBackQueue(targetWriteBackVec);
                    } else if (file_hdl->gc_status == kMayGC) {
                        // Mark this file as could not GC;
                        file_hdl->gc_status = kNoGC;
                        file_hdl->file_ownership = 0;
                        debug_error("File ID = %lu contains only %lu different keys, marked as kNoGC\n", file_hdl->file_id, gcResultMap.size());
//                        debug_info("File ID = %lu contains only %lu different keys, marked as kNoGC\n", file_hdl->file_id, gcResultMap.size());
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        pushObjectsToWriteBackQueue(targetWriteBackVec);
                    }
                } else {
                    // single file rewrite
                    debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write\n", file_hdl->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first);
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
                    STAT_PROCESS(singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag), StatsType::REWRITE);
                    file_hdl->file_ownership = 0;
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                    pushObjectsToWriteBackQueue(targetWriteBackVec);
                }
                clearMemoryForTemporaryMergedDeltas(gcResultMap, shouldDelete);
                continue;
            }
            // perform split into two buckets via extend prefix bit (+1)
            if (targetFileSizeWoIndexBlock <= singleFileSplitGCTriggerSize_) {
                debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write, target file size = %lu, split threshold = %lu\n", file_hdl->file_id, remainObjectNumberPair.second, remainObjectNumberPair.first, targetFileSizeWoIndexBlock, singleFileSplitGCTriggerSize_);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
                STAT_PROCESS(singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag), StatsType::REWRITE);
                file_hdl->file_ownership = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                pushObjectsToWriteBackQueue(targetWriteBackVec);
            } else {
                debug_info("try split for key number = %lu\n", gcResultMap.size());
                uint64_t currentUsedPrefixBitNumber = file_hdl->prefix_bit;
                uint64_t prefix_len = currentUsedPrefixBitNumber + 1;

                uint64_t remainEmptyFileNumber = file_trie_.getRemainFileNumber();
                if (remainEmptyFileNumber >= singleFileGCWorkerThreadsNumebr_ + 2) {
                    // cerr << "Perform split " << endl;
                    debug_info("Still not reach max file number, split directly, current remain empty file numebr = %lu\n", remainEmptyFileNumber);
                    debug_info("Perform split GC for file ID (without merge) = %lu\n", file_hdl->file_id);
                    bool singleFileGCStatus;
                    STAT_PROCESS(singleFileGCStatus = singleFileSplit(file_hdl, gcResultMap, prefix_len, fileContainsReWriteKeysFlag), StatsType::SPLIT);
                    if (singleFileGCStatus == false) {
                        debug_error("[ERROR] Could not perform split GC for file ID = %lu\n", file_hdl->file_id);
                        file_hdl->gc_status = kNoGC;
                        file_hdl->file_ownership = 0;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        pushObjectsToWriteBackQueue(targetWriteBackVec);
                    } else {
                        debug_info("Perform split GC for file ID (without merge) = %lu done\n", file_hdl->file_id);
                        file_hdl->gc_status = kShouldDelete;
                        file_hdl->file_ownership = 0;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        pushObjectsToWriteBackQueue(targetWriteBackVec);
                    }
                } else {
                    // Case 3 in the paper: push all KD pairs in the bucket to the queue 
                    if (remainObjectNumberPair.first < remainObjectNumberPair.second) {
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE, tv);
                        STAT_PROCESS(singleFileRewrite(file_hdl, gcResultMap, targetFileSizeWoIndexBlock, fileContainsReWriteKeysFlag), StatsType::REWRITE);
                    }
                    file_hdl->file_ownership = 0;
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                    pushObjectsToWriteBackQueue(targetWriteBackVec);
                }
            }
            clearMemoryForTemporaryMergedDeltas(gcResultMap, shouldDelete);
        }
    }
    workingThreadExitFlagVec_ += 1;
    return;
}

void HashStoreFileManager::scheduleMetadataUpdateWorker()
{
    while (true) {
        {
            std::unique_lock<std::mutex> lk(metaCommitMtx_);
            while (metadataUpdateShouldExit_ == false &&
                    operationCounterForMetadataCommit_ <
                    operationNumberForMetadataCommitThreshold_) {
                metaCommitCV_.wait(lk);
            }
        }
        if (operationCounterForMetadataCommit_ >= operationNumberForMetadataCommitThreshold_) {
            bool status; 
            if (enable_crash_consistency_) {
                STAT_PROCESS(status = UpdateHashStoreFileMetaDataList(),
                        StatsType::FM_UPDATE_META);
            } else {
                STAT_PROCESS(status = RemoveObsoleteFiles(),
                        StatsType::FM_UPDATE_META);
            }
            if (status != true) {
                debug_error("[ERROR] commit metadata for %lu operations error\n", operationCounterForMetadataCommit_);
            } else {
                debug_info("commit metadata for %lu operations success\n", operationCounterForMetadataCommit_);
                operationCounterMtx_.lock();
                operationCounterForMetadataCommit_ = 0;
                operationCounterMtx_.unlock();
            }
        }
        if (metadataUpdateShouldExit_ == true) {
            break;
        }
    }
    return;
}

bool HashStoreFileManager::forcedManualGCAllFiles()
{
    vector<hashStoreFileMetaDataHandler*> validFilesVec;
    file_trie_.getCurrentValidNodesNoKey(validFilesVec);
    for (auto file_handler : validFilesVec) {
	if (file_handler->file_ownership != 0) {
	    debug_error("file id %lu not zero %d\n", file_handler->file_id, file_handler->file_ownership);
	}
        while (file_handler->file_ownership != 0) {
            asm volatile("");
        }
        // cerr << "File ID = " << file_handler->file_id << ", file size on disk = " << file_handler->total_on_disk_bytes << endl;
        if (file_handler->gc_status == kNoGC) {
            if (file_handler->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
                debug_info("Current file ID = %lu, file size = %lu, has been"
                        " marked as kNoGC, but size overflow\n",
                        file_handler->file_id,
                        file_handler->total_on_disk_bytes);
                notifyGCMQ_->push(file_handler);
                operationNotifyCV_.notify_one();
                // cerr << "Push file ID = " << file_handler->file_id << endl;
                continue;
            } else {
                debug_info("Current file ID = %lu, file size = %lu, has been marked as kNoGC, skip\n", file_handler->file_id, file_handler->total_on_disk_bytes);
                continue;
            }
        } else if (file_handler->gc_status == kShouldDelete) {
            debug_error("[ERROR] During forced GC, should not find file marked as kShouldDelete, file ID = %lu, file size = %lu, prefix bit number = %lu\n", file_handler->file_id, file_handler->total_on_disk_bytes, file_handler->prefix_bit);
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(file_handler->file_id);
            fileDeleteVecMtx_.unlock();
            // cerr << "Delete file ID = " << file_handler->file_id << endl;
            continue;
        } else {
            if (file_handler->DiskAndBufferSizeExceeds(singleFileGCTriggerSize_)) {
                // cerr << "Push file ID = " << file_handler->file_id << endl;
                notifyGCMQ_->push(file_handler);
                operationNotifyCV_.notify_one();
            }
        }
    }
    if (notifyGCMQ_->isEmpty() != true) {
        debug_trace("Wait for gc job done in forced GC%s\n", "");
        while (notifyGCMQ_->isEmpty() != true) {
            asm volatile("");
            // wait for gc job done
        }
        debug_trace("Wait for gc job done in forced GC%s over\n", "");
    }
    return true;
}
}
