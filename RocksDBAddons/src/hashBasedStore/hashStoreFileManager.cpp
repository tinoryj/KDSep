#include "hashBasedStore/hashStoreFileManager.hpp"
#include "utils/statsRecorder.hh"
#include <unordered_map>

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue)
{
    maxBucketNumber_ = options->hashStore_max_file_number_;
    uint64_t k = 0;
    while (pow((double)2, (double)k) <= maxBucketNumber_) {
        k++;
    }
    k = k - 1;
    initialTrieBitNumber_ = k;
    singleFileGCTriggerSize_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileMergeGCUpperBoundSize_ = options->deltaStore_single_file_maximum_size - singleFileGCTriggerSize_;
    debug_info("[Message]: singleFileGCTriggerSize_ = %lu, singleFileMergeGCUpperBoundSize_ = %lu, initialTrieBitNumber_ = %lu\n", singleFileGCTriggerSize_, singleFileMergeGCUpperBoundSize_, initialTrieBitNumber_);
    globalGCTriggerSize_ = options->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * options->deltaStore_total_storage_maximum_size;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    enableWriteBackDuringGCFlag_ = true;
    writeBackOperationsQueue_ = writeBackOperationsQueue;
    gcWriteBackDeltaNum_ = options->deltaStore_write_back_during_gc_threshold;
    fileOperationMethod_ = options->fileOperationMethod_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    operationNumberForMetadataCommitThreshold_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    singleFileSplitGCTriggerSize_ = options->deltaStore_split_garbage_collection_start_single_file_minimum_occupancy_ * options->deltaStore_single_file_maximum_size;
    objectFileMetaDataTrie_.init(initialTrieBitNumber_, maxBucketNumber_);
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::HashStoreFileManager(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ)
{
    maxBucketNumber_ = options->hashStore_max_file_number_;
    uint64_t k = 0;
    while (pow((double)2, (double)k) <= maxBucketNumber_) {
        k++;
    }
    k = k - 1;
    initialTrieBitNumber_ = k;
    singleFileGCTriggerSize_ = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    singleFileMergeGCUpperBoundSize_ = options->deltaStore_single_file_maximum_size - singleFileGCTriggerSize_;
    debug_info("[Message]: singleFileGCTriggerSize_ = %lu, singleFileMergeGCUpperBoundSize_ = %lu, initialTrieBitNumber_ = %lu\n", singleFileGCTriggerSize_, singleFileMergeGCUpperBoundSize_, initialTrieBitNumber_);
    globalGCTriggerSize_ = options->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * options->deltaStore_total_storage_maximum_size;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    enableWriteBackDuringGCFlag_ = false;
    gcWriteBackDeltaNum_ = options->deltaStore_write_back_during_gc_threshold;
    fileOperationMethod_ = options->fileOperationMethod_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    operationNumberForMetadataCommitThreshold_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    singleFileSplitGCTriggerSize_ = options->deltaStore_split_garbage_collection_start_single_file_minimum_occupancy_ * options->deltaStore_single_file_maximum_size;
    objectFileMetaDataTrie_.init(initialTrieBitNumber_, maxBucketNumber_);
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::~HashStoreFileManager()
{
    if (enableGCFlag_ == true) {
        if (gcThreadJobDoneFlag_ == false) {
            debug_info("Wait for gcThreadJobDoneFlag_ == true to prevent metadata update before forced gc done%s\n", "");
            while (gcThreadJobDoneFlag_ == false) {
                asm volatile("");
            }
            debug_info("Wait for gcThreadJobDoneFlag_ == true over%s\n", "");
        }
    }
    CloseHashStoreFileMetaDataList();
}

bool HashStoreFileManager::setJobDone()
{
    metadataUpdateShouldExit_ = true;
    return true;
}

// Recovery
/*
fileContentBuffer start after file header
resultMap include key - <is_anchor, value> map
*/
uint64_t HashStoreFileManager::deconstructAndGetAllContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone)
{
    uint64_t processedTotalObjectNumber = 0;
    uint64_t currentProcessLocationIndex = 0;

    while (currentProcessLocationIndex != fileSize) {
        processedTotalObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            // is anchor, skip value only
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            debug_trace("deconstruct current record is anchor, key = %s\n", currentKeyStr.c_str());
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr = "";
                resultMap.at(currentKeyStr).push_back(make_pair(true, currentValueStr));
                continue;
            } else {
                vector<pair<bool, string>> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr = "";
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(true, currentValueStr));
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            // is gc mark, skip key and value
            debug_trace("deconstruct current record is gc flushed flag = %d\n", currentObjectRecordHeader.is_gc_done_);
            isGCFlushDone = true;
            continue;
        } else {
            // is content, keep key and value
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            debug_trace("deconstruct current record is anchor, key = %s\n", currentKeyStr.c_str());
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(make_pair(false, currentValueStr));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<pair<bool, string>> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(false, currentValueStr));
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
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
    vector<pair<string, hashStoreFileMetaDataHandler*>> validPrefixToFileHandlerVec;
    objectFileMetaDataTrie_.getCurrentValidNodes(validPrefixToFileHandlerVec);
    unordered_map<uint64_t, pair<string, hashStoreFileMetaDataHandler*>> hashStoreFileIDToPrefixMap;
    for (auto validFileIt : validPrefixToFileHandlerVec) {
        uint64_t currentFileID = validFileIt.second->target_file_id_;
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
                FileOperation tempReadFileStream(fileOperationMethod_);
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
                    hashStoreFileHeader currentFileHeader;
                    memcpy(&currentFileHeader, readContentBuffer, sizeof(hashStoreFileHeader));
                    // process file content
                    bool isGCFlushedDoneFlag = false;
                    unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMap;
                    uint64_t currentFileObjectNumber = deconstructAndGetAllContentsFromFile(readContentBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMap, isGCFlushedDoneFlag);

                    if (currentFileHeader.file_create_reason_ == kInternalGCFile) {
                        // GC file with ID > targetNewFileID
                        // judge previous file ID
                        if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_first_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == false) {
                            // previous ID not in metadata && gc file is not correctly flushed;
                            debug_error("[ERROR] find kGC file that previous file ID not in metadata, seems error, previous file ID = %lu\n", currentFileHeader.previous_file_id_first_);
                            return false;
                        } else if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_first_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == true) {
                            // gc flushed done, kGC file should add
                            // generate prefix
                            string currentFilePrefix;
                            bool generatePrefixStatus = generateHashBasedPrefix(currentFileRecoveryMap.begin()->first, currentFilePrefix);
                            currentFilePrefix = currentFilePrefix.substr(0, currentFileHeader.current_prefix_used_bit_);
                            // generate file handler
                            hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                            currentRecoveryFileHandler->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
                            currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                            currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                            currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                            currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                            // open current file for further usage
                            currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                            // update metadata
                            objectFileMetaDataTrie_.insertWithFixedBitNumber(currentFilePrefix, currentFileHeader.current_prefix_used_bit_, currentRecoveryFileHandler);
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
                        } else if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_first_) != hashStoreFileIDToPrefixMap.end()) {
                            // previous file metadata exist
                            uint64_t prefixBitNumberUsedInPreviousFile = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_first_).second->current_prefix_used_bit_;
                            if (prefixBitNumberUsedInPreviousFile == currentFileHeader.current_prefix_used_bit_) {
                                // single file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    // get file handler
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_first_).second;
                                    currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                                    currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                                    currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                                    // open current file for further usage
                                    if (currentRecoveryFileHandler->file_operation_func_ptr_->isFileOpen() == true) {
                                        currentRecoveryFileHandler->file_operation_func_ptr_->closeFile();
                                    }
                                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    // update metadata
                                    hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_first_).second->target_file_id_ = fileIDIt;
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
                                    targetDeleteFileIDVec.push_back(currentFileHeader.previous_file_id_first_);
                                    continue;
                                } else {
                                    // gc not success, keep old file, and delete current one
                                    targetDeleteFileIDVec.push_back(fileIDIt);
                                    continue;
                                }
                            } else if (prefixBitNumberUsedInPreviousFile < currentFileHeader.current_prefix_used_bit_) {
                                // file created by split, cache to find the other one
                                if (mapForBatchedkInternalGCFiles.find(currentFileHeader.previous_file_id_first_) == mapForBatchedkInternalGCFiles.end()) {
                                    vector<uint64_t> tempVec;
                                    tempVec.push_back(fileIDIt);
                                    mapForBatchedkInternalGCFiles.insert(make_pair(currentFileHeader.previous_file_id_first_, tempVec));
                                } else {
                                    mapForBatchedkInternalGCFiles.at(currentFileHeader.previous_file_id_first_).push_back(fileIDIt);
                                }
                            } else if (prefixBitNumberUsedInPreviousFile > currentFileHeader.current_prefix_used_bit_) {
                                // merge file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    string leftFatherFilePrefixStr = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_first_).first; // we store left file as previous file ID (last bit in use = 0)
                                    string rightFatherFilePrefixStr = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_first_).first.substr(0, currentFileHeader.current_prefix_used_bit_) + "1";
                                    uint64_t leftFatherFileID = currentFileHeader.previous_file_id_first_;
                                    hashStoreFileMetaDataHandler* tempHandlerForRightFileID;
                                    objectFileMetaDataTrie_.get(rightFatherFilePrefixStr, tempHandlerForRightFileID);
                                    uint64_t rightFatherFileID = tempHandlerForRightFileID->target_file_id_;
                                    // delete left father
                                    uint64_t findFileAtLevel = 0;
                                    objectFileMetaDataTrie_.remove(leftFatherFilePrefixStr, findFileAtLevel);
                                    if (findFileAtLevel != leftFatherFilePrefixStr.size()) {
                                        debug_error("[ERROR] In merged gc, find previous left file prefix mismatch, in prefix tree, bit number = %lu, but the file used %lu in header\n", findFileAtLevel, leftFatherFilePrefixStr.size());
                                        return false;
                                    }
                                    targetDeleteFileIDVec.push_back(leftFatherFileID);
                                    // delete right father
                                    objectFileMetaDataTrie_.remove(rightFatherFilePrefixStr, findFileAtLevel);
                                    if (findFileAtLevel != rightFatherFilePrefixStr.size()) {
                                        debug_error("[ERROR] In merged gc, find previous right file prefix mismatch, in prefix tree, bit number = %lu, but the file used %lu in header\n", findFileAtLevel, rightFatherFilePrefixStr.size());
                                        return false;
                                    }
                                    targetDeleteFileIDVec.push_back(rightFatherFileID);
                                    // insert new file into metadata
                                    string currentFilePrefix = leftFatherFilePrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_);
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                                    currentRecoveryFileHandler->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
                                    currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                                    currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                                    currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                                    // open current file for further usage
                                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    // update metadata
                                    objectFileMetaDataTrie_.insertWithFixedBitNumber(currentFilePrefix, currentFileHeader.current_prefix_used_bit_, currentRecoveryFileHandler);
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
                    } else if (currentFileHeader.file_create_reason_ == kNewFile) {
                        // new file with ID > targetNewFileID, should add into metadata
                        hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                        currentRecoveryFileHandler->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
                        currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                        currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                        currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                        currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                        // open current file for further usage
                        currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                        // update metadata
                        string targetRecoveryPrefixStr;
                        generateHashBasedPrefix(currentFileRecoveryMap.begin()->first, targetRecoveryPrefixStr);
                        objectFileMetaDataTrie_.insertWithFixedBitNumber(targetRecoveryPrefixStr, currentFileHeader.current_prefix_used_bit_, currentRecoveryFileHandler);
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
            objectFileMetaDataTrie_.get(hashStoreFileIDToPrefixMap.at(fileIDIt).first, currentIDInMetadataFileHandlerPtr);
            uint64_t onDiskFileSize = currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->getFileSize();
            if (currentIDInMetadataFileHandlerPtr->total_object_bytes_ > onDiskFileSize) {
                // metadata size > filesystem size, error
                debug_error("[ERROR] file ID = %lu, file size in metadata = %lu larger than file size in file system = %lu\n", fileIDIt, currentIDInMetadataFileHandlerPtr->total_object_bytes_, onDiskFileSize);
            } else if (currentIDInMetadataFileHandlerPtr->total_object_bytes_ < onDiskFileSize) {
                // file may append, should recovery
                debug_trace("target file ID = %lu, file size (system) = %lu != file size (metadata) = %lu, try recovery\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes_);

                // start read
                int targetReadSize = onDiskFileSize;
                char readBuffer[targetReadSize];
                debug_trace("target read file content for recovery size = %lu\n", currentIDInMetadataFileHandlerPtr->total_object_bytes_);
                currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->readFile(readBuffer, targetReadSize);
                // read done, start process
                bool isGCFlushedDoneFlag = false;
                uint64_t recoveredObjectNumber = deconstructAndGetAllContentsFromFile(readBuffer + currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetReadSize - currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetListForRedo, isGCFlushedDoneFlag);
                // update metadata
                currentIDInMetadataFileHandlerPtr->total_object_count_ += recoveredObjectNumber;
                currentIDInMetadataFileHandlerPtr->total_object_bytes_ += targetReadSize;
                currentIDInMetadataFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;

            } else {
                // file size match, skip current file
                debug_trace("target file ID = %lu, file size (system) = %lu, file size (metadata) = %lu\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes_);
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
            hashStoreFileHeader currentFileHeader;
            FileOperation tempReadFileStream(fileOperationMethod_);
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
            memcpy(&currentFileHeader, readBuffer, sizeof(hashStoreFileHeader));
            prefixBitNumber = currentFileHeader.current_prefix_used_bit_;
            uint64_t targetFileRemainReadSize = currentFileSize - sizeof(hashStoreFileHeader);
            tempReadFileStream.closeFile();
            // process file content
            objectNumberCount[i] = deconstructAndGetAllContentsFromFile(readBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMapTemp[i], gcFlagStatus[i]);
        }
        if ((exactFileNumber == 1 && gcFlagStatus[0]) == true || (exactFileNumber == 2 && gcFlagStatus[0] && gcFlagStatus[1]) == true) {
            // keep two new files
            for (int i = 0; i < exactFileNumber; i++) {
                hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                currentRecoveryFileHandler->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
                currentRecoveryFileHandler->target_file_id_ = splitFileIt.second[i];
                currentRecoveryFileHandler->current_prefix_used_bit_ = prefixBitNumber;
                currentRecoveryFileHandler->total_object_count_ = objectNumberCount[i];
                currentRecoveryFileHandler->total_object_bytes_ = targetFileRealSize[i];
                // open current file for further usage
                currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(splitFileIt.second[i]) + ".delta");
                // update metadata
                string targetRecoveryPrefixStr;
                generateHashBasedPrefix(currentFileRecoveryMapTemp[i].begin()->first, targetRecoveryPrefixStr);
                targetRecoveryPrefixStr = targetRecoveryPrefixStr.substr(0, prefixBitNumber);
                objectFileMetaDataTrie_.insertWithFixedBitNumber(targetRecoveryPrefixStr, prefixBitNumber, currentRecoveryFileHandler);
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
            objectFileMetaDataTrie_.get(hashStoreFileIDToPrefixMap.at(splitFileIt.first).first, tempRemoveHandler);
            if (tempRemoveHandler->file_operation_func_ptr_->isFileOpen()) {
                tempRemoveHandler->file_operation_func_ptr_->closeFile();
            }
            uint64_t findPrefixInTreeAtLevelID;
            objectFileMetaDataTrie_.remove(hashStoreFileIDToPrefixMap.at(splitFileIt.first).first, findPrefixInTreeAtLevelID);
            if (hashStoreFileIDToPrefixMap.at(splitFileIt.first).first.size() != findPrefixInTreeAtLevelID) {
                debug_error("[ERROR] Remove object in prefix tree error, the prefix length mismatch, in tree length = %lu, in file length = %lu\n", findPrefixInTreeAtLevelID, hashStoreFileIDToPrefixMap.at(splitFileIt.first).first.size());
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
        uint64_t currentPointerInt = stoull(currentPointerStr);
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
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + currentPointerStr, ios::in);
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
            hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
            currentFileHandlerPtr->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
            currentFileHandlerPtr->target_file_id_ = hashStoreFileID;
            currentFileHandlerPtr->current_prefix_used_bit_ = currentFileUsedPrefixLength;
            currentFileHandlerPtr->total_object_count_ = currentFileStoredObjectCount;
            currentFileHandlerPtr->total_object_bytes_ = currentFileStoredBytes;
            // open current file for further usage
            currentFileHandlerPtr->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
            uint64_t onDiskFileSize = currentFileHandlerPtr->file_operation_func_ptr_->getFileSize();
            if (onDiskFileSize > currentFileHandlerPtr->total_object_bytes_ && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, currentFileHandlerPtr->total_object_bytes_);
            } else if (onDiskFileSize < currentFileHandlerPtr->total_object_bytes_ && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, currentFileHandlerPtr->total_object_bytes_);
            }
            // re-insert into trie and map for build index
            objectFileMetaDataTrie_.insertWithFixedBitNumber(prefixHashStr, currentFileUsedPrefixLength, currentFileHandlerPtr);
        }
        vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec;
        objectFileMetaDataTrie_.getCurrentValidNodes(validObjectVec);
        for (auto it : validObjectVec) {
            debug_info("Read prefix = %s, file ID = %lu from metadata\n", it.first.c_str(), it.second->target_file_id_);
        }
    } else {
        return false;
    }
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList()
{
    vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec, invalidObjectVec;
    objectFileMetaDataTrie_.getCurrentValidNodes(validObjectVec);
    debug_info("Start update metadata, current valid trie size = %lu\n", validObjectVec.size());
    bool shouldUpdateFlag = false;
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                shouldUpdateFlag = true;
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
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                hashStoreFileManifestStream << it.first << endl;
                hashStoreFileManifestStream << it.second->target_file_id_ << endl;
                hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
                hashStoreFileManifestStream << it.second->total_object_count_ << endl;
                hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
                it.second->fileOperationMutex_.lock();
                it.second->file_operation_func_ptr_->flushFile();
                debug_trace("flushed file ID = %lu, file correspond prefix = %s\n", it.second->target_file_id_, it.first.c_str());
                it.second->fileOperationMutex_.unlock();
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
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
        if (filesystem::exists(targetRemoveFileName) != false) {
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
            }
        }
        objectFileMetaDataTrie_.getInValidNodes(invalidObjectVec);
        debug_info("Start delete obslate files, current invalid trie size = %lu\n", invalidObjectVec.size());
        if (invalidObjectVec.size() != 0) {
            for (auto it : invalidObjectVec) {
                if (it.second) {
                    if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                        it.second->file_operation_func_ptr_->closeFile();
                        debug_trace("Closed file ID = %lu, file correspond prefix = %s\n", it.second->target_file_id_, it.first.c_str());
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
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    vector<uint64_t> targetDeleteFileIDVec;
    vector<pair<string, hashStoreFileMetaDataHandler*>> validObjectVec;
    objectFileMetaDataTrie_.getCurrentValidNodes(validObjectVec);
    debug_info("Final commit metadata, current valid trie size = %lu\n", validObjectVec.size());
    if (validObjectVec.size() != 0) {
        for (auto it : validObjectVec) {
            if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                hashStoreFileManifestStream << it.first << endl;
                hashStoreFileManifestStream << it.second->target_file_id_ << endl;
                hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
                hashStoreFileManifestStream << it.second->total_object_count_ << endl;
                hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
                it.second->file_operation_func_ptr_->flushFile();
                it.second->file_operation_func_ptr_->closeFile();
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
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
        if (filesystem::exists(targetRemoveFileName) != false) {
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveFileName.c_str());
            }
        }
        vector<pair<string, hashStoreFileMetaDataHandler*>> possibleValidObjectVec;
        objectFileMetaDataTrie_.getPossibleValidNodes(possibleValidObjectVec);
        for (auto it : possibleValidObjectVec) {
            if (it.second) {
                if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                    it.second->file_operation_func_ptr_->closeFile();
                }
                delete it.second->file_operation_func_ptr_;
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
        objectFileMetaDataTrie_.getPossibleValidNodes(possibleValidObjectVec);
        for (auto it : possibleValidObjectVec) {
            if (it.second) {
                if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                    it.second->file_operation_func_ptr_->closeFile();
                }
                delete it.second->file_operation_func_ptr_;
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
        hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt), ios::out);
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
bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr, bool getForAnchorWriting)
{
    if (opType == kPut) {
        operationCounterMtx_.lock();
        operationCounterForMetadataCommit_++;
        operationCounterMtx_.unlock();
    }

    string prefixStr;
    bool genPrefixStatus = generateHashBasedPrefix(keyStr, prefixStr);
    if (!genPrefixStatus) {
        debug_error("[ERROR]  generate prefix hash for current key error, key = %s\n", keyStr.c_str());
        return false;
    }
    bool fileHandlerExistFlag = getHashStoreFileHandlerExistFlag(prefixStr);
    if (fileHandlerExistFlag == false && opType == kGet) {
        debug_error("[ERROR] get operation meet not stored buckets, key = %s\n", keyStr.c_str());
        return false;
    } else if (fileHandlerExistFlag == false && opType == kPut) {
        // Anchor: handler does not exist, do not create the file and directly return
        if (getForAnchorWriting) {
            fileHandlerPtr = nullptr;
            return true;
        }
        bool createNewFileHandlerStatus;
        STAT_PROCESS(createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefixForUser(prefixStr, fileHandlerPtr, initialTrieBitNumber_), StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
        if (!createNewFileHandlerStatus) {
            debug_error("[ERROR] create new bucket for put operation error, key = %s\n", keyStr.c_str());
            return false;
        } else {
            debug_info("[Insert] Create new file ID = %lu, for key = %s, file gc status flag = %d, prefix bit number used = %lu\n", fileHandlerPtr->target_file_id_, keyStr.c_str(), fileHandlerPtr->gc_result_status_flag_, fileHandlerPtr->current_prefix_used_bit_);
            fileHandlerPtr->file_ownership_flag_ = 1;
            return true;
        }
    } else if (fileHandlerExistFlag == true) {
        while (true) {
            bool getFileHandlerStatus = getHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr);
            if (!getFileHandlerStatus && (opType == kPut || opType == kMultiPut)) {
                bool createNewFileHandlerStatus;
                STAT_PROCESS(createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefixForUser(prefixStr, fileHandlerPtr, initialTrieBitNumber_), StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
                if (!createNewFileHandlerStatus) {
                    debug_error("[ERROR] Previous file may deleted during GC, and splited new files not contains current key prefix, create new bucket for put operation error, key = %s\n", keyStr.c_str());
                    return false;
                } else {
                    debug_warn("[Insert] Previous file may deleted during GC, and splited new files not contains current key prefix, create new file ID = %lu, for key = %s, file gc status flag = %d, prefix bit number used = %lu\n", fileHandlerPtr->target_file_id_, keyStr.c_str(), fileHandlerPtr->gc_result_status_flag_, fileHandlerPtr->current_prefix_used_bit_);
                    fileHandlerPtr->file_ownership_flag_ = 1;
                    return true;
                }
            } else {
                // avoid get file handler which is in GC;
                if (fileHandlerPtr->file_ownership_flag_ != 0) {
                    debug_trace("Wait for file ownership, file ID = %lu, for key = %s\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    while (fileHandlerPtr->file_ownership_flag_ != 0) {
                        asm volatile("");
                        // wait if file is using in gc
                    }
                    debug_trace("Wait for file ownership, file ID = %lu, for key = %s over\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                }
                if (fileHandlerPtr->gc_result_status_flag_ == kShouldDelete) {
                    // retry if the file should delete;
                    debug_warn("Get exist file ID = %lu, for key = %s, this file is marked as kShouldDelete\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    continue;
                } else {
                    debug_trace("Get exist file ID = %lu, for key = %s\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    fileHandlerPtr->file_ownership_flag_ = 1;
                    return true;
                }
            }
        }
    } else {
        debug_error("[ERROR] Unknown operation type = %d\n", opType);
        return false;
    }
}

bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStrForMultiPut(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr, string& prefixStr, bool getForAnchorWriting)
{
    if (opType == kMultiPut) {
        operationCounterMtx_.lock();
        operationCounterForMetadataCommit_++;
        operationCounterMtx_.unlock();
    }

    bool genPrefixStatus = generateHashBasedPrefix(keyStr, prefixStr);
    if (!genPrefixStatus) {
        debug_error("[ERROR]  generate prefix hash for current key error, key = %s\n", keyStr.c_str());
        return false;
    }
    bool fileHandlerExistFlag = getHashStoreFileHandlerExistFlag(prefixStr);
    if (fileHandlerExistFlag == false && getForAnchorWriting == true) {
        // Anchor: handler does not exist, do not create the file and directly return
        debug_info("Return nullptr for key = %s, since it's an anchor\n", keyStr.c_str());
        fileHandlerPtr = nullptr;
        return true;
    } else if (fileHandlerExistFlag == false && getForAnchorWriting == false) {
        // Anchor: handler does not exist, need create the file
        bool createNewFileHandlerStatus;
        STAT_PROCESS(createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefixForUser(prefixStr, fileHandlerPtr, initialTrieBitNumber_), StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
        if (!createNewFileHandlerStatus) {
            debug_error("[ERROR] create new bucket for put operation error, key = %s\n", keyStr.c_str());
            return false;
        } else {
            debug_info("[Insert] Create new file ID = %lu, for key = %s, file gc status flag = %d, prefix bit number used = %lu\n", fileHandlerPtr->target_file_id_, keyStr.c_str(), fileHandlerPtr->gc_result_status_flag_, fileHandlerPtr->current_prefix_used_bit_);
            return true;
        }
    } else if (fileHandlerExistFlag == true) {
        while (true) {
            bool getFileHandlerStatus = getHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr);
            if (!getFileHandlerStatus) {
                bool createNewFileHandlerStatus;
                STAT_PROCESS(createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefixForUser(prefixStr, fileHandlerPtr, initialTrieBitNumber_), StatsType::DELTAKV_HASHSTORE_CREATE_NEW_BUCKET);
                if (!createNewFileHandlerStatus) {
                    debug_error("[ERROR] Previous file may deleted during GC, and splited new files not contains current key prefix, create new bucket for put operation error, key = %s\n", keyStr.c_str());
                    return false;
                } else {
                    debug_warn("[Insert] Previous file may deleted during GC, and splited new files not contains current key prefix, create new file ID = %lu, for key = %s, file gc status flag = %d, prefix bit number used = %lu\n", fileHandlerPtr->target_file_id_, keyStr.c_str(), fileHandlerPtr->gc_result_status_flag_, fileHandlerPtr->current_prefix_used_bit_);
                    return true;
                }
            } else {
                // avoid get file handler which is in GC;
                if (fileHandlerPtr->file_ownership_flag_ != 0) {
                    debug_trace("Wait for file ownership, exist file ID = %lu, for key = %s\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    while (fileHandlerPtr->file_ownership_flag_ != 0) {
                        asm volatile("");
                        // wait if file is using in gc
                    }
                    debug_trace("Wait for file ownership, file ID = %lu, for key = %s over\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                }
                if (fileHandlerPtr->gc_result_status_flag_ == kShouldDelete) {
                    // retry if the file should delete;
                    debug_warn("Get exist file ID = %lu, for key = %s, this file is marked as kShouldDelete\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    continue;
                } else {
                    debug_trace("Get exist file ID = %lu, for key = %s\n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                    return true;
                }
            }
        }
    } else {
        debug_error("[ERROR] Unknown operation for key = %s\n", keyStr.c_str());
        return false;
    }
}

// file operations - private
bool HashStoreFileManager::generateHashBasedPrefix(const string rawStr, string& prefixStr)
{
    u_char murmurHashResultBuffer[16];
    MurmurHash3_x64_128((void*)rawStr.c_str(), rawStr.size(), 0, murmurHashResultBuffer);
    uint64_t firstFourByte;
    memcpy(&firstFourByte, murmurHashResultBuffer, sizeof(uint64_t));
    while (firstFourByte != 0) {
        prefixStr += (firstFourByte & 1) + '0';
        firstFourByte >>= 1;
    }
    return true;
}

bool HashStoreFileManager::getHashStoreFileHandlerExistFlag(const string prefixStr)
{
    uint64_t prefixLen;
    bool handlerFindStatus = objectFileMetaDataTrie_.find(prefixStr, prefixLen);
    if (handlerFindStatus == true) {
        return true;
    } else {
        debug_trace("Could not find prefix = %s, need to create\n", prefixStr.c_str());
        return false;
    }
    return 0;
}

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr)
{
    uint64_t prefixLen;
    bool handlerFindStatus = objectFileMetaDataTrie_.find(prefixStr, prefixLen);
    if (handlerFindStatus == true) {
        bool handlerGetStatus = objectFileMetaDataTrie_.get(prefixStr, fileHandlerPtr);
        if (handlerGetStatus == false) {
            debug_error("[ERROR] get file hendler fail after check existence for prefix = %s\n", prefixStr.c_str());
            return false;
        } else {
            return true;
        }
    } else {
        debug_trace("Could not find prefix = %s for any length in trie, need to create\n", prefixStr.c_str());
        return false;
    }
}

bool HashStoreFileManager::createAndGetNewHashStoreFileHandlerByPrefixForUser(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber)
{
    uint64_t remainingAvaliableFileNumber = objectFileMetaDataTrie_.getRemainFileNumber();
    if (remainingAvaliableFileNumber == 0) {
        string targetPrefixStr;
        hashStoreFileMetaDataHandler* tempHandler1;
        hashStoreFileMetaDataHandler* tempHandler2;
        bool selecteFileForMergeStatus = selectFileForMerge(0, tempHandler1, tempHandler2, targetPrefixStr);
        if (selecteFileForMergeStatus == false) {
            debug_error("[ERROR] Could not select file for merge to reclaim space to create new file%s\n", "");
            return false;
        } else {
            bool performTwoFileMergeStatus = twoAdjacentFileMerge(tempHandler1, tempHandler2, targetPrefixStr);
            if (performTwoFileMergeStatus == false) {
                debug_error("[ERROR] Could perform merge to reclaim space to create new file based on selected file ID 1 = %lu, and ID 2 = %lu\n", tempHandler1->target_file_id_, tempHandler2->target_file_id_);
                return false;
            }
        }
        debug_info("Perform bucket merge before create new file success, target prefix = %s\n", targetPrefixStr.c_str());
    }
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
    currentFileHandlerPtr->current_prefix_used_bit_ = prefixBitNumber;
    currentFileHandlerPtr->target_file_id_ = generateNewFileID();
    currentFileHandlerPtr->file_ownership_flag_ = 0;
    currentFileHandlerPtr->gc_result_status_flag_ = kNew;
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_on_disk_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    currentFileHandlerPtr->previous_file_id_first_ = 0xffffffffffffffff;
    currentFileHandlerPtr->file_create_reason_ = kNewFile;
    // move pointer for return
    uint64_t finalInsertLevel = objectFileMetaDataTrie_.insert(prefixStr, currentFileHandlerPtr);
    if (finalInsertLevel == 0) {
        debug_error("[ERROR] Error insert to prefix tree, prefix length used = %lu, inserted file ID = %lu\n", currentFileHandlerPtr->current_prefix_used_bit_, currentFileHandlerPtr->target_file_id_);
        fileHandlerPtr = nullptr;
        return false;
    } else {
        if (currentFileHandlerPtr->current_prefix_used_bit_ != finalInsertLevel) {
            debug_info("After insert to prefix tree, get handler at level = %lu, but prefix length used = %lu, inserted file ID = %lu, update the current bit number used in the file handler\n", finalInsertLevel, currentFileHandlerPtr->current_prefix_used_bit_, currentFileHandlerPtr->target_file_id_);
            currentFileHandlerPtr->current_prefix_used_bit_ = finalInsertLevel;
            fileHandlerPtr = currentFileHandlerPtr;
            return true;
        } else {
            fileHandlerPtr = currentFileHandlerPtr;
            return true;
        }
    }
}

bool HashStoreFileManager::createHashStoreFileHandlerByPrefixStrForGC(string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID1, uint64_t previousFileID2, hashStoreFileHeader& newFileHeader)
{
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
    currentFileHandlerPtr->current_prefix_used_bit_ = targetPrefixLen;
    currentFileHandlerPtr->target_file_id_ = generateNewFileID();
    currentFileHandlerPtr->file_ownership_flag_ = -1;
    currentFileHandlerPtr->gc_result_status_flag_ = kNew;
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_on_disk_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    currentFileHandlerPtr->previous_file_id_first_ = previousFileID1;
    currentFileHandlerPtr->previous_file_id_second_ = previousFileID2;
    currentFileHandlerPtr->file_create_reason_ = kInternalGCFile;
    // set up new file header for write
    newFileHeader.current_prefix_used_bit_ = targetPrefixLen;
    newFileHeader.previous_file_id_first_ = previousFileID1;
    newFileHeader.previous_file_id_second_ = previousFileID2;
    newFileHeader.file_create_reason_ = kInternalGCFile;
    newFileHeader.file_id_ = currentFileHandlerPtr->target_file_id_;
    // write header to current file
    string targetFilePathStr = workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta";
    bool createAndOpenNewFileStatus = currentFileHandlerPtr->file_operation_func_ptr_->createThenOpenFile(targetFilePathStr);
    if (createAndOpenNewFileStatus == true) {
        // move pointer for return
        debug_info("Newly created file ID = %lu, target prefix bit number = %lu, prefix = %s, corresponding previous file ID = %lu and %lu\n", currentFileHandlerPtr->target_file_id_, targetPrefixLen, prefixStr.c_str(), previousFileID1, previousFileID2);
        fileHandlerPtr = currentFileHandlerPtr;
        return true;
    } else {
        debug_error("[ERROR] Could not create file ID = %lu, target prefix bit number = %lu, prefix = %s, corresponding previous file ID = %lu and %lu\n", currentFileHandlerPtr->target_file_id_, targetPrefixLen, prefixStr.c_str(), previousFileID1, previousFileID2);
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

pair<uint64_t, uint64_t> HashStoreFileManager::deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, uint32_t>& savedAnchors, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& resultMap)
{
    uint64_t processedKeepObjectNumber = 0;
    uint64_t processedTotalObjectNumber = 0;

    uint64_t currentProcessLocationIndex = 0;
    uint64_t anchors = 0;
    // skip file header
    hashStoreFileHeader currentFileHeader;
    memcpy(&currentFileHeader, fileContentBuffer, sizeof(currentFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);
    while (currentProcessLocationIndex != fileSize) {
        processedKeepObjectNumber++;
        processedTotalObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            anchors++;
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                processedKeepObjectNumber -= resultMap.at(currentKeyStr).first.size();
                resultMap.at(currentKeyStr).first.clear();
                resultMap.at(currentKeyStr).second.clear();
                resultMap.erase(currentKeyStr);
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            } else {
                processedKeepObjectNumber--;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                continue;
            }
        } else if (currentObjectRecordHeader.is_gc_done_ == true) {
            processedKeepObjectNumber--;
            continue;
        } else {
            string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).first.push_back(currentValueStr);
                resultMap.at(currentKeyStr).second.push_back(currentObjectRecordHeader);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                vector<hashStoreRecordHeader> newRecordRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                newRecordRelatedToCurrentKeyVec.push_back(currentObjectRecordHeader);
                resultMap.insert(make_pair(currentKeyStr, make_pair(newValuesRelatedToCurrentKeyVec, newRecordRelatedToCurrentKeyVec)));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }

    for (auto& keyIt : savedAnchors) {
        anchors++;
        if (resultMap.find(keyIt.first) != resultMap.end()) {
            processedKeepObjectNumber -= (resultMap.at(keyIt.first).first.size() + 1);
            resultMap.at(keyIt.first).first.clear();
            resultMap.at(keyIt.first).second.clear();
            resultMap.erase(keyIt.first);
        }
    }

    debug_info("deconstruct current file header done, file ID = %lu, create reason = %u, prefix length used in this file = %lu, target process file size = %lu, find different key number = %lu, total processed object number = %lu, total anchors = %lu with saved anchors %lu, target keep object number = %lu\n", currentFileHeader.file_id_, currentFileHeader.file_create_reason_, currentFileHeader.current_prefix_used_bit_, fileSize, resultMap.size(), processedTotalObjectNumber, anchors, savedAnchors.size(), processedKeepObjectNumber);
    return make_pair(processedKeepObjectNumber, processedTotalObjectNumber);
}

bool HashStoreFileManager::singleFileRewrite(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& gcResultMap, uint64_t targetFileSize, bool fileContainsReWriteKeysFlag)
{
    // reclaimed space success, rewrite current file to new file
    debug_trace("Before rewrite size = %lu, rewrite processed size = %lu\n", currentHandlerPtr->total_on_disk_bytes_, targetFileSize);
    char currentWriteBuffer[targetFileSize];
    uint64_t newObjectNumber = 0;
    uint64_t currentProcessLocationIndex = 0;
    hashStoreFileHeader currentFileHeader;
    currentFileHeader.current_prefix_used_bit_ = currentHandlerPtr->current_prefix_used_bit_;
    if (fileContainsReWriteKeysFlag == true) {
        currentFileHeader.file_create_reason_ = kRewritedObjectFile;
    } else {
        currentFileHeader.file_create_reason_ = kInternalGCFile;
    }
    currentFileHeader.file_id_ = generateNewFileID();
    currentFileHeader.previous_file_id_first_ = currentHandlerPtr->target_file_id_;
    memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentFileHeader, sizeof(hashStoreFileHeader));
    currentProcessLocationIndex += sizeof(currentFileHeader);
    // add file header
    for (auto keyIt : gcResultMap) {
        for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < keyIt.second.first.size(); valueAndRecordHeaderIt++) {
            newObjectNumber++;
            memcpy(currentWriteBuffer + currentProcessLocationIndex, &keyIt.second.second[valueAndRecordHeaderIt], sizeof(hashStoreRecordHeader));
            currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
            memcpy(currentWriteBuffer + currentProcessLocationIndex, keyIt.first.c_str(), keyIt.first.size());
            currentProcessLocationIndex += keyIt.first.size();
            memcpy(currentWriteBuffer + currentProcessLocationIndex, keyIt.second.first[valueAndRecordHeaderIt].c_str(), keyIt.second.first[valueAndRecordHeaderIt].size());
            currentProcessLocationIndex += keyIt.second.first[valueAndRecordHeaderIt].size();
        }
    }
    // add gc done flag into bucket file
    hashStoreRecordHeader currentGCJobDoneRecordHeader;
    currentGCJobDoneRecordHeader.is_anchor_ = false;
    currentGCJobDoneRecordHeader.is_gc_done_ = true;
    currentGCJobDoneRecordHeader.sequence_number_ = 0;
    currentGCJobDoneRecordHeader.key_size_ = 0;
    currentGCJobDoneRecordHeader.value_size_ = 0;
    memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentGCJobDoneRecordHeader, sizeof(hashStoreRecordHeader));
    debug_trace("Rewrite done buffer size = %lu, total target write size = %lu\n", currentProcessLocationIndex, targetFileSize);
    string targetOpenFileName = workingDir_ + "/" + to_string(currentFileHeader.file_id_) + ".delta";
    // create since file not exist
    if (currentHandlerPtr->file_operation_func_ptr_->isFileOpen() == true) {
        currentHandlerPtr->file_operation_func_ptr_->closeFile();
    } // close old file
    currentHandlerPtr->file_operation_func_ptr_->createFile(targetOpenFileName);
    if (currentHandlerPtr->file_operation_func_ptr_->isFileOpen() == true) {
        currentHandlerPtr->file_operation_func_ptr_->closeFile();
    }
    // write content and update current file stream to new one.
    currentHandlerPtr->file_operation_func_ptr_->openFile(targetOpenFileName);
    uint64_t onDiskWriteSize;
    STAT_PROCESS(onDiskWriteSize = currentHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetFileSize), StatsType::DELTAKV_GC_WRITE);
    StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSize);

    currentHandlerPtr->file_operation_func_ptr_->flushFile();
    debug_trace("Rewrite done file size = %lu, file path = %s\n", currentHandlerPtr->file_operation_func_ptr_->getFileSize(), targetOpenFileName.c_str());
    // update metadata
    currentHandlerPtr->target_file_id_ = currentFileHeader.file_id_;
    currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
    currentHandlerPtr->total_object_count_ = newObjectNumber + 1;
    currentHandlerPtr->total_object_bytes_ = currentProcessLocationIndex + sizeof(hashStoreRecordHeader);
    currentHandlerPtr->total_on_disk_bytes_ = onDiskWriteSize;
    debug_trace("Rewrite file size in metadata = %lu, file ID = %lu\n", currentHandlerPtr->total_object_bytes_, currentHandlerPtr->target_file_id_);
    // remove old file
    fileDeleteVecMtx_.lock();
    targetDeleteFileHandlerVec_.push_back(currentFileHeader.previous_file_id_first_);
    fileDeleteVecMtx_.unlock();
    // check if after rewrite, file size still exceed threshold, mark as no GC.
    if (currentHandlerPtr->total_on_disk_bytes_ > singleFileGCTriggerSize_) {
        currentHandlerPtr->gc_result_status_flag_ = kNoGC;
    }
    debug_info("flushed new file to filesystem since single file gc, the new file ID = %lu, corresponding previous file ID = %lu, target file size = %lu\n", currentFileHeader.file_id_, currentFileHeader.previous_file_id_first_, targetFileSize);
    return true;
}

bool HashStoreFileManager::singleFileSplit(hashStoreFileMetaDataHandler* currentHandlerPtr, unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>>& gcResultMap, uint64_t targetPrefixBitNumber, bool fileContainsReWriteKeysFlag)
{
    bool isSplitDoneFlag = true;
    string previousPrefixStr;
    generateHashBasedPrefix(gcResultMap.begin()->first, previousPrefixStr);
    previousPrefixStr = previousPrefixStr.substr(0, targetPrefixBitNumber - 1);
    unordered_map<string, pair<unordered_map<string, uint64_t>, uint64_t>> tempPrefixToKeysVecAndTotalSizeMap;
    for (auto keyIt : gcResultMap) {
        string prefixTempStr;
        generateHashBasedPrefix(keyIt.first, prefixTempStr);
        prefixTempStr = prefixTempStr.substr(0, targetPrefixBitNumber);
        if (tempPrefixToKeysVecAndTotalSizeMap.find(prefixTempStr) != tempPrefixToKeysVecAndTotalSizeMap.end()) {
            // current prefix exist, update
            uint64_t currentKeyTargetAllocateSpace = 0;
            for (auto valueIt : keyIt.second.first) {
                currentKeyTargetAllocateSpace += (keyIt.first.size() + valueIt.size() + sizeof(hashStoreRecordHeader));
            }
            tempPrefixToKeysVecAndTotalSizeMap.at(prefixTempStr).first.insert(make_pair(keyIt.first, currentKeyTargetAllocateSpace));
            tempPrefixToKeysVecAndTotalSizeMap.at(prefixTempStr).second += currentKeyTargetAllocateSpace;
        } else {
            uint64_t currentKeyTargetAllocateSpace = 0;
            for (auto valueIt : keyIt.second.first) {
                currentKeyTargetAllocateSpace += (keyIt.first.size() + valueIt.size() + sizeof(hashStoreRecordHeader));
            }
            pair<unordered_map<string, uint64_t>, uint64_t> tempNewKeyMap;
            tempNewKeyMap.first.insert(make_pair(keyIt.first, currentKeyTargetAllocateSpace));
            tempNewKeyMap.second += currentKeyTargetAllocateSpace;
            tempPrefixToKeysVecAndTotalSizeMap.insert(make_pair(prefixTempStr, tempNewKeyMap));
        }
    }
    if (tempPrefixToKeysVecAndTotalSizeMap.size() > 2) {
        debug_error("[ERROR] Need to generate more than 2 files during split GC, current target file number = %lu\n", tempPrefixToKeysVecAndTotalSizeMap.size());
        return false;
    } else {
        debug_info("Generate new files since split GC, target file number = %lu\n", tempPrefixToKeysVecAndTotalSizeMap.size());
        for (auto prefixIt : tempPrefixToKeysVecAndTotalSizeMap) {
            debug_info("During split GC, target prefix = %s, prefix bit number = %lu\n", prefixIt.first.c_str(), prefixIt.first.size());
        }
    }
    vector<pair<string, hashStoreFileMetaDataHandler*>> needUpdateMetaDataHandlers;
    for (auto prefixIt : tempPrefixToKeysVecAndTotalSizeMap) {
        char currentWriteBuffer[prefixIt.second.second + sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader)];
        hashStoreFileMetaDataHandler* currentFileHandlerPtr;
        hashStoreFileHeader newFileHeader;
        bool getFileHandlerStatus = createHashStoreFileHandlerByPrefixStrForGC(prefixIt.first, currentFileHandlerPtr, targetPrefixBitNumber, currentHandlerPtr->target_file_id_, 0, newFileHeader);
        if (getFileHandlerStatus == false) {
            debug_error("[ERROR] Failed to create hash store file handler by prefix string %s when split GC\n", prefixIt.first.c_str());
            return false;
        }
        debug_info("Generate new file since split GC, current prefix = %s, prefix bit number = %lu, target file ID = %lu\n", prefixIt.first.c_str(), prefixIt.first.size(), currentFileHandlerPtr->target_file_id_);
        memcpy(currentWriteBuffer, &newFileHeader, sizeof(hashStoreFileHeader));
        uint64_t currentWritePos = sizeof(hashStoreFileHeader);
        currentFileHandlerPtr->temp_not_flushed_data_bytes_ += sizeof(hashStoreFileHeader);
        currentFileHandlerPtr->total_object_bytes_ += sizeof(hashStoreFileHeader);
        for (auto keyToSizeIt : prefixIt.second.first) {
            uint64_t keySize = keyToSizeIt.first.size();
            for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < gcResultMap.at(keyToSizeIt.first).first.size(); valueAndRecordHeaderIt++) {
                memcpy(currentWriteBuffer + currentWritePos, &gcResultMap.at(keyToSizeIt.first).second[valueAndRecordHeaderIt], sizeof(hashStoreRecordHeader));
                currentWritePos += sizeof(hashStoreRecordHeader);
                memcpy(currentWriteBuffer + currentWritePos, keyToSizeIt.first.c_str(), keySize);
                currentWritePos += keySize;
                uint64_t valueSize = gcResultMap.at(keyToSizeIt.first).first[valueAndRecordHeaderIt].size();
                memcpy(currentWriteBuffer + currentWritePos, gcResultMap.at(keyToSizeIt.first).first[valueAndRecordHeaderIt].c_str(), valueSize);
                currentWritePos += valueSize;
                currentFileHandlerPtr->temp_not_flushed_data_bytes_ += (sizeof(hashStoreRecordHeader) + keySize + valueSize);
                currentFileHandlerPtr->total_object_bytes_ += (sizeof(hashStoreRecordHeader) + keySize + valueSize);
                currentFileHandlerPtr->total_object_count_++;
            }
        }
        hashStoreRecordHeader GCJobDoneRecord;
        GCJobDoneRecord.is_anchor_ = false;
        GCJobDoneRecord.is_gc_done_ = true;
        GCJobDoneRecord.sequence_number_ = 0;
        GCJobDoneRecord.key_size_ = 0;
        GCJobDoneRecord.value_size_ = 0;
        memcpy(currentWriteBuffer + currentWritePos, &GCJobDoneRecord, sizeof(hashStoreRecordHeader));
        currentWritePos += sizeof(hashStoreRecordHeader);
        // start write file
        uint64_t onDiskWriteSize;
        currentFileHandlerPtr->fileOperationMutex_.lock();
        STAT_PROCESS(onDiskWriteSize = currentFileHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, currentWritePos), StatsType::DELTAKV_GC_WRITE);
        StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSize);
        currentFileHandlerPtr->total_object_bytes_ += sizeof(hashStoreRecordHeader);
        currentFileHandlerPtr->total_on_disk_bytes_ += onDiskWriteSize;
        currentFileHandlerPtr->total_object_count_++;
        currentFileHandlerPtr->file_operation_func_ptr_->flushFile();
        debug_trace("Flushed new file to filesystem since split gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHandlerPtr->target_file_id_, currentHandlerPtr->target_file_id_);
        currentFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
        currentFileHandlerPtr->fileOperationMutex_.unlock();
        // update metadata
        needUpdateMetaDataHandlers.push_back(make_pair(prefixIt.first, currentFileHandlerPtr));
    }
    if (needUpdateMetaDataHandlers.size() == 1) {
        uint64_t insertAtLevel = objectFileMetaDataTrie_.insert(needUpdateMetaDataHandlers[0].first, needUpdateMetaDataHandlers[0].second);
        if (insertAtLevel == 0) {
            debug_error("[ERROR] Error insert to prefix tree, prefix length used = %lu, inserted file ID = %lu\n", needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_, needUpdateMetaDataHandlers[0].second->target_file_id_);
            needUpdateMetaDataHandlers[0].second->file_operation_func_ptr_->closeFile();
            deleteObslateFileWithFileIDAsInput(needUpdateMetaDataHandlers[0].second->target_file_id_);
            delete needUpdateMetaDataHandlers[0].second->file_operation_func_ptr_;
            delete needUpdateMetaDataHandlers[0].second;
            return false;
        } else {
            if (needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_ != insertAtLevel) {
                debug_info("After insert to prefix tree, get handler at level = %lu, but prefix length used = %lu, prefix = %s, inserted file ID = %lu, update the current bit number used in the file handler\n", insertAtLevel, needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_, needUpdateMetaDataHandlers[0].first.c_str(), needUpdateMetaDataHandlers[0].second->target_file_id_);
                needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_ = insertAtLevel;
            }
            needUpdateMetaDataHandlers[0].second->file_ownership_flag_ = 0;
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(currentHandlerPtr->target_file_id_);
            fileDeleteVecMtx_.unlock();
            currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
            debug_info("Split file ID = %lu for gc success, mark as should delete done\n", currentHandlerPtr->target_file_id_);
            return true;
        }
    } else if (needUpdateMetaDataHandlers.size() == 2) {
        pair<uint64_t, uint64_t> insertPrefixTreeStatus = objectFileMetaDataTrie_.insertPairOfNodes(needUpdateMetaDataHandlers[0].first, needUpdateMetaDataHandlers[0].second, needUpdateMetaDataHandlers[1].first, needUpdateMetaDataHandlers[1].second);
        if (insertPrefixTreeStatus.first == 0 || insertPrefixTreeStatus.second == 0) {
            debug_error("[ERROR] Error insert to prefix tree: target prefix 1 = %s, insert at level = %lu, file ID = %lu; target prefix 2 = %s, insert at level = %lu, file ID = %lu\n", needUpdateMetaDataHandlers[0].first.c_str(), insertPrefixTreeStatus.first, needUpdateMetaDataHandlers[0].second->target_file_id_, needUpdateMetaDataHandlers[1].first.c_str(), insertPrefixTreeStatus.second, needUpdateMetaDataHandlers[1].second->target_file_id_);
            // clean up temporary info
            for (int i = 0; i < 2; i++) {
                needUpdateMetaDataHandlers[i].second->file_operation_func_ptr_->closeFile();
                deleteObslateFileWithFileIDAsInput(needUpdateMetaDataHandlers[i].second->target_file_id_);
                delete needUpdateMetaDataHandlers[i].second->file_operation_func_ptr_;
                delete needUpdateMetaDataHandlers[i].second;
            }
            return false;
        } else {
            if (needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_ != insertPrefixTreeStatus.first) {
                debug_info("After insert to prefix tree, get handler at level = %lu, but prefix length used = %lu, prefix = %s, inserted file ID = %lu, update the current bit number used in the file handler\n", insertPrefixTreeStatus.first, needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_, needUpdateMetaDataHandlers[0].first.c_str(), needUpdateMetaDataHandlers[0].second->target_file_id_);
                needUpdateMetaDataHandlers[0].second->current_prefix_used_bit_ = insertPrefixTreeStatus.first;
            }
            needUpdateMetaDataHandlers[0].second->file_ownership_flag_ = 0;
            if (needUpdateMetaDataHandlers[1].second->current_prefix_used_bit_ != insertPrefixTreeStatus.second) {
                debug_info("After insert to prefix tree, get handler at level = %lu, but prefix length used = %lu, prefix = %s, inserted file ID = %lu, update the current bit number used in the file handler\n", insertPrefixTreeStatus.second, needUpdateMetaDataHandlers[1].second->current_prefix_used_bit_, needUpdateMetaDataHandlers[1].first.c_str(), needUpdateMetaDataHandlers[1].second->target_file_id_);
                needUpdateMetaDataHandlers[1].second->current_prefix_used_bit_ = insertPrefixTreeStatus.second;
            }
            needUpdateMetaDataHandlers[1].second->file_ownership_flag_ = 0;
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(currentHandlerPtr->target_file_id_);
            fileDeleteVecMtx_.unlock();
            currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
            debug_info("Split file ID = %lu for gc success, mark as should delete done\n", currentHandlerPtr->target_file_id_);
            return true;
        }
    } else {
        currentHandlerPtr->gc_result_status_flag_ = kMayGC;
        debug_error("[ERROR] Split file ID = %lu for gc error, generate too many files, the file numebr = %lu\n", currentHandlerPtr->target_file_id_, needUpdateMetaDataHandlers.size());
        return false;
    }
}

bool HashStoreFileManager::twoAdjacentFileMerge(hashStoreFileMetaDataHandler* currentHandlerPtr1, hashStoreFileMetaDataHandler* currentHandlerPtr2, string targetPrefixStr)
{
    std::scoped_lock<std::shared_mutex> w_lock1(currentHandlerPtr1->fileOperationMutex_);
    std::scoped_lock<std::shared_mutex> w_lock2(currentHandlerPtr2->fileOperationMutex_);
    debug_info("Perform merge GC for file ID 1 = %lu, ID 2 = %lu\n", currentHandlerPtr1->target_file_id_, currentHandlerPtr2->target_file_id_);
    hashStoreFileMetaDataHandler* mergedFileHandler;
    hashStoreFileHeader newFileHeaderForMergedFile;
    bool generateFileHandlerStatus = createHashStoreFileHandlerByPrefixStrForGC(targetPrefixStr, mergedFileHandler, targetPrefixStr.size(), currentHandlerPtr1->target_file_id_, currentHandlerPtr2->target_file_id_, newFileHeaderForMergedFile);
    if (generateFileHandlerStatus == false) {
        debug_error("[ERROR] Could not generate new file handler for merge GC,previous file ID 1 = %lu, ID 2 = %lu\n", currentHandlerPtr1->target_file_id_, currentHandlerPtr2->target_file_id_);
        currentHandlerPtr1->file_ownership_flag_ = 0;
        currentHandlerPtr2->file_ownership_flag_ = 0;
        return false;
    }
    std::scoped_lock<std::shared_mutex> w_lock3(mergedFileHandler->fileOperationMutex_);
    // process file 1
    char readWriteBuffer1[currentHandlerPtr1->total_object_bytes_];
    currentHandlerPtr1->file_operation_func_ptr_->flushFile();
    currentHandlerPtr1->temp_not_flushed_data_bytes_ = 0;
    STAT_PROCESS(currentHandlerPtr1->file_operation_func_ptr_->readFile(readWriteBuffer1, currentHandlerPtr1->total_object_bytes_), StatsType::DELTAKV_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(currentHandlerPtr1->total_on_disk_bytes_);
    // process GC contents
    unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>> gcResultMap1;
    pair<uint64_t, uint64_t> remainObjectNumberPair1 = deconstructAndGetValidContentsFromFile(readWriteBuffer1, currentHandlerPtr1->total_object_bytes_, currentHandlerPtr1->bufferedUnFlushedAnchorsVec_, gcResultMap1);
    debug_info("Merge GC read file ID 1 = %lu done, valid object number = %lu, total object number = %lu\n", currentHandlerPtr1->target_file_id_, remainObjectNumberPair1.first, remainObjectNumberPair1.second);

    // process file2
    char readWriteBuffer2[currentHandlerPtr2->total_object_bytes_];
    currentHandlerPtr2->file_operation_func_ptr_->flushFile();
    currentHandlerPtr2->temp_not_flushed_data_bytes_ = 0;
    STAT_PROCESS(currentHandlerPtr2->file_operation_func_ptr_->readFile(readWriteBuffer2, currentHandlerPtr2->total_object_bytes_), StatsType::DELTAKV_GC_READ);
    StatsRecorder::getInstance()->DeltaGcBytesRead(currentHandlerPtr2->total_on_disk_bytes_);
    // process GC contents
    unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>> gcResultMap2;
    pair<uint64_t, uint64_t> remainObjectNumberPair2 = deconstructAndGetValidContentsFromFile(readWriteBuffer2, currentHandlerPtr2->total_object_bytes_, currentHandlerPtr2->bufferedUnFlushedAnchorsVec_, gcResultMap2);
    debug_info("Merge GC read file ID 2 = %lu done, valid object number = %lu, total object number = %lu\n", currentHandlerPtr2->target_file_id_, remainObjectNumberPair2.first, remainObjectNumberPair2.second);

    uint64_t targetWriteSize = 0;
    for (auto& keyIt : gcResultMap1) {
        for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < keyIt.second.first.size(); valueAndRecordHeaderIt++) {
            targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + keyIt.second.first[valueAndRecordHeaderIt].size());
        }
    }
    for (auto& keyIt : gcResultMap2) {
        for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < keyIt.second.first.size(); valueAndRecordHeaderIt++) {
            targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + keyIt.second.first[valueAndRecordHeaderIt].size());
        }
    }
    targetWriteSize += (sizeof(hashStoreRecordHeader) + sizeof(hashStoreFileHeader));
    debug_info("Merge GC target write file size = %lu\n", targetWriteSize);
    char currentWriteBuffer[targetWriteSize];
    memcpy(currentWriteBuffer, &newFileHeaderForMergedFile, sizeof(hashStoreFileHeader));
    mergedFileHandler->temp_not_flushed_data_bytes_ += sizeof(hashStoreFileHeader);
    mergedFileHandler->total_object_bytes_ += sizeof(hashStoreFileHeader);
    uint64_t currentWritePos = sizeof(hashStoreFileHeader);
    for (auto& keyIt : gcResultMap1) {
        for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < keyIt.second.first.size(); valueAndRecordHeaderIt++) {
            memcpy(currentWriteBuffer + currentWritePos, &keyIt.second.second[valueAndRecordHeaderIt], sizeof(hashStoreRecordHeader));
            currentWritePos += sizeof(hashStoreRecordHeader);
            memcpy(currentWriteBuffer + currentWritePos, keyIt.first.c_str(), keyIt.first.size());
            currentWritePos += keyIt.first.size();
            memcpy(currentWriteBuffer + currentWritePos, keyIt.second.first[valueAndRecordHeaderIt].c_str(), keyIt.second.first[valueAndRecordHeaderIt].size());
            currentWritePos += keyIt.second.first[valueAndRecordHeaderIt].size();
            mergedFileHandler->temp_not_flushed_data_bytes_ += (sizeof(hashStoreRecordHeader) + keyIt.second.second[valueAndRecordHeaderIt].key_size_ + keyIt.second.second[valueAndRecordHeaderIt].value_size_);
            mergedFileHandler->total_object_bytes_ += (sizeof(hashStoreRecordHeader) + keyIt.second.second[valueAndRecordHeaderIt].key_size_ + keyIt.second.second[valueAndRecordHeaderIt].value_size_);
            mergedFileHandler->total_object_count_++;
        }
    }
    for (auto& keyIt : gcResultMap2) {
        for (auto valueAndRecordHeaderIt = 0; valueAndRecordHeaderIt < keyIt.second.first.size(); valueAndRecordHeaderIt++) {
            memcpy(currentWriteBuffer + currentWritePos, &keyIt.second.second[valueAndRecordHeaderIt], sizeof(hashStoreRecordHeader));
            currentWritePos += sizeof(hashStoreRecordHeader);
            memcpy(currentWriteBuffer + currentWritePos, keyIt.first.c_str(), keyIt.first.size());
            currentWritePos += keyIt.first.size();
            memcpy(currentWriteBuffer + currentWritePos, keyIt.second.first[valueAndRecordHeaderIt].c_str(), keyIt.second.first[valueAndRecordHeaderIt].size());
            currentWritePos += keyIt.second.first[valueAndRecordHeaderIt].size();
            mergedFileHandler->temp_not_flushed_data_bytes_ += (sizeof(hashStoreRecordHeader) + keyIt.second.second[valueAndRecordHeaderIt].key_size_ + keyIt.second.second[valueAndRecordHeaderIt].value_size_);
            mergedFileHandler->total_object_bytes_ += (sizeof(hashStoreRecordHeader) + keyIt.second.second[valueAndRecordHeaderIt].key_size_ + keyIt.second.second[valueAndRecordHeaderIt].value_size_);
            mergedFileHandler->total_object_count_++;
        }
    }
    debug_info("Merge GC processed write file size = %lu\n", currentWritePos);
    // write gc done flag into bucket file
    hashStoreRecordHeader currentObjectRecordHeader;
    currentObjectRecordHeader.is_anchor_ = false;
    currentObjectRecordHeader.is_gc_done_ = true;
    currentObjectRecordHeader.sequence_number_ = 0;
    currentObjectRecordHeader.key_size_ = 0;
    currentObjectRecordHeader.value_size_ = 0;
    memcpy(currentWriteBuffer + currentWritePos, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
    debug_info("Merge GC processed total write file size = %lu\n", currentWritePos + sizeof(hashStoreRecordHeader));
    uint64_t onDiskWriteSize;
    STAT_PROCESS(onDiskWriteSize = mergedFileHandler->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetWriteSize), StatsType::DELTAKV_GC_WRITE);
    StatsRecorder::getInstance()->DeltaGcBytesWrite(onDiskWriteSize);
    debug_info("Merge GC write file size = %lu done\n", targetWriteSize);
    mergedFileHandler->total_object_bytes_ += sizeof(hashStoreRecordHeader);
    mergedFileHandler->total_on_disk_bytes_ += onDiskWriteSize;
    mergedFileHandler->total_object_count_++;
    mergedFileHandler->file_operation_func_ptr_->flushFile();
    debug_info("Flushed new file to filesystem since merge gc, the new file ID = %lu, corresponding previous file ID 1 = %lu, ID 2 = %lu\n", mergedFileHandler->target_file_id_, currentHandlerPtr1->target_file_id_, currentHandlerPtr2->target_file_id_);
    mergedFileHandler->temp_not_flushed_data_bytes_ = 0;

    // update metadata
    uint64_t newLeafNodeBitNumber = 0;
    bool mergeNodeStatus = objectFileMetaDataTrie_.mergeNodesToNewLeafNode(targetPrefixStr, newLeafNodeBitNumber);
    if (mergeNodeStatus == false) {
        debug_error("[ERROR] Could not merge two existing node corresponding file ID 1 = %lu, ID 2 = %lu\n", currentHandlerPtr1->target_file_id_, currentHandlerPtr2->target_file_id_);
        if (mergedFileHandler->file_operation_func_ptr_->isFileOpen() == true) {
            mergedFileHandler->file_operation_func_ptr_->closeFile();
        }
        deleteObslateFileWithFileIDAsInput(mergedFileHandler->target_file_id_);
        currentHandlerPtr1->file_ownership_flag_ = 0;
        currentHandlerPtr2->file_ownership_flag_ = 0;
        delete mergedFileHandler->file_operation_func_ptr_;
        delete mergedFileHandler;
        return false;
    } else {
        hashStoreFileMetaDataHandler* tempHandler = nullptr;
        bool getPreviousHandlerStatus = objectFileMetaDataTrie_.get(targetPrefixStr, tempHandler);
        if (tempHandler != nullptr) {
            // delete old handler;
            debug_info("Find exist data handler = %p\n", tempHandler);
            if (tempHandler->file_operation_func_ptr_ != nullptr) {
                if (tempHandler->file_operation_func_ptr_->isFileOpen() == true) {
                    tempHandler->file_operation_func_ptr_->closeFile();
                }
                deleteObslateFileWithFileIDAsInput(tempHandler->target_file_id_);
                delete tempHandler->file_operation_func_ptr_;
            }
            delete tempHandler;
        }
        debug_info("Start update metadata for merged file ID = %lu\n", mergedFileHandler->target_file_id_);
        bool updateFileHandlerToNewLeafNodeStatus = objectFileMetaDataTrie_.updateDataObjectForTargetLeafNode(targetPrefixStr, newLeafNodeBitNumber, mergedFileHandler);
        if (updateFileHandlerToNewLeafNodeStatus == true) {
            fileDeleteVecMtx_.lock();
            targetDeleteFileHandlerVec_.push_back(currentHandlerPtr1->target_file_id_);
            targetDeleteFileHandlerVec_.push_back(currentHandlerPtr2->target_file_id_);
            fileDeleteVecMtx_.unlock();
            currentHandlerPtr1->gc_result_status_flag_ = kShouldDelete;
            currentHandlerPtr2->gc_result_status_flag_ = kShouldDelete;
            currentHandlerPtr1->file_ownership_flag_ = 0;
            currentHandlerPtr2->file_ownership_flag_ = 0;
            if (currentHandlerPtr1->file_operation_func_ptr_->isFileOpen() == true) {
                currentHandlerPtr1->file_operation_func_ptr_->closeFile();
            }
            deleteObslateFileWithFileIDAsInput(currentHandlerPtr1->target_file_id_);
            delete currentHandlerPtr1->file_operation_func_ptr_;
            delete currentHandlerPtr1;
            if (currentHandlerPtr2->file_operation_func_ptr_->isFileOpen() == true) {
                currentHandlerPtr2->file_operation_func_ptr_->closeFile();
            }
            deleteObslateFileWithFileIDAsInput(currentHandlerPtr2->target_file_id_);
            delete currentHandlerPtr2->file_operation_func_ptr_;
            delete currentHandlerPtr2;
            mergedFileHandler->file_ownership_flag_ = 0;
            return true;
        } else {
            debug_error("[ERROR] Could update metadata for file ID = %lu\n", mergedFileHandler->target_file_id_);
            if (mergedFileHandler->file_operation_func_ptr_->isFileOpen() == true) {
                mergedFileHandler->file_operation_func_ptr_->closeFile();
            }
            deleteObslateFileWithFileIDAsInput(mergedFileHandler->target_file_id_);
            delete mergedFileHandler->file_operation_func_ptr_;
            delete mergedFileHandler;
            currentHandlerPtr1->file_ownership_flag_ = 0;
            currentHandlerPtr2->file_ownership_flag_ = 0;
            return false;
        }
    }
    return true;
}

bool HashStoreFileManager::selectFileForMerge(uint64_t targetFileIDForSplit, hashStoreFileMetaDataHandler*& currentHandlerPtr1, hashStoreFileMetaDataHandler*& currentHandlerPtr2, string& targetPrefixStr)
{
    vector<pair<string, hashStoreFileMetaDataHandler*>> validNodes;
    bool getValidNodesStatus = objectFileMetaDataTrie_.getCurrentValidNodes(validNodes);
    if (getValidNodesStatus == false) {
        debug_error("[ERROR] Could not get valid tree nodes from prefixTree, current validNodes vector size = %lu\n", validNodes.size());
        return false;
    } else {
        debug_trace("Current validNodes vector size = %lu\n", validNodes.size());
        unordered_map<string, hashStoreFileMetaDataHandler*> targetFileForMergeMap;
        for (auto nodeIt : validNodes) {
            if (nodeIt.second->target_file_id_ == targetFileIDForSplit) {
                debug_trace("Skip file ID = %lu, prefix bit number = %lu, size = %lu, which is currently during GC\n", nodeIt.second->target_file_id_, nodeIt.second->current_prefix_used_bit_, nodeIt.second->total_object_bytes_);
                continue;
            }
            if (nodeIt.second->total_object_bytes_ <= singleFileMergeGCUpperBoundSize_) {
                targetFileForMergeMap.insert(nodeIt);
                debug_trace("Select file ID = %lu, prefix bit number = %lu, size = %lu, which should not exceed threshould = %lu\n", nodeIt.second->target_file_id_, nodeIt.second->current_prefix_used_bit_, nodeIt.second->total_object_bytes_, singleFileMergeGCUpperBoundSize_);
            } else {
                debug_trace("Skip file ID = %lu, prefix bit number = %lu, size = %lu, which may exceed threshould = %lu\n", nodeIt.second->target_file_id_, nodeIt.second->current_prefix_used_bit_, nodeIt.second->total_object_bytes_, singleFileMergeGCUpperBoundSize_);
            }
        }
        debug_info("Selected from file number = %lu for merge GC\n", targetFileForMergeMap.size());
        if (targetFileForMergeMap.size() != 0) {
            for (auto mapIt : targetFileForMergeMap) {
                string tempPrefixToFindNodeAtSameLevelStr = mapIt.first;
                string tempPrefixToFindAnotherNodeAtSameLevelStr;
                if (tempPrefixToFindNodeAtSameLevelStr[tempPrefixToFindNodeAtSameLevelStr.size() - 1] == '1') {
                    tempPrefixToFindAnotherNodeAtSameLevelStr.assign(tempPrefixToFindNodeAtSameLevelStr.substr(0, tempPrefixToFindNodeAtSameLevelStr.size() - 1));
                    tempPrefixToFindAnotherNodeAtSameLevelStr.append("0");
                } else {
                    tempPrefixToFindAnotherNodeAtSameLevelStr.assign(tempPrefixToFindNodeAtSameLevelStr.substr(0, tempPrefixToFindNodeAtSameLevelStr.size() - 1));
                    tempPrefixToFindAnotherNodeAtSameLevelStr.append("1");
                }
                debug_info("Search original prefix = %s, target pair prefix = %s\n", tempPrefixToFindNodeAtSameLevelStr.c_str(), tempPrefixToFindAnotherNodeAtSameLevelStr.c_str());
                hashStoreFileMetaDataHandler* tempHandler;
                if (objectFileMetaDataTrie_.get(tempPrefixToFindAnotherNodeAtSameLevelStr, tempHandler) == true) {
                    if (tempHandler->target_file_id_ == targetFileIDForSplit) {
                        debug_trace("Skip file ID = %lu, prefix bit number = %lu, size = %lu, which is currently during GC\n", tempHandler->target_file_id_, tempHandler->current_prefix_used_bit_, tempHandler->total_object_bytes_);
                        continue;
                    }
                    if (tempHandler->total_object_bytes_ < singleFileGCTriggerSize_) {
                        if (mapIt.second->file_ownership_flag_ != 0) {
                            debug_trace("Waiting for file ownership for select file ID = %lu\n", mapIt.second->target_file_id_);
                            while (mapIt.second->file_ownership_flag_ != 0) {
                                asm volatile("");
                            }
                        }
                        mapIt.second->file_ownership_flag_ = -1;
                        targetPrefixStr = mapIt.first.substr(0, tempPrefixToFindNodeAtSameLevelStr.size() - 1);
                        currentHandlerPtr1 = mapIt.second;
                        if (tempHandler->file_ownership_flag_ != 0) {
                            debug_trace("Waiting for file ownership for select file ID = %lu\n", tempHandler->target_file_id_);
                            while (tempHandler->file_ownership_flag_ != 0) {
                                asm volatile("");
                            }
                        }
                        tempHandler->file_ownership_flag_ = -1;
                        currentHandlerPtr2 = tempHandler;
                        debug_info("Find two file for merge GC success, fileHandler 1 ptr = %p, fileHandler 2 ptr = %p, target prefix = %s\n", currentHandlerPtr1, currentHandlerPtr2, targetPrefixStr.c_str());
                        return true;
                    }
                } else {
                    debug_info("Could not find adjacent node for current node, skip this node, current node prefix = %s, target node prefix = %s\n", tempPrefixToFindNodeAtSameLevelStr.c_str(), tempPrefixToFindAnotherNodeAtSameLevelStr.c_str());
                    continue;
                }
            }
            debug_info("Could not get could merge tree nodes from prefixTree, current targetFileForMergeMap size = %lu\n", targetFileForMergeMap.size());
            return false;
        } else {
            debug_info("Could not get could merge tree nodes from prefixTree, current targetFileForMergeMap size = %lu\n", targetFileForMergeMap.size());
            return false;
        }
    }
}

// threads workers
void HashStoreFileManager::processGCRequestWorker()
{
    while (true) {
        if (notifyGCMQ_->done_ == true && notifyGCMQ_->isEmpty() == true) {
            break;
        }
        hashStoreFileMetaDataHandler* fileHandler;
        if (notifyGCMQ_->pop(fileHandler)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_info("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, wait for lock\n", fileHandler->target_file_id_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
            std::scoped_lock<std::shared_mutex> w_lock(fileHandler->fileOperationMutex_);
            debug_info("new file request for GC, file ID = %lu, existing size = %lu, total disk size = %lu, file gc status = %d, start process\n", fileHandler->target_file_id_, fileHandler->total_object_bytes_, fileHandler->total_on_disk_bytes_, fileHandler->gc_result_status_flag_);
            // read contents
            char readWriteBuffer[fileHandler->total_object_bytes_];
            fileHandler->file_operation_func_ptr_->flushFile();
            fileHandler->temp_not_flushed_data_bytes_ = 0;
            uint64_t currentObjectBytes = fileHandler->file_operation_func_ptr_->getFileSize();
            if (currentObjectBytes != fileHandler->total_object_bytes_) {
                debug_error("[ERROR] file size in metadata = %lu, in filesystem = %lu\n", fileHandler->total_object_bytes_, currentObjectBytes);
            }
            STAT_PROCESS(fileHandler->file_operation_func_ptr_->readFile(readWriteBuffer, fileHandler->total_object_bytes_), StatsType::DELTAKV_GC_READ);
            StatsRecorder::getInstance()->DeltaGcBytesRead(fileHandler->total_on_disk_bytes_);

            // process GC contents
            unordered_map<string, pair<vector<string>, vector<hashStoreRecordHeader>>> gcResultMap;
            pair<uint64_t, uint64_t> remainObjectNumberPair = deconstructAndGetValidContentsFromFile(readWriteBuffer, fileHandler->total_object_bytes_, fileHandler->bufferedUnFlushedAnchorsVec_, gcResultMap);

            bool fileContainsReWriteKeysFlag = false;
            // calculate target file size
            vector<writeBackObjectStruct*> targetWriteBackVec;
            uint64_t targetValidObjectSize = 0;
            for (auto keyIt : gcResultMap) {
                if (enableWriteBackDuringGCFlag_ == true) {
                    debug_info("key = %s has %lu deltas\n", keyIt.first.c_str(), keyIt.second.first.size());
                    if (keyIt.second.first.size() > gcWriteBackDeltaNum_ && gcWriteBackDeltaNum_ != 0) {
                        fileContainsReWriteKeysFlag = true;
                        writeBackObjectStruct* newWriteBackObject = new writeBackObjectStruct(keyIt.first, "", 0);
                        targetWriteBackVec.push_back(newWriteBackObject);
                    }
                }
                for (auto valueIt : keyIt.second.first) {
                    targetValidObjectSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                }
            }
            uint64_t targetFileSize = targetValidObjectSize + sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader);

            // count valid object size to determine GC method;
            if (remainObjectNumberPair.second == 0) {
                debug_error("[ERROR] File ID = %lu contains no object, should just delete, total contains object number = %lu, should keep object number = %lu\n", fileHandler->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                fileHandler->file_ownership_flag_ = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                for (auto writeBackIt : targetWriteBackVec) {
                    writeBackOperationsQueue_->push(writeBackIt);
                }
                continue;
            }

            if (remainObjectNumberPair.first > 0 && gcResultMap.size() == 0) {
                debug_error("[ERROR] File ID = %lu contains valid objects but result map is zero\n", fileHandler->target_file_id_);
                fileHandler->file_ownership_flag_ = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                for (auto writeBackIt : targetWriteBackVec) {
                    writeBackOperationsQueue_->push(writeBackIt);
                }
                continue;
            }

            if (remainObjectNumberPair.first == 0 && gcResultMap.size() == 0) {
                debug_info("File ID = %lu total disk size %lu have no valid objects\n", fileHandler->target_file_id_, fileHandler->total_on_disk_bytes_);
                singleFileRewrite(fileHandler, gcResultMap, targetFileSize, fileContainsReWriteKeysFlag);
                fileHandler->file_ownership_flag_ = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                for (auto writeBackIt : targetWriteBackVec) {
                    writeBackOperationsQueue_->push(writeBackIt);
                }
                continue;
            }

            if (remainObjectNumberPair.first > 0 && gcResultMap.size() == 1) {
                // No invalid objects, cannot save space
                if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
                    if (fileHandler->gc_result_status_flag_ == kNew) {
                        // keep tracking until forced gc threshold;
                        fileHandler->gc_result_status_flag_ = kMayGC;
                        fileHandler->file_ownership_flag_ = 0;
                        debug_info("File ID = %lu contains only %lu different keys, marked as kMayGC\n", fileHandler->target_file_id_, gcResultMap.size());
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        for (auto writeBackIt : targetWriteBackVec) {
                            writeBackOperationsQueue_->push(writeBackIt);
                        }
                        continue;
                    } else if (fileHandler->gc_result_status_flag_ == kMayGC) {
                        // Mark this file as could not GC;
                        fileHandler->gc_result_status_flag_ = kNoGC;
                        fileHandler->file_ownership_flag_ = 0;
                        debug_info("File ID = %lu contains only %lu different keys, marked as kNoGC\n", fileHandler->target_file_id_, gcResultMap.size());
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        for (auto writeBackIt : targetWriteBackVec) {
                            writeBackOperationsQueue_->push(writeBackIt);
                        }
                        continue;
                    }
                } else {
                    // single file rewrite
                    debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write\n", fileHandler->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                    singleFileRewrite(fileHandler, gcResultMap, targetFileSize, fileContainsReWriteKeysFlag);
                    fileHandler->file_ownership_flag_ = 0;
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                    for (auto writeBackIt : targetWriteBackVec) {
                        writeBackOperationsQueue_->push(writeBackIt);
                    }
                    continue;
                }
            }

            // perform split into two buckets via extend prefix bit (+1)
            if (targetFileSize <= singleFileSplitGCTriggerSize_) {
                debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write, target file size = %lu, split threshold = %lu\n", fileHandler->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first, targetFileSize, singleFileSplitGCTriggerSize_);
                singleFileRewrite(fileHandler, gcResultMap, targetFileSize, fileContainsReWriteKeysFlag);
                fileHandler->file_ownership_flag_ = 0;
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                for (auto writeBackIt : targetWriteBackVec) {
                    writeBackOperationsQueue_->push(writeBackIt);
                }
                continue;
            } else {
                debug_info("try split for key number = %lu\n", gcResultMap.size());
                uint64_t currentUsedPrefixBitNumber = fileHandler->current_prefix_used_bit_;
                uint64_t targetPrefixBitNumber = currentUsedPrefixBitNumber + 1;

                uint64_t remainEmptyFileNumber = objectFileMetaDataTrie_.getRemainFileNumber();
                if (remainEmptyFileNumber > 0) {
                    debug_info("Still not reach max file number, split directly, current remain empty file numebr = %lu\n", remainEmptyFileNumber);
                    debug_info("Perform split GC for file ID (without merge) = %lu\n", fileHandler->target_file_id_);
                    bool singleFileGCStatus = singleFileSplit(fileHandler, gcResultMap, targetPrefixBitNumber, fileContainsReWriteKeysFlag);
                    if (singleFileGCStatus == false) {
                        debug_error("[ERROR] Could not perform split GC for file ID = %lu\n", fileHandler->target_file_id_);
                        fileHandler->gc_result_status_flag_ = kNoGC;
                        fileHandler->file_ownership_flag_ = 0;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        for (auto writeBackIt : targetWriteBackVec) {
                            writeBackOperationsQueue_->push(writeBackIt);
                        }
                        continue;
                    } else {
                        debug_info("Perform split GC for file ID (without merge) = %lu done\n", fileHandler->target_file_id_);
                        fileHandler->gc_result_status_flag_ = kShouldDelete;
                        fileHandler->file_ownership_flag_ = 0;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        for (auto writeBackIt : targetWriteBackVec) {
                            writeBackOperationsQueue_->push(writeBackIt);
                        }
                        continue;
                    }
                } else {
                    debug_info("Reached max file number, need to split after merge, current remain empty file numebr = %lu\n", remainEmptyFileNumber);
                    // perfrom merge before split, keep the total file number not changed
                    hashStoreFileMetaDataHandler* targetFileHandler1;
                    hashStoreFileMetaDataHandler* targetFileHandler2;
                    string targetMergedPrefixStr;
                    bool selectFileForMergeStatus = selectFileForMerge(fileHandler->target_file_id_, targetFileHandler1, targetFileHandler2, targetMergedPrefixStr);
                    if (selectFileForMergeStatus == true) {
                        debug_info("Select two file for merge GC success, fileHandler 1 ptr = %p, fileHandler 2 ptr = %p, target prefix = %s\n", targetFileHandler1, targetFileHandler2, targetMergedPrefixStr.c_str());
                        bool performFileMergeStatus = twoAdjacentFileMerge(targetFileHandler1, targetFileHandler2, targetMergedPrefixStr);
                        if (performFileMergeStatus == true) {
                            debug_info("Perform split GC for file ID (with merge) = %lu\n", fileHandler->target_file_id_);
                            bool singleFileGCStatus = singleFileSplit(fileHandler, gcResultMap, targetPrefixBitNumber, fileContainsReWriteKeysFlag);
                            if (singleFileGCStatus == false) {
                                debug_error("[ERROR] Could not perform split GC for file ID = %lu\n", fileHandler->target_file_id_);
                                fileHandler->gc_result_status_flag_ = kNoGC;
                                fileHandler->file_ownership_flag_ = 0;
                                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                                for (auto writeBackIt : targetWriteBackVec) {
                                    writeBackOperationsQueue_->push(writeBackIt);
                                }
                                continue;
                            } else {
                                debug_info("Perform split GC for file ID (with merge) = %lu done\n", fileHandler->target_file_id_);
                                fileHandler->gc_result_status_flag_ = kShouldDelete;
                                fileHandler->file_ownership_flag_ = 0;
                                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                                for (auto writeBackIt : targetWriteBackVec) {
                                    writeBackOperationsQueue_->push(writeBackIt);
                                }
                                continue;
                            }
                        } else {
                            debug_error("[ERROR] Could not perform pre merge to reclaim files before perform split GC for file ID = %lu\n", fileHandler->target_file_id_);
                            fileHandler->gc_result_status_flag_ = kNoGC;
                            fileHandler->file_ownership_flag_ = 0;
                            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                            for (auto writeBackIt : targetWriteBackVec) {
                                writeBackOperationsQueue_->push(writeBackIt);
                            }
                            continue;
                        }
                    } else {
                        fileHandler->gc_result_status_flag_ = kNoGC;
                        fileHandler->file_ownership_flag_ = 0;
                        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WORKER_GC, tv);
                        for (auto writeBackIt : targetWriteBackVec) {
                            writeBackOperationsQueue_->push(writeBackIt);
                        }
                        continue;
                    }
                }
            }
        }
    }
    gcThreadJobDoneFlag_ = true;
    return;
}

void HashStoreFileManager::scheduleMetadataUpdateWorker()
{
    while (true) {
        if (operationCounterForMetadataCommit_ >= operationNumberForMetadataCommitThreshold_) {
            if (UpdateHashStoreFileMetaDataList() != true) {
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
    vector<pair<string, hashStoreFileMetaDataHandler*>> validFilesVec;
    objectFileMetaDataTrie_.getCurrentValidNodes(validFilesVec);
    for (auto fileHandlerIt : validFilesVec) {
        if (fileHandlerIt.second->gc_result_status_flag_ == kNoGC) {
            debug_info("Current file ID = %lu, file size = %lu, has been marked as kNoGC, skip\n", fileHandlerIt.second->target_file_id_, fileHandlerIt.second->total_object_bytes_);
            continue;
        } else if (fileHandlerIt.second->gc_result_status_flag_ == kShouldDelete) {
            debug_error("[ERROR] During forced GC, should not find file marked as kShouldDelete, file ID = %lu, file size = %lu, prefix bit number = %lu\n", fileHandlerIt.second->target_file_id_, fileHandlerIt.second->total_object_bytes_, fileHandlerIt.second->current_prefix_used_bit_);
            continue;
        } else {
            if (fileHandlerIt.second->total_on_disk_bytes_ > singleFileGCTriggerSize_) {
                notifyGCMQ_->push(fileHandlerIt.second);
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
