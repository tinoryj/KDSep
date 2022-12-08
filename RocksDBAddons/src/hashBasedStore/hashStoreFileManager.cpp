#include "hashBasedStore/hashStoreFileManager.hpp"
#include <unordered_map>

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(DeltaKVOptions* options, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ)
{
    initialTrieBitNumber_ = options->hashStore_init_prefix_bit_number;
    maxTrieBitNumber_ = options->hashStore_max_prefix_bit_number;
    uint64_t singleFileGCThreshold = options->deltaStore_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    uint64_t totalHashStoreFileGCThreshold = options->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * options->deltaStore_total_storage_maximum_size;
    singleFileGCTriggerSize_ = singleFileGCThreshold;
    globalGCTriggerSize_ = totalHashStoreFileGCThreshold;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    fileOperationMethod_ = options->fileOperationMethod_;
    enableGCFlag_ = options->enable_deltaStore_garbage_collection;
    operationNumberForMetadataCommitThreshold_ = options->deltaStore_operationNumberForMetadataCommitThreshold_;
    singleFileSplitGCTriggerSize_ = options->deltaStore_split_garbage_collection_start_single_file_minimum_occupancy * options->deltaStore_single_file_maximum_size;
    objectFileMetaDataTrie_.init(initialTrieBitNumber_, maxTrieBitNumber_);
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::~HashStoreFileManager()
{
    if (enableGCFlag_ == true) {
        while (gcThreadJobDoneFlag_ == false) {
            asm volatile("");
            // prevent metadata update before forced gc done;
        }
    }
    CloseHashStoreFileMetaDataList();
}

// Recovery
/*
fileContentBuffer start after file header
resultMap include key - <is_anchor, value> map
*/
uint64_t HashStoreFileManager::deconstructTargetRecoveryContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap, bool& isGCFlushDone)
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
    unordered_map<uint64_t, vector<uint64_t>> mapForBatchedkGCFiles; // previous file ID to new file ID and file obj
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
                    uint64_t currentFileObjectNumber = deconstructTargetRecoveryContentsFromFile(readContentBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMap, isGCFlushedDoneFlag);

                    if (currentFileHeader.file_create_reason_ == kGCFile) {
                        // GC file with ID > targetNewFileID
                        // judge previous file ID
                        if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == false) {
                            // previous ID not in metadata && gc file is not correctly flushed;
                            debug_error("[ERROR] find kGC file that previous file ID not in metadata, seems error, previous file ID = %lu\n", currentFileHeader.previous_file_id_);
                            return false;
                        } else if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_) == hashStoreFileIDToPrefixMap.end() && isGCFlushedDoneFlag == true) {
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
                            currentRecoveryFileHandler->fileOperationMutex_.lock();
                            currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                            currentRecoveryFileHandler->fileOperationMutex_.unlock();
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
                        } else if (hashStoreFileIDToPrefixMap.find(currentFileHeader.previous_file_id_) != hashStoreFileIDToPrefixMap.end()) {
                            // previous file metadata exist
                            uint64_t prefixBitNumberUsedInPreviousFile = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_).second->current_prefix_used_bit_;
                            if (prefixBitNumberUsedInPreviousFile == currentFileHeader.current_prefix_used_bit_) {
                                // single file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    // get file handler
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_).second;
                                    currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                                    currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                                    currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                                    // open current file for further usage
                                    currentRecoveryFileHandler->fileOperationMutex_.lock();
                                    if (currentRecoveryFileHandler->file_operation_func_ptr_->isFileOpen() == true) {
                                        currentRecoveryFileHandler->file_operation_func_ptr_->closeFile();
                                    }
                                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    currentRecoveryFileHandler->fileOperationMutex_.unlock();
                                    // update metadata
                                    hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_).second->target_file_id_ = fileIDIt;
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
                                    targetDeleteFileIDVec.push_back(currentFileHeader.previous_file_id_);
                                    continue;
                                } else {
                                    // gc not success, keep old file, and delete current one
                                    targetDeleteFileIDVec.push_back(fileIDIt);
                                    continue;
                                }
                            } else if (prefixBitNumberUsedInPreviousFile < currentFileHeader.current_prefix_used_bit_) {
                                // file created by split, cache to find the other one
                                if (mapForBatchedkGCFiles.find(currentFileHeader.previous_file_id_) == mapForBatchedkGCFiles.end()) {
                                    vector<uint64_t> tempVec;
                                    tempVec.push_back(fileIDIt);
                                    mapForBatchedkGCFiles.insert(make_pair(currentFileHeader.previous_file_id_, tempVec));
                                } else {
                                    mapForBatchedkGCFiles.at(currentFileHeader.previous_file_id_).push_back(fileIDIt);
                                }
                            } else if (prefixBitNumberUsedInPreviousFile > currentFileHeader.current_prefix_used_bit_) {
                                // merge file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    string leftFatherFilePrefixStr = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_).first; // we store left file as previous file ID (last bit in use = 0)
                                    string rightFatherFilePrefixStr = hashStoreFileIDToPrefixMap.at(currentFileHeader.previous_file_id_).first.substr(0, currentFileHeader.current_prefix_used_bit_) + "1";
                                    uint64_t leftFatherFileID = currentFileHeader.previous_file_id_;
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
                                    currentRecoveryFileHandler->fileOperationMutex_.lock();
                                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    currentRecoveryFileHandler->fileOperationMutex_.unlock();
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
                        currentRecoveryFileHandler->fileOperationMutex_.lock();
                        currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                        currentRecoveryFileHandler->fileOperationMutex_.unlock();
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

                currentIDInMetadataFileHandlerPtr->fileOperationMutex_.lock();
                // start read
                int targetReadSize = onDiskFileSize;
                char readBuffer[targetReadSize];
                debug_trace("target read file content for recovery size = %lu\n", currentIDInMetadataFileHandlerPtr->total_object_bytes_);
                currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->readFile(readBuffer, targetReadSize);
                // read done, start process
                bool isGCFlushedDoneFlag = false;
                uint64_t recoveredObjectNumber = deconstructTargetRecoveryContentsFromFile(readBuffer + currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetReadSize - currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetListForRedo, isGCFlushedDoneFlag);
                // update metadata
                currentIDInMetadataFileHandlerPtr->total_object_count_ += recoveredObjectNumber;
                currentIDInMetadataFileHandlerPtr->total_object_bytes_ += targetReadSize;
                currentIDInMetadataFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                currentIDInMetadataFileHandlerPtr->fileOperationMutex_.unlock();

            } else {
                // file size match, skip current file
                debug_trace("target file ID = %lu, file size (system) = %lu, file size (metadata) = %lu\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes_);
                continue;
            }
        }
    }
    // process not in metadata files created by split old file
    for (auto splitFileIt : mapForBatchedkGCFiles) {
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
            objectNumberCount[i] = deconstructTargetRecoveryContentsFromFile(readBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMapTemp[i], gcFlagStatus[i]);
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
                currentRecoveryFileHandler->fileOperationMutex_.lock();
                currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(splitFileIt.second[i]) + ".delta");
                currentRecoveryFileHandler->fileOperationMutex_.unlock();
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
    hashStoreFileManifestStream.open(
        workingDir_ + "/hashStoreFileManifest." + currentPointerStr, ios::in);
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
            currentFileHandlerPtr->fileOperationMutex_.lock();
            currentFileHandlerPtr->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
            uint64_t onDiskFileSize = currentFileHandlerPtr->file_operation_func_ptr_->getFileSize();
            if (onDiskFileSize > currentFileHandlerPtr->total_object_bytes_ && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, currentFileHandlerPtr->total_object_bytes_);
            } else if (onDiskFileSize < currentFileHandlerPtr->total_object_bytes_ && shouldDoRecoveryFlag_ == false) {
                debug_error("[ERROR] Should not recovery, but on diks file size = %lu, in metadata file size = %lu. The flushed metadata not correct\n", onDiskFileSize, currentFileHandlerPtr->total_object_bytes_);
            }
            currentFileHandlerPtr->fileOperationMutex_.unlock();
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
                        it.second->fileOperationMutex_.lock();
                        it.second->file_operation_func_ptr_->flushFile();
                        it.second->file_operation_func_ptr_->closeFile();
                        debug_trace("Closed file ID = %lu, file correspond prefix = %s\n", it.second->target_file_id_, it.first.c_str());
                        it.second->fileOperationMutex_.unlock();
                        string targetRemoveObslateFileName = workingDir_ + "/" + to_string(it.second->target_file_id_) + ".delta";
                        if (filesystem::exists(targetRemoveObslateFileName) != false) {
                            auto removeOldManifestStatus = remove(targetRemoveObslateFileName.c_str());
                            if (removeOldManifestStatus == -1) {
                                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveObslateFileName.c_str());
                            }
                        }
                    }
                }
            }
        }
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
            if (it.second->gc_result_status_flag_ == kShouldDelete) {
                targetDeleteFileIDVec.push_back(it.second->target_file_id_);
                if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                    it.second->file_operation_func_ptr_->closeFile();
                }
                // skip and delete should deleted files
                continue;
            }
            if (it.second->file_operation_func_ptr_->isFileOpen() == true) {
                hashStoreFileManifestStream << it.first << endl;
                hashStoreFileManifestStream << it.second->target_file_id_ << endl;
                hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
                hashStoreFileManifestStream << it.second->total_object_count_ << endl;
                hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
                it.second->fileOperationMutex_.lock();
                it.second->file_operation_func_ptr_->flushFile();
                it.second->file_operation_func_ptr_->closeFile();
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
        for (auto removeFileIDIt : targetDeleteFileIDVec) {
            string targetRemoveBucketFileName = workingDir_ + "/" + to_string(removeFileIDIt) + ".delta";
            auto removeOldBucketStatus = remove(targetRemoveBucketFileName.c_str());
            if (removeOldBucketStatus == -1) {
                debug_error("[ERROR] Could not delete the obsolete file, file path = %s\n", targetRemoveBucketFileName.c_str());
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
bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr)
{
    if (opType == kPut || opType == kMultiPut) {
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
        bool createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr, initialTrieBitNumber_, false, 0);
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
            if (!getFileHandlerStatus) {
                bool createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr, initialTrieBitNumber_, false, 0);
                if (!createNewFileHandlerStatus) {
                    debug_error("[ERROR] Previous file may deleted during GC, and splited new files not contains current key prefix, create new bucket for put operation error, key = %s\n", keyStr.c_str());
                    return false;
                } else {
                    debug_info("[Insert] Previous file may deleted during GC, and splited new files not contains current key prefix, create new file ID = %lu, for key = %s, file gc status flag = %d, prefix bit number used = %lu\n", fileHandlerPtr->target_file_id_, keyStr.c_str(), fileHandlerPtr->gc_result_status_flag_, fileHandlerPtr->current_prefix_used_bit_);
                    fileHandlerPtr->file_ownership_flag_ = 1;
                    return true;
                }
            } else {
                // avoid get file handler which is in GC;
                debug_trace("Get exist file ID = %lu, for key = %s, waiting for file ownership \n", fileHandlerPtr->target_file_id_, keyStr.c_str());
                while (fileHandlerPtr->file_ownership_flag_ != 0) {
                    asm volatile("");
                    // wait if file is using in gc
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

bool HashStoreFileManager::createAndGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber, bool createByGCFlag, uint64_t previousFileID)
{
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
    currentFileHandlerPtr->current_prefix_used_bit_ = prefixBitNumber;
    currentFileHandlerPtr->target_file_id_ = generateNewFileID();
    currentFileHandlerPtr->file_ownership_flag_ = 0;
    currentFileHandlerPtr->gc_result_status_flag_ = kNew;
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    if (createByGCFlag == true) {
        currentFileHandlerPtr->previous_file_id_ = previousFileID;
        currentFileHandlerPtr->file_create_reason_ = kGCFile;
    } else {
        currentFileHandlerPtr->previous_file_id_ = 0xffffffffffffffff;
        currentFileHandlerPtr->file_create_reason_ = kNewFile;
    }
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

bool HashStoreFileManager::createHashStoreFileHandlerByPrefixStrForSplitGC(string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID)
{
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
    currentFileHandlerPtr->current_prefix_used_bit_ = targetPrefixLen;
    currentFileHandlerPtr->target_file_id_ = generateNewFileID();
    currentFileHandlerPtr->file_ownership_flag_ = -1;
    currentFileHandlerPtr->gc_result_status_flag_ = kNew;
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    currentFileHandlerPtr->previous_file_id_ = previousFileID;
    currentFileHandlerPtr->file_create_reason_ = kGCFile;
    // set up new file header for write
    hashStoreFileHeader newFileHeader;
    newFileHeader.current_prefix_used_bit_ = targetPrefixLen;
    newFileHeader.previous_file_id_ = previousFileID;
    newFileHeader.file_create_reason_ = kGCFile;
    newFileHeader.file_id_ = currentFileHandlerPtr->target_file_id_;
    char fileHeaderWriteBuffer[sizeof(newFileHeader)];
    memcpy(fileHeaderWriteBuffer, &newFileHeader, sizeof(newFileHeader));
    // write header to current file
    currentFileHandlerPtr->fileOperationMutex_.lock();
    debug_trace("Newly created file ID = %lu, target prefix bit number = %lu, prefix = %s\n", currentFileHandlerPtr->target_file_id_, targetPrefixLen, prefixStr.c_str());
    currentFileHandlerPtr->file_operation_func_ptr_->createFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
    if (currentFileHandlerPtr->file_operation_func_ptr_->isFileOpen() == true) {
        currentFileHandlerPtr->file_operation_func_ptr_->closeFile();
    }
    currentFileHandlerPtr->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
    uint64_t onDiskWriteSize = currentFileHandlerPtr->file_operation_func_ptr_->writeFile(fileHeaderWriteBuffer, sizeof(newFileHeader));
    currentFileHandlerPtr->total_object_bytes_ += sizeof(newFileHeader);
    currentFileHandlerPtr->total_on_disk_bytes_ += onDiskWriteSize;
    currentFileHandlerPtr->temp_not_flushed_data_bytes_ = sizeof(newFileHeader);
    currentFileHandlerPtr->fileOperationMutex_.unlock();
    // move pointer for return
    fileHandlerPtr = currentFileHandlerPtr;
    return true;
}

uint64_t HashStoreFileManager::generateNewFileID()
{
    fileIDGeneratorMtx_.lock();
    targetNewFileID_ += 1;
    uint64_t tempIDForReturn = targetNewFileID_;
    fileIDGeneratorMtx_.unlock();
    return tempIDForReturn;
}

pair<uint64_t, uint64_t> HashStoreFileManager::deconstructAndGetValidContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<string>>& resultMap)
{
    uint64_t processedKeepObjectNumber = 0;
    uint64_t processedTotalObjectNumber = 0;

    uint64_t currentProcessLocationIndex = 0;
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
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                processedKeepObjectNumber -= (resultMap.at(currentKeyStr).size());
                resultMap.at(currentKeyStr).clear();
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
                resultMap.at(currentKeyStr).push_back(currentValueStr);
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<string> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(currentValueStr);
                resultMap.insert(make_pair(currentKeyStr, newValuesRelatedToCurrentKeyVec));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            }
        }
    }
    debug_info("deconstruct current file header done, file ID = %lu, create reason = %u, prefix length used in this file = %lu, target process file size = %lu, find different key number = %lu, total processed object number = %lu, target keep object number = %lu\n", currentFileHeader.file_id_, currentFileHeader.file_create_reason_, currentFileHeader.current_prefix_used_bit_, fileSize, resultMap.size(), processedTotalObjectNumber, processedKeepObjectNumber);
    return make_pair(processedKeepObjectNumber, processedTotalObjectNumber);
}

// threads workers
void HashStoreFileManager::processGCRequestWorker()
{
    while (true) {
        if (notifyGCMQ_->done_ == true && notifyGCMQ_->isEmpty() == true) {
            break;
        }
        hashStoreFileMetaDataHandler* currentHandlerPtr;
        if (notifyGCMQ_->pop(currentHandlerPtr)) {
            debug_info("new file request for GC, file ID = %lu, existing size = %lu, file gc status = %d\n", currentHandlerPtr->target_file_id_, currentHandlerPtr->total_object_bytes_, currentHandlerPtr->gc_result_status_flag_);
            // read contents
            char readWriteBuffer[currentHandlerPtr->total_object_bytes_];
            currentHandlerPtr->fileOperationMutex_.lock();
            currentHandlerPtr->file_ownership_flag_ = -1;
            currentHandlerPtr->file_operation_func_ptr_->flushFile();
            currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
            uint64_t currentFileOnDiskSize = currentHandlerPtr->file_operation_func_ptr_->getFileSize();
            if (currentFileOnDiskSize != currentHandlerPtr->total_object_bytes_) {
                debug_error("[ERROR] file size in metadata = %lu, in filesystem = %lu\n", currentHandlerPtr->total_object_bytes_, currentFileOnDiskSize);
            }
            currentHandlerPtr->file_operation_func_ptr_->readFile(readWriteBuffer, currentHandlerPtr->total_object_bytes_);
            // process GC contents
            unordered_map<string, vector<string>> gcResultMap;
            pair<uint64_t, uint64_t> remainObjectNumberPair = deconstructAndGetValidContentsFromFile(readWriteBuffer, currentHandlerPtr->total_object_bytes_, gcResultMap);

            // count valid object size to determine GC method;
            if (remainObjectNumberPair.second == 0) {
                debug_info("File ID = %lu contains no object, should just delete, total contains object number = %lu, should keep object number = %lu\n", currentHandlerPtr->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
                currentHandlerPtr->file_ownership_flag_ = 0;
                currentHandlerPtr->fileOperationMutex_.unlock();
                continue;
            }
            if (gcResultMap.size() == 0) {
                currentHandlerPtr->file_ownership_flag_ = 0;
                debug_error("[ERROR] File ID = %lu contains no keys, but processed object number = %lu, target keep object number = %lu, skip this file\n", currentHandlerPtr->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                currentHandlerPtr->fileOperationMutex_.unlock();
                continue;
            }
            if (gcResultMap.size() == 1) {
                if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
                    if (currentHandlerPtr->gc_result_status_flag_ == kNew) {
                        // keep tracking until forced gc threshold;
                        currentHandlerPtr->gc_result_status_flag_ = kMayGC;
                        currentHandlerPtr->file_ownership_flag_ = 0;
                        debug_info("File ID = %lu contains only %lu different keys, marked as kMayGC\n", currentHandlerPtr->target_file_id_, gcResultMap.size());
                        currentHandlerPtr->fileOperationMutex_.unlock();
                        continue;
                    }
                    if (currentHandlerPtr->gc_result_status_flag_ == kMayGC) {
                        // Mark this file as could not GC;
                        currentHandlerPtr->gc_result_status_flag_ = kNoGC;
                        currentHandlerPtr->file_ownership_flag_ = 0;
                        debug_info("File ID = %lu contains only %lu different keys, marked as kNoGC\n", currentHandlerPtr->target_file_id_, gcResultMap.size());
                        currentHandlerPtr->fileOperationMutex_.unlock();
                        continue;
                    }
                } else {
                    debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write\n", currentHandlerPtr->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                    // reclaimed space success, rewrite current file to new file
                    if (currentHandlerPtr->file_operation_func_ptr_->isFileOpen() == true) {
                        currentHandlerPtr->file_operation_func_ptr_->closeFile();
                    }
                    uint64_t targetFileSize = 0;
                    for (auto keyIt : gcResultMap) {
                        for (auto valueIt : keyIt.second) {
                            targetFileSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                            // debug_info("Rewrite: key = %s, value = %s\n", keyIt.first.c_str(), valueIt.c_str());
                        }
                    }
                    debug_trace("Rewrite processed size = %lu\n", targetFileSize);
                    targetFileSize += (sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader));
                    char currentWriteBuffer[targetFileSize];
                    uint64_t newObjectNumber = 0;
                    uint64_t currentProcessLocationIndex = 0;
                    hashStoreFileHeader currentFileHeader;
                    currentFileHeader.current_prefix_used_bit_ = currentHandlerPtr->current_prefix_used_bit_;
                    currentFileHeader.file_create_reason_ = kGCFile;
                    currentFileHeader.file_id_ = generateNewFileID();
                    currentFileHeader.previous_file_id_ = currentHandlerPtr->target_file_id_;
                    memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentFileHeader, sizeof(hashStoreFileHeader));
                    currentProcessLocationIndex += sizeof(currentFileHeader);
                    // add file header
                    for (auto keyIt : gcResultMap) {
                        for (auto valueIt : keyIt.second) {
                            newObjectNumber++;
                            hashStoreRecordHeader currentObjectRecordHeader;
                            currentObjectRecordHeader.is_anchor_ = false;
                            currentObjectRecordHeader.key_size_ = keyIt.first.size();
                            currentObjectRecordHeader.value_size_ = valueIt.size();
                            memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                            currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
                            memcpy(currentWriteBuffer + currentProcessLocationIndex, keyIt.first.c_str(), keyIt.first.size());
                            currentProcessLocationIndex += keyIt.first.size();
                            memcpy(currentWriteBuffer + currentProcessLocationIndex, valueIt.c_str(), valueIt.size());
                            currentProcessLocationIndex += valueIt.size();
                        }
                    }
                    // add gc done flag into bucket file
                    hashStoreRecordHeader currentObjectRecordHeader;
                    currentObjectRecordHeader.is_anchor_ = false;
                    currentObjectRecordHeader.is_gc_done_ = true;
                    currentObjectRecordHeader.key_size_ = 0;
                    currentObjectRecordHeader.value_size_ = 0;
                    memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
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
                    uint64_t onDiskWriteSize = currentHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetFileSize);
                    currentHandlerPtr->file_operation_func_ptr_->flushFile();
                    debug_trace("Rewrite done file size = %lu, file path = %s\n", currentHandlerPtr->file_operation_func_ptr_->getFileSize(), targetOpenFileName.c_str());
                    // update metadata
                    currentHandlerPtr->target_file_id_ = currentFileHeader.file_id_;
                    currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                    currentHandlerPtr->total_object_count_ = newObjectNumber + 1;
                    currentHandlerPtr->total_object_bytes_ = currentProcessLocationIndex + sizeof(hashStoreRecordHeader);
                    currentHandlerPtr->total_on_disk_bytes_ += onDiskWriteSize;
                    debug_trace("Rewrite file size in metadata = %lu, file ID = %lu\n", currentHandlerPtr->total_object_bytes_, currentHandlerPtr->target_file_id_);
                    string targetRemoveFilePath = workingDir_ + "/" + to_string(currentFileHeader.previous_file_id_) + ".delta";
                    if (filesystem::exists(targetRemoveFilePath) != false) {
                        auto removeOldManifestStatus = remove(targetRemoveFilePath.c_str());
                        if (removeOldManifestStatus == -1) {
                            debug_error("[ERROR] Could not delete the obsolete file, file path = %s, the new file ID = %lu\n", targetRemoveFilePath.c_str(), currentFileHeader.file_id_);
                        } else {
                            debug_info("Deleted old file ID = %lu since single file gc create new file ID = %lu\n", currentFileHeader.previous_file_id_, currentFileHeader.file_id_);
                        }
                    }
                    if (currentHandlerPtr->total_object_bytes_ > singleFileGCTriggerSize_) {
                        currentHandlerPtr->gc_result_status_flag_ = kNoGC;
                    }
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    // remove old file
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    debug_info("flushed new file to filesystem since single file gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHeader.file_id_, currentFileHeader.previous_file_id_);
                    continue;
                }
            } else {
                if (currentHandlerPtr->gc_result_status_flag_ == kNew || currentHandlerPtr->gc_result_status_flag_ == kMayGC) {
                    // perform split into two buckets via extend prefix bit (+1)
                    debug_info("start split for key number = %lu\n", gcResultMap.size());
                    uint64_t currentUsedPrefixBitNumber = currentHandlerPtr->current_prefix_used_bit_;
                    uint64_t targetPrefixBitNumber = currentUsedPrefixBitNumber + 1;
                    if (targetPrefixBitNumber > maxTrieBitNumber_) {
                        debug_info("GC for file ID = %lu, current file gc type = %d, gcResultMap size = %lu, but next prefix bit number will exceed limit, mark as never GC\n", currentHandlerPtr->target_file_id_, currentHandlerPtr->gc_result_status_flag_, gcResultMap.size());
                        currentHandlerPtr->gc_result_status_flag_ = kNoGC;
                        currentHandlerPtr->file_ownership_flag_ = 0;
                        currentHandlerPtr->fileOperationMutex_.unlock();
                        continue;
                    }
                    uint64_t validObjectSizeCounter = 0;
                    for (auto keyIt : gcResultMap) {
                        for (auto valueIt : keyIt.second) {
                            validObjectSizeCounter += (keyIt.first.size() + valueIt.size());
                        }
                    }
                    if (validObjectSizeCounter > singleFileSplitGCTriggerSize_) {
                        // split
                        bool isSplitDoneFlag = true;
                        unordered_map<hashStoreFileMetaDataHandler*, string> tempHandlerToPrefixMapForMetadataUpdate;
                        unordered_map<string, hashStoreFileMetaDataHandler*> tempPrefixToHandlerMapForFileSplitGC;
                        for (auto keyIt : gcResultMap) {
                            debug_info("write new file for key = %s\n", keyIt.first.c_str());
                            hashStoreFileMetaDataHandler* currentFileHandlerPtr;
                            bool getFileHandlerStatus;
                            string prefixTempStr;
                            generateHashBasedPrefix(keyIt.first, prefixTempStr);
                            if (tempPrefixToHandlerMapForFileSplitGC.find(prefixTempStr.substr(0, targetPrefixBitNumber)) != tempPrefixToHandlerMapForFileSplitGC.end()) {
                                currentFileHandlerPtr = tempPrefixToHandlerMapForFileSplitGC.at(prefixTempStr.substr(0, targetPrefixBitNumber));
                                getFileHandlerStatus = true;
                            } else {
                                getFileHandlerStatus = createHashStoreFileHandlerByPrefixStrForSplitGC(prefixTempStr, currentFileHandlerPtr, targetPrefixBitNumber, currentHandlerPtr->target_file_id_);
                                // update metadata for currentFileHandler;
                                tempHandlerToPrefixMapForMetadataUpdate.insert(make_pair(currentFileHandlerPtr, prefixTempStr));
                                tempPrefixToHandlerMapForFileSplitGC.insert(make_pair(prefixTempStr.substr(0, targetPrefixBitNumber), currentFileHandlerPtr));
                            }
                            if (getFileHandlerStatus == false) {
                                debug_error("[ERROR] could not get/create new bucket for key = %s during split gc\n", keyIt.first.c_str());
                                isSplitDoneFlag = false;
                                break;
                            } else {
                                debug_info("Get/Create new bucket for key = %s during split gc, new file ID = %lu, current prefix bit number = %lu\n", keyIt.first.c_str(), currentFileHandlerPtr->target_file_id_, currentFileHandlerPtr->current_prefix_used_bit_);
                                uint64_t targetWriteSize = 0;
                                for (auto valueIt : keyIt.second) {
                                    targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                                }
                                debug_info("Target write size = %lu, for new file ID = %lu\n", targetWriteSize, currentFileHandlerPtr->target_file_id_);
                                currentFileHandlerPtr->fileOperationMutex_.lock();
                                char currentWriteBuffer[targetWriteSize + sizeof(hashStoreRecordHeader)];
                                uint64_t currentWritePos = 0;
                                for (auto valueIt : keyIt.second) {
                                    hashStoreRecordHeader currentObjectRecordHeader;
                                    currentObjectRecordHeader.is_anchor_ = false;
                                    currentObjectRecordHeader.is_gc_done_ = false;
                                    currentObjectRecordHeader.key_size_ = keyIt.first.size();
                                    currentObjectRecordHeader.value_size_ = valueIt.size();
                                    memcpy(currentWriteBuffer + currentWritePos, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                                    memcpy(currentWriteBuffer + currentWritePos + sizeof(hashStoreRecordHeader), keyIt.first.c_str(), keyIt.first.size());
                                    memcpy(currentWriteBuffer + currentWritePos + sizeof(hashStoreRecordHeader) + keyIt.first.size(), valueIt.c_str(), valueIt.size());
                                    currentWritePos += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                                    currentFileHandlerPtr->temp_not_flushed_data_bytes_ += (sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                                    currentFileHandlerPtr->total_object_bytes_ += (sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                                    currentFileHandlerPtr->total_object_count_++;
                                }
                                // write gc done flag into bucket file
                                hashStoreRecordHeader currentObjectRecordHeader;
                                currentObjectRecordHeader.is_anchor_ = false;
                                currentObjectRecordHeader.is_gc_done_ = true;
                                currentObjectRecordHeader.key_size_ = 0;
                                currentObjectRecordHeader.value_size_ = 0;
                                memcpy(currentWriteBuffer + currentWritePos, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                                uint64_t onDiskWriteSize = currentFileHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetWriteSize + sizeof(hashStoreRecordHeader));
                                currentFileHandlerPtr->total_object_bytes_ += sizeof(hashStoreRecordHeader);
                                currentFileHandlerPtr->total_on_disk_bytes_ += onDiskWriteSize;
                                currentFileHandlerPtr->total_object_count_++;
                                currentFileHandlerPtr->file_operation_func_ptr_->flushFile();
                                debug_trace("flushed new file to filesystem since split gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHandlerPtr->target_file_id_, currentHandlerPtr->target_file_id_);
                                currentFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                                currentFileHandlerPtr->fileOperationMutex_.unlock();
                            }
                        }
                        if (isSplitDoneFlag == true) {
                            // mark current file as should delete, prevent further use
                            debug_info("Split file ID = %lu for gc success, mark as should delete\n", currentHandlerPtr->target_file_id_);
                            // update all metadata
                            if (tempHandlerToPrefixMapForMetadataUpdate.size() > 2) {
                                debug_error("[ERROR] Split file ID = %lu for gc success, but generate more than 2 files, generated file number = %lu\n", currentHandlerPtr->target_file_id_, tempHandlerToPrefixMapForMetadataUpdate.size());
                                currentHandlerPtr->file_ownership_flag_ = 0;
                                currentHandlerPtr->fileOperationMutex_.unlock();
                                continue;
                            }
                            for (auto mapIt : tempHandlerToPrefixMapForMetadataUpdate) {
                                hashStoreFileMetaDataHandler* tempHandler = mapIt.first;
                                uint64_t insertAtLevel = objectFileMetaDataTrie_.insert(mapIt.second, tempHandler);
                                if (insertAtLevel == 0) {
                                    debug_error("[ERROR] Error insert to prefix tree, prefix length used = %lu, inserted file ID = %lu\n", tempHandler->current_prefix_used_bit_, tempHandler->target_file_id_);
                                    debug_error("[ERROR] Split file ID = %lu for gc not success, could not update metadata for file ID = %lu\n", currentHandlerPtr->target_file_id_, tempHandler->target_file_id_);
                                    currentHandlerPtr->file_ownership_flag_ = 0;
                                    currentHandlerPtr->fileOperationMutex_.unlock();
                                    continue;
                                } else {
                                    if (tempHandler->current_prefix_used_bit_ != insertAtLevel) {
                                        debug_info("After insert to prefix tree, get handler at level = %lu, but prefix length used = %lu, inserted file ID = %lu, update the current bit number used in the file handler\n", insertAtLevel, tempHandler->current_prefix_used_bit_, tempHandler->target_file_id_);
                                        tempHandler->current_prefix_used_bit_ = insertAtLevel;
                                    }
                                }
                            }
                            for (auto mapIt : tempHandlerToPrefixMapForMetadataUpdate) {
                                hashStoreFileMetaDataHandler* tempHandler = mapIt.first;
                                tempHandler->file_ownership_flag_ = 0;
                            }
                            currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
                            currentHandlerPtr->file_ownership_flag_ = 0;
                            currentHandlerPtr->fileOperationMutex_.unlock();
                        } else {
                            debug_error("[ERROR] Split file ID = %lu for gc not success, skip this file\n", currentHandlerPtr->target_file_id_);
                            currentHandlerPtr->file_ownership_flag_ = 0;
                            currentHandlerPtr->fileOperationMutex_.unlock();
                        }
                        continue;
                    } else {
                        // single file rewrite
                        debug_info("File ID = %lu, total contains object number = %lu, should keep object number = %lu, reclaim empty space success, start re-write\n", currentHandlerPtr->target_file_id_, remainObjectNumberPair.second, remainObjectNumberPair.first);
                        // reclaimed space success, rewrite current file to new file
                        if (currentHandlerPtr->file_operation_func_ptr_->isFileOpen() == true) {
                            currentHandlerPtr->file_operation_func_ptr_->closeFile();
                        }
                        uint64_t targetFileSize = 0;
                        for (auto keyIt : gcResultMap) {
                            for (auto valueIt : keyIt.second) {
                                targetFileSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                                // debug_info("Rewrite: key = %s, value = %s\n", keyIt.first.c_str(), valueIt.c_str());
                            }
                        }
                        debug_trace("Rewrite processed size = %lu\n", targetFileSize);
                        targetFileSize += (sizeof(hashStoreFileHeader) + sizeof(hashStoreRecordHeader));
                        char currentWriteBuffer[targetFileSize];
                        uint64_t newObjectNumber = 0;
                        uint64_t currentProcessLocationIndex = 0;
                        hashStoreFileHeader currentFileHeader;
                        currentFileHeader.current_prefix_used_bit_ = currentHandlerPtr->current_prefix_used_bit_;
                        currentFileHeader.file_create_reason_ = kGCFile;
                        currentFileHeader.file_id_ = generateNewFileID();
                        currentFileHeader.previous_file_id_ = currentHandlerPtr->target_file_id_;
                        memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentFileHeader, sizeof(hashStoreFileHeader));
                        currentProcessLocationIndex += sizeof(currentFileHeader);
                        // add file header
                        for (auto keyIt : gcResultMap) {
                            for (auto valueIt : keyIt.second) {
                                newObjectNumber++;
                                hashStoreRecordHeader currentObjectRecordHeader;
                                currentObjectRecordHeader.is_anchor_ = false;
                                currentObjectRecordHeader.key_size_ = keyIt.first.size();
                                currentObjectRecordHeader.value_size_ = valueIt.size();
                                memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                                currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
                                memcpy(currentWriteBuffer + currentProcessLocationIndex, keyIt.first.c_str(), keyIt.first.size());
                                currentProcessLocationIndex += keyIt.first.size();
                                memcpy(currentWriteBuffer + currentProcessLocationIndex, valueIt.c_str(), valueIt.size());
                                currentProcessLocationIndex += valueIt.size();
                            }
                        }
                        // add gc done flag into bucket file
                        hashStoreRecordHeader currentObjectRecordHeader;
                        currentObjectRecordHeader.is_anchor_ = false;
                        currentObjectRecordHeader.is_gc_done_ = true;
                        currentObjectRecordHeader.key_size_ = 0;
                        currentObjectRecordHeader.value_size_ = 0;
                        memcpy(currentWriteBuffer + currentProcessLocationIndex, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
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
                        uint64_t onDiskWriteSize = currentHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetFileSize);
                        currentHandlerPtr->file_operation_func_ptr_->flushFile();
                        debug_trace("Rewrite done file size = %lu, file path = %s\n", currentHandlerPtr->file_operation_func_ptr_->getFileSize(), targetOpenFileName.c_str());
                        // update metadata
                        currentHandlerPtr->target_file_id_ = currentFileHeader.file_id_;
                        currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                        currentHandlerPtr->total_object_count_ = newObjectNumber + 1;
                        currentHandlerPtr->total_object_bytes_ = currentProcessLocationIndex + sizeof(hashStoreRecordHeader);
                        currentHandlerPtr->total_on_disk_bytes_ += onDiskWriteSize;
                        debug_trace("Rewrite file size in metadata = %lu, file ID = %lu\n", currentHandlerPtr->total_object_bytes_, currentHandlerPtr->target_file_id_);
                        string targetRemoveFilePath = workingDir_ + "/" + to_string(currentFileHeader.previous_file_id_) + ".delta";
                        if (filesystem::exists(targetRemoveFilePath) != false) {
                            auto removeOldManifestStatus = remove(targetRemoveFilePath.c_str());
                            if (removeOldManifestStatus == -1) {
                                debug_error("[ERROR] Could not delete the obsolete file, file path = %s, the new file ID = %lu\n", targetRemoveFilePath.c_str(), currentFileHeader.file_id_);
                            } else {
                                debug_info("Deleted old file ID = %lu since single file gc create new file ID = %lu\n", currentFileHeader.previous_file_id_, currentFileHeader.file_id_);
                            }
                        }
                        if (currentHandlerPtr->total_object_bytes_ > singleFileGCTriggerSize_) {
                            currentHandlerPtr->gc_result_status_flag_ = kNoGC;
                        }
                        currentHandlerPtr->fileOperationMutex_.unlock();
                        // remove old file
                        currentHandlerPtr->file_ownership_flag_ = 0;
                        debug_info("flushed new file to filesystem since single file gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHeader.file_id_, currentFileHeader.previous_file_id_);
                        continue;
                    }
                } else {
                    debug_error("[ERROR] GC for file ID = %lu error, not fit, current file gc type = %d, gcResultMap size = %lu\n", currentHandlerPtr->target_file_id_, currentHandlerPtr->gc_result_status_flag_, gcResultMap.size());
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    continue;
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
        if (notifyGCMQ_->done_ == true) {
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
            debug_warn("Current file ID = %lu, file size = %lu, has been marked as kNoGC, skip\n", fileHandlerIt.second->target_file_id_, fileHandlerIt.second->total_object_bytes_);
            continue;
        } else if (fileHandlerIt.second->gc_result_status_flag_ == kShouldDelete) {
            debug_error("[ERROR] During forced GC, should not find file marked as kShouldDelete, file ID = %lu, file size = %lu, prefix bit number = %lu\n", fileHandlerIt.second->target_file_id_, fileHandlerIt.second->total_object_bytes_, fileHandlerIt.second->current_prefix_used_bit_);
            continue;
        } else {
            if (fileHandlerIt.second->total_object_bytes_ > singleFileGCTriggerSize_) {
                notifyGCMQ_->push(fileHandlerIt.second);
            }
        }
    }
    while (notifyGCMQ_->isEmpty() != true) {
        asm volatile("");
        // wait for gc job done
    }
    return true;
}
}