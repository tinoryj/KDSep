#include "hashBasedStore/hashStoreFileManager.hpp"
#include <unordered_map>

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(uint64_t initialBitNumber, uint64_t maxBitNumber, uint64_t objectGCTriggerSize,
    uint64_t objectGlobalGCTriggerSize, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ, fileOperationType fileOperationType)
{
    initialTrieBitNumber_ = initialBitNumber;
    maxTrieBitNumber_ = maxBitNumber;
    singleFileGCTriggerSize_ = objectGCTriggerSize;
    globalGCTriggerSize_ = objectGlobalGCTriggerSize;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    fileOperationMethod_ = fileOperationType;
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::~HashStoreFileManager()
{
    CloseHashStoreFileMetaDataList();
}

bool HashStoreFileManager::setOperationNumberThresholdForMetadataUpdata(uint64_t threshold)
{
    operationNumberForMetadataCommitThreshold_ = threshold;
    if (operationNumberForMetadataCommitThreshold_ != threshold) {
        return false;
    } else {
        return true;
    }
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
    if (shouldDoRecoveryFlag_ == false) {
        debug_trace("DB closed success, do not need recovery, flag = %d\n", shouldDoRecoveryFlag_);
        return true;
    }
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
    // buffer target delete file IDs
    vector<uint64_t> targetDeleteFileIDVec;
    // buffer no metadata kGC files generated by split
    unordered_map<uint64_t, vector<uint64_t>> mapForBatchedkGCFiles; // previous file ID to new file ID and file obj
    // process files
    for (auto fileIDIt : scannedOnDiskFileIDList) {
        if (hashStoreFileIDToPrefixMap_.find(fileIDIt) == hashStoreFileIDToPrefixMap_.end()) {
            // file not exist in metadata, should scan and update into metadata
            debug_trace("file ID = %lu not exist in metadata, try recovery\n", fileIDIt);
            if (fileIDIt >= targetNewFileID_) {
                // the file is newly created, should scan
                FileOperation tempReadFileStream(fileOperationMethod_);
                string targetOpenFileName = workingDir_ + "/" + to_string(fileIDIt) + ".delta";
                bool openCurrentFileStatus = tempReadFileStream.openFile(targetOpenFileName);
                if (openCurrentFileStatus == false) {

                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open file for recovery, file path = " << targetOpenFileName << RESET << endl;

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
                        if (hashStoreFileIDToPrefixMap_.find(currentFileHeader.previous_file_id_) == hashStoreFileIDToPrefixMap_.end() && isGCFlushedDoneFlag == false) {
                            // previous ID not in metadata && gc file is not correctly flushed;

                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find kGC file that previous file ID not in metadata, seems error, previous file id = " << currentFileHeader.previous_file_id_ << RESET << endl;

                            return false;
                        } else if (hashStoreFileIDToPrefixMap_.find(currentFileHeader.previous_file_id_) == hashStoreFileIDToPrefixMap_.end() && isGCFlushedDoneFlag == true) {
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
                            currentRecoveryFileHandler->file_operation_func_ptr_->resetPointer(kEnd);
                            currentRecoveryFileHandler->fileOperationMutex_.unlock();
                            // update metadata
                            objectFileMetaDataTrie_.insert(make_pair(currentFilePrefix, currentRecoveryFileHandler));
                            hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt, currentFilePrefix));
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
                        } else if (hashStoreFileIDToPrefixMap_.find(currentFileHeader.previous_file_id_) != hashStoreFileIDToPrefixMap_.end()) {
                            // previous file metadata exist
                            uint64_t prefixBitNumberUsedInPreviousFile = objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_))->current_prefix_used_bit_;
                            if (prefixBitNumberUsedInPreviousFile == currentFileHeader.current_prefix_used_bit_) {
                                // single file gc
                                if (isGCFlushedDoneFlag == true) {
                                    // gc success, keep current file, and delete old one
                                    // get file handler
                                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_));
                                    currentRecoveryFileHandler->target_file_id_ = fileIDIt;
                                    currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                                    currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                                    currentRecoveryFileHandler->total_object_bytes_ = targetFileSize;
                                    // open current file for further usage
                                    currentRecoveryFileHandler->fileOperationMutex_.lock();
                                    currentRecoveryFileHandler->file_operation_func_ptr_->closeFile();
                                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(fileIDIt) + ".delta");
                                    currentRecoveryFileHandler->file_operation_func_ptr_->resetPointer(kEnd);
                                    currentRecoveryFileHandler->fileOperationMutex_.unlock();
                                    // update metadata
                                    string tempCurrentFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_);
                                    hashStoreFileIDToPrefixMap_.erase(currentFileHeader.previous_file_id_);
                                    hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt, tempCurrentFilePrefixStr));
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
                                    string leftFatherFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_); // we store left file as previous file id (last bit in use = 1)
                                    string rightFatherFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_).substr(0, currentFileHeader.current_prefix_used_bit_) + "0";
                                    uint64_t leftFatherFileID = objectFileMetaDataTrie_.at(leftFatherFilePrefixStr)->target_file_id_;
                                    uint64_t rightFatherFileID = objectFileMetaDataTrie_.at(rightFatherFilePrefixStr)->target_file_id_;
                                    // delete left father
                                    objectFileMetaDataTrie_.at(leftFatherFilePrefixStr)->file_operation_func_ptr_->closeFile();
                                    delete objectFileMetaDataTrie_.at(leftFatherFilePrefixStr)->file_operation_func_ptr_;
                                    delete objectFileMetaDataTrie_.at(leftFatherFilePrefixStr);
                                    objectFileMetaDataTrie_.erase(leftFatherFilePrefixStr);
                                    hashStoreFileIDToPrefixMap_.erase(leftFatherFileID);
                                    targetDeleteFileIDVec.push_back(leftFatherFileID);
                                    // delete right father
                                    objectFileMetaDataTrie_.at(rightFatherFilePrefixStr)->file_operation_func_ptr_->closeFile();
                                    delete objectFileMetaDataTrie_.at(rightFatherFilePrefixStr)->file_operation_func_ptr_;
                                    delete objectFileMetaDataTrie_.at(rightFatherFilePrefixStr);
                                    objectFileMetaDataTrie_.erase(rightFatherFilePrefixStr);
                                    hashStoreFileIDToPrefixMap_.erase(rightFatherFileID);
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
                                    currentRecoveryFileHandler->file_operation_func_ptr_->resetPointer(kEnd);
                                    currentRecoveryFileHandler->fileOperationMutex_.unlock();
                                    // update metadata
                                    objectFileMetaDataTrie_.insert(make_pair(currentFilePrefix, currentRecoveryFileHandler));
                                    hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt, currentFilePrefix));
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
                        currentRecoveryFileHandler->file_operation_func_ptr_->resetPointer(kEnd);
                        currentRecoveryFileHandler->fileOperationMutex_.unlock();
                        // update metadata
                        string targetRecoveryPrefixStr;
                        generateHashBasedPrefix(currentFileRecoveryMap.begin()->first, targetRecoveryPrefixStr);
                        objectFileMetaDataTrie_.insert(make_pair(targetRecoveryPrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_), currentRecoveryFileHandler));
                        hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt, targetRecoveryPrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_)));
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

                        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file header with unknown create reason, file path = " << targetOpenFileName << RESET << endl;

                        return false;
                    }
                }
            } else {
                // the file not in metadata, but ID smaller than committed ID, should delete
                targetDeleteFileIDVec.push_back(fileIDIt);
            }
        } else {
            // file exist in metadata
            debug_trace("ile ID = %lu exist in metadata, try skip or partial recovery\n", fileIDIt);
            // get metadata file
            hashStoreFileMetaDataHandler* currentIDInMetadataFileHandlerPtr;
            currentIDInMetadataFileHandlerPtr = objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(fileIDIt));
            uint64_t onDiskFileSize = currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->getFileSize();
            if (currentIDInMetadataFileHandlerPtr->total_object_bytes_ > onDiskFileSize) {
                // metadata size > filesystem size, error

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file ID = " << fileIDIt << ", file size in metadata = " << currentIDInMetadataFileHandlerPtr->total_object_bytes_ << " larger than file size in file system = " << onDiskFileSize << RESET << endl;

            } else if (currentIDInMetadataFileHandlerPtr->total_object_bytes_ < onDiskFileSize) {
                // file may append, should recovery

                debug_trace("target file id = %lu, file size (system) = %lu != file size (metadata) = %lu, try recovery\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes_);

                currentIDInMetadataFileHandlerPtr->fileOperationMutex_.lock();
                currentIDInMetadataFileHandlerPtr->file_ownership_flag_ = -1; // mark as during gc
                // start read
                int targetReadSize = onDiskFileSize;
                char readBuffer[targetReadSize];
                debug_trace("target read file content for recovery size = %lu\n", currentIDInMetadataFileHandlerPtr->total_object_bytes_);
                currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->resetPointer(kBegin);
                currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->readFile(readBuffer, targetReadSize);
                currentIDInMetadataFileHandlerPtr->file_operation_func_ptr_->resetPointer(kEnd);
                // read done, start process
                bool isGCFlushedDoneFlag = false;
                uint64_t recoveredObjectNumber = deconstructTargetRecoveryContentsFromFile(readBuffer + currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetReadSize - currentIDInMetadataFileHandlerPtr->total_object_bytes_, targetListForRedo, isGCFlushedDoneFlag);
                // update metadata
                currentIDInMetadataFileHandlerPtr->total_object_count_ += recoveredObjectNumber;
                currentIDInMetadataFileHandlerPtr->total_object_bytes_ += targetReadSize;
                currentIDInMetadataFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                currentIDInMetadataFileHandlerPtr->file_ownership_flag_ = 0;
                currentIDInMetadataFileHandlerPtr->fileOperationMutex_.unlock();

            } else {
                // file size match, skip current file
                debug_trace("target file id = %lu, file size (system) = %lu, file size (metadata) = %lu\n", fileIDIt, onDiskFileSize, currentIDInMetadataFileHandlerPtr->total_object_bytes_);
                continue;
            }
        }
    }
    // process not in metadata files created by split old file
    for (auto splitFileIt : mapForBatchedkGCFiles) {
        if (splitFileIt.second.size() != 2) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get file created by split gc number error, current file number = " << splitFileIt.second.size() << RESET << endl;

            return false;
        } else {
            // check split status;
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
                tempReadFileStream.openFile(targetOpenFileName);
                char readBuffer[currentFileSize];
                tempReadFileStream.readFile(readBuffer, currentFileSize);
                memcpy(&currentFileHeader, readBuffer, sizeof(hashStoreFileHeader));
                prefixBitNumber = currentFileHeader.current_prefix_used_bit_;
                uint64_t targetFileRemainReadSize = currentFileSize - sizeof(hashStoreFileHeader);
                tempReadFileStream.closeFile();
                // process file content
                objectNumberCount[i] = deconstructTargetRecoveryContentsFromFile(readBuffer + sizeof(hashStoreFileHeader), targetFileRemainReadSize, currentFileRecoveryMapTemp[i], gcFlagStatus[i]);
            }
            if (gcFlagStatus[0] && gcFlagStatus[1]) {
                // keep two new files
                for (int i = 0; i < 2; i++) {
                    hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                    currentRecoveryFileHandler->file_operation_func_ptr_ = new FileOperation(fileOperationMethod_);
                    currentRecoveryFileHandler->target_file_id_ = splitFileIt.second[i];
                    currentRecoveryFileHandler->current_prefix_used_bit_ = prefixBitNumber;
                    currentRecoveryFileHandler->total_object_count_ = objectNumberCount[i];
                    currentRecoveryFileHandler->total_object_bytes_ = targetFileRealSize[i];
                    // open current file for further usage
                    currentRecoveryFileHandler->fileOperationMutex_.lock();
                    currentRecoveryFileHandler->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(splitFileIt.second[i]) + ".delta");
                    currentRecoveryFileHandler->file_operation_func_ptr_->resetPointer(kEnd);
                    currentRecoveryFileHandler->fileOperationMutex_.unlock();
                    // update metadata
                    string targetRecoveryPrefixStr;
                    generateHashBasedPrefix(currentFileRecoveryMapTemp[i].begin()->first, targetRecoveryPrefixStr);
                    targetRecoveryPrefixStr = targetRecoveryPrefixStr.substr(0, prefixBitNumber);
                    objectFileMetaDataTrie_.insert(make_pair(targetRecoveryPrefixStr, currentRecoveryFileHandler));
                    hashStoreFileIDToPrefixMap_.insert(make_pair(splitFileIt.second[i], targetRecoveryPrefixStr));
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
                objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(splitFileIt.first))->file_operation_func_ptr_->closeFile();
                delete objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(splitFileIt.first))->file_operation_func_ptr_;
                delete objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(splitFileIt.first));
                objectFileMetaDataTrie_.erase(hashStoreFileIDToPrefixMap_.at(splitFileIt.first));
                hashStoreFileIDToPrefixMap_.erase(splitFileIt.first);
                targetDeleteFileIDVec.push_back(splitFileIt.first);
            } else {
                // keep old file
                targetDeleteFileIDVec.push_back(splitFileIt.second[0]);
                targetDeleteFileIDVec.push_back(splitFileIt.second[1]);
            }
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
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveFileName << RESET << endl;
            return false;
        } else {
            debug_trace("delete the obsolete delta file, file path = %s\n", targetRemoveFileName.c_str());
            continue;
        }
    }
    // debug check metadata
    for (auto fileIDIt : hashStoreFileIDToPrefixMap_) {
        debug_trace("File ID = %lu, prefix = %s, file size in filesystem = %lu, file size in metadata = %lu\n", fileIDIt.first, fileIDIt.second.c_str(), objectFileMetaDataTrie_.at(fileIDIt.second)->file_operation_func_ptr_->getFileSize(), objectFileMetaDataTrie_.at(fileIDIt.second)->total_object_bytes_);
    }
    debug_trace("Current prefix to file map size = %lu, file ID to prefix map size =  %lu\n", objectFileMetaDataTrie_.size(), hashStoreFileIDToPrefixMap_.size());
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
            return true;
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): create hashStore file metadata list error" << RESET << endl;

            return false;
        }
    }
    ifstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(
        workingDir_ + "/hashStoreFileManifest." + currentPointerStr, ios::in);
    string currentLineStr;
    if (hashStoreFileManifestStream.is_open()) {
        getline(hashStoreFileManifestStream, currentLineStr);
        targetNewFileID_ = stoull(currentLineStr); // update next file ID from metadata
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
            currentFileHandlerPtr->file_operation_func_ptr_->resetPointer(kEnd);
            currentFileHandlerPtr->fileOperationMutex_.unlock();
            // re-insert into trie and map for build index
            objectFileMetaDataTrie_.insert(make_pair(prefixHashStr, currentFileHandlerPtr));
            hashStoreFileIDToPrefixMap_.insert(make_pair(hashStoreFileID, prefixHashStr));
        }
    } else {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open hashStore file metadata list (manifest)" << RESET << endl;

        return false;
    }
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList()
{
    fstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "/hashStoreFileManifest.pointer", ios::in);
    uint64_t currentPointerInt = 0;
    if (hashStoreFileManifestPointerStream.is_open()) {
        hashStoreFileManifestPointerStream >> currentPointerInt;
        currentPointerInt++;
    } else {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;

        return false;
    }
    hashStoreFileManifestPointerStream.close();
    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    vector<uint64_t> targetDeleteFileIDVec;
    if (objectFileMetaDataTrie_.size() != 0) {
        for (auto it : objectFileMetaDataTrie_) {
            if (it.second->gc_result_status_flag_ == kShouldDelete) {
                it.second->file_operation_func_ptr_->closeFile();
                targetDeleteFileIDVec.push_back(it.second->target_file_id_);
                // skip deleted file
                continue;
            }
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << endl;
            hashStoreFileManifestStream << it.second->target_file_id_ << endl;
            hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << it.second->total_object_count_ << endl;
            hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
            it.second->fileOperationMutex_.lock();
            it.second->file_operation_func_ptr_->flushFile();
            debug_trace("flushed file id = %lu, file correspond prefix = %s\n", it.second->target_file_id_, hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_).c_str());
            it.second->fileOperationMutex_.unlock();
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

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
            }
        }
        for (auto removeFileIDIt : targetDeleteFileIDVec) {
            string targetRemoveBucketFileName = workingDir_ + "/" + to_string(removeFileIDIt) + ".delta";
            auto removeOldBucketStatus = remove(targetRemoveBucketFileName.c_str());
            if (removeOldBucketStatus == -1) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old bucket file, file path = " << targetRemoveBucketFileName << RESET << endl;
            }
        }
        return true;
    } else {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not update hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;

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
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
        return false;
    }
    hashStoreFileManifestPointerStream.close();
    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt), ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    vector<uint64_t> targetDeleteFileIDVec;
    if (objectFileMetaDataTrie_.size() != 0) {
        for (auto it : objectFileMetaDataTrie_) {
            if (it.second->gc_result_status_flag_ == kShouldDelete) {
                targetDeleteFileIDVec.push_back(it.second->target_file_id_);
                it.second->file_operation_func_ptr_->closeFile();
                // skip and delete should deleted files
                continue;
            }
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << endl;
            hashStoreFileManifestStream << it.second->target_file_id_ << endl;
            hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << it.second->total_object_count_ << endl;
            hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
            it.second->fileOperationMutex_.lock();
            it.second->file_operation_func_ptr_->flushFile();
            it.second->file_operation_func_ptr_->closeFile();
            it.second->fileOperationMutex_.unlock();
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
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
            }
        }
        for (auto removeFileIDIt : targetDeleteFileIDVec) {
            string targetRemoveBucketFileName = workingDir_ + "/" + to_string(removeFileIDIt) + ".delta";
            auto removeOldBucketStatus = remove(targetRemoveBucketFileName.c_str());
            if (removeOldBucketStatus == -1) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old bucket file, file path = " << targetRemoveBucketFileName << RESET << endl;
            }
        }
        for (auto it : objectFileMetaDataTrie_) {
            delete it.second->file_operation_func_ptr_;
            delete it.second;
        }
        return true;
    } else {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not update hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
        for (auto it : objectFileMetaDataTrie_) {
            delete it.second->file_operation_func_ptr_;
            delete it.second;
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
        hashStoreFileManifestPointerStream << currentPointerInt;
        hashStoreFileManifestPointerStream.close();
    } else {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open hashStore file metadata list pointer file for create" << RESET << endl;

        return false;
    }

    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt),
        ios::out);
    hashStoreFileManifestStream << targetNewFileID_ << endl; // flush nextFileIDInfo
    if (objectFileMetaDataTrie_.size() != 0) {
        for (auto it : objectFileMetaDataTrie_) {
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << endl;
            hashStoreFileManifestStream << it.second->target_file_id_ << endl;
            hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << it.second->total_object_count_ << endl;
            hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        return true;
    } else {
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        return true;
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

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): generate prefix hash for current key error, key = " << keyStr << RESET << endl;

        return false;
    }
    while (true) {
        uint64_t fileHandlerUsedPrefixLength = getHashStoreFileHandlerStatusAndPrefixLenInUseByPrefix(prefixStr);
        if (fileHandlerUsedPrefixLength == 0 && opType == kGet) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get operation meet not stored buckets, key = " << keyStr << RESET << endl;

            return false;
        } else if (fileHandlerUsedPrefixLength == 0 && opType == kPut) {
            bool createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr, initialTrieBitNumber_, false, 0);
            if (!createNewFileHandlerStatus) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): create new bucket for put operation error, key = " << keyStr << RESET << endl;

                return false;
            } else {
                fileHandlerPtr->file_ownership_flag_ = 1;
                return true;
            }
        } else if (fileHandlerUsedPrefixLength >= initialTrieBitNumber_ && fileHandlerUsedPrefixLength <= maxTrieBitNumber_) {
            bool getFileHandlerStatus = getHashStoreFileHandlerByPrefix(prefixStr, fileHandlerUsedPrefixLength, fileHandlerPtr);
            if (!getFileHandlerStatus) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get existing bucket file handler for put/get operation error, key = " << keyStr << RESET << endl;

                return false;
            } else {
                // avoid get file handler which is in GC;
                while (fileHandlerPtr->file_ownership_flag_ == -1) {
                    asm volatile("");
                    // wait if file is using in gc
                }
                if (fileHandlerPtr->gc_result_status_flag_ == kShouldDelete) {
                    // retry if the file should delete;
                    continue;
                } else {
                    fileHandlerPtr->file_ownership_flag_ = 1;
                    return true;
                }
            }
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get used prefix hash length in tire error, returned length = " << fileHandlerUsedPrefixLength << RESET << endl;

            return false;
        }
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

uint64_t HashStoreFileManager::getHashStoreFileHandlerStatusAndPrefixLenInUseByPrefix(const string prefixStr)
{
    for (auto prefixLength = maxTrieBitNumber_; prefixLength >= initialTrieBitNumber_; prefixLength--) {
        if (objectFileMetaDataTrie_.find(prefixStr.substr(0, prefixLength)) != objectFileMetaDataTrie_.end()) {
            return prefixLength;
        }
    }
    return 0;
}

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(const string prefixStr, uint64_t prefixUsageLength, hashStoreFileMetaDataHandler*& fileHandlerPtr)
{
    fileHandlerPtr = objectFileMetaDataTrie_.at(prefixStr.substr(0, prefixUsageLength));
    return true;
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
    // set up new file header for write
    hashStoreFileHeader newFileHeader;
    newFileHeader.current_prefix_used_bit_ = prefixBitNumber;
    if (createByGCFlag == true) {
        newFileHeader.previous_file_id_ = previousFileID;
        newFileHeader.file_create_reason_ = kGCFile;
    } else {
        newFileHeader.previous_file_id_ = 0xffffffffffffffff;
        newFileHeader.file_create_reason_ = kNewFile;
    }
    newFileHeader.file_id_ = currentFileHandlerPtr->target_file_id_;
    char fileHeaderWriteBuffer[sizeof(newFileHeader)];
    memcpy(fileHeaderWriteBuffer, &newFileHeader, sizeof(newFileHeader));
    // write header to current file
    currentFileHandlerPtr->fileOperationMutex_.lock();
    debug_trace("in lock, created file id = %lu\n", currentFileHandlerPtr->target_file_id_);
    currentFileHandlerPtr->file_operation_func_ptr_->createFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
    currentFileHandlerPtr->file_operation_func_ptr_->closeFile();
    currentFileHandlerPtr->file_operation_func_ptr_->openFile(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta");
    currentFileHandlerPtr->file_operation_func_ptr_->writeFile(fileHeaderWriteBuffer, sizeof(newFileHeader));
    currentFileHandlerPtr->total_object_bytes_ += sizeof(newFileHeader);
    currentFileHandlerPtr->fileOperationMutex_.unlock();
    // move pointer for return
    objectFileMetaDataTrie_.insert(make_pair(prefixStr.substr(0, prefixBitNumber), currentFileHandlerPtr));
    hashStoreFileIDToPrefixMap_.insert(make_pair(currentFileHandlerPtr->target_file_id_, prefixStr.substr(0, prefixBitNumber)));
    fileHandlerPtr = currentFileHandlerPtr;
    return true;
}

bool HashStoreFileManager::getOrCreateHashStoreFileHandlerByKeyStrForSplitGC(const string keyStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t targetPrefixLen, uint64_t previousFileID)
{
    string prefixStr;
    bool genPrefixStatus = generateHashBasedPrefix(keyStr, prefixStr);
    if (!genPrefixStatus) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): generate prefix hash for current key error, key = " << keyStr << RESET << endl;

        return false;
    }
    bool getFileHandlerStatus = getHashStoreFileHandlerByPrefix(prefixStr, targetPrefixLen, fileHandlerPtr);
    if (!getFileHandlerStatus) {
        bool createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr, targetPrefixLen, true, previousFileID);
        if (!createNewFileHandlerStatus) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): create new bucket for put operation error, key = " << keyStr << RESET << endl;

            return false;
        } else {
            fileHandlerPtr->file_ownership_flag_ = 1;
            return true;
        }
    } else {
        // avoid get file handler which is in GC;
        while (fileHandlerPtr->file_ownership_flag_ == -1) {
            asm volatile("");
            // wait if file is using in gc
        }
        fileHandlerPtr->file_ownership_flag_ = 1;
        return true;
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
        string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        if (currentObjectRecordHeader.is_anchor_ == true) {
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                processedKeepObjectNumber -= (resultMap.at(currentKeyStr).size());
                resultMap.at(currentKeyStr).clear();
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
    debug_trace("deconstruct current file header done, file ID = %lu, create reason = %u, prefix length used in this file = %lu, target process file size = %lu, find different key number = %lu, total processed object number = %lu, target keep object number = %lu\n", currentFileHeader.file_id_, currentFileHeader.file_create_reason_, currentFileHeader.current_prefix_used_bit_, fileSize, resultMap.size(), processedTotalObjectNumber, processedKeepObjectNumber);
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
            debug_info("new file request for GC, file id = %lu\n", currentHandlerPtr->target_file_id_);

            currentHandlerPtr->file_ownership_flag_ = -1;
            // read contents
            char readWriteBuffer[currentHandlerPtr->total_object_bytes_];
            currentHandlerPtr->fileOperationMutex_.lock();
            debug_trace("target read file content for gc size = %lu\n", currentHandlerPtr->total_object_bytes_);
            currentHandlerPtr->file_operation_func_ptr_->resetPointer(kBegin);
            currentHandlerPtr->file_operation_func_ptr_->readFile(readWriteBuffer, currentHandlerPtr->total_object_bytes_);
            currentHandlerPtr->file_operation_func_ptr_->resetPointer(kEnd);

            // process GC contents
            unordered_map<string, vector<string>> gcResultMap;
            pair<uint64_t, uint64_t> remainObjectNumberPair = deconstructAndGetValidContentsFromFile(readWriteBuffer, currentHandlerPtr->total_object_bytes_, gcResultMap);
            if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
                debug_info("File id = %lu reclain empty space fail, try split\n", currentHandlerPtr->target_file_id_);

                // no space could be reclaimed, may request split
                if (gcResultMap.size() == 1 && currentHandlerPtr->gc_result_status_flag_ != kMayGC) {
                    // keep tracking until forced gc threshold;
                    currentHandlerPtr->gc_result_status_flag_ = kMayGC;
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    continue;
                } else if (gcResultMap.size() == 1 && currentHandlerPtr->gc_result_status_flag_ == kMayGC) {
                    // Mark this file as could not GC;
                    currentHandlerPtr->gc_result_status_flag_ = kNoGC;
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    continue;
                } else if (gcResultMap.size() != 1 && currentHandlerPtr->gc_result_status_flag_ == kNew) {
                    // perform split into two buckets via extend prefix bit (+1)
                    uint64_t currentUsedPrefixBitNumber = currentHandlerPtr->current_prefix_used_bit_;
                    uint64_t targetPrefixBitNumber = currentUsedPrefixBitNumber + 1;
                    for (auto keyIt : gcResultMap) {
                        string currentPrefixStr;
                        bool generatePrefixStatus = generateHashBasedPrefix(keyIt.first, currentPrefixStr);
                        if (generatePrefixStatus == false) {
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could generate prefix hash for key = " << keyIt.first << " gc error and skip the file's gc" << RESET << endl;
                            break;
                        } else {
                            hashStoreFileMetaDataHandler* currentFileHandlerPtr;
                            bool getFileHandlerStatus = getOrCreateHashStoreFileHandlerByKeyStrForSplitGC(currentPrefixStr, currentFileHandlerPtr, targetPrefixBitNumber, currentHandlerPtr->target_file_id_);
                            if (getFileHandlerStatus == false) {

                                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could get/create new bucket for prefix hash = " << currentPrefixStr << " gc error and skip the file's gc" << RESET << endl;

                                break;
                            } else {
                                currentFileHandlerPtr->fileOperationMutex_.lock();
                                currentFileHandlerPtr->file_ownership_flag_ = -1;
                                uint64_t targetWriteSize = 0;
                                for (auto valueIt : keyIt.second) {
                                    targetWriteSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                                }
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
                                currentFileHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetWriteSize + sizeof(hashStoreRecordHeader));
                                currentFileHandlerPtr->total_object_bytes_ += sizeof(hashStoreRecordHeader);
                                currentFileHandlerPtr->total_object_count_++;
                                currentFileHandlerPtr->file_operation_func_ptr_->flushFile();
                                debug_trace("flushed new file to filesystem since split gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHandlerPtr->target_file_id_, currentHandlerPtr->target_file_id_);
                                currentFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                                currentFileHandlerPtr->file_ownership_flag_ = 0;
                                currentFileHandlerPtr->fileOperationMutex_.unlock();
                            }
                        }
                    }
                    currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    // update metadata (delete the previous file's meta)
                    string tempDeletedFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentHandlerPtr->target_file_id_);
                    hashStoreFileIDToPrefixMap_.erase(currentHandlerPtr->target_file_id_);
                    delete objectFileMetaDataTrie_.at(tempDeletedFilePrefixStr)->file_operation_func_ptr_;
                    delete objectFileMetaDataTrie_.at(tempDeletedFilePrefixStr);
                    objectFileMetaDataTrie_.erase(tempDeletedFilePrefixStr);
                    continue;
                }
            } else {
                debug_info("File id =  %lu reclaim empty space success, start re-write\n", currentHandlerPtr->target_file_id_);

                // reclaimed space success, rewrite current file to new file
                currentHandlerPtr->file_operation_func_ptr_->closeFile();
                uint64_t targetFileSize = 0;
                for (auto keyIt : gcResultMap) {
                    for (auto valueIt : keyIt.second) {
                        targetFileSize += (sizeof(hashStoreRecordHeader) + keyIt.first.size() + valueIt.size());
                    }
                }
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
                debug_trace("processed buffer size = %lu, total target write size = %lu\n", currentProcessLocationIndex, targetFileSize);
                string targetOpenFileName = workingDir_ + "/" + to_string(currentFileHeader.file_id_) + ".delta";
                // create since file not exist
                currentHandlerPtr->file_operation_func_ptr_->createFile(targetOpenFileName);
                currentHandlerPtr->file_operation_func_ptr_->closeFile();
                // write content and update current file stream to new one.
                currentHandlerPtr->file_operation_func_ptr_->openFile(targetOpenFileName);
                currentHandlerPtr->file_operation_func_ptr_->writeFile(currentWriteBuffer, targetFileSize);
                currentHandlerPtr->file_operation_func_ptr_->flushFile();
                // update metadata
                currentHandlerPtr->target_file_id_ = currentFileHeader.file_id_;
                currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                currentHandlerPtr->total_object_count_ = newObjectNumber + 1;
                currentHandlerPtr->total_object_bytes_ = currentProcessLocationIndex + sizeof(hashStoreRecordHeader);
                currentHandlerPtr->file_ownership_flag_ = 0;
                currentHandlerPtr->fileOperationMutex_.unlock();
                // remove old file
                string originalPrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_);
                hashStoreFileIDToPrefixMap_.erase(currentFileHeader.previous_file_id_);
                hashStoreFileIDToPrefixMap_.insert(make_pair(currentFileHeader.file_id_, originalPrefixStr));
                debug_trace("flushed new file to filesystem since single file gc, the new file ID = %lu, corresponding previous file ID = %lu\n", currentFileHeader.file_id_, currentFileHeader.previous_file_id_);
                continue;
            }
        }
    }
    return;
}

void HashStoreFileManager::scheduleMetadataUpdateWorker()
{
    while (true) {
        if (operationCounterForMetadataCommit_ >= operationNumberForMetadataCommitThreshold_) {
            if (UpdateHashStoreFileMetaDataList() != true) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): commit metadata for " << operationCounterForMetadataCommit_ << " operations error" << RESET << endl;
            } else {
                debug_info("commit metadata for %lu operations success\n", operationCounterForMetadataCommit_);
                operationCounterForMetadataCommit_ = 0;
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
    for (auto fileHandlerIt : objectFileMetaDataTrie_) {
        notifyGCMQ_->push(fileHandlerIt.second);
    }
    while (notifyGCMQ_->isEmpty() != true) {
        asm volatile("");
        // wait for gc job done
    }
    return true;
}
}