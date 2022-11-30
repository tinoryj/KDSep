#include "hashBasedStore/hashStoreFileManager.hpp"
#include <unordered_map>

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(uint64_t initialBitNumber, uint64_t maxBitNumber, uint64_t objectGCTriggerSize,
    uint64_t objectGlobalGCTriggerSize, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* notifyGCMQ)
{
    initialTrieBitNumber_ = initialBitNumber;
    maxTrieBitNumber_ = maxBitNumber;
    singleFileGCTriggerSize_ = objectGCTriggerSize;
    globalGCTriggerSize_ = objectGlobalGCTriggerSize;
    workingDir_ = workingDirStr;
    notifyGCMQ_ = notifyGCMQ;
    RetriveHashStoreFileMetaDataList();
}

HashStoreFileManager::~HashStoreFileManager()
{
    CloseHashStoreFileMetaDataList();
    notifyGCMQ_->done_ = true;
    delete notifyGCMQ_;
}

// Recovery
uint64_t HashStoreFileManager::deconstructTargetRecoveryContentsFromFile(char* fileContentBuffer, uint64_t fileSize, unordered_map<string, vector<pair<bool, string>>>& resultMap)
{
    uint64_t processedTotalObjectNumber = 0;
    uint64_t currentProcessLocationIndex = 0;

    while (currentProcessLocationIndex != fileSize) {
        processedTotalObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): deconstruct current record header done, record is anchor flag = " << currentObjectRecordHeader.is_anchor_ << ", key size = " << currentObjectRecordHeader.key_size_ << ", value size = " << currentObjectRecordHeader.value_size_ << RESET << endl;
        currentProcessLocationIndex += sizeof(currentObjectRecordHeader);
        string currentKeyStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.key_size_);
        if (currentObjectRecordHeader.is_anchor_ == true) {
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
        } else {
            if (resultMap.find(currentKeyStr) != resultMap.end()) {
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                resultMap.at(currentKeyStr).push_back(make_pair(true, currentValueStr));
                currentProcessLocationIndex += currentObjectRecordHeader.value_size_;
                continue;
            } else {
                vector<pair<bool, string>> newValuesRelatedToCurrentKeyVec;
                currentProcessLocationIndex += currentObjectRecordHeader.key_size_;
                string currentValueStr(fileContentBuffer + currentProcessLocationIndex, currentObjectRecordHeader.value_size_);
                newValuesRelatedToCurrentKeyVec.push_back(make_pair(true, currentValueStr));
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
    - file ID > next ID:
        - kNew file -> add file to metadata
        - kGC file:
            - previous ID in metadata && prefix bit number equal -> (after single file gc) delete previous file, add current file.
            - previous ID in metadata && prefix bit number not equal -> (after split or merge):
                - Buffer all these files with same previous file ID
                - Delete previous file handler and file, add current files
            - previous ID not in metadata -> error; single file should not deleted; split/merge file should only be deleted after metadata commit
    - file ID < next ID:
        - should be single file after gc or not deleted files after commit- > delete files
*/

bool HashStoreFileManager::recoveryFromFailure(unordered_map<string, vector<pair<bool, string>>>& targetListForRedo) // return key to isAnchor + value pair
{
    unordered_map<uint64_t, filesystem::directory_entry> scannedFileList;
    // scan file list
    for (const auto& dirEntry : filesystem::recursive_directory_iterator(workingDir_)) {
        cout << dirEntry << endl;
        string currentFilePath = dirEntry.path();
        if (currentFilePath.find(".delta") != string::npos) {
            currentFilePath = currentFilePath.substr(currentFilePath.find("/") + 1);
            uint64_t currentFileID = stoull(currentFilePath);
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find file name = " << currentFilePath << ", file ID in int = " << currentFileID << RESET << endl;
            scannedFileList.insert(make_pair(currentFileID, dirEntry));
        }
    }
    unordered_map<uint64_t, vector<pair<uint64_t, filesystem::directory_entry>>> mapForBatchedkGCFiles; // prefix file ID to new file ID and file map
    // process files
    for (auto fileIDIt : scannedFileList) {
        if (hashStoreFileIDToPrefixMap_.find(fileIDIt.first) == hashStoreFileIDToPrefixMap_.end()) {
            // file not exist in metadata, should scan and update into metadata
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file ID in int = " << fileIDIt.first << " not exist in metadata, try recovery" << RESET << endl;
            if (fileIDIt.first > targetNewFileID_) {
                // the file is newly created, should scan
                fstream tempReadFileStream;
                string targetOpenFileName = workingDir_ + "/" + to_string(fileIDIt.first) + ".delta";
                tempReadFileStream.open(targetOpenFileName, ios::in | ios::binary);
                if (tempReadFileStream.is_open() == false) {
                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not open file for recovery, file path = " << targetOpenFileName << RESET << endl;
                    return false;
                } else {
                    // read file header for check
                    hashStoreFileHeader currentFileHeader;
                    char readBuffer[sizeof(hashStoreFileHeader)];
                    tempReadFileStream.read(readBuffer, sizeof(hashStoreFileHeader));
                    memcpy(&currentFileHeader, readBuffer, sizeof(hashStoreFileHeader));
                    if (currentFileHeader.file_create_reason_ == kGCFile) {
                        // GC file with ID > targetNewFileID
                        uint64_t targetFileRemainReadSize = fileIDIt.second.file_size() - sizeof(hashStoreFileHeader);
                        char readContentBuffer[targetFileRemainReadSize];
                        tempReadFileStream.open(targetOpenFileName, ios::in | ios::binary);
                        tempReadFileStream.read(readContentBuffer, targetFileRemainReadSize);
                        tempReadFileStream.close();
                        unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMap;
                        uint64_t currentFileObjectNumber = deconstructTargetRecoveryContentsFromFile(readContentBuffer, targetFileRemainReadSize, currentFileRecoveryMap);
                        if (hashStoreFileIDToPrefixMap_.find(currentFileHeader.previous_file_id_) == hashStoreFileIDToPrefixMap_.end()) {
                            // previous ID not in metadata -> error;
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): find kGC file that previous file ID not in metadata, seems error, previous file id = " << currentFileHeader.previous_file_id_ << RESET << endl;
                            return false;
                        } else {
                            // previous ID in metadata -> check prefix bit number;
                            if (objectFileMetaDataTrie_.find(hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_)) == objectFileMetaDataTrie_.end()) {
                                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): trie and map mismatch" << RESET << endl;
                                return false;
                            } else {
                                if (objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_))->current_prefix_used_bit_ == currentFileHeader.current_prefix_used_bit_) {
                                    // prefix bit number match, should delete old file and keep this one
                                    string currentFilePrefix = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_);
                                    objectFileMetaDataTrie_.at(currentFilePrefix)->file_operation_stream_.close();
                                    objectFileMetaDataTrie_.at(currentFilePrefix)->target_file_id_ = fileIDIt.first;
                                    string targetRemoveFileName = workingDir_ + "/" + to_string(currentFileHeader.previous_file_id_) + ".delta";
                                    auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
                                    if (removeObsoleteFileStatus == -1) {
                                        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveFileName << RESET << endl;
                                        return false;
                                    } else {
                                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveFileName << RESET << endl;
                                        hashStoreFileIDToPrefixMap_.erase(currentFileHeader.previous_file_id_);
                                        hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt.first, currentFilePrefix));
                                        objectFileMetaDataTrie_.at(currentFilePrefix)->total_object_count_ = currentFileObjectNumber;
                                        objectFileMetaDataTrie_.at(currentFilePrefix)->total_object_bytes_ = fileIDIt.second.file_size();
                                    }
                                } else {
                                    // prefix bit number not match, created by split/merge, should cache for further process
                                    if (objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_))->current_prefix_used_bit_ > currentFileHeader.current_prefix_used_bit_) {
                                        // file created by merge, delete old files with current prefix length +1
                                        string leftFatherFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_); // we store left file as previous file id
                                        string rightFatherFilePrefixStr = hashStoreFileIDToPrefixMap_.at(currentFileHeader.previous_file_id_).substr(0, currentFileHeader.current_prefix_used_bit_) + "0";
                                        uint64_t leftFatherFileID = objectFileMetaDataTrie_.at(leftFatherFilePrefixStr)->target_file_id_;
                                        uint64_t rightFatherFileID = objectFileMetaDataTrie_.at(rightFatherFilePrefixStr)->target_file_id_;
                                        // delete left father
                                        objectFileMetaDataTrie_.at(leftFatherFilePrefixStr)->file_operation_stream_.close();
                                        delete objectFileMetaDataTrie_.at(leftFatherFilePrefixStr);
                                        objectFileMetaDataTrie_.erase(leftFatherFilePrefixStr);
                                        hashStoreFileIDToPrefixMap_.erase(leftFatherFileID);
                                        string targetRemoveLeftFileName = workingDir_ + "/" + to_string(leftFatherFileID) + ".delta";
                                        auto removeLeftObsoleteFileStatus = remove(targetRemoveLeftFileName.c_str());
                                        if (removeLeftObsoleteFileStatus == -1) {
                                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveLeftFileName << RESET << endl;
                                            return false;
                                        } else {
                                            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveLeftFileName << RESET << endl;
                                        }
                                        // delete right father
                                        objectFileMetaDataTrie_.at(rightFatherFilePrefixStr)->file_operation_stream_.close();
                                        delete objectFileMetaDataTrie_.at(rightFatherFilePrefixStr);
                                        objectFileMetaDataTrie_.erase(rightFatherFilePrefixStr);
                                        hashStoreFileIDToPrefixMap_.erase(rightFatherFileID);
                                        string targetRemoveRightFileName = workingDir_ + "/" + to_string(leftFatherFileID) + ".delta";
                                        auto removeObsoleteRightFileStatus = remove(targetRemoveRightFileName.c_str());
                                        if (removeObsoleteRightFileStatus == -1) {
                                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveRightFileName << RESET << endl;
                                            return false;
                                        } else {
                                            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveRightFileName << RESET << endl;
                                        }
                                        // insert new file into metadata
                                        string currentFilePrefix = leftFatherFilePrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_);
                                        hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                                        currentRecoveryFileHandler->target_file_id_ = fileIDIt.first;
                                        currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                                        currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                                        currentRecoveryFileHandler->total_object_bytes_ = fileIDIt.second.file_size();
                                        // open current file for further usage
                                        currentRecoveryFileHandler->fileOperationMutex_.lock();
                                        currentRecoveryFileHandler->file_operation_stream_.open(workingDir_ + "/" + to_string(fileIDIt.first) + ".delta", ios::in | ios::out | ios::binary);
                                        currentRecoveryFileHandler->fileOperationMutex_.unlock();
                                        // update metadata
                                        objectFileMetaDataTrie_.insert(make_pair(currentFilePrefix, currentRecoveryFileHandler));
                                        hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt.first, currentFilePrefix));
                                    } else {
                                        // file created by split, cache to find the other one
                                        if (mapForBatchedkGCFiles.find(currentFileHeader.previous_file_id_) == mapForBatchedkGCFiles.end()) {
                                            vector<pair<uint64_t, filesystem::directory_entry>> tempVec;
                                            tempVec.push_back(make_pair(fileIDIt.first, fileIDIt.second));
                                            mapForBatchedkGCFiles.insert(make_pair(currentFileHeader.previous_file_id_, tempVec));
                                        } else {
                                            mapForBatchedkGCFiles.at(currentFileHeader.previous_file_id_).push_back(make_pair(fileIDIt.first, fileIDIt.second));
                                        }
                                    }
                                }
                            }
                        }
                        string targetRemoveFileName = workingDir_ + "/" + to_string(fileIDIt.first) + ".delta";
                        auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
                        if (removeObsoleteFileStatus == -1) {
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveFileName << RESET << endl;
                            return false;
                        } else {
                            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveFileName << RESET << endl;
                        }
                    } else if (currentFileHeader.file_create_reason_ == kNewFile) {
                        // new file with ID > targetNewFileID, should add into metadata
                        uint64_t targetFileRemainReadSize = fileIDIt.second.file_size() - sizeof(hashStoreFileHeader);
                        char readContentBuffer[targetFileRemainReadSize];
                        tempReadFileStream.read(readContentBuffer, targetFileRemainReadSize);
                        tempReadFileStream.close();
                        unordered_map<string, vector<pair<bool, string>>> currentFileRecoveryMap;
                        uint64_t currentFileObjectNumber = deconstructTargetRecoveryContentsFromFile(readContentBuffer, targetFileRemainReadSize, currentFileRecoveryMap);
                        hashStoreFileMetaDataHandler* currentRecoveryFileHandler = new hashStoreFileMetaDataHandler;
                        currentRecoveryFileHandler->target_file_id_ = fileIDIt.first;
                        currentRecoveryFileHandler->current_prefix_used_bit_ = currentFileHeader.current_prefix_used_bit_;
                        currentRecoveryFileHandler->total_object_count_ = currentFileObjectNumber;
                        currentRecoveryFileHandler->total_object_bytes_ = fileIDIt.second.file_size();
                        // open current file for further usage
                        currentRecoveryFileHandler->fileOperationMutex_.lock();
                        currentRecoveryFileHandler->file_operation_stream_.open(workingDir_ + "/" + to_string(fileIDIt.first) + ".delta", ios::in | ios::out | ios::binary);
                        currentRecoveryFileHandler->fileOperationMutex_.unlock();
                        // update metadata
                        string targetRecoveryPrefixStr;
                        generateHashBasedPrefix(currentFileRecoveryMap.begin()->first, targetRecoveryPrefixStr);
                        objectFileMetaDataTrie_.insert(make_pair(targetRecoveryPrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_), currentRecoveryFileHandler));
                        hashStoreFileIDToPrefixMap_.insert(make_pair(fileIDIt.first, targetRecoveryPrefixStr.substr(0, currentFileHeader.current_prefix_used_bit_)));
                    } else {
                        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file header with unknown create reason, file path = " << targetOpenFileName << RESET << endl;
                        return false;
                    }
                }
            } else {
                // the file not in metadata, but ID smaller than committed ID, should delete
                string targetRemoveFileName = workingDir_ + "/" + to_string(fileIDIt.first) + ".delta";
                auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
                if (removeObsoleteFileStatus == -1) {
                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the obsolete file, file path = " << targetRemoveFileName << RESET << endl;
                    return false;
                } else {
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveFileName << RESET << endl;
                }
            }
        } else {
            // file exist in metadata
            if (objectFileMetaDataTrie_.find(hashStoreFileIDToPrefixMap_.at(fileIDIt.first)) == objectFileMetaDataTrie_.end()) {
                // metadata not consistent, error retrive metadata
                cout << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file ID in int = " << fileIDIt.first << ", prefix = " << hashStoreFileIDToPrefixMap_.at(fileIDIt.first) << " not exist in metadata trie" << RESET << endl;
                return false;
            } else {
                // get current file
                hashStoreFileMetaDataHandler* currentFileHandlerPtr;
                currentFileHandlerPtr = objectFileMetaDataTrie_.at(hashStoreFileIDToPrefixMap_.at(fileIDIt.first));
                if (currentFileHandlerPtr->total_object_bytes_ != fileIDIt.second.file_size()) {
                    // file size mismatch, should recovery
                    if (currentFileHandlerPtr->total_object_bytes_ > fileIDIt.second.file_size()) {
                        // file size in metadata should not larger than file size in file system
                        cout << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file ID in int = " << fileIDIt.first << ", prefix = " << hashStoreFileIDToPrefixMap_.at(fileIDIt.first) << " file size in metadata = " << currentFileHandlerPtr->total_object_bytes_ << " larger than file size in file system = " << fileIDIt.second.file_size() << RESET << endl;
                        return false;
                    } else {
                        // start recovery
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target file id = " << fileIDIt.first << ", file size (system) = " << fileIDIt.second.file_size() << " != file size (metadata) = " << currentFileHandlerPtr->total_object_bytes_ << ", try recovery" << RESET << endl;
                        currentFileHandlerPtr->fileOperationMutex_.lock();
                        currentFileHandlerPtr->file_ownership_flag_ = -1;
                        // start read
                        int targetReadSize = fileIDIt.second.file_size() - currentFileHandlerPtr->total_object_bytes_;
                        char readBuffer[targetReadSize];
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for recovery size = " << currentFileHandlerPtr->total_object_bytes_ << ", current file read pointer = " << currentFileHandlerPtr->file_operation_stream_.tellg() << ", current file write pointer = " << currentFileHandlerPtr->file_operation_stream_.tellp() << RESET << endl;
                        currentFileHandlerPtr->file_operation_stream_.seekg(currentFileHandlerPtr->total_object_bytes_, ios::beg);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for recovery after reset file read pointer = " << currentFileHandlerPtr->file_operation_stream_.tellg() << RESET << endl;
                        currentFileHandlerPtr->file_operation_stream_.read(readBuffer, targetReadSize);
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for recovery after read file read pointer = " << currentFileHandlerPtr->file_operation_stream_.tellg() << RESET << endl;
                        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file content buffer size for recovery = " << sizeof(readBuffer) << RESET << endl;
                        currentFileHandlerPtr->file_operation_stream_.seekp(0, ios::end);
                        // read done, start process
                        uint64_t recoveredObjectNumber = deconstructTargetRecoveryContentsFromFile(readBuffer, targetReadSize, targetListForRedo);
                        // update metadata
                        currentFileHandlerPtr->total_object_count_ += recoveredObjectNumber;
                        currentFileHandlerPtr->total_object_bytes_ += targetReadSize;
                        currentFileHandlerPtr->file_ownership_flag_ = 0;
                        currentFileHandlerPtr->fileOperationMutex_.unlock();
                    }
                } else {
                    // file size match, skip current file
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target file id = " << fileIDIt.first << ", file size (system) = " << fileIDIt.second.file_size() << " = file size (metadata) = " << currentFileHandlerPtr->total_object_bytes_ << RESET << endl;
                    continue;
                }
            }
        }
    }
    // process not in metadata files created by split old file
    for (auto splitFileIt : mapForBatchedkGCFiles) {
    }
    return true;
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
            currentFileHandlerPtr->target_file_id_ = hashStoreFileID;
            currentFileHandlerPtr->current_prefix_used_bit_ = currentFileUsedPrefixLength;
            currentFileHandlerPtr->total_object_count_ = currentFileStoredObjectCount;
            currentFileHandlerPtr->total_object_bytes_ = currentFileStoredBytes;
            // open current file for further usage
            currentFileHandlerPtr->fileOperationMutex_.lock();
            currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta", ios::in | ios::out | ios::binary);
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
    if (objectFileMetaDataTrie_.size() != 0) {
        for (auto it : objectFileMetaDataTrie_) {
            if (it.second->gc_result_status_flag_ == kShouldDelete) {
                it.second->file_operation_stream_.close();
                string targetRemoveFileName = workingDir_ + "/" + to_string(it.second->target_file_id_) + ".delta";
                auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
                if (removeObsoleteFileStatus == -1) {
                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
                } else {
                    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveFileName << RESET << endl;
                    hashStoreFileIDToPrefixMap_.erase(it.second->target_file_id_);
                    objectFileMetaDataTrie_.erase(it.first);
                }
                // skip deleted file
                continue;
            }
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << endl;
            hashStoreFileManifestStream << it.second->target_file_id_ << endl;
            hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << it.second->total_object_count_ << endl;
            hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
            it.second->fileOperationMutex_.lock();
            it.second->file_operation_stream_.flush();
            it.second->file_operation_stream_.close();
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): flushed file id = " << it.second->target_file_id_ << ", file correspond prefix = " << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << RESET << endl;
            it.second->fileOperationMutex_.unlock();
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        // Update manifest pointer
        fstream hashStoreFileManifestPointerUpdateStream;
        hashStoreFileManifestPointerUpdateStream.open(
            workingDir_ + "/hashStoreFileManifest.pointer", ios::out);
        if (hashStoreFileManifestPointerUpdateStream.is_open()) {
            hashStoreFileManifestPointerUpdateStream << currentPointerInt;
            hashStoreFileManifestPointerUpdateStream.flush();
            hashStoreFileManifestPointerUpdateStream.close();
            string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
            }
            return true;
        } else {
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not update hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
            return false;
        }
    } else {
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
        auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
        if (removeOldManifestStatus == -1) {
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
        }
        return true;
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
    if (objectFileMetaDataTrie_.size() != 0) {
        for (auto it : objectFileMetaDataTrie_) {
            if (it.second->gc_result_status_flag_ == kShouldDelete) {
                it.second->file_operation_stream_.close();
                string targetRemoveFileName = workingDir_ + "/" + to_string(it.second->target_file_id_) + ".delta";
                auto removeObsoleteFileStatus = remove(targetRemoveFileName.c_str());
                if (removeObsoleteFileStatus == -1) {
                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
                } else {
                    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): delete the obsolete delta file, file path = " << targetRemoveFileName << RESET << endl;
                    hashStoreFileIDToPrefixMap_.erase(it.second->target_file_id_);
                    objectFileMetaDataTrie_.erase(it.first);
                }
                // skip and delete should deleted files
                continue;
            }
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << endl;
            hashStoreFileManifestStream << it.second->target_file_id_ << endl;
            hashStoreFileManifestStream << it.second->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << it.second->total_object_count_ << endl;
            hashStoreFileManifestStream << it.second->total_object_bytes_ << endl;
            it.second->fileOperationMutex_.lock();
            it.second->file_operation_stream_.flush();
            it.second->file_operation_stream_.close();
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): flush and closed file id = " << it.second->target_file_id_ << ", file correspond prefix = " << hashStoreFileIDToPrefixMap_.at(it.second->target_file_id_) << RESET << endl;
            it.second->fileOperationMutex_.unlock();
            delete it.second;
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        // Update manifest pointer
        fstream hashStoreFileManifestPointerUpdateStream;
        hashStoreFileManifestPointerUpdateStream.open(
            workingDir_ + "/hashStoreFileManifest.pointer", ios::out);
        if (hashStoreFileManifestPointerUpdateStream.is_open()) {
            hashStoreFileManifestPointerUpdateStream << currentPointerInt;
            hashStoreFileManifestPointerUpdateStream.flush();
            hashStoreFileManifestPointerUpdateStream.close();
            string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
            auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
            if (removeOldManifestStatus == -1) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
            }
            return true;
        } else {
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not update hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
            return false;
        }
    } else {
        string targetRemoveFileName = workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt - 1);
        auto removeOldManifestStatus = remove(targetRemoveFileName.c_str());
        if (removeOldManifestStatus == -1) {
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could not delete the old manifest file, file path = " << targetRemoveFileName << RESET << endl;
        }
        return true;
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
                    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target file handler is marked as should delete, retry" << RESET << endl;
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
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): create new fileHandler" << RESET << endl;
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
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
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): in lock, file name = " << workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta" << RESET << endl;
    currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta", ios::out | ios::binary);
    currentFileHandlerPtr->file_operation_stream_.close();
    currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_) + ".delta", ios::in | ios::out | ios::binary);
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): current file read pointer = " << currentFileHandlerPtr->file_operation_stream_.tellg() << ", current file write pointer = " << currentFileHandlerPtr->file_operation_stream_.tellp() << RESET << endl;
    currentFileHandlerPtr->file_operation_stream_.write(fileHeaderWriteBuffer, sizeof(newFileHeader));
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): after write current file read pointer = " << currentFileHandlerPtr->file_operation_stream_.tellg() << ", current file write pointer = " << currentFileHandlerPtr->file_operation_stream_.tellp() << RESET << endl;
    currentFileHandlerPtr->total_object_bytes_ += sizeof(newFileHeader);
    currentFileHandlerPtr->fileOperationMutex_.unlock();
    // move pointer for return
    objectFileMetaDataTrie_.insert(make_pair(prefixStr.substr(0, prefixBitNumber), currentFileHandlerPtr));
    hashStoreFileIDToPrefixMap_.insert(make_pair(currentFileHandlerPtr->target_file_id_, prefixStr.substr(0, prefixBitNumber)));
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
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): deconstruct current file header done, file ID = " << currentFileHeader.file_id_ << ", create reason = " << currentFileHeader.file_create_reason_ << ", prefix length used in this file = " << currentFileHeader.current_prefix_used_bit_ << ", target process file size = " << fileSize << RESET << endl;
    while (currentProcessLocationIndex != fileSize) {
        processedKeepObjectNumber++;
        processedTotalObjectNumber++;
        hashStoreRecordHeader currentObjectRecordHeader;
        memcpy(&currentObjectRecordHeader, fileContentBuffer + currentProcessLocationIndex, sizeof(currentObjectRecordHeader));
        cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): deconstruct current record header done, record is anchor flag = " << currentObjectRecordHeader.is_anchor_ << ", key size = " << currentObjectRecordHeader.key_size_ << ", value size = " << currentObjectRecordHeader.value_size_ << RESET << endl;
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
    return make_pair(processedKeepObjectNumber, processedTotalObjectNumber);
}

// threads workers
void HashStoreFileManager::processGCRequestWorker()
{
    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): start processGCRequestWorker thread success" << RESET << endl;
    while (true) {
        if (notifyGCMQ_->done_ == true) {
            break;
        }
        hashStoreFileMetaDataHandler* currentHandlerPtr;
        if (notifyGCMQ_->pop(currentHandlerPtr)) {
            cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): new file request for GC, file id = " << currentHandlerPtr->target_file_id_ << RESET << endl;
            currentHandlerPtr->file_ownership_flag_ = -1;
            // read contents
            char readWriteBuffer[currentHandlerPtr->total_object_bytes_];
            currentHandlerPtr->fileOperationMutex_.lock();
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for gc size = " << currentHandlerPtr->total_object_bytes_ << ", current file read pointer = " << currentHandlerPtr->file_operation_stream_.tellg() << ", current file write pointer = " << currentHandlerPtr->file_operation_stream_.tellp() << RESET << endl;
            currentHandlerPtr->file_operation_stream_.seekg(0, ios::beg);
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for gc after reset file read pointer = " << currentHandlerPtr->file_operation_stream_.tellg() << RESET << endl;
            currentHandlerPtr->file_operation_stream_.read(readWriteBuffer, currentHandlerPtr->total_object_bytes_);
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target read file content for gc after read file read pointer = " << currentHandlerPtr->file_operation_stream_.tellg() << RESET << endl;
            cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): read file content buffer size for gc = " << sizeof(readWriteBuffer) << RESET << endl;
            currentHandlerPtr->file_operation_stream_.seekp(0, ios::end);

            // process GC contents
            unordered_map<string, vector<string>> gcResultMap;
            pair<uint64_t, uint64_t> remainObjectNumberPair = deconstructAndGetValidContentsFromFile(readWriteBuffer, currentHandlerPtr->total_object_bytes_, gcResultMap);
            if (remainObjectNumberPair.first == remainObjectNumberPair.second) {
                cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file id = " << currentHandlerPtr->target_file_id_ << " recliam empty space fail, try split" << RESET << endl;
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
                            bool getFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(currentPrefixStr, currentFileHandlerPtr, targetPrefixBitNumber, true, currentHandlerPtr->target_file_id_);
                            if (getFileHandlerStatus == false) {
                                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): could get/create new bucket for prefix hash = " << currentPrefixStr << " gc error and skip the file's gc" << RESET << endl;
                                break;
                            } else {
                                currentFileHandlerPtr->fileOperationMutex_.lock();
                                for (auto valueIt : keyIt.second) {
                                    hashStoreRecordHeader currentObjectRecordHeader;
                                    currentObjectRecordHeader.is_anchor_ = false;
                                    currentObjectRecordHeader.key_size_ = keyIt.first.size();
                                    currentObjectRecordHeader.value_size_ = valueIt.size();
                                    char currentWriteBuffer[sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_];
                                    memcpy(currentWriteBuffer, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                                    memcpy(currentWriteBuffer + sizeof(hashStoreRecordHeader), keyIt.first.c_str(), keyIt.first.size());
                                    memcpy(currentWriteBuffer + sizeof(hashStoreRecordHeader) + keyIt.first.size(), valueIt.c_str(), valueIt.size());
                                    currentFileHandlerPtr->file_operation_stream_.write(currentWriteBuffer, sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                                    currentFileHandlerPtr->temp_not_flushed_data_bytes_ += (sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                                    currentFileHandlerPtr->total_object_bytes_ += (sizeof(hashStoreRecordHeader) + currentObjectRecordHeader.key_size_ + currentObjectRecordHeader.value_size_);
                                    currentFileHandlerPtr->total_object_count_++;
                                }
                                currentFileHandlerPtr->file_operation_stream_.flush();
                                currentFileHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                                currentFileHandlerPtr->fileOperationMutex_.unlock();
                            }
                        }
                    }
                    currentHandlerPtr->gc_result_status_flag_ = kShouldDelete;
                    currentHandlerPtr->file_ownership_flag_ = 0;
                    currentHandlerPtr->fileOperationMutex_.unlock();
                    continue;
                }
            } else {
                cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): file id = " << currentHandlerPtr->target_file_id_ << " recliam empty space success, start re-write" << RESET << endl;
                // reclaimed space success, rewrite current file to new file
                memset(readWriteBuffer, 0, currentHandlerPtr->total_object_bytes_);
                uint64_t newObjectNumber = 0;
                uint64_t currentProcessLocationIndex = 0;
                hashStoreFileHeader currentFileHeader;
                currentFileHeader.current_prefix_used_bit_ = currentHandlerPtr->current_prefix_used_bit_;
                currentFileHeader.file_create_reason_ = kGCFile;
                currentFileHeader.file_id_ = generateNewFileID();
                currentFileHeader.previous_file_id_ = currentHandlerPtr->target_file_id_;
                memcpy(readWriteBuffer + currentProcessLocationIndex, &currentFileHeader, sizeof(hashStoreFileHeader));
                currentProcessLocationIndex += sizeof(currentFileHeader);
                // add file header
                for (auto keyIt : gcResultMap) {
                    for (auto valueIt : keyIt.second) {
                        newObjectNumber++;
                        hashStoreRecordHeader currentObjectRecordHeader;
                        currentObjectRecordHeader.is_anchor_ = false;
                        currentObjectRecordHeader.key_size_ = keyIt.first.size();
                        currentObjectRecordHeader.value_size_ = valueIt.size();
                        memcpy(readWriteBuffer + currentProcessLocationIndex, &currentObjectRecordHeader, sizeof(hashStoreRecordHeader));
                        currentProcessLocationIndex += sizeof(hashStoreRecordHeader);
                        memcpy(readWriteBuffer + currentProcessLocationIndex, keyIt.first.c_str(), keyIt.first.size());
                        currentProcessLocationIndex += keyIt.first.size();
                        memcpy(readWriteBuffer + currentProcessLocationIndex, valueIt.c_str(), valueIt.size());
                        currentProcessLocationIndex += valueIt.size();
                    }
                }
                currentHandlerPtr->file_operation_stream_.close();
                string targetOpenFileName = workingDir_ + "/" + to_string(currentFileHeader.file_id_) + ".delta";
                // create since file not exist
                currentHandlerPtr->file_operation_stream_.open(targetOpenFileName, ios::out);
                currentHandlerPtr->file_operation_stream_.close();
                // write content and update current file stream to new one.
                currentHandlerPtr->file_operation_stream_.open(targetOpenFileName, ios::in | ios::out | ios::binary);
                currentHandlerPtr->file_operation_stream_.write(readWriteBuffer, currentProcessLocationIndex);
                currentHandlerPtr->file_operation_stream_.flush();
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): after write file content for gc, current file pointer = " << currentHandlerPtr->file_operation_stream_.tellg() << ", target write size = " << currentProcessLocationIndex << RESET << endl;
                // update metadata
                currentHandlerPtr->temp_not_flushed_data_bytes_ = 0;
                currentHandlerPtr->total_object_count_ = newObjectNumber;
                currentHandlerPtr->total_object_bytes_ = currentProcessLocationIndex;
                currentHandlerPtr->file_ownership_flag_ = 0;
                currentHandlerPtr->fileOperationMutex_.unlock();
                continue;
            }
        }
    }
    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): stop processGCRequestWorker thread success" << RESET << endl;
    return;
}

void HashStoreFileManager::scheduleMetadataUpdateWorker()
{
    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): start scheduleMetadataUpdateWorker thread success" << RESET << endl;
    while (true) {
        if (notifyGCMQ_->done_ == true) {
            break;
        }
    }
    cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): stop scheduleMetadataUpdateWorker thread success" << RESET << endl;
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