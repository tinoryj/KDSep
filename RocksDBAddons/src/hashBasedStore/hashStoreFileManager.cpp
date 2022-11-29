#include "hashBasedStore/hashStoreFileManager.hpp"

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
bool HashStoreFileManager::recoveryFromFailuer(unordered_map<string, pair<bool, string>>*& targetListForRedo)
{
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
            currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_), ios::out | ios::binary);
            currentFileHandlerPtr->file_operation_stream_.close();
            currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + "/" + to_string(currentFileHandlerPtr->target_file_id_), ios::in | ios::out | ios::binary);
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
    hashStoreFileManifestStream.open(
        workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt),
        ios::out);
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
    hashStoreFileManifestStream.open(
        workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt),
        ios::out);
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
    if (objectFileMetaDataTrie_.size() != 0) {
        ofstream hashStoreFileManifestStream;
        hashStoreFileManifestStream.open(workingDir_ + "/hashStoreFileManifest." + to_string(currentPointerInt),
            ios::out);
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
        return true;
    }
}

// file operations
bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler*& fileHandlerPtr)
{
    string prefixStr;
    bool genPrefixStatus = generateHashBasedPrefix(keyStr, prefixStr);
    if (!genPrefixStatus) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): generate prefix hash for current key error, key = " << keyStr << RESET << endl;
        return false;
    }
    while (true) {
        uint64_t fileHandlerUsedPrefixLength = getHashStoreFileHandlerStatusByPrefix(prefixStr);
        if (fileHandlerUsedPrefixLength == 0 && opType == kGet) {
            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get operation meet not stored buckets, key = " << keyStr << RESET << endl;
            return false;
        } else if (fileHandlerUsedPrefixLength == 0 && opType == kPut) {
            bool createNewFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr, initialTrieBitNumber_, false);
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

uint64_t HashStoreFileManager::getHashStoreFileHandlerStatusByPrefix(const string prefixStr)
{
    for (auto prefixLength = maxTrieBitNumber_; prefixLength >= initialTrieBitNumber_; prefixLength--) {
        if (objectFileMetaDataTrie_.find(prefixStr.substr(0, prefixLength)) != objectFileMetaDataTrie_.end()) {
            return prefixLength;
        }
    }
    return 0;
}

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

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(const string prefixStr, uint64_t prefixUsageLength, hashStoreFileMetaDataHandler*& fileHandlerPtr)
{
    fileHandlerPtr = objectFileMetaDataTrie_.at(prefixStr.substr(0, prefixUsageLength));
    return true;
}

bool HashStoreFileManager::createAndGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler*& fileHandlerPtr, uint64_t prefixBitNumber, bool createByGCFlag)
{
    cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): create new fileHandler" << RESET << endl;
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->current_prefix_used_bit_ = prefixBitNumber;
    currentFileHandlerPtr->target_file_id_ = newFileIDGenerator();
    currentFileHandlerPtr->file_ownership_flag_ = 0;
    currentFileHandlerPtr->gc_result_status_flag_ = kNew;
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    // set up new file header for write
    hashStoreFileHeader newFileHeader;
    newFileHeader.current_prefix_used_bit_ = prefixBitNumber;
    if (createByGCFlag == true) {
        newFileHeader.file_create_reason_ = kGCFile;
    } else {
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

uint64_t HashStoreFileManager::newFileIDGenerator()
{
    targetNewFileID_ += 1;
    return targetNewFileID_;
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
                            bool getFileHandlerStatus = createAndGetNewHashStoreFileHandlerByPrefix(currentPrefixStr, currentFileHandlerPtr, targetPrefixBitNumber, true);
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
                // reclaimed space success, rewrite current file
                memset(readWriteBuffer, 0, currentHandlerPtr->total_object_bytes_);
                uint64_t newObjectNumber = 0;
                uint64_t currentProcessLocationIndex = 0;
                hashStoreFileHeader currentFileHeader;
                currentFileHeader.current_prefix_used_bit_ = currentHandlerPtr->current_prefix_used_bit_;
                currentFileHeader.file_create_reason_ = kGCFile;
                currentFileHeader.file_id_ = currentHandlerPtr->target_file_id_;
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
                currentHandlerPtr->file_operation_stream_.seekp(0, ios::beg);
                cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): target write file content for gc, current file write pointer = " << currentHandlerPtr->file_operation_stream_.tellg() << RESET << endl;
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