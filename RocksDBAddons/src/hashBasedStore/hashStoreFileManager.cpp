#include "hashBasedStore/hashStoreFileManager.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreFileManager::HashStoreFileManager(uint64_t initialBitNumber, uint64_t maxBitNumber, uint64_t objectGCTriggerSize,
    uint64_t objectGlobalGCTriggerSize, string workingDirStr, messageQueue<hashStoreFileMetaDataHandler*>* fileManagerNotifyGCMQ, messageQueue<hashStoreFileMetaDataHandler*>* GCNotifyFileMetaDataUpdateMQ)
{
    initialTrieBitNumber_ = initialBitNumber;
    maxTrieBitNumber_ = maxBitNumber;
    singleFileGCTriggerSize_ = objectGCTriggerSize;
    globalGCTriggerSize_ = objectGlobalGCTriggerSize;
    workingDir_ = workingDirStr;
    fileManagerNotifyGCMQ_ = fileManagerNotifyGCMQ;
    GCNotifyFileMetaDataUpdateMQ_ = GCNotifyFileMetaDataUpdateMQ;
}

HashStoreFileManager::~HashStoreFileManager()
{
}

// Recovery
bool HashStoreFileManager::recoveryFromFailuer(unordered_map<string, pair<bool, string>>* targetListForRedo)
{
}

// Manager's metadata management
bool HashStoreFileManager::RetriveHashStoreFileMetaDataList()
{
    ifstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "hashStoreFileManifest.pointer", ios::in);
    string currentPointerStr;
    if (hashStoreFileManifestPointerStream.is_open()) {
        getline(hashStoreFileManifestPointerStream, currentPointerStr);
        uint64_t currentPointerInt = stoull(currentPointerStr);
    } else {
        if (CreateHashStoreFileMetaDataListIfNotExist()) {
            return true;
        } else {
            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[RetriveHashStoreFileMetaDataList] create hashStore file metadata list error" << RESET << endl;
            return false;
        }
    }
    ifstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(
        workingDir_ + "hashStoreFileManifest." + currentPointerStr, ios::in);
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
            currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + to_string(currentFileHandlerPtr->target_file_id_), ios::in | ios::out);
            currentFileHandlerPtr->fileOperationMutex_.unlock();
            // re-insert into trie and map for build index
            objectFileMetaDataTrie_.insert(prefixHashStr, currentFileHandlerPtr);
            hashStoreFileIDToPrefixMap_.insert(make_pair(hashStoreFileID, prefixHashStr));
        }
    } else {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[RetriveHashStoreFileMetaDataList] could not open hashStore file metadata list (manifest)" << RESET << endl;
        return false;
    }
    return true;
}

bool HashStoreFileManager::UpdateHashStoreFileMetaDataList()
{
    ifstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "hashStoreFileManifest.pointer", ios::in);
    uint64_t currentPointerInt = 0;
    if (hashStoreFileManifestPointerStream.is_open()) {
        hashStoreFileManifestPointerStream >> currentPointerInt;
        currentPointerInt++;
    } else {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[UpdateHashStoreFileMetaDataList] could not open hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
        return false;
    }
    hashStoreFileManifestPointerStream.close();
    ofstream hashStoreFileManifestStream;
    hashStoreFileManifestStream.open(
        workingDir_ + "hashStoreFileManifest." + to_string(currentPointerInt),
        ios::out);
    if (objectFileMetaDataTrie_.size() != 0) {
        for (Trie<hashStoreFileMetaDataHandler*>::iterator it = objectFileMetaDataTrie_.begin();
             it != objectFileMetaDataTrie_.end(); it++) {
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at((*it)->target_file_id_) << endl;
            hashStoreFileManifestStream << (*it)->target_file_id_ << endl;
            hashStoreFileManifestStream << (*it)->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << (*it)->total_object_count_ << endl;
            hashStoreFileManifestStream << (*it)->total_object_bytes_ << endl;
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        // Update manifest pointer
        ofstream hashStoreFileManifestPointerUpdateStream;
        hashStoreFileManifestPointerUpdateStream.open(
            workingDir_ + "hashStoreFileManifest.pointer", ios::out);
        if (hashStoreFileManifestPointerUpdateStream.is_open()) {
            hashStoreFileManifestPointerUpdateStream << currentPointerInt;
            hashStoreFileManifestPointerUpdateStream.flush();
            hashStoreFileManifestPointerUpdateStream.close();
            return true;
        } else {
            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[UpdateHashStoreFileMetaDataList] could not update hashStore file metadata list pointer file (currentDeltaPointer)" << RESET << endl;
            return false;
        }
    } else {
        return true;
    }
}

bool HashStoreFileManager::CreateHashStoreFileMetaDataListIfNotExist()
{
    ofstream hashStoreFileManifestPointerStream;
    hashStoreFileManifestPointerStream.open(
        workingDir_ + "hashStoreFileManifest.pointer", ios::out);
    uint64_t currentPointerInt = 0;
    hashStoreFileManifestPointerStream << currentPointerInt << endl;
    hashStoreFileManifestPointerStream.flush();
    hashStoreFileManifestPointerStream.close();
    if (objectFileMetaDataTrie_.size() != 0) {
        ofstream hashStoreFileManifestStream;
        hashStoreFileManifestStream.open(workingDir_ + "hashStoreFileManifest." + to_string(currentPointerInt),
            ios::out);
        for (Trie<hashStoreFileMetaDataHandler*>::iterator it = objectFileMetaDataTrie_.begin();
             it != objectFileMetaDataTrie_.end(); it++) {
            hashStoreFileManifestStream << hashStoreFileIDToPrefixMap_.at((*it)->target_file_id_) << endl;
            hashStoreFileManifestStream << (*it)->target_file_id_ << endl;
            hashStoreFileManifestStream << (*it)->current_prefix_used_bit_ << endl;
            hashStoreFileManifestStream << (*it)->total_object_count_ << endl;
            hashStoreFileManifestStream << (*it)->total_object_bytes_ << endl;
        }
        hashStoreFileManifestStream.flush();
        hashStoreFileManifestStream.close();
        return true;
    } else {
        return true;
    }
}

// file operations
bool HashStoreFileManager::getHashStoreFileHandlerByInputKeyStr(string keyStr, hashStoreFileOperationType opType, hashStoreFileMetaDataHandler* fileHandlerPtr)
{
    string prefixStr;
    bool genPrefixStatus = generateHashBasedPrefix(keyStr, prefixStr);
    if (!genPrefixStatus) {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[getHashStoreFileHandlerByInputKeyStr] generate prefix hash for current key error, key = " << keyStr << RESET << endl;
        return false;
    }
    uint64_t fileHandlerUsedPrefixLength = getHashStoreFileHandlerStatusByPrefix(prefixStr);
    if (fileHandlerUsedPrefixLength == 0 && opType == kGet) {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[getHashStoreFileHandlerByInputKeyStr] get operation meet not stored buckets, key = " << keyStr << RESET << endl;
        return false;
    } else if (fileHandlerUsedPrefixLength == 0 && opType == kPut) {
        bool createNewFileHandlerStatus = createAnfGetNewHashStoreFileHandlerByPrefix(prefixStr, fileHandlerPtr);
        if (!createNewFileHandlerStatus) {
            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[getHashStoreFileHandlerByInputKeyStr] create new bucket for put operation error, key = " << keyStr << RESET << endl;
            return false;
        } else {
            return true;
        }
    } else if (fileHandlerUsedPrefixLength >= initialTrieBitNumber_ && fileHandlerUsedPrefixLength <= maxTrieBitNumber_) {
        bool getFileHandlerStatus = getHashStoreFileHandlerByPrefix(prefixStr, fileHandlerUsedPrefixLength, fileHandlerPtr);
        if (!getFileHandlerStatus) {
            cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[getHashStoreFileHandlerByInputKeyStr] get existing bucket file handler for put/get operation error, key = " << keyStr << RESET << endl;
            return false;
        } else {
            return true;
        }
    } else {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreFileManager]-[getHashStoreFileHandlerByInputKeyStr] get used prefix hash length in tire error, returned length = " << fileHandlerUsedPrefixLength << RESET << endl;
        return false;
    }
}

uint64_t HashStoreFileManager::getHashStoreFileHandlerStatusByPrefix(const string prefixStr)
{
    for (auto prefixLength = initialTrieBitNumber_; prefixLength <= maxTrieBitNumber_; prefixLength++) {
        if (objectFileMetaDataTrie_.exist(prefixStr.substr(0, prefixLength)) == true) {
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

bool HashStoreFileManager::getHashStoreFileHandlerByPrefix(const string prefixStr, uint64_t prefixUsageLength, hashStoreFileMetaDataHandler* fileHandlerPtr)
{
    Trie<hashStoreFileMetaDataHandler*>::iterator it = objectFileMetaDataTrie_.find(prefixStr.substr(0, prefixUsageLength));
    fileHandlerPtr = (*it);
    return true;
}

bool HashStoreFileManager::createAnfGetNewHashStoreFileHandlerByPrefix(const string prefixStr, hashStoreFileMetaDataHandler* fileHandlerPtr)
{
    hashStoreFileMetaDataHandler* currentFileHandlerPtr = new hashStoreFileMetaDataHandler;
    currentFileHandlerPtr->current_prefix_used_bit_ = initialTrieBitNumber_;
    currentFileHandlerPtr->target_file_id_ = newFileIDGenerator();
    currentFileHandlerPtr->total_object_bytes_ = 0;
    currentFileHandlerPtr->total_object_count_ = 0;
    // set up new file header for write
    hashStoreFileHeader newFileHeader;
    newFileHeader.current_prefix_used_bit_ = initialTrieBitNumber_;
    newFileHeader.file_create_reason_ = kNewFile;
    newFileHeader.file_id_ = currentFileHandlerPtr->target_file_id_;
    char fileHeaderWriteBuffer[sizeof(newFileHeader)];
    memcpy(fileHeaderWriteBuffer, &newFileHeader, sizeof(newFileHeader));
    // write header to current file
    currentFileHandlerPtr->fileOperationMutex_.lock();
    currentFileHandlerPtr->file_operation_stream_.open(workingDir_ + to_string(currentFileHandlerPtr->target_file_id_), ios::in | ios::out);
    currentFileHandlerPtr->file_operation_stream_.write(fileHeaderWriteBuffer, sizeof(newFileHeader));
    currentFileHandlerPtr->fileOperationMutex_.unlock();
    // move pointer for return
    fileHandlerPtr = currentFileHandlerPtr;
    return true;
}

uint64_t HashStoreFileManager::newFileIDGenerator()
{
    targetNewFileID_ += 1;
    return targetNewFileID_;
}

void HashStoreFileManager::processGCRequestWorker()
{
}
}