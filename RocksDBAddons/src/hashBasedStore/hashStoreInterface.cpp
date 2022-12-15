#include "hashBasedStore/hashStoreInterface.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
    HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue)
{
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;

    notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;

    uint64_t singleFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_single_file_minimum_occupancy * internalOptionsPtr_->deltaStore_single_file_maximum_size;
    uint64_t totalHashStoreFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * internalOptionsPtr_->deltaStore_total_storage_maximum_size;

    hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_, writeBackOperationsQueue);
    hashStoreFileOperator = new HashStoreFileOperator(options, workingDirStr, notifyGCMQ_);
    if (!hashStoreFileManager) {
        debug_error("[ERROR] Create HashStoreFileManager error,  file path = %s\n", workingDirStr.c_str());
    }
    if (!hashStoreFileOperator) {
        debug_error("[ERROR] Create HashStoreFileOperator error, file path = %s\n", workingDirStr.c_str());
    }
    hashStoreFileManagerPtr_ = hashStoreFileManager;
    hashStoreFileOperatorPtr_ = hashStoreFileOperator;
    unordered_map<string, vector<pair<bool, string>>> targetListForRedo;
    hashStoreFileManagerPtr_->recoveryFromFailure(targetListForRedo);
    uint64_t totalNumberOfThreadsAllowed = options->deltaStore_thread_number_limit - 1;
    if (totalNumberOfThreadsAllowed >= 2) {
        if (options->enable_deltaStore_garbage_collection == true) {
            uint64_t totalNumberOfThreadsForOperationAllowed = totalNumberOfThreadsAllowed / 2 + 1;
            if (totalNumberOfThreadsForOperationAllowed > 1) {
                shouldUseDirectOperationsFlag_ = false;
            } else {
                shouldUseDirectOperationsFlag_ = true;
            }
        } else {
            shouldUseDirectOperationsFlag_ = false;
        }
    } else {
        shouldUseDirectOperationsFlag_ = true;
    }
    debug_info("shouldUseDirectOperationsFlag = %d\n", shouldUseDirectOperationsFlag_);
}

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
    HashStoreFileOperator*& hashStoreFileOperator)
{
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;

    notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;

    uint64_t singleFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_single_file_minimum_occupancy * internalOptionsPtr_->deltaStore_single_file_maximum_size;
    uint64_t totalHashStoreFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * internalOptionsPtr_->deltaStore_total_storage_maximum_size;

    hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_);
    hashStoreFileOperator = new HashStoreFileOperator(options, workingDirStr, notifyGCMQ_);
    if (!hashStoreFileManager) {
        debug_error("[ERROR] Create HashStoreFileManager error,  file path = %s\n", workingDirStr.c_str());
    }
    if (!hashStoreFileOperator) {
        debug_error("[ERROR] Create HashStoreFileOperator error, file path = %s\n", workingDirStr.c_str());
    }
    hashStoreFileManagerPtr_ = hashStoreFileManager;
    hashStoreFileOperatorPtr_ = hashStoreFileOperator;
    unordered_map<string, vector<pair<bool, string>>> targetListForRedo;
    hashStoreFileManagerPtr_->recoveryFromFailure(targetListForRedo);
    uint64_t totalNumberOfThreadsAllowed = options->deltaStore_thread_number_limit - 1;
    if (totalNumberOfThreadsAllowed >= 2) {
        if (options->enable_deltaStore_garbage_collection == true) {
            uint64_t totalNumberOfThreadsForOperationAllowed = totalNumberOfThreadsAllowed / 2 + 1;
            if (totalNumberOfThreadsForOperationAllowed > 1) {
                shouldUseDirectOperationsFlag_ = false;
            } else {
                shouldUseDirectOperationsFlag_ = true;
            }
        } else {
            shouldUseDirectOperationsFlag_ = false;
        }
    } else {
        shouldUseDirectOperationsFlag_ = true;
    }
    debug_info("shouldUseDirectOperationsFlag = %d\n", shouldUseDirectOperationsFlag_);
}

HashStoreInterface::~HashStoreInterface()
{
    delete notifyGCMQ_;
}

bool HashStoreInterface::setJobDone()
{
    notifyGCMQ_->done_ = true;
    if (hashStoreFileOperatorPtr_->setJobDone() == true) {
        return true;
    } else {
        return false;
    }
}

uint64_t HashStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool HashStoreInterface::put(const string& keyStr, const string& valueStr, uint32_t sequenceNumber, bool isAnchor)
{
    debug_info("New OP: put key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kPut, tempFileHandler), StatsType::DELTAKV_PUT_HASHSTORE_GET_HANDLER);
    if (ret != true) {
        debug_error("[ERROR] get fileHandler from file manager error for key = %s\n", keyStr.c_str());
        return false;
    } else {
        if (isAnchor == true && (tempFileHandler == nullptr || tempFileHandler->total_object_bytes_ == 0)) {
            if (tempFileHandler != nullptr) {
                tempFileHandler->file_ownership_flag_ = 0;
            }
            return true;
        }
        if (shouldUseDirectOperationsFlag_ == true) {
            ret = hashStoreFileOperatorPtr_->directlyWriteOperation(tempFileHandler, keyStr, valueStr, sequenceNumber, isAnchor);
        } else {
            ret = hashStoreFileOperatorPtr_->putWriteOperationIntoJobQueue(tempFileHandler, keyStr, valueStr, sequenceNumber, isAnchor);
        }
        if (ret != true) {
            debug_error("[ERROR] write to dLog error for key = %s\n", keyStr.c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool HashStoreInterface::multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<uint32_t> sequenceNumberVec, vector<bool> isAnchorVec)
{
    unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> tempFileHandlerMap;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr;
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStrVec[i], kPut, currentFileHandlerPtr) != true) {
            return false;
        } else {
            if (tempFileHandlerMap.find(currentFileHandlerPtr) != tempFileHandlerMap.end()) {
                std::get<0>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(keyStrVec[i]);
                std::get<1>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(valueStrPtrVec[i]);
                std::get<2>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(sequenceNumberVec[i]);
                std::get<3>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(isAnchorVec[i]);
            } else {
            }
        }
    }
    if (hashStoreFileOperatorPtr_->putWriteOperationsVectorIntoJobQueue(tempFileHandlerMap) != true) {
        debug_error("[ERROR] write to dLog error for keys, number = %lu\n", keyStrVec.size());
        return false;
    } else {
        return true;
    }
}

bool HashStoreInterface::get(const string& keyStr, vector<string>*& valueStrVec)
{
    debug_info("New OP: get key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kGet, tempFileHandler);
    if (ret != true) {
        debug_error("[ERROR] get fileHandler from file manager error for key = %s\n", keyStr.c_str());
        return false;
    } else {
        if (shouldUseDirectOperationsFlag_ == true) {
            ret = hashStoreFileOperatorPtr_->directlyReadOperation(tempFileHandler, keyStr, valueStrVec);
        } else {
            ret = hashStoreFileOperatorPtr_->putReadOperationIntoJobQueue(tempFileHandler, keyStr, valueStrVec);
        }
        if (ret != true) {
            debug_error("[ERROR] Could not read content with file handler for key = %s\n", keyStr.c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool HashStoreInterface::multiGet(vector<string> keyStrVec, vector<vector<string>*>*& valueStrVecVec)
{
    vector<hashStoreFileMetaDataHandler*> tempFileHandlerVec;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr;
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStrVec[i], kGet, currentFileHandlerPtr) != true) {
            return false;
        } else {
            tempFileHandlerVec.push_back(currentFileHandlerPtr);
        }
    }
    if (hashStoreFileOperatorPtr_->putReadOperationsVectorIntoJobQueue(tempFileHandlerVec, keyStrVec, valueStrVecVec) != true) {
        return false;
    } else {
        return true;
    }
}

bool HashStoreInterface::forcedManualGarbageCollection()
{
    bool forcedGCStatus = hashStoreFileManagerPtr_->forcedManualGCAllFiles();
    if (forcedGCStatus == true) {
        return true;
    } else {
        return false;
    }
}

}
