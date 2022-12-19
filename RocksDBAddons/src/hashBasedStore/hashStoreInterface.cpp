#include "hashBasedStore/hashStoreInterface.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
    HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue)
{
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;
    if (options->enable_deltaStore_garbage_collection == true) {
        notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;
    }
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
    if (options->enable_deltaStore_garbage_collection == true) {
        totalNumberOfThreadsAllowed--;
    }
    if (totalNumberOfThreadsAllowed >= 2) {
        shouldUseDirectOperationsFlag_ = false;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    } else {
        shouldUseDirectOperationsFlag_ = true;
        debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
    }
}

HashStoreInterface::~HashStoreInterface()
{
    if (notifyGCMQ_ != nullptr) {
        delete notifyGCMQ_;
    }
}

bool HashStoreInterface::setJobDone()
{
    if (notifyGCMQ_ != nullptr) {
        notifyGCMQ_->done_ = true;
    }
    if (hashStoreFileManagerPtr_->setJobDone() == true) {
        if (hashStoreFileOperatorPtr_->setJobDone() == true) {
            return true;
        } else {
            return false;
        }
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
    if (isAnchor == true) {
        debug_info("New OP: put delta key [Anchor] = %s\n", keyStr.c_str());
    } else {
        debug_info("New OP: put delta key [Data] = %s\n", keyStr.c_str());
    }

    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kPut, tempFileHandler, isAnchor), StatsType::DELTAKV_PUT_HASHSTORE_GET_HANDLER);
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
    debug_info("New OP: put deltas key number = %lu, %lu, %lu, %lu\n", keyStrVec.size(), valueStrPtrVec.size(), sequenceNumberVec.size(), isAnchorVec.size());
    unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> tempFileHandlerMap;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr = nullptr;
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStrForMultiPut(keyStrVec[i], kPut, currentFileHandlerPtr, isAnchorVec[i]) != true) {
            debug_error("[ERROR] Get file handler for key = %s error during multiput\n", keyStrVec[i].c_str());
            return false;
        } else {
            if (currentFileHandlerPtr == nullptr) {
                // should skip current key since it is only an anchor
                continue;
            }
            if (tempFileHandlerMap.find(currentFileHandlerPtr) != tempFileHandlerMap.end()) {
                std::get<0>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(keyStrVec[i]);
                std::get<1>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(valueStrPtrVec[i]);
                std::get<2>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(sequenceNumberVec[i]);
                std::get<3>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(isAnchorVec[i]);
            } else {
                vector<string> keyVecTemp;
                vector<string> valueVecTemp;
                vector<uint32_t> sequenceNumberVecTemp;
                vector<bool> anchorFlagVecTemp;
                keyVecTemp.push_back(keyStrVec[i]);
                valueVecTemp.push_back(valueStrPtrVec[i]);
                sequenceNumberVecTemp.push_back(sequenceNumberVec[i]);
                anchorFlagVecTemp.push_back(isAnchorVec[i]);
                tempFileHandlerMap.insert(make_pair(currentFileHandlerPtr, make_tuple(keyVecTemp, valueVecTemp, sequenceNumberVecTemp, anchorFlagVecTemp)));
            }
        }
    }
    for (auto mapIt : tempFileHandlerMap) {
        mapIt.first->file_ownership_flag_ = 1;
        debug_trace("Test: for file ID = %lu, put deltas key number = %lu, %lu, %lu, %lu\n", mapIt.first->target_file_id_, std::get<0>(mapIt.second).size(), std::get<1>(mapIt.second).size(), std::get<2>(mapIt.second).size(), std::get<3>(mapIt.second).size());
    }
    debug_info("Current handler map size = %lu\n", tempFileHandlerMap.size());
    if (tempFileHandlerMap.size() == 0) {
        return true;
    } else {
        if (shouldUseDirectOperationsFlag_ == true) {
            bool putStatus = hashStoreFileOperatorPtr_->directlyMultiWriteOperation(tempFileHandlerMap);
            if (putStatus != true) {
                debug_error("[ERROR] write to dLog error for keys, number = %lu\n", keyStrVec.size());
                return false;
            } else {
                debug_trace("Write to dLog success for keys via direct operations, number = %lu\n", keyStrVec.size());
                return true;
            }
        } else {
            bool putStatus = hashStoreFileOperatorPtr_->putWriteOperationsVectorIntoJobQueue(tempFileHandlerMap);
            if (putStatus != true) {
                debug_error("[ERROR] write to dLog error for keys, number = %lu\n", keyStrVec.size());
                return false;
            } else {
                debug_trace("Write to dLog success for keys via job queue operations, number = %lu\n", keyStrVec.size());
                return true;
            }
        }
    }
}

bool HashStoreInterface::get(const string& keyStr, vector<string>*& valueStrVec)
{
    debug_info("New OP: get deltas for key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kGet, tempFileHandler, false);
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
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStrVec[i], kGet, currentFileHandlerPtr, false) != true) {
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
