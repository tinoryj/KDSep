#include "hashBasedStore/hashStoreInterface.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager, HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue)
{
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;
    if (options->enable_deltaStore_garbage_collection == true) {
        notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;
    }

    if (options->enable_write_back_optimization_ == true) {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_, writeBackOperationsQueue);
    } else {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_);
    }
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
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        shouldUseDirectOperationsFlag_ = false;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    } else {
        shouldUseDirectOperationsFlag_ = true;
        debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
    }
    objectMemPool_ = new KeyValueMemPool(options->deltaStore_mem_pool_object_number_, options->deltaStore_mem_pool_object_size_);
    fileFlushThreshold_ = options->deltaStore_file_flush_buffer_size_limit_;
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
    if (isAnchor == true && anyBucketInitedFlag_ == false) {
        return true;
        debug_info("New OP: put delta key [Anchor] = %s\n", keyStr.c_str());
    } else {
        anyBucketInitedFlag_ = true;
        debug_info("New OP: put delta key [Data] = %s\n", keyStr.c_str());
    }

    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kPut, tempFileHandler, isAnchor), StatsType::DELTAKV_HASHSTORE_PUT);
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
        ret = hashStoreFileOperatorPtr_->directlyWriteOperation(tempFileHandler, keyStr, valueStr, sequenceNumber, isAnchor);
        // if (shouldUseDirectOperationsFlag_ == true) {
        // } else {
        //     ret = hashStoreFileOperatorPtr_->putWriteOperationIntoJobQueue(tempFileHandler, keyStr, valueStr, sequenceNumber, isAnchor);
        // }
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
    bool allAnchoarsFlag = true;
    for (auto it : isAnchorVec) {
        allAnchoarsFlag = allAnchoarsFlag && it;
    }
    if (allAnchoarsFlag == true && anyBucketInitedFlag_ == false) {
        return true;
    } else {
        anyBucketInitedFlag_ = true;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);

    debug_info("New OP: put deltas key number = %lu, %lu, %lu, %lu\n", keyStrVec.size(), valueStrPtrVec.size(), sequenceNumberVec.size(), isAnchorVec.size());
    unordered_map<hashStoreFileMetaDataHandler*, vector<int>> fileHandlerToIndexMap;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr = nullptr;
        bool getFileHandlerStatus;
        string prefixStr;
        STAT_PROCESS(getFileHandlerStatus = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStrForMultiPut(keyStrVec[i], kPut, currentFileHandlerPtr, prefixStr, isAnchorVec[i]), StatsType::DELTAKV_HASHSTORE_PUT);
        if (getFileHandlerStatus != true) {
            debug_error("[ERROR] Get file handler for key = %s error during multiput\n", keyStrVec[i].c_str());
            return false;
        } else {
            if (currentFileHandlerPtr == nullptr) {
                // should skip current key since it is only an anchor
                continue;
            }
            if (fileHandlerToIndexMap.find(currentFileHandlerPtr) != fileHandlerToIndexMap.end()) {
                fileHandlerToIndexMap.at(currentFileHandlerPtr).push_back(i);
            } else {
                vector<int> indexVec;
                indexVec.push_back(i);
                fileHandlerToIndexMap.insert(make_pair(currentFileHandlerPtr, indexVec));
            }
        }
    }
    if (fileHandlerToIndexMap.size() == 0) {
        return true;
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_GET_HANDLER, tv);
    gettimeofday(&tv, 0);
    if (shouldUseDirectOperationsFlag_ == true) {
        unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> tempFileHandlerMap;
        for (auto mapIt : fileHandlerToIndexMap) {
            vector<string> keyVecTemp;
            vector<string> valueVecTemp;
            vector<uint32_t> sequenceNumberVecTemp;
            vector<bool> anchorFlagVecTemp;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                keyVecTemp.push_back(keyStrVec[mapIt.second[index]]);
                valueVecTemp.push_back(valueStrPtrVec[mapIt.second[index]]);
                sequenceNumberVecTemp.push_back(sequenceNumberVec[mapIt.second[index]]);
                anchorFlagVecTemp.push_back(isAnchorVec[mapIt.second[index]]);
            }
            // mapIt.first->file_ownership_flag_ = 1;
            mapIt.first->markedByMultiPut_ = false;
            tempFileHandlerMap.insert(make_pair(mapIt.first, make_tuple(keyVecTemp, valueVecTemp, sequenceNumberVecTemp, anchorFlagVecTemp)));
        }
        debug_info("Current handler map size = %lu\n", tempFileHandlerMap.size());
        bool putStatus = hashStoreFileOperatorPtr_->directlyMultiWriteOperation(tempFileHandlerMap);
        if (putStatus != true) {
            debug_error("[ERROR] write to dLog error for keys, number = %lu\n", keyStrVec.size());
            return false;
        } else {
            debug_trace("Write to dLog success for keys via direct operations, number = %lu\n", keyStrVec.size());
            return true;
        }
    } else {
        vector<string> keyVecTemp[fileHandlerToIndexMap.size()];
        vector<string> valueVecTemp[fileHandlerToIndexMap.size()];
        vector<uint32_t> sequenceNumberVecTemp[fileHandlerToIndexMap.size()];
        vector<bool> anchorFlagVecTemp[fileHandlerToIndexMap.size()];
        // for selective putinto job queue
        auto processedHandlerIndex = 0;
        unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<uint32_t>, vector<bool>>> tempFileHandlerMap;
        for (auto mapIt : fileHandlerToIndexMap) {
            // mapIt.first->file_ownership_flag_ = 1;
            uint64_t targetWriteSize = 0;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                keyVecTemp[processedHandlerIndex].push_back(keyStrVec[mapIt.second[index]]);
                valueVecTemp[processedHandlerIndex].push_back(valueStrPtrVec[mapIt.second[index]]);
                sequenceNumberVecTemp[processedHandlerIndex].push_back(sequenceNumberVec[mapIt.second[index]]);
                anchorFlagVecTemp[processedHandlerIndex].push_back(isAnchorVec[mapIt.second[index]]);
                if (isAnchorVec[mapIt.second[index]] == true) {
                    targetWriteSize += (sizeof(hashStoreRecordHeader) + keyStrVec[mapIt.second[index]].size());
                } else {
                    targetWriteSize += (sizeof(hashStoreRecordHeader) + keyStrVec[mapIt.second[index]].size() + valueStrPtrVec[mapIt.second[index]].size());
                }
            }
            if (targetWriteSize + mapIt.first->file_operation_func_ptr_->getFileBufferedSize() < fileFlushThreshold_) {
                tempFileHandlerMap.insert(make_pair(mapIt.first, make_tuple(keyVecTemp[processedHandlerIndex], valueVecTemp[processedHandlerIndex], sequenceNumberVecTemp[processedHandlerIndex], anchorFlagVecTemp[processedHandlerIndex])));
            } else {
                mapIt.first->markedByMultiPut_ = false;
                hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(mapIt.first);
                currentOperationHandler->batched_write_operation_.key_str_vec_ptr_ = &keyVecTemp[processedHandlerIndex];
                currentOperationHandler->batched_write_operation_.value_str_vec_ptr_ = &valueVecTemp[processedHandlerIndex];
                currentOperationHandler->batched_write_operation_.sequence_number_vec_ptr_ = &sequenceNumberVecTemp[processedHandlerIndex];
                currentOperationHandler->batched_write_operation_.is_anchor_vec_ptr_ = &anchorFlagVecTemp[processedHandlerIndex];
                currentOperationHandler->jobDone_ = kNotDone;
                currentOperationHandler->opType_ = kMultiPut;
                hashStoreFileOperatorPtr_->putWriteOperationsVectorIntoJobQueue(currentOperationHandler);
            }
            processedHandlerIndex++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE, tv);
        gettimeofday(&tv, 0);
        if (tempFileHandlerMap.size() != 0) {
            bool putStatus = hashStoreFileOperatorPtr_->directlyMultiWriteOperation(tempFileHandlerMap);
            if (putStatus != true) {
                debug_error("[ERROR] write to dLog error for keys, number = %lu\n", keyStrVec.size());
                return false;
            } else {
                debug_trace("Write to dLog success for keys via direct operations, number = %lu\n", keyStrVec.size());
                return true;
            }
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_DIRECT_OP, tv);
        debug_trace("Write to dLog success for keys via job queue operations, number = %lu\n", keyStrVec.size());
        return true;
    }
}

bool HashStoreInterface::get(const string& keyStr, vector<string>*& valueStrVec)
{
    debug_info("New OP: get deltas for key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kGet, tempFileHandler, false), StatsType::DELTAKV_HASHSTORE_GET);
    if (ret != true) {
        debug_error("[ERROR] get fileHandler from file manager error for key = %s\n", keyStr.c_str());
        return false;
    } else {
        // if (shouldUseDirectOperationsFlag_ == true) {
        ret = hashStoreFileOperatorPtr_->directlyReadOperation(tempFileHandler, keyStr, valueStrVec);
        // } else {
        //     ret = hashStoreFileOperatorPtr_->putReadOperationIntoJobQueue(tempFileHandler, keyStr, valueStrVec);
        // }
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
