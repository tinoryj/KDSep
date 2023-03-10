#include "hashBasedStore/hashStoreInterface.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager, HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObjectStruct*>* writeBackOperationsQueue)
{
    if (options->hashStore_max_file_number_ == 0) {
        options->hashStore_max_file_number_ = 1;
    }
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;
    enable_lsm_tree_delta_meta_ = options->enable_lsm_tree_delta_meta;
    enable_index_block_ = options->enable_index_block;
    if (options->enable_deltaStore_garbage_collection == true) {
        notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;
    }

    if (options->enable_deltaStore_KDLevel_cache == true) {
        options->keyToValueListCacheStr_ = new AppendAbleLRUCacheStrT(options->deltaStore_KDLevel_cache_item_number);
    }
    if (options->enable_write_back_optimization_ == true) {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_, writeBackOperationsQueue);
    } else {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_);
    }
    hashStoreFileOperator = new HashStoreFileOperator(options, workingDirStr, hashStoreFileManager);
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
        notifyGCMQ_->done = true;
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

bool HashStoreInterface::put(mempoolHandler_t objectPairMempoolHandler)
{
    if (objectPairMempoolHandler.isAnchorFlag_ == true && anyBucketInitedFlag_ == false) {
        return true;
        debug_info("New OP: put delta key [Anchor] = %s\n", objectPairMempoolHandler.keyPtr_);
    } else {
        anyBucketInitedFlag_ = true;
        debug_info("New OP: put delta key [Data] = %s\n", objectPairMempoolHandler.keyPtr_);
    }

    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(objectPairMempoolHandler.keyPtr_, objectPairMempoolHandler.keySize_, kPut, tempFileHandler, objectPairMempoolHandler.isAnchorFlag_), StatsType::DS_PUT_GET_HANDLER);
    if (ret != true) {
        debug_error("[ERROR] get fileHandler from file manager error for key = %s\n", objectPairMempoolHandler.keyPtr_);
        return false;
    } else {
        if (objectPairMempoolHandler.isAnchorFlag_ == true && (tempFileHandler == nullptr || tempFileHandler->total_object_bytes_ == 0)) {
            if (tempFileHandler != nullptr) {
                tempFileHandler->file_ownership = 0;
            }
            return true;
        }
        ret = hashStoreFileOperatorPtr_->directlyWriteOperation(tempFileHandler, &objectPairMempoolHandler);
        tempFileHandler->file_ownership = 0;
        if (ret != true) {
            debug_error("[ERROR] write to dLog error for key = %s\n", objectPairMempoolHandler.keyPtr_);
            return false;
        } else {
            return true;
        }
    }
}

bool HashStoreInterface::multiPut(vector<mempoolHandler_t> objectPairMemPoolHandlerVec)
{
    bool allAnchoarsFlag = true;
    for (auto it : objectPairMemPoolHandlerVec) {
        allAnchoarsFlag = allAnchoarsFlag && it.isAnchorFlag_;
    }
    if (allAnchoarsFlag == true && anyBucketInitedFlag_ == false) {
        return true;
    } else {
        anyBucketInitedFlag_ = true;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);
    unordered_map<hashStoreFileMetaDataHandler*, vector<int>> fileHandlerToIndexMap;
    for (auto i = 0; i < objectPairMemPoolHandlerVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr = nullptr;
        bool getFileHandlerStatus;
        STAT_PROCESS(getFileHandlerStatus = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStrForMultiPut(objectPairMemPoolHandlerVec[i].keyPtr_, objectPairMemPoolHandlerVec[i].keySize_, kPut, currentFileHandlerPtr, objectPairMemPoolHandlerVec[i].isAnchorFlag_), StatsType::DS_MULTIPUT_GET_SINGLE_HANDLER);
        if (getFileHandlerStatus != true) {
            debug_error("[ERROR] Get file handler for key = %s error during multiput\n", objectPairMemPoolHandlerVec[i].keyPtr_);
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
        unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> tempFileHandlerMap;
        for (auto mapIt : fileHandlerToIndexMap) {
            vector<mempoolHandler_t> handlerVec;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                handlerVec.push_back(objectPairMemPoolHandlerVec[mapIt.second[index]]);
            }
            mapIt.first->markedByMultiPut_ = false;
            tempFileHandlerMap.insert(make_pair(mapIt.first, handlerVec));
        }
        debug_info("Current handler map size = %lu\n", tempFileHandlerMap.size());
        bool putStatus = hashStoreFileOperatorPtr_->directlyMultiWriteOperation(tempFileHandlerMap);
        if (putStatus != true) {
            debug_error("[ERROR] write to dLog error for keys, number = %lu\n", objectPairMemPoolHandlerVec.size());
            return false;
        } else {
            debug_trace("Write to dLog success for keys via direct operations, number = %lu\n", objectPairMemPoolHandlerVec.size());
            return true;
        }
    } else {
        uint32_t handlerVecIndex = 0;
        uint32_t handlerStartVecIndex = 0;
        uint32_t opHandlerIndex = 0;
        mempoolHandler_t handlerVecTemp[objectPairMemPoolHandlerVec.size()];
        vector<hashStoreOperationHandler*> handlers; 
        handlers.resize(fileHandlerToIndexMap.size());
        for (auto mapIt : fileHandlerToIndexMap) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            handlerStartVecIndex = handlerVecIndex;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                handlerVecTemp[handlerVecIndex++] = objectPairMemPoolHandlerVec[mapIt.second[index]];
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PROCESS_HANDLERS, tv);
            mapIt.first->markedByMultiPut_ = false;
            hashStoreOperationHandler* currentOperationHandler = new hashStoreOperationHandler(mapIt.first);
            currentOperationHandler->batched_write_operation_.mempool_handler_vec_ptr_ = handlerVecTemp + handlerStartVecIndex;
            currentOperationHandler->batched_write_operation_.size = handlerVecIndex - handlerStartVecIndex;
            currentOperationHandler->job_done = kNotDone;
            currentOperationHandler->op_type = kMultiPut;
            STAT_PROCESS(hashStoreFileOperatorPtr_->putWriteOperationsVectorIntoJobQueue(currentOperationHandler), StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE_OPERATOR);
            handlers[opHandlerIndex++] = currentOperationHandler;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE, tv);
        gettimeofday(&tv, 0);

        for (auto& it : handlers) {
            hashStoreFileOperatorPtr_->waitOperationHandlerDone(it);
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_WAIT_HANDLERS, tv);
        return true;
    }
}

bool HashStoreInterface::get(const string& keyStr, vector<string>& valueStrVec)
{
    debug_info("New OP: get deltas for key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr((char*)keyStr.c_str(), keyStr.size(), kGet, tempFileHandler, false), StatsType::DELTAKV_HASHSTORE_GET_FILE_HANDLER);
    if (ret != true) {
        valueStrVec.clear();
        return true;
    } else {
        StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_TIMES, 1, 1);
        if (enable_lsm_tree_delta_meta_ == true || tempFileHandler->filter->MayExist(keyStr)) {
            ret = hashStoreFileOperatorPtr_->directlyReadOperation(tempFileHandler, keyStr, valueStrVec);
            bool deltaExistFlag = (!valueStrVec.empty());
            if (deltaExistFlag) {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_TRUE, 1, 1);
            } else {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_FALSE, 1, 1);
            }
            return ret;
        } else {
            tempFileHandler->file_ownership = 0;
            return true;
        }
    }
}

bool HashStoreInterface::multiGet(vector<string> keyStrVec, vector<vector<string>>& valueStrVecVec)
{
    debug_error("Not implemented %s\n", "");
    return false;
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
