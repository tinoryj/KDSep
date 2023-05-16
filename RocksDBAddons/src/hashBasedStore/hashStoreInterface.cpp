#include "hashBasedStore/hashStoreInterface.hpp"
#include "utils/bucketKeyFilter.hpp"
#include "utils/statsRecorder.hh"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager, HashStoreFileOperator*& hashStoreFileOperator, messageQueue<writeBackObject*>* writeBackOperationsQueue)
{
    if (options->hashStore_max_file_number_ == 0) {
        options->hashStore_max_file_number_ = 1;
    }
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;
    enable_lsm_tree_delta_meta_ = options->enable_lsm_tree_delta_meta;
    if (options->enable_deltaStore_garbage_collection == true) {
        notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;
    }

    if (options->enable_deltaStore_KDLevel_cache == true) {
        options->kd_cache = new KDLRUCache(options->deltaStore_KDLevel_cache_item_number);
        kd_cache_ = options->kd_cache;
    }
    if (options->enable_write_back_optimization_ == true) {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_, writeBackOperationsQueue);
    } else {
        hashStoreFileManager = new HashStoreFileManager(options, workingDirStr, notifyGCMQ_, nullptr);
    }
    hashStoreFileOperator = new HashStoreFileOperator(options, workingDirStr, hashStoreFileManager);
    if (!hashStoreFileManager) {
        debug_error("[ERROR] Create HashStoreFileManager error,  file path = %s\n", workingDirStr.c_str());
    }
    if (!hashStoreFileOperator) {
        debug_error("[ERROR] Create HashStoreFileOperator error, file path = %s\n", workingDirStr.c_str());
    }
    file_manager_ = hashStoreFileManager;
    file_operator_ = hashStoreFileOperator;
    unordered_map<string, vector<pair<bool, string>>> targetListForRedo;
    file_manager_->recoveryFromFailure(targetListForRedo);
    if (options->deltaStore_op_worker_thread_number_limit_ >= 2) {
        shouldUseDirectOperationsFlag_ = false;
        debug_info("Total thread number for operationWorker >= 2, use multithread operation%s\n", "");
    } else {
        shouldUseDirectOperationsFlag_ = true;
        debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
    }
    fileFlushThreshold_ = options->deltaStore_file_flush_buffer_size_limit_;
    enable_crash_consistency_ = options->enable_crash_consistency;

    if (enable_parallel_get_hdl_) {
        printf("enable parallel get hdl\n");
    } else {
        printf("disable parallel get hdl\n");
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
        notifyGCMQ_->done = true;
    }
    if (file_manager_->setJobDone() == true) {
        if (file_operator_->setJobDone() == true) {
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
    STAT_PROCESS(ret = file_manager_->getFileHandlerWithKey(objectPairMempoolHandler.keyPtr_, objectPairMempoolHandler.keySize_, kPut, tempFileHandler, objectPairMempoolHandler.isAnchorFlag_), StatsType::DS_PUT_GET_HANDLER);
    if (ret != true) {
        debug_error("[ERROR] get fileHandler from file manager error for key = %s\n", objectPairMempoolHandler.keyPtr_);
        return false;
    } else {
        if (objectPairMempoolHandler.isAnchorFlag_ == true && (tempFileHandler == nullptr || tempFileHandler->total_object_bytes == 0)) {
            if (tempFileHandler != nullptr) {
                tempFileHandler->file_ownership = 0;
            }
            return true;
        }
        ret = file_operator_->directlyWriteOperation(tempFileHandler, &objectPairMempoolHandler);
        tempFileHandler->file_ownership = 0;
        if (ret != true) {
            debug_error("[ERROR] write to dLog error for key = %s\n", objectPairMempoolHandler.keyPtr_);
            return false;
        } else {
            return true;
        }
    }
}

bool HashStoreInterface::multiPut(vector<mempoolHandler_t> objects)
{
    bool allAnchoarsFlag = true;
    for (auto it : objects) {
        if (it.isAnchorFlag_ == false) {
            allAnchoarsFlag = false;
            break;
        }
    }
    if (allAnchoarsFlag == true && anyBucketInitedFlag_ == false) {
        return true;
    } else {
        anyBucketInitedFlag_ = true;
    }

    struct timeval tv;
    gettimeofday(&tv, 0);
    unordered_map<hashStoreFileMetaDataHandler*, vector<int>> fileHandlerToIndexMap;

    bool need_flush = false;
    if (enable_crash_consistency_ == true) {
        // write to the commit log
        bool write_commit_log_status = 
            file_manager_->writeToCommitLog(objects, need_flush);
        if (write_commit_log_status == false) {
            debug_error("[ERROR] write to commit log failed: %lu objects\n",
                    objects.size());
            exit(1);
        }
    }

    // get all the file handlers 

    if (enable_parallel_get_hdl_) { 
        while (true) {
            gettimeofday(&tv, 0);
            hashStoreOperationHandler hdls[objects.size()];
            for (auto i = 0; i < objects.size(); i++) {
                hdls[i].op_type = kFind;
                hdls[i].object = &objects[i]; 
                file_operator_->putIntoJobQueue(&hdls[i]);
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_GET_HANDLER, tv);

            for (auto i = 0; i < objects.size(); i++) {
                file_operator_->waitOperationHandlerDone(&hdls[i], false);
                auto file_hdl = hdls[i].file_hdl;
                if (file_hdl == nullptr) {
                    // should skip current key since it is only an anchor
                    continue;
                }

                if (fileHandlerToIndexMap.find(file_hdl) != fileHandlerToIndexMap.end()) {
                    fileHandlerToIndexMap.at(file_hdl).push_back(i);
                } else {
                    vector<int> indexVec;
                    indexVec.push_back(i);
                    fileHandlerToIndexMap.insert(make_pair(file_hdl, indexVec));
                }
            }

            if (enable_bucket_size_limit_ == false) {
                break;
            }
        }
    } else {
        gettimeofday(&tv, 0);
        for (auto i = 0; i < objects.size(); i++) {
            hashStoreFileMetaDataHandler* currentFileHandlerPtr = nullptr;
            bool getFileHandlerStatus;
            STAT_PROCESS(getFileHandlerStatus =
                    file_manager_->getFileHandlerWithKey(
                        objects[i].keyPtr_, objects[i].keySize_, kMultiPut,
                        currentFileHandlerPtr, objects[i].isAnchorFlag_),
                    StatsType::DS_MULTIPUT_GET_SINGLE_HANDLER);
            if (getFileHandlerStatus != true) {
                debug_error("[ERROR] Get file handler for key = %s error "
                        "during multiput\n", objects[i].keyPtr_);
                return false;
            } 

	    if (currentFileHandlerPtr == nullptr) {
		// should skip current key since it is only an anchor
		continue;
	    }
	    if (fileHandlerToIndexMap.find(currentFileHandlerPtr) !=
		    fileHandlerToIndexMap.end()) {
		fileHandlerToIndexMap.at(currentFileHandlerPtr).push_back(i);
	    } else {
		vector<int> indexVec;
		indexVec.push_back(i);
		fileHandlerToIndexMap.insert(make_pair(currentFileHandlerPtr,
			    indexVec));
	    }
        }
        if (fileHandlerToIndexMap.size() == 0) {
            return true;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_GET_HANDLER, tv);
    }

    gettimeofday(&tv, 0);
    if (shouldUseDirectOperationsFlag_ == true) {
        unordered_map<hashStoreFileMetaDataHandler*, vector<mempoolHandler_t>> tempFileHandlerMap;
        for (auto mapIt : fileHandlerToIndexMap) {
            vector<mempoolHandler_t> handlerVec;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                handlerVec.push_back(objects[mapIt.second[index]]);
            }
            mapIt.first->markedByMultiPut = false;
            tempFileHandlerMap.insert(make_pair(mapIt.first, handlerVec));
        }
        debug_info("Current handler map size = %lu\n", tempFileHandlerMap.size());
        bool putStatus = file_operator_->directlyMultiWriteOperation(tempFileHandlerMap);
        if (putStatus != true) {
            debug_error("[ERROR] write to dLog error for keys, number = %lu\n", objects.size());
            return false;
        } else {
            debug_trace("Write to dLog success for keys via direct operations, number = %lu\n", objects.size());
            return true;
        }
    } else {
        uint32_t handlerVecIndex = 0;
        uint32_t handlerStartVecIndex = 0;
        uint32_t opHandlerIndex = 0;
        mempoolHandler_t handlerVecTemp[objects.size()];
        vector<hashStoreOperationHandler*> handlers; 
        handlers.resize(fileHandlerToIndexMap.size());
        for (auto mapIt : fileHandlerToIndexMap) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            handlerStartVecIndex = handlerVecIndex;
            for (auto index = 0; index < mapIt.second.size(); index++) {
                handlerVecTemp[handlerVecIndex++] = objects[mapIt.second[index]];
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PROCESS_HANDLERS, tv);
            mapIt.first->markedByMultiPut = false;
            hashStoreOperationHandler* op_hdl = new hashStoreOperationHandler(mapIt.first);
            op_hdl->multiput_op.objects = handlerVecTemp + handlerStartVecIndex;
            op_hdl->multiput_op.size = handlerVecIndex - handlerStartVecIndex;
            op_hdl->op_type = kMultiPut;
            op_hdl->need_flush = need_flush;
            STAT_PROCESS(file_operator_->putIntoJobQueue(op_hdl), StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE_OPERATOR);
            handlers[opHandlerIndex++] = op_hdl;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_PUT_TO_JOB_QUEUE, tv);
        gettimeofday(&tv, 0);

        for (auto& it : handlers) {
            file_operator_->waitOperationHandlerDone(it);
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DS_MULTIPUT_WAIT_HANDLERS, tv);
    }

    if (need_flush) {
        // Flush all buffers in the buckets. In the FileManager
        bool status;
        vector<hashStoreFileMetaDataHandler*> file_hdls; 
        status = file_manager_->prepareForUpdatingMetadata(file_hdls); 

        if (status == false) {
            debug_error("Error for updating meta: %d\n", (int)status);
            exit(1);
        }

        vector<hashStoreOperationHandler*> handlers; 

        struct timeval tv;
        gettimeofday(&tv, 0);

        for (auto& it : file_hdls) {
            if (it != nullptr && 
                    it->file_op_ptr->getFileBufferedSize() > 0) {
                if (it->file_ownership != 0) {
                    debug_error("wait file owner ship %lu %d\n",
                            it->file_id,
                            (int)it->file_ownership);
                }
                while (it->file_ownership != 0) {
                    asm volatile("");
                }
                if (it->gc_status == kShouldDelete) {
                    continue;
                }

                it->file_ownership = 1;
                hashStoreOperationHandler* op_hdl = new
                    hashStoreOperationHandler(it);
                op_hdl->op_type = kFlush;
                file_operator_->putIntoJobQueue(op_hdl);
                handlers.push_back(op_hdl);
            }
        }

        for (auto& it : handlers) {
            STAT_PROCESS(
            file_operator_->waitOperationHandlerDone(it),
            StatsType::DELTAKV_HASHSTORE_WAIT_SYNC);
        }

        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_SYNC, tv);
    }
    return true;
}

bool HashStoreInterface::get(const string& keyStr, vector<string>& valueStrVec)
{
    debug_info("New OP: get deltas for key = %s\n", keyStr.c_str());
    hashStoreFileMetaDataHandler* tempFileHandler;
    bool ret;
    STAT_PROCESS(ret = file_manager_->getFileHandlerWithKey((char*)keyStr.c_str(), keyStr.size(), kGet, tempFileHandler, false), StatsType::DELTAKV_HASHSTORE_GET_FILE_HANDLER);
    if (ret != true) {
        valueStrVec.clear();
        return false;
    } else {
        StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_TIMES, 1, 1);
        // No bucket. Exit
        if (tempFileHandler == nullptr) {
            valueStrVec.clear();
            return true;
        }

        if (enable_lsm_tree_delta_meta_ == true ||
                tempFileHandler->filter->MayExist(keyStr) ||
                tempFileHandler->sorted_filter->MayExist(keyStr)) {
            ret = file_operator_->directlyReadOperation(tempFileHandler, keyStr, valueStrVec);
            bool deltaExistFlag = (!valueStrVec.empty());
            if (deltaExistFlag) {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_TRUE, 1, 1);
            } else {
                StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_EXIST_FALSE, 1, 1);
            }
            if (ret == false) {
                debug_error("failed %s\n", keyStr.c_str());
                exit(1);
            }
            return ret;
        } else {
            tempFileHandler->file_ownership = 0;
            return true;
        }
    }
}

bool HashStoreInterface::multiGet(vector<string>& keys, vector<vector<string>>& valueStrVecVec)
{
    bool ret;

    bool get_result[keys.size()]; 
    valueStrVecVec.resize(keys.size());
    int need_read = keys.size();
    int all = keys.size();


    // Go through the cache one by one
    // TODO parallelize
    for (int i = 0; i < keys.size(); i++) {
        get_result[i] = false;
        if (kd_cache_ != nullptr) {
            str_t key(keys[i].data(), keys[i].size());
            str_t delta = kd_cache_->getFromCache(key);
            if (delta.data_ != nullptr && delta.size_ > 0) {
                valueStrVecVec[i].push_back(string(delta.data_, delta.size_));
                // non-empty delta for this key
                get_result[i] = true;
                need_read--;
            } else if (delta.data_ == nullptr && delta.size_ == 0) {
                valueStrVecVec[i].clear();
                // empty delta for this key
                get_result[i] = true;
                need_read--;
            }
        }
    }

    // Check the file pointer one by one
    // TODO parallelize
    vector<hashStoreFileMetaDataHandler*> file_hdls;
    file_hdls.resize(all);
    unordered_map<hashStoreFileMetaDataHandler*, vector<int>> fileHandlerToIndexMap;

    for (int i = 0; i < all; i++) {
        if (get_result[i] == false) {
	    // get the file handlers in parallel
            STAT_PROCESS(ret = 
                    file_manager_->getFileHandlerWithKey(keys[i].data(),
                        keys[i].size(), kMultiGet, file_hdls[i], false),
                    StatsType::DELTAKV_HASHSTORE_GET_FILE_HANDLER);
            if (ret == false) {
                debug_error("Get handler error for key %s\n", keys[i].c_str());
                exit(1);
            }

            auto& file_hdl = file_hdls[i];

            if (file_hdl == nullptr) { 
                // no bucket
                valueStrVecVec[i].clear();
                get_result[i] = true;
                need_read--;
            } else if (file_hdl->filter->MayExist(keys[i]) == false && 
                    file_hdl->sorted_filter->MayExist(keys[i]) == false) {
                // bucket does not have the delta
                valueStrVecVec[i].clear();
                get_result[i] = true;
                need_read--;
            } else {
		// need to 
		if (fileHandlerToIndexMap.find(file_hdl) !=
			fileHandlerToIndexMap.end()) {
		    fileHandlerToIndexMap.at(file_hdl).push_back(i);
		} else {
		    vector<int> indexVec;
		    indexVec.push_back(i);
		    fileHandlerToIndexMap.insert(make_pair(file_hdl, indexVec));
		}
	    }
        } else {
            file_hdls[i] = nullptr;
        }
    }

    StatsRecorder::getInstance()->totalProcess(StatsType::FILTER_READ_TIMES, 1, 1);

    // operation handlers
    vector<hashStoreOperationHandler*> handlers;
    handlers.resize(fileHandlerToIndexMap.size());

    // map the small array (all needs read) to the large array (some needs)
    int needed_i = 0;

    for (auto& mapIt : fileHandlerToIndexMap) {
	auto& file_hdl = mapIt.first;
	auto& index_vec = mapIt.second;
        auto op_hdl = new hashStoreOperationHandler(file_hdl);
	file_hdl->markedByMultiGet = false;
        op_hdl->multiget_op.keys = new vector<string*>; 
        op_hdl->multiget_op.values = new vector<string*>; 
	op_hdl->multiget_op.key_indices = index_vec;
        op_hdl->op_type = kMultiGet;

	for (auto index = 0; index < index_vec.size(); index++) {
	    auto& key_i = index_vec[index];
	    if (key_i >= keys.size() || get_result[key_i] == true) {
		debug_error("key_i %d keys.size() %lu get result %d\n",
			key_i, keys.size(), (int)get_result[key_i]);
		exit(1);
	    }
	    op_hdl->multiget_op.keys->push_back(&keys[index_vec[index]]); 
	}
        
        file_operator_->putIntoJobQueue(op_hdl);
        handlers[needed_i++] = op_hdl;
    }

    for (int i = 0; i < needed_i; i++) {
        auto& op_hdl = handlers[i];
	// do not delete the handler
        file_operator_->waitOperationHandlerDone(op_hdl, false);
	auto& values = *(op_hdl->multiget_op.values);

	for (int index_i = 0; index_i < values.size(); index_i++) {
	    auto& key_i = op_hdl->multiget_op.key_indices[index_i];
	    if (get_result[key_i] == true) {
		debug_error("key_i %d has result?\n", key_i);
	    }
	    if (values[index_i] != nullptr) {
		valueStrVecVec[key_i].clear();
		valueStrVecVec[key_i].push_back(*values[index_i]);

		delete values[index_i];
	    }
	}

	delete op_hdl->multiget_op.keys;
	delete op_hdl->multiget_op.values;
	delete op_hdl;
    }

    return true;
}

bool HashStoreInterface::forcedManualGarbageCollection()
{
    bool forcedGCStatus = file_manager_->forcedManualGCAllFiles();
    if (forcedGCStatus == true) {
        return true;
    } else {
        return false;
    }
}

}
