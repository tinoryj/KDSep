#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

DeltaKV::DeltaKV()
{
}

DeltaKV::~DeltaKV()
{
    cerr << "[DeltaKV Interface] Try delete write batch" << endl;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        delete notifyWriteBatchMQ_;
        delete writeBatchMapForSearch_[0];
        delete writeBatchMapForSearch_[1];
    }
    cerr << "[DeltaKV Interface] Try delete write back" << endl;
    if (enableWriteBackOperationsFlag_ == true) {
        delete writeBackOperationsQueue_;
    }
    cerr << "[DeltaKV Interface] Try delete lsm interface" << endl;
    if (enableParallelLsmInterface == true) {
        delete lsmInterfaceOperationsQueue_;
    }
    cerr << "[DeltaKV Interface] Try delete Read Cache" << endl;
    if (enableKeyValueCache_ == true) {
        delete keyToValueListCache_;
    }
    cerr << "[DeltaKV Interface] Try delete HashStore" << endl;
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        cerr << "interface" << endl;
        delete HashStoreInterfaceObjPtr_;
        cerr << "file manager" << endl;
        delete hashStoreFileManagerPtr_;
        cerr << "file operator" << endl;
        delete hashStoreFileOperatorPtr_;
    }
    delete objectPairMemPool_;
    cerr << "[DeltaKV Interface] Try delete RocksDB" << endl;
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
    // object mem pool
    lsmTreeInterface_.Open(options, name);

    objectPairMemPool_ = new KeyValueMemPool(options.deltaStore_mem_pool_object_number_, options.deltaStore_mem_pool_object_size_);
    // Rest merge function if delta/value separation enabled
    deltaKVMergeOperatorPtr_ = options.deltaKV_merge_operation_ptr;
    enableLsmTreeDeltaMeta_ = options.enable_lsm_tree_delta_meta;
    if (options.enable_key_value_cache_ == true && options.key_value_cache_object_number_ != 0) {
        enableKeyValueCache_ = true;
        keyToValueListCache_ = new AppendAbleLRUCache<string, string>(options.key_value_cache_object_number_);
    } else {
        enableKeyValueCache_ = false;
    }
    if (options.enable_batched_operations_ == true) {
        writeBatchMapForSearch_[0] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        writeBatchMapForSearch_[1] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        notifyWriteBatchMQ_ = new messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        thList_.push_back(th);
        isBatchedOperationsWithBufferInUse_ = true;
        maxBatchOperationBeforeCommitNumber_ = options.write_buffer_num;
        maxBatchOperationBeforeCommitSize_ = options.write_buffer_size;
    }

    if (options.enable_write_back_optimization_ == true) {
        enableWriteBackOperationsFlag_ = true;
        writeBackWhenReadDeltaNumerThreshold_ = options.deltaStore_write_back_during_reads_threshold;
        writeBackWhenReadDeltaSizeThreshold_ = options.deltaStore_write_back_during_reads_size_threshold;
        writeBackOperationsQueue_ = new messageQueue<writeBackObject*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processWriteBackOperationsWorker, this));
        thList_.push_back(th);
    }

    if (options.enable_parallel_lsm_interface_ == true) {
        enableParallelLsmInterface = true;
        lsmInterfaceOperationsQueue_ = new messageQueue<lsmInterfaceOperationStruct*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processLsmInterfaceOperationsWorker, this));
        thList_.push_back(th);
    } else {
        enableParallelLsmInterface = false;
    }

    if (options.enable_deltaStore == true && HashStoreInterfaceObjPtr_ == nullptr) {
        isDeltaStoreInUseFlag_ = true;
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, writeBackOperationsQueue_);
        deltaExtractSize_ = HashStoreInterfaceObjPtr_->getExtractSizeThreshold();
        // create deltaStore related threads
        boost::thread* th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::scheduleMetadataUpdateWorker, hashStoreFileManagerPtr_));
        thList_.push_back(th);
        if (options.enable_deltaStore_garbage_collection == true) {
            enableDeltaStoreWithBackgroundGCFlag_ = true;
            th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::processMergeGCRequestWorker, hashStoreFileManagerPtr_));
            thList_.push_back(th);
            for (auto threadID = 0; threadID < options.deltaStore_gc_worker_thread_number_limit_; threadID++) {
                th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::processSingleFileGCRequestWorker, hashStoreFileManagerPtr_, threadID));
                thList_.push_back(th);
            }
        }
        if (options.deltaStore_op_worker_thread_number_limit_ >= 2) {
//            th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::notifyOperationWorkerThread, hashStoreFileOperatorPtr_));
//            thList_.push_back(th);
            for (auto threadID = 0; threadID < options.deltaStore_op_worker_thread_number_limit_; threadID++) {
                th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_, threadID));
                thList_.push_back(th);
            }
        } else {
            debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
        }
    }

    // process runnning mode
    if (options.enable_deltaStore) {
        deltaKVRunningMode_ = options.enable_batched_operations_ ? kBatchedWithDeltaStore : kWithDeltaStore;
    } else {
        deltaKVRunningMode_ = options.enable_batched_operations_ ? kBatchedWithNoDeltaStore : kWithNoDeltaStore;
    }
    cerr << "deltaKVRunningMode_ = " << deltaKVRunningMode_ << endl;

    return true;
}

bool DeltaKV::Close()
{
    cerr << "[DeltaKV Close DB] Force GC" << endl;
    if (isDeltaStoreInUseFlag_ == true) {
        if (enableDeltaStoreWithBackgroundGCFlag_ == true) {
            HashStoreInterfaceObjPtr_->forcedManualGarbageCollection();
            cerr << "\tDeltaStore forced GC done" << endl;
        }
    }
    cerr << "[DeltaKV Close DB] Wait write back" << endl;
    if (enableWriteBackOperationsFlag_ == true) {
        writeBackOperationsQueue_->done = true;
        while (writeBackOperationsQueue_->isEmpty() == false) {
            asm volatile("");
        }
        cerr << "\tWrite back done" << endl;
    }
    cerr << "[DeltaKV Close DB] Flush write buffer" << endl;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        for (auto i = 0; i < 2; i++) {
            if (writeBatchMapForSearch_[i]->size() != 0) {
                notifyWriteBatchMQ_->push(writeBatchMapForSearch_[i]);
            }
        }
        notifyWriteBatchMQ_->done = true;
        while (writeBatchOperationWorkExitFlag == false) {
            asm volatile("");
        }
        cerr << "\tFlush write batch done" << endl;
    }
    cerr << "[DeltaKV Close DB] Set job done" << endl;
    if (enableParallelLsmInterface == true) {
        lsmInterfaceOperationsQueue_->done = true;
        lsm_interface_cv.notify_one();
        while (lsmInterfaceOperationsQueue_->isEmpty() == false) {
            asm volatile("");
        }
        cerr << "\tLSM tree interface operations done" << endl;
    }
    cerr << "[DeltaKV Close DB] LSM-tree interface" << endl;
    if (isDeltaStoreInUseFlag_ == true) {
        HashStoreInterfaceObjPtr_->setJobDone();
        cerr << "\tHashStore set job done" << endl;
    }
    cerr << "[DeltaKV Close DB] Delete existing threads" << endl;
    deleteExistingThreads();
    lsmTreeInterface_.Close();
    cerr << "\tJoin all existing threads done" << endl;
    return true;
}

bool DeltaKV::SinglePutInternal(const mempoolHandler_t& mempoolHandler) 
{
    if (deltaKVRunningMode_ == kWithNoDeltaStore) {
        return lsmTreeInterface_.Put(mempoolHandler);
    } else {
        bool updateLsmTreeStatus = lsmTreeInterface_.Put(mempoolHandler);
        if (updateLsmTreeStatus == false) {
            debug_error("[ERROR] Put LSM-tree failed, key = %s\n", mempoolHandler.keyPtr_);
        }
        bool updateAnchorStatus;
        STAT_PROCESS(updateAnchorStatus = HashStoreInterfaceObjPtr_->put(mempoolHandler), StatsType::DKV_PUT_DSTORE);
        return updateAnchorStatus;
    }
}

bool DeltaKV::SingleMergeInternal(const mempoolHandler_t& mempoolHandler)
{
    if (deltaKVRunningMode_ == kWithNoDeltaStore) {
        return lsmTreeInterface_.Merge(mempoolHandler);
    } else if (enableLsmTreeDeltaMeta_ == true) {
        // Large enough, do separation
        if (mempoolHandler.valueSize_ >= deltaExtractSize_) {
            bool status;
            STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(mempoolHandler), StatsType::DELTAKV_MERGE_HASHSTORE);
            if (status == false) {
                debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", mempoolHandler.keyPtr_, mempoolHandler.valuePtr_);
                return false;
            }

            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType(false, true, mempoolHandler.sequenceNumber_, mempoolHandler.valueSize_);
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            return lsmTreeInterface_.Merge(mempoolHandler.keyPtr_, mempoolHandler.keySize_, writeInternalValueBuffer, sizeof(internalValueType));
        } else { // do not do separation
            char writeInternalValueBuffer[sizeof(internalValueType) + mempoolHandler.valueSize_];
            internalValueType currentInternalValueType(false, false, mempoolHandler.sequenceNumber_, mempoolHandler.valueSize_);
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            memcpy(writeInternalValueBuffer + sizeof(internalValueType), mempoolHandler.valuePtr_, mempoolHandler.valueSize_);
            return lsmTreeInterface_.Merge(mempoolHandler.keyPtr_, mempoolHandler.keySize_, writeInternalValueBuffer, sizeof(internalValueType) + mempoolHandler.valueSize_);
        }
    } else {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(mempoolHandler), StatsType::DELTAKV_MERGE_HASHSTORE);
        return status;
    }
}

bool DeltaKV::GetInternal(const string& key, string* value, uint32_t maxSequenceNumber, bool getByWriteBackFlag)
{
    // Do not use deltaStore
    if (deltaKVRunningMode_ == kWithNoDeltaStore || deltaKVRunningMode_ == kBatchedWithNoDeltaStore) {
        string internalValueStr;
        bool ret = lsmTreeInterface_.Get(key, &internalValueStr);
        if (ret == false) {
            debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
            exit(1);
        }
        value->assign(internalValueStr.substr(sizeof(internalValueType)));
        return true;
    }

    if (enableLsmTreeDeltaMeta_ == true) {
        // Use deltaStore
        string internalValueStr;
        bool ret;
        STAT_PROCESS(ret = lsmTreeInterface_.Get(key, &internalValueStr), StatsType::LSM_INTERFACE_GET); // (, maxSequenceNumber, getByWriteBackFlag);

        if (ret == false) {
            debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
            return false;
        } 

        internalValueType tempInternalValueHeader;
        memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
        string rawValueStr;

        if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
            debug_error("[ERROR] value separated but not retrieved %s\n", key.c_str());
            assert(0);
        }

        if (tempInternalValueHeader.mergeFlag_ == true) {
            if (enableLsmTreeDeltaMeta_ == false) {
                debug_error("[ERROR] settings with no metadata but LSM-tree has metadata, key %s\n", key.c_str());
                exit(1);
            }

            // get deltas from delta store
            vector<pair<bool, string>> deltaInfoVec;
            STAT_PROCESS(processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, maxSequenceNumber), StatsType::DELTAKV_GET_PROCESS_BUFFER);

            char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
            memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
            string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
            rawValueStr.assign(internalRawValueStr);

            bool isAnyDeltasAreExtratedFlag = false;
            for (auto& it : deltaInfoVec) {
                if (it.first == true) {
                    isAnyDeltasAreExtratedFlag = true;
                }
            }

            if (isAnyDeltasAreExtratedFlag == true) {
                // should read external delta store
                vector<string> deltasFromDeltaStoreVec;
                bool ret;
                STAT_PROCESS(ret = HashStoreInterfaceObjPtr_->get(key, deltasFromDeltaStoreVec), StatsType::DS_GET);
                if (ret != true) {
                    debug_error("[ERROR] Read external deltaStore fault, key = %s\n", key.c_str());
                    return false;
                } else {
                    vector<string> finalDeltaOperatorsVec;
                    auto index = 0;
                    debug_trace("Read from deltaStore object number = %lu, target delta number in RocksDB = %lu\n", deltasFromDeltaStoreVec.size(), deltaInfoVec.size());
                    for (auto i = 0; i < deltaInfoVec.size(); i++) {
                        if (deltaInfoVec[i].first == true) {
                            if (index >= deltasFromDeltaStoreVec.size()) {
                                debug_error("[ERROR] Read external deltaStore number mismatch with requested number (may overflow), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltasFromDeltaStoreVec.size(), deltaInfoVec.size());
                                exit(1);
                                return false;
                            }
                            finalDeltaOperatorsVec.push_back(deltasFromDeltaStoreVec.at(index));
                            index++;
                        } else {
                            finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                        }
                    }

                    if (index != deltasFromDeltaStoreVec.size()) {
                        debug_error("[ERROR] Read external deltaStore number mismatch with requested number (Inconsistent), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltasFromDeltaStoreVec.size(), deltaInfoVec.size());
                        exit(1);
                        return false;
                    } 

                    debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                    STAT_PROCESS(deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value), StatsType::DELTAKV_GET_FULL_MERGE);
                    if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                        bool ret;
                        STAT_PROCESS(ret = PutImpl(key, *value),
                                StatsType::DELTAKV_GET_PUT_WRITE_BACK);
                        if (ret == false) {
                            debug_error("Write back failed key %s\n",
                                    key.c_str());
                            exit(1);
                        }
                        //                    writeBackObject* newPair = new writeBackObject(key, "", 0);
                        //                    writeBackOperationsQueue_->push(newPair);
                    }
                    return true;
                }
            } else {
                // all deltas are stored internal
                vector<string> finalDeltaOperatorsVec;
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                }
                debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                STAT_PROCESS(deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value), StatsType::DELTAKV_GET_FULL_MERGE);
                if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                    writeBackObject* newPair = new writeBackObject(key, "", 0);
                    writeBackOperationsQueue_->push(newPair);
                }
                return true;
            }
        } else  {
            maxSequenceNumber = tempInternalValueHeader.sequenceNumber_;
            if (internalValueStr.size() < sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_) {
                debug_error("string size %lu raw value size %u\n", internalValueStr.size(), tempInternalValueHeader.rawValueSize_); 
                exit(1);
            }
            value->assign(string(internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_));
            return true;
        }
    } else {
        // do not have metadata

        // Use deltaStore
        string internalValueStr;
        bool ret;
        struct lsmInterfaceOperationStruct* lsmInterfaceOpPtr;
       
        if (enableParallelLsmInterface == true && deltaKVRunningMode_ == kWithDeltaStore) {
            lsmInterfaceOpPtr = new lsmInterfaceOperationStruct;
            lsmInterfaceOpPtr->key = key;
            lsmInterfaceOpPtr->value = &internalValueStr;
            lsmInterfaceOpPtr->is_write = false;
            lsmInterfaceOpPtr->job_done = kNotDone;
            lsmInterfaceOperationsQueue_->push(lsmInterfaceOpPtr);
            lsm_interface_cv.notify_one();
        } else {
            STAT_PROCESS(ret = lsmTreeInterface_.Get(key, &internalValueStr), StatsType::LSM_INTERFACE_GET); // (, maxSequenceNumber, getByWriteBackFlag);
            if (ret == false) {
                debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
                return false;
            }
        }

        // get deltas from delta store
        vector<string> deltasFromDeltaStoreVec;
        ret = false;
        STAT_PROCESS(ret = HashStoreInterfaceObjPtr_->get(key, deltasFromDeltaStoreVec), StatsType::DS_GET);
        if (ret != true) {
            debug_trace("Read external deltaStore fault, key = %s\n", key.c_str());
            return false;
        }

        if (enableParallelLsmInterface == true && deltaKVRunningMode_ == kWithDeltaStore) {
//            struct timeval tv1, tv2;
//            gettimeofday(&tv1, 0);
//            uint64_t mx = 100000;
//            lsm_interface_cv.notify_one();
//            while (lsmInterfaceOpPtr->job_done == kNotDone) {
//                gettimeofday(&tv2, 0);
//                if ((tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_sec -
//                        tv1.tv_sec > mx) {
//                    mx += 100000; 
//                    if (mx > 10 * 1e6) {
//                        debug_error("Wait for %.2lf second. Notify\n", mx / 1000000.0);
//                        lsm_interface_cv.notify_one();
//                    }
//                }
//            }
            while (lsmInterfaceOpPtr->job_done == kNotDone) {
                asm volatile("");
            }
            if (lsmInterfaceOpPtr->job_done == kError) {
                debug_error("lsmInterfaceOp error %s\n", ""); 
            }
            delete lsmInterfaceOpPtr;
        }

        internalValueType tempInternalValueHeader;
        memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
        string rawValueStr;

        if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
            debug_error("[ERROR] value separated but not retrieved %s\n", key.c_str());
            assert(0);
        }

        str_t internalRawValueStrT(internalValueStr.data() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
        maxSequenceNumber = tempInternalValueHeader.sequenceNumber_;
        
        if (deltasFromDeltaStoreVec.empty() == false) {
            bool mergeOperationStatus;
            vector<str_t> deltaInStrT;
            int totalDeltaSizes = 0;
            for (auto& it : deltasFromDeltaStoreVec) {
                deltaInStrT.push_back(str_t(it.data(), it.size()));
                totalDeltaSizes += it.size();
            }
            STAT_PROCESS(mergeOperationStatus = deltaKVMergeOperatorPtr_->Merge(internalRawValueStrT, deltaInStrT /*deltasFromDeltaStoreVec*/, value), StatsType::DELTAKV_GET_FULL_MERGE);
            if (mergeOperationStatus == true) {
                if (enableWriteBackOperationsFlag_ == true && !getByWriteBackFlag &&  
                        ((deltasFromDeltaStoreVec.size() >
                          writeBackWhenReadDeltaNumerThreshold_ &&
                          writeBackWhenReadDeltaNumerThreshold_ != 0) ||
                        (totalDeltaSizes > writeBackWhenReadDeltaSizeThreshold_
                         && writeBackWhenReadDeltaSizeThreshold_ != 0))) {
                    bool ret;
                    STAT_PROCESS(ret = PutImpl(key, *value), StatsType::DELTAKV_GET_PUT_WRITE_BACK);
                    if (ret == false) {
                        debug_error("write back failed key %s value %.*s\n", key.c_str(), 
                                (int)internalRawValueStrT.size_, internalRawValueStrT.data_);
                        for (auto& it : deltaInStrT) {
                            debug_error("delta %.*s\n", (int)it.size_,
                                    it.data_);
                        }
                        exit(1);
                    }
//                    writeBackObject* newPair = new writeBackObject(key, "", 0);
//                    writeBackOperationsQueue_->push(newPair);
                }
                return true;
            } else {
                debug_error("[ERROR] Perform merge operation fail, key = %s\n", key.c_str());
                return false;
            }
        } else {
            value->assign(internalRawValueStrT.data_, internalRawValueStrT.size_);
            return true;
        }
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    scoped_lock<shared_mutex> w_lock(DeltaKVOperationsMtx_);
    // insert to cache if is value update
    if (enableKeyValueCache_ == true) {
        string cacheKey = key;
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (keyToValueListCache_->existsInCache(cacheKey) == true) {
            keyToValueListCache_->getFromCache(cacheKey).assign(value);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_NEW, tv);
        }
    }

    bool ret = PutImpl(key, value);
    if (ret == false) {
        debug_error("write failed %s\n", key.c_str());
        exit(1);
    }
    return ret;
}

bool DeltaKV::PutImpl(const string& key, const string& value) {
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    mempoolHandler_t mempoolHandler;
    bool insertMemPoolStatus;
    STAT_PROCESS(insertMemPoolStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, true, mempoolHandler), StatsType::DELTAKV_INSERT_MEMPOOL);
    if (insertMemPoolStatus == false) {
        debug_error("insert to mempool failed, key %s value size %lu\n", key.c_str(), value.size());
        return false;
    }
    bool putOperationStatus = true;
    bool deleteMemPoolHandlerStatus = false;
    switch (deltaKVRunningMode_) {
    case kBatchedWithNoDeltaStore:
    case kBatchedWithDeltaStore:
        putOperationStatus = PutWithWriteBatch(mempoolHandler);
        break;
    case kWithDeltaStore:
    case kWithNoDeltaStore:
        putOperationStatus = SinglePutInternal(mempoolHandler);
        deleteMemPoolHandlerStatus = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        putOperationStatus = false;
        deleteMemPoolHandlerStatus = true;
        break;
    }
    if (deleteMemPoolHandlerStatus == true) {
        objectPairMemPool_->eraseContentFromMemPool(mempoolHandler);
    }

    if (putOperationStatus == false) {
        debug_error("[ERROR] Could not put back current value, skip write back, key = %s, value = %s\n", key.c_str(), value.c_str());
        return false;
    }

    return true;
}

bool DeltaKV::Get(const string& key, string* value)
{
    scoped_lock<shared_mutex> w_lock(DeltaKVOperationsMtx_);
    // search first
    if (enableKeyValueCache_ == true) {
        string cacheKey = key;
        if (keyToValueListCache_->existsInCache(cacheKey) == true) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            value->assign(keyToValueListCache_->getFromCache(cacheKey));
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_GET, tv);
            return true;
        }
    }
    vector<str_t> tempNewMergeOperatorsStrTVec;
    vector<string> tempNewMergeOperatorsVec;
    bool needMergeWithInBufferOperationsFlag = false;
    struct timeval tvAll;
    gettimeofday(&tvAll, 0);

    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
        scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_GET_WAIT_BUFFER, tvAll);
        struct timeval tv;
        gettimeofday(&tv, 0);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        char keyBuffer[key.size()];
        memcpy(keyBuffer, key.c_str(), key.size());
        str_t currentKey(keyBuffer, key.length());
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            if (mapIt->second.size() != 0 && mapIt->second.back().first == kPutOp) {
                value->assign(mapIt->second.back().second.valuePtr_, mapIt->second.back().second.valueSize_);
                debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_GET_KEY, tv);
                return true;
            }
            struct timeval tv0;
            gettimeofday(&tv0, 0);
            str_t newValueStr;
            bool findNewValueFlag = false;
            for (auto queueIt : mapIt->second) {
                if (queueIt.first == kPutOp) {
                    newValueStr = str_t(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    tempNewMergeOperatorsStrTVec.clear();
                    findNewValueFlag = true;
                } else {
                    str_t currentValue(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    tempNewMergeOperatorsStrTVec.push_back(currentValue);
                }
            }
            if (findNewValueFlag == true) {
                STAT_PROCESS(deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsStrTVec, value), StatsType::DKV_GET_FULL_MERGE);
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsStrTVec.size());
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_MERGE, tv0);
                return true;
            }
            if (tempNewMergeOperatorsStrTVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsStrTVec.size());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_MERGE_ALL, tv0);
        }
        
        for (auto& it : tempNewMergeOperatorsStrTVec) {
            tempNewMergeOperatorsVec.push_back(string(it.data_, it.size_));
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_GET_READ_BUFFER, tv);
    } 

    struct timeval tv;
    gettimeofday(&tv, 0);
    uint32_t maxSequenceNumberPlaceHolder = 0;
    bool ret;

    // Read from deltastore (or no deltastore)
    ret = GetInternal(key, value, maxSequenceNumberPlaceHolder, false);
    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_STORE, tv);
    if (ret == false) {
        return false;
    } else {
        if (needMergeWithInBufferOperationsFlag == true) {
            string tempValueStr;
            tempValueStr.assign(*value);
            str_t tempValueStrT(tempValueStr.data(), tempValueStr.size());
            vector<str_t> tempVec;
            for (auto& it : tempNewMergeOperatorsVec) {
                tempVec.push_back(str_t(it.data(), it.size()));
            }
            value->clear();
            bool mergeStatus;
            STAT_PROCESS(mergeStatus = deltaKVMergeOperatorPtr_->Merge(tempValueStrT, tempVec, value), StatsType::DKV_GET_FULL_MERGE);
            if (mergeStatus == false) {
                debug_error("[ERROR] merge failed: key %s number of deltas %lu raw value size %lu\n", key.c_str(), tempVec.size(), tempValueStr.size());
                exit(1);
            }
        }
        if (enableKeyValueCache_ == true) {
            string cacheKey = key;
            if (keyToValueListCache_->existsInCache(cacheKey) == false) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                string cacheValue;
                cacheValue.assign(*value);
                keyToValueListCache_->insertToCache(cacheKey, cacheValue);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_NEW, tv);
            }
        }
        return true;
    }
}

// The correct procedure: (Not considering deleted keys)
// 1. Scan the RocksDB/vLog for keys and values
// 2. Scan the buffer to check whether some keys are updated
// 3. Scan delta store to find deltas.
bool DeltaKV::Scan(const string& startKey, int len, vector<string>& keys, vector<string>& values) 
{
    // 1. Scan the RocksDB/vLog for keys and values
    lsmTreeInterface_.Scan(startKey, len, keys, values);
//    fprintf(stderr, "Start key %s len %d\n", startKey.c_str(), len);
//    fprintf(stderr, "keys.size() %lu values.size() %lu\n", keys.size(),
//            values.size());
//    for (int i = 0; i < (int)keys.size(); i++) {
//        fprintf(stderr, "%s %lu\n", keys[i].c_str(), values[i].size());
//    }
//    exit(1);

    return true;
}

bool DeltaKV::Merge(const string& key, const string& value)
{
    scoped_lock<shared_mutex> w_lock(DeltaKVOperationsMtx_);
    if (enableKeyValueCache_ == true) {
        string cacheKey = key;
        if (keyToValueListCache_->existsInCache(cacheKey) == true) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            string oldValue = keyToValueListCache_->getFromCache(cacheKey);
            string finalValue;
            vector<string> operandListForCacheUpdate;
            operandListForCacheUpdate.push_back(value);
            STAT_PROCESS(deltaKVMergeOperatorPtr_->Merge(oldValue, operandListForCacheUpdate, &finalValue), StatsType::DKV_MERGE_FULL_MERGE);
            keyToValueListCache_->getFromCache(cacheKey).assign(finalValue);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_MERGE, tv);
        }
    }
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();

    mempoolHandler_t mempoolHandler;
    ;
    bool insertStatus = false;
    STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, false, mempoolHandler), StatsType::DELTAKV_INSERT_MEMPOOL);
    if (insertStatus == false) {
        debug_error("Insert error, size %lu %lu\n", key.size(), value.size());
        exit(1);
    }
    bool shouldDeleteMemPoolHandler = false;
    bool mergeOperationStatus = false;
    switch (deltaKVRunningMode_) {
    case kBatchedWithNoDeltaStore:
    case kBatchedWithDeltaStore:
        mergeOperationStatus = MergeWithWriteBatch(mempoolHandler); 
        break;
    case kWithDeltaStore:
    case kWithNoDeltaStore:
        mergeOperationStatus = SingleMergeInternal(mempoolHandler);
        shouldDeleteMemPoolHandler = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        mergeOperationStatus = false;
        shouldDeleteMemPoolHandler = true;
        break;
    }
    if (shouldDeleteMemPoolHandler == true) {
        objectPairMemPool_->eraseContentFromMemPool(mempoolHandler);
    }
    if (mergeOperationStatus == true) {
        return true;
    } else {
        return false;
    }
}

/*
bool DeltaKV::GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    scoped_lock<shared_mutex> w_lock(DeltaKVOperationsMtx_);
    switch (deltaKVRunningMode_) {
    case kBatchedWithDeltaStore:
    case kWithDeltaStore:
        if (GetWithDeltaStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBatchedWithNoDeltaStore:
    case kWithNoDeltaStore:
        if (GetWithNoDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        return false;
        break;
    }
    return false;
}
*/

bool DeltaKV::GetCurrentValueThenWriteBack(const string& key)
{
    scoped_lock<shared_mutex> w_lock(DeltaKVOperationsMtx_);

    vector<string> tempNewMergeOperatorsVec;
    bool needMergeWithInBufferOperationsFlag = false;

    struct timeval tvAll;
    gettimeofday(&tvAll, 0);
    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
        batchedBufferOperationMtx_.lock();
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK_WAIT_BUFFER, tvAll);
        struct timeval tv;
        gettimeofday(&tv, 0);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        char keyBuffer[key.size()];
        memcpy(keyBuffer, key.c_str(), key.size());
        str_t currentKey(keyBuffer, key.length());
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            struct timeval tv0;
            gettimeofday(&tv0, 0);
            for (auto queueIt : mapIt->second) {
                if (queueIt.first == kPutOp) {
                    debug_info("Get current value in write buffer, skip write back, key = %s\n", key.c_str());
                    batchedBufferOperationMtx_.unlock();
                    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK_NO_WAIT_BUFFER, tv);
                    return true;
                } else {
                    tempNewMergeOperatorsVec.push_back(string(queueIt.second.valuePtr_, queueIt.second.valueSize_));
                }
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
        }
        batchedBufferOperationMtx_.unlock();
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK_NO_WAIT_BUFFER, tv);
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK_CHECK_BUFFER, tvAll);
    // get content from underlying DB
    string newValueStr;
    uint32_t maxSequenceNumber = 0;
    string tempRawValueStr;
    bool getNewValueStrSuccessFlag;
    STAT_PROCESS(getNewValueStrSuccessFlag = GetInternal(key, &tempRawValueStr, maxSequenceNumber, true), StatsType::DELTAKV_WRITE_BACK_GET);
    bool mergeStatus;
    if (getNewValueStrSuccessFlag) { 
        if (needMergeWithInBufferOperationsFlag == true) {
            STAT_PROCESS(mergeStatus = deltaKVMergeOperatorPtr_->Merge(tempRawValueStr, tempNewMergeOperatorsVec, &newValueStr), StatsType::DELTAKV_WRITE_BACK_FULL_MERGE);
            if (mergeStatus == false) {
                debug_error("merge failed: key %s raw value size %lu\n", key.c_str(), tempRawValueStr.size());
                exit(1);
            }
        } else {
            newValueStr.assign(tempRawValueStr);
        }
    }

    if (getNewValueStrSuccessFlag == false) {
        debug_error("Could not get current value, skip write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
        return false;
    }

    debug_warn("Get current value done, start write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());

    bool ret;
    STAT_PROCESS(ret = PutImpl(key, newValueStr), StatsType::DELTAKV_WRITE_BACK_PUT);
    if (ret == false) {
        debug_error("write back failed, key %s\n", key.c_str());
    }

    return ret;
}

//bool DeltaKV::SingleDelete(const string& key)
//{
//    rocksdb::Status rocksDBStatus = pointerToRawRocksDB_->SingleDelete(internalWriteOption_, key);
//    if (!rocksDBStatus.ok()) {
//        debug_error("[ERROR] Deplete underlying rocksdb fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
//        return false;
//    } else {
//        return true;
//    }
//}

bool DeltaKV::PutWithWriteBatch(mempoolHandler_t mempoolHandler)
{
//    static uint64_t cnt = 0;
    if (mempoolHandler.isAnchorFlag_ == false) {
        debug_error("[ERROR] put operation should has an anchor flag%s\n", "");
        return false;
    }

    // cerr << "Key size = " << mempoolHandler.keySize_ << endl;
    struct timeval tv;
    gettimeofday(&tv, 0);
//    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == ) 
    if (batchedOperationsSizes[currentWriteBatchDequeInUse] >=
            maxBatchOperationBeforeCommitSize_)
    {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_PUT_LOCK_1, tv);
    gettimeofday(&tv, 0);
    scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_PUT_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, batchedOperationsCounter[currentWriteBatchDequeInUse]);
    if (batchedOperationsSizes[currentWriteBatchDequeInUse] >=
            maxBatchOperationBeforeCommitSize_)
    {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchMapForSearch_[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
//        cnt++;
//        if (cnt % 100 == 0) {
//            debug_error("put operations %lu count %lu\n", 
//                    batchedOperationsCounter[currentWriteBatchDequeInUse],
//                    cnt); 
//        }
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        batchedOperationsCounter[currentWriteBatchDequeInUse] = 0;
        batchedOperationsSizes[currentWriteBatchDequeInUse] = 0;
        str_t currentKey(mempoolHandler.keyPtr_, mempoolHandler.keySize_);
        // cerr << "Key in pool = " << mempoolHandler.keyPtr_ << endl;
        // cerr << "Key in str_t = " << currentKey.data_ << endl;
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            for (auto it : mapIt->second) {
                objectPairMemPool_->eraseContentFromMemPool(it.second);
//                batchedOperationsCounter[currentWriteBatchDequeInUse]--;
            }
            mapIt->second.clear();
            mapIt->second.push_back(make_pair(kPutOp, mempoolHandler));
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kPutOp, mempoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_PUT_APPEND_BUFFER, tv);
        return true;
    } else {
        // only insert
        str_t currentKey(mempoolHandler.keyPtr_, mempoolHandler.keySize_);
        // cerr << "Key in pool = " << mempoolHandler.keyPtr_ << endl;
        // cerr << "Key in str_t = " << currentKey.data_ << endl;
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            for (auto it : mapIt->second) {
                objectPairMemPool_->eraseContentFromMemPool(it.second);
//                batchedOperationsCounter[currentWriteBatchDequeInUse]--;
            }
            mapIt->second.clear();
            mapIt->second.push_back(make_pair(kPutOp, mempoolHandler));
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kPutOp, mempoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_PUT_APPEND_BUFFER, tv);
        return true;
    }
}

bool DeltaKV::MergeWithWriteBatch(mempoolHandler_t mempoolHandler)
{
    debug_info("[MergeOp] key = %s, sequence number = %u\n", string(mempoolHandler.keyPtr_, mempoolHandler.keySize_).c_str(), mempoolHandler.sequenceNumber_);
    if (mempoolHandler.isAnchorFlag_ == true) {
        debug_error("[ERROR] merge operation should has no anchor flag%s\n", "");
    }
    struct timeval tv;
    gettimeofday(&tv, 0);
//    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == ) 
    if (batchedOperationsSizes[currentWriteBatchDequeInUse] >=
            maxBatchOperationBeforeCommitSize_)
    {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_LOCK_1, tv);
    gettimeofday(&tv, 0);
    scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, batchedOperationsCounter[currentWriteBatchDequeInUse]);
//    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == ) 
    if (batchedOperationsSizes[currentWriteBatchDequeInUse] >=
            maxBatchOperationBeforeCommitSize_)
    {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchMapForSearch_[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
        batchedOperationsSizes[currentWriteBatchDequeInUse] = 0;
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        batchedOperationsCounter[currentWriteBatchDequeInUse] = 0;
        batchedOperationsSizes[currentWriteBatchDequeInUse] = 0;
        str_t currentKey(mempoolHandler.keyPtr_, mempoolHandler.keySize_);
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            mapIt->second.push_back(make_pair(kMergeOp, mempoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kMergeOp, mempoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_APPEND_BUFFER, tv);
        return true;
    } else {
        // only insert
        str_t currentKey(mempoolHandler.keyPtr_, mempoolHandler.keySize_);
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            mapIt->second.push_back(make_pair(kMergeOp, mempoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kMergeOp, mempoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
            batchedOperationsSizes[currentWriteBatchDequeInUse] += mempoolHandler.keySize_ + mempoolHandler.valueSize_;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_APPEND_BUFFER, tv);
        return true;
    }
}

bool DeltaKV::performInBatchedBufferDeduplication(unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*& operationsMap)
{
    uint32_t totalObjectNumber = 0;
    uint32_t validObjectNumber = 0;
    for (auto it = operationsMap->begin(); it != operationsMap->end(); it++) {
        totalObjectNumber += it->second.size();
        validObjectNumber += it->second.size();
        if (it->second.front().first == kPutOp && it->second.size() >= 2) {
            string finalValue;
            string firstValue(it->second.front().second.valuePtr_, it->second.front().second.valueSize_);
            string newKeyStr(it->second.front().second.keyPtr_, it->second.front().second.keySize_);
            vector<string> operandList;
            for (auto i = it->second.begin() + 1; i != it->second.end(); i++) {
                string operandStr(i->second.valuePtr_, i->second.valueSize_);
                operandList.push_back(operandStr);
            }
            bool mergeStatus;
            STAT_PROCESS(mergeStatus = deltaKVMergeOperatorPtr_->Merge(firstValue, operandList, &finalValue), StatsType::DKV_DEDUP_FULL_MERGE);
            if (mergeStatus == false) {
                debug_error("[ERROR] Could not merge for key = %s, delta number = %lu\n", newKeyStr.c_str(), it->second.size() - 1);
                return false;
            }
            for (auto index : it->second) {
                objectPairMemPool_->eraseContentFromMemPool(index.second);
                validObjectNumber--;
            }
            it->second.clear();
            globalSequenceNumberGeneratorMtx_.lock();
            uint32_t currentSequenceNumber = globalSequenceNumber_++;
            globalSequenceNumberGeneratorMtx_.unlock();
            mempoolHandler_t newHandler;
            bool insertStatus;
            STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalValue, currentSequenceNumber, true, newHandler), StatsType::DELTAKV_INSERT_MEMPOOL);
            if (insertStatus == false) {
                debug_error("insert error, size %lu %lu\n", newKeyStr.size(), finalValue.size());
                exit(1);
            }
            it->second.push_back(make_pair(kPutOp, newHandler));
            validObjectNumber++;
        } else if (it->second.front().first == kMergeOp && it->second.size() >= 2) {
            string newKeyStr(it->second.front().second.keyPtr_, it->second.front().second.keySize_);
            vector<string> operandList;
            for (auto i = it->second.begin(); i != it->second.end(); i++) {
                string operandStr(i->second.valuePtr_, i->second.valueSize_);
                operandList.push_back(operandStr);
            }
            vector<string> finalOperandList;
            bool mergeStatus;
            STAT_PROCESS(mergeStatus = deltaKVMergeOperatorPtr_->PartialMerge(operandList, finalOperandList), StatsType::DKV_DEDUP_PARTIAL_MERGE);
            if (mergeStatus == false) {
                debug_error("[ERROR] Could not partial merge for key = %s, delta number = %lu\n", newKeyStr.c_str(), it->second.size());
                return false;
            }
            for (auto index : it->second) {
                objectPairMemPool_->eraseContentFromMemPool(index.second);
                validObjectNumber--;
            }
            it->second.clear();
            globalSequenceNumberGeneratorMtx_.lock();
            uint32_t currentSequenceNumber = globalSequenceNumber_++;
            globalSequenceNumberGeneratorMtx_.unlock();
            mempoolHandler_t newHandler;
            bool insertStatus;
            STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalOperandList[0], currentSequenceNumber, false, newHandler), StatsType::DELTAKV_INSERT_MEMPOOL);
            if (insertStatus == false) {
                debug_error("insert error, size %lu %lu\n", newKeyStr.size(), finalOperandList[0].size());
                exit(1);
            }
            it->second.push_back(make_pair(kMergeOp, newHandler));
            validObjectNumber++;
        }
    }
    // uint32_t counter = 0;
    // for (auto it = operationsMap->begin(); it != operationsMap->end(); it++) {
    //     counter += it->second.size();
    // }
    debug_info("Total object number = %u, valid object number = %u, "
            "map size = %lu\n",
            totalObjectNumber, validObjectNumber, operationsMap->size());
    return true;
}

void DeltaKV::processBatchedOperationsWorker()
{
    while (true) {
        if (notifyWriteBatchMQ_->done == true && notifyWriteBatchMQ_->isEmpty() == true) {
            break;
        }
        unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* currentHandler;
        if (notifyWriteBatchMQ_->pop(currentHandler)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
            oneBufferDuringProcessFlag_ = true;
            debug_info("process batched contents for object number = %lu\n", currentHandler->size());
//            if (deltaKVRunningMode_ != kBatchedWithNoDeltaStore) {
                STAT_PROCESS(performInBatchedBufferDeduplication(currentHandler), StatsType::DKV_FLUSH_DEDUP);
                StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_PLAIN_ROCKSDB, tv);
//            }
            vector<mempoolHandler_t> handlerToValueStoreVec, handlerToDeltaStoreVec;
            for (auto it = currentHandler->begin(); it != currentHandler->end(); it++) {
                for (auto dequeIt : it->second) {
                    if (dequeIt.first == kPutOp) {
                        handlerToValueStoreVec.push_back(dequeIt.second);
                        if (dequeIt.second.isAnchorFlag_ == false) {
                            debug_error("[ERROR] Current key value pair not fit requirement, kPutOp should be anchor %s\n", "");
                        } else {
                            handlerToDeltaStoreVec.push_back(dequeIt.second);
                        }
                    } else {
                        if (dequeIt.second.isAnchorFlag_ == true) {
                            debug_error("[ERROR] Current key value pair not fit requirement, kMergeOp should not be anchor %s\n", "");
                        } else {
                            handlerToDeltaStoreVec.push_back(dequeIt.second);
                        }
                    }
                }
            }
            bool putToDeltaStoreStatus = false;
            switch (deltaKVRunningMode_) {
            case kBatchedWithNoDeltaStore: 
             {
                struct timeval tv;
                gettimeofday(&tv, 0);
                rocksdb::Status rocksDBStatus;
                rocksdb::WriteBatch mergeBatch;
                for (auto index = 0; index < handlerToDeltaStoreVec.size(); index++) {
                    if (handlerToDeltaStoreVec[index].isAnchorFlag_ == false) {
                        auto& it = handlerToDeltaStoreVec[index];
                        internalValueType currentInternalValueType(false, false, it.sequenceNumber_, it.valueSize_);
                        char buffer[it.valueSize_ + sizeof(internalValueType)];

                        memcpy(buffer, &currentInternalValueType, sizeof(internalValueType));
                        memcpy(buffer + sizeof(internalValueType), it.valuePtr_, it.valueSize_);

                        rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
                        rocksdb::Slice newValue(buffer, it.valueSize_ + sizeof(internalValueType));
                        mergeBatch.Merge(newKey, newValue);
                    }
                }

                bool lsmTreeInterfaceStatus = lsmTreeInterface_.MultiWriteWithBatch(handlerToValueStoreVec, &mergeBatch);
                if (lsmTreeInterfaceStatus == false) {
                    debug_error("lsmTreeInterfaceStatus %d\n", (int)lsmTreeInterfaceStatus);
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::DKV_FLUSH_WITH_NO_DSTORE, tv);
                break;
            }
            case kBatchedWithDeltaStore:
            {
                struct timeval tv;
                gettimeofday(&tv, 0);
                vector<bool> separateFlagVec;
                vector<mempoolHandler_t> notSeparatedDeltasVec;
                uint32_t spearateTrueCounter = 0, separateFalseCounter = 0;
                for (auto deltaIt = handlerToDeltaStoreVec.begin(); deltaIt != handlerToDeltaStoreVec.end(); deltaIt++) {
                    if (deltaIt->valueSize_ <= deltaExtractSize_ && deltaIt->isAnchorFlag_ == false) {
                        separateFlagVec.push_back(false);
                        notSeparatedDeltasVec.push_back(*deltaIt);
                        handlerToDeltaStoreVec.erase(deltaIt);
                        separateFalseCounter++;
                    } else {
                        separateFlagVec.push_back(true);
                        spearateTrueCounter++;
                    }
                }

                rocksdb::WriteBatch mergeBatch;

                auto separatedID = 0, notSeparatedID = 0;
                if (enableLsmTreeDeltaMeta_ == true) {
                    for (auto separatedDeltaFlagIndex = 0; separatedDeltaFlagIndex < separateFlagVec.size(); separatedDeltaFlagIndex++) {
                        if (separateFlagVec[separatedDeltaFlagIndex] == false) {
                            if (notSeparatedDeltasVec[notSeparatedID].isAnchorFlag_ == false) {
                                char writeInternalValueBuffer[sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                internalValueType currentInternalValueType(false, false, notSeparatedDeltasVec[notSeparatedID].sequenceNumber_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                memcpy(writeInternalValueBuffer + sizeof(internalValueType), notSeparatedDeltasVec[notSeparatedID].valuePtr_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                rocksdb::Slice newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                                mergeBatch.Merge(newKey, newValue);
                            } else {
                                string newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                            }
                            notSeparatedID++;
                        } else {
                            if (handlerToDeltaStoreVec[separatedID].isAnchorFlag_ == false) {
                                char writeInternalValueBuffer[sizeof(internalValueType)];
                                internalValueType currentInternalValueType(false, true, handlerToDeltaStoreVec[separatedID].sequenceNumber_, handlerToDeltaStoreVec[separatedID].valueSize_);
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                rocksdb::Slice newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
                                debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                                mergeBatch.Merge(newKey, newValue);
                            } else {
                                string newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                            }
                            separatedID++;
                        }
                    } 
                } else {
                    // don't do anything
                }

                // LSM interface
                struct lsmInterfaceOperationStruct* lsmInterfaceOpPtr = nullptr;
                if (enableParallelLsmInterface == true) {
                    lsmInterfaceOpPtr = new lsmInterfaceOperationStruct;
                    lsmInterfaceOpPtr->mergeBatch = &mergeBatch; 
                    lsmInterfaceOpPtr->handlerToValueStoreVecPtr = &handlerToValueStoreVec;
                    lsmInterfaceOpPtr->is_write = true;
                    lsmInterfaceOpPtr->job_done = kNotDone;
                    lsmInterfaceOperationsQueue_->push(lsmInterfaceOpPtr);
                    lsm_interface_cv.notify_one();
                } else {
                    STAT_PROCESS(lsmTreeInterface_.MultiWriteWithBatch(handlerToValueStoreVec, &mergeBatch), 
                            StatsType::DKV_FLUSH_LSM_INTERFACE);
                }

                // DeltaStore interface
                STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(handlerToDeltaStoreVec), 
                        StatsType::DKV_FLUSH_MUTIPUT_DSTORE);
                if (putToDeltaStoreStatus == false) {
                    debug_error("[ERROR] could not put %zu object into delta store,"
                            " as well as not separated object number = %zu\n", 
                            handlerToDeltaStoreVec.size(), notSeparatedDeltasVec.size());
                    break;
                }

                // Check LSM interface
                if (enableParallelLsmInterface == true) {
                    struct timeval tv1, tv2;
                    gettimeofday(&tv1, 0);
                    int mx = 0;
                    lsm_interface_cv.notify_one();
                    while (lsmInterfaceOpPtr->job_done == kNotDone) {
                        gettimeofday(&tv2, 0);
                        if ((tv2.tv_sec - tv1.tv_sec) % 10 == 0 && 
                                tv2.tv_sec - tv1.tv_sec > mx) {
                            mx = tv2.tv_sec - tv1.tv_sec;
                            debug_error("Wait for %d seconds. Notify\n", mx);
                            lsm_interface_cv.notify_one();
                        }
                    }
                    if (lsmInterfaceOpPtr->job_done == kError) {
                        debug_error("lsmInterfaceOp error %s\n", ""); 
                    }
                    delete lsmInterfaceOpPtr;
                }
                
                StatsRecorder::getInstance()->timeProcess(StatsType::DKV_FLUSH_WITH_DSTORE, tv);
                break;
            }
            default:
                debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
                break;
            }
            // update write buffers
            debug_info("process batched contents done, start update write buffer's map, target update key number = %lu\n", handlerToDeltaStoreVec.size());
            // uint32_t erasedObjectCounter = 0;
            for (auto index : *currentHandler) {
                for (auto it : index.second) {
                    // erasedObjectCounter++;
                    objectPairMemPool_->eraseContentFromMemPool(it.second);
                }
            }
            // cerr << "Erased object number = " << erasedObjectCounter << endl;
            currentHandler->clear();
            debug_info("process batched contents done, not cleaned object number = %lu\n", currentHandler->size());
            oneBufferDuringProcessFlag_ = false;
            StatsRecorder::getInstance()->timeProcess(StatsType::DKV_FLUSH, tv);
        }
    }
    writeBatchOperationWorkExitFlag = true;
    debug_info("Process batched operations done, exit thread%s\n", "");
    return;
}

void DeltaKV::processWriteBackOperationsWorker()
{
    while (true) {
        if (writeBackOperationsQueue_->done == true && writeBackOperationsQueue_->isEmpty() == true) {
            break;
        }
        writeBackObject* currentProcessPair;
        while (writeBackOperationsQueue_->pop(currentProcessPair)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_warn("Target Write back key = %s\n", currentProcessPair->key.c_str());
            bool writeBackStatus = GetCurrentValueThenWriteBack(currentProcessPair->key);
            if (writeBackStatus == false) {
                debug_error("Could not write back target key = %s\n", currentProcessPair->key.c_str());
                exit(1);
            } else {
                debug_warn("Write back key = %s success\n", currentProcessPair->key.c_str());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK, tv);
            delete currentProcessPair;
        }
    }
    return;
}

void DeltaKV::processLsmInterfaceOperationsWorker()
{
    int counter = 0;
    uint64_t mx = 1200;
    struct timeval tvs, tve;
    gettimeofday(&tvs, 0);
    while (true) {
        gettimeofday(&tve, 0);
        if (tve.tv_sec - tvs.tv_sec > mx) {
            debug_error("lsm thread heart beat %lu\n", mx); 
            mx += 1200;
        }
        if (lsmInterfaceOperationsQueue_->done == true && lsmInterfaceOperationsQueue_->isEmpty() == true) {
            break;
        }
        lsmInterfaceOperationStruct* op;

//        {
//            std::unique_lock<std::mutex> lock(lsm_interface_mutex);
//            struct timeval tv1, tv2, res;
//            gettimeofday(&tv1, 0);
//            while (lsmInterfaceOperationsQueue_->isEmpty() == true && 
//                    this->lsmInterfaceOperationsQueue_->done == false) {
//                gettimeofday(&tv2, 0);
//                timersub(&tv2, &tv1, &res);
//                auto t = timevalToMicros(res);
//
//                if (t > 10000) {
//                    lsm_interface_cv.wait(lock);
//                    tv1 = tv2;
//                }
//            }
//        }
        {
            std::unique_lock<std::mutex> lock(lsm_interface_mutex);
            if (counter == 0 && 
                    lsmInterfaceOperationsQueue_->isEmpty() == true && 
                    this->lsmInterfaceOperationsQueue_->done == false) {
//                lsm_interface_cv.wait(lock);
                counter++; 
            }
        }

        while (lsmInterfaceOperationsQueue_->pop(op)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (op->is_write == false) {
                STAT_PROCESS(lsmTreeInterface_.Get(op->key, op->value), StatsType::DKV_LSM_INTERFACE_GET); // (, maxSequenceNumber, getByWriteBackFlag);
            } else {
                STAT_PROCESS(lsmTreeInterface_.MultiWriteWithBatch(*(op->handlerToValueStoreVecPtr), op->mergeBatch), 
                            StatsType::DKV_FLUSH_LSM_INTERFACE);
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DKV_LSM_INTERFACE_OP, tv);
            op->job_done = kDone;
            if (counter > 0) {
                counter--;
            }
        }
    }
    return;
}

// TODO: upper functions are not complete

bool DeltaKV::deleteExistingThreads()
{
    // threadpool_->join();
    // delete threadpool_;
    debug_info("Start threads join, number = %lu\n", thList_.size());
    for (auto thIt : thList_) {
        thIt->join();
        debug_info("Thread exit success = %p\n", thIt);
        delete thIt;
    }
    debug_info("All threads exit success, number = %lu\n", thList_.size());
    return true;
}

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, uint32_t& maxSequenceNumber)
{
    uint64_t internalValueSize = internalValue.size();
    debug_trace("internalValueSize = %lu, skipSize = %lu\n", internalValueSize, skipSize);
    uint64_t currentProcessLocationIndex = skipSize;
    while (currentProcessLocationIndex != internalValueSize) {
        internalValueType currentInternalValueTypeHeader;
        memcpy(&currentInternalValueTypeHeader, internalValue.c_str() + currentProcessLocationIndex, sizeof(internalValueType));
        currentProcessLocationIndex += sizeof(internalValueType);
        if (maxSequenceNumber < currentInternalValueTypeHeader.sequenceNumber_) {
            maxSequenceNumber = currentInternalValueTypeHeader.sequenceNumber_;
        }
        if (currentInternalValueTypeHeader.mergeFlag_ == true) {
            debug_error("[ERROR] Find new value index in merge operand list, this index refer to raw value size = %u\n", currentInternalValueTypeHeader.rawValueSize_);
            assert(0);
        }
        if (currentInternalValueTypeHeader.valueSeparatedFlag_ != true) {
            assert(currentProcessLocationIndex + currentInternalValueTypeHeader.rawValueSize_ <= internalValue.size());
            string currentValue(internalValue.c_str() + currentProcessLocationIndex, currentInternalValueTypeHeader.rawValueSize_);
            currentProcessLocationIndex += currentInternalValueTypeHeader.rawValueSize_;
            mergeOperatorsVec.push_back(make_pair(false, currentValue));
        } else {
            mergeOperatorsVec.push_back(make_pair(true, ""));
        }
    }
    return true;
}

void DeltaKV::GetRocksDBProperty(const string& property, string* str) {
    lsmTreeInterface_.GetRocksDBProperty(property, str);
}

} // namespace DELTAKV_NAMESPACE
