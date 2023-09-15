#include "interface/KDSepInterface.hpp"

namespace KDSEP_NAMESPACE {

KDSep::KDSep()
{
}

KDSep::~KDSep()
{
    cerr << "[KDSep Interface] Try delete write batch: " << endl;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        delete notifyWriteBatchMQ_;
        delete batch_map_[0];
        delete batch_map_[1];
    }
    cerr << "[KDSep Interface] Try delete lsm interface " << endl;
    if (enableParallelLsmInterface == true) {
        delete lsmInterfaceOperationsQueue_;
    }
    cerr << "[KDSep Interface] Try delete Read Cache " << endl;
//    if (wb_keys != nullptr) {
//	delete wb_keys;
//	delete wb_keys_mutex;
//    }
    if (write_stall_ != nullptr) {
	delete write_stall_;
    }
    cerr << "[KDSep Interface] Try delete HashStore " << endl;
    if (delta_store_ != nullptr) {
        cerr << "interface " << endl;
        delete delta_store_;
        cerr << "file manager " << endl;
        delete bucket_manager_;
        cerr << "file operator " << endl;
        delete bucket_operator_;
    }
    cerr << "[KDSep Interface] Try delete mempool " << endl;
    delete objectPairMemPool_;
    cerr << "[KDSep Interface] Try delete RocksDB " << endl;
}

bool KDSep::Open(KDSepOptions& options, const string& name)
{
//    KvHeader header;
//    char buf[15];
//    header.rawValueSize_ = 1024;
//    printHeader();
//    PutKVHeaderVarint(buf, header, true, true);
    cout << "rss before open: " << getRss() / 1024.0 << endl;
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
    // object mem pool
    struct timeval tv, tv2;
    gettimeofday(&tv, 0);
    lsmTreeInterface_.Open(options, name);
    gettimeofday(&tv2, 0);

    tv_tune_cache_ = tv2;
    rocks_block_cache_ = options.rocks_block_cache;
    max_kd_cache_size_ = options.deltaStore_KDCache_item_number_;
    min_block_cache_size_ = options.min_block_cache_size; 
    memory_budget_ = options.memory_budget;

    printf("restore lsmTree interface time: %.6lf\n", 
	    tv2.tv_sec + tv2.tv_usec / 1000000.0 - tv.tv_sec -
	    tv.tv_usec / 1000000.0);

    write_stall_ = options.write_stall;
    options.wb_keys.reset(new queue<string>);
    options.wb_keys_mutex.reset(new mutex);
    wb_keys = options.wb_keys; 
    wb_keys_mutex = options.wb_keys_mutex;

    objectPairMemPool_ = new KeyValueMemPool(options.deltaStore_mem_pool_object_number_, options.deltaStore_mem_pool_object_size_);
    // Rest merge function if delta/value separation enabled
    KDSepMergeOperatorPtr_ = options.KDSep_merge_operation_ptr;
    enableLsmTreeDeltaMeta_ = options.enable_lsm_tree_delta_meta;
    enable_crash_consistency_ = options.enable_crash_consistency;

    if (options.enable_batched_operations_ == true) {
        batch_map_[0] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        batch_map_[1] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        notifyWriteBatchMQ_ = new messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&KDSep::processBatchedOperationsWorker, this));
        thList_.push_back(th);
        isBatchedOperationsWithBufferInUse_ = true;
        maxBatchOperationBeforeCommitNumber_ = options.write_buffer_num;
        maxBatchOperationBeforeCommitSize_ = options.write_buffer_size;
    }

    if (options.enable_write_back_optimization_ == true) {
        enable_write_back_ = true;
        writeBackWhenReadDeltaNumerThreshold_ = options.deltaStore_write_back_during_reads_threshold;
        writeBackWhenReadDeltaSizeThreshold_ = options.deltaStore_write_back_during_reads_size_threshold;
        write_back_queue_.reset(new messageQueue<writeBackObject*>);
        write_back_cv_.reset(new condition_variable);
        write_back_mutex_.reset(new mutex);
        options.write_back_queue = write_back_queue_;
        options.write_back_cv = write_back_cv_;
        options.write_back_mutex = write_back_mutex_;
        boost::thread* th = new boost::thread(attrs, boost::bind(&KDSep::processWriteBackOperationsWorker, this));
        thList_.push_back(th);
    }

    if (options.enable_parallel_lsm_interface_ == true) {
        enableParallelLsmInterface = true;
        lsmInterfaceOperationsQueue_ = new messageQueue<lsmInterfaceOperationStruct*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&KDSep::processLsmInterfaceOperationsWorker, this));
        thList_.push_back(th);
    } else {
        enableParallelLsmInterface = false;
    }

    if (options.enable_deltaStore == true && delta_store_ == nullptr) {
        isDeltaStoreInUseFlag_ = true;
        delta_store_ = new HashStoreInterface(&options, name, bucket_manager_, bucket_operator_);
        // create deltaStore related threads
        boost::thread* th;
	// will not need this later
        th = new boost::thread(attrs, boost::bind(&BucketManager::scheduleMetadataUpdateWorker, bucket_manager_));
        thList_.push_back(th);

        if (options.enable_deltaStore_garbage_collection == true) {
            enableDeltaStoreWithBackgroundGCFlag_ = true;
            if (options.enable_bucket_merge) {
                enable_bucket_merge_ = true;
                th = new boost::thread(attrs, boost::bind(&BucketManager::processMergeGCRequestWorker, bucket_manager_));
                thList_.push_back(th);
            } else {
                enable_bucket_merge_ = false;
            }
            for (auto threadID = 0; threadID < options.deltaStore_gc_worker_thread_number_limit_; threadID++) {
                th = new boost::thread(attrs, boost::bind(&BucketManager::processSingleFileGCRequestWorker, bucket_manager_, threadID));
                thList_.push_back(th);
            }
        }
        if (options.deltaStore_op_worker_thread_number_limit_ >= 2) {
//            for (auto threadID = 0; threadID < options.deltaStore_op_worker_thread_number_limit_; threadID++) {
//                th = new boost::thread(attrs, boost::bind(&BucketOperator::operationWorker, bucket_operator_, threadID));
//                thList_.push_back(th);
//            }
        } else {
            debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
        }
    }

    // process runnning mode
    if (options.enable_deltaStore) {
        KDSepRunningMode_ = options.enable_batched_operations_ ? kBatchedWithDeltaStore : kWithDeltaStore;
    } else {
        KDSepRunningMode_ = options.enable_batched_operations_ ? kBatchedWithNoDeltaStore : kWithNoDeltaStore;
    }

    // assign the kd cache at the end
    kd_cache_ = options.kd_cache;
    Recovery();
    cerr << "rss after open: " << getRss() / 1024.0 << endl;

    return true;
}

bool KDSep::Close()
{
    cerr << "[KDSep Close DB] Flush write buffer" << endl;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        for (auto i = 0; i < 2; i++) {
            if (batch_map_[i]->size() != 0) {
                notifyWriteBatchMQ_->push(batch_map_[i]);
            }
        }
        notifyWriteBatchMQ_->done = true;
        while (writeBatchOperationWorkExitFlag == false) {
            asm volatile("");
        }
        cerr << "\tFlush write batch done" << endl;
    }
    usleep(100000);
    cerr << "[KDSep Close DB] Force GC" << endl;
    if (isDeltaStoreInUseFlag_ == true) {
        if (enableDeltaStoreWithBackgroundGCFlag_ == true) {
            delta_store_->forcedManualGarbageCollection();
            cerr << "\tDeltaStore forced GC done" << endl;
        }
    }
    cerr << "[KDSep Close DB] Wait write back" << endl;
    if (enable_write_back_ == true) {
        write_back_queue_->done = true;
        write_back_cv_->notify_one();
        while (wb_keys->size() > 0) {
            usleep(10);
        }
        while (write_back_queue_->isEmpty() == false) {
            usleep(10);
        }
        cerr << "\tWrite back done" << endl;
    }
    cerr << "[KDSep Close DB] Set job done" << endl;
    if (enableParallelLsmInterface == true) {
        lsmInterfaceOperationsQueue_->done = true;
        lsm_interface_cv.notify_one();
        while (lsmInterfaceOperationsQueue_->isEmpty() == false) {
            asm volatile("");
        }
        cerr << "\tLSM tree interface operations done" << endl;
    }
    cerr << "[KDSep Close DB] LSM-tree interface" << endl;
    if (isDeltaStoreInUseFlag_ == true) {
        delta_store_->setJobDone();
        cerr << "\tHashStore set job done" << endl;
    }
    cerr << "[KDSep Close DB] Delete existing threads" << endl;
    deleteExistingThreads();
    if (kd_cache_.get() != nullptr) {
        cerr << "KD cache: " << kd_cache_->getUsage() << "\n";
    }
    lsmTreeInterface_.Close();
    cerr << "\tJoin all existing threads done" << endl;
    return true;
}

bool KDSep::SinglePutInternal(const mempoolHandler_t& obj) 
{
    if (KDSepRunningMode_ == kWithNoDeltaStore) {
        return lsmTreeInterface_.Put(obj);
    } else {
        bool updateLsmTreeStatus = lsmTreeInterface_.Put(obj);
        if (updateLsmTreeStatus == false) {
            debug_error("[ERROR] Put LSM-tree failed, key = %s\n", obj.keyPtr_);
        }
        bool updateAnchorStatus;
        STAT_PROCESS(updateAnchorStatus = delta_store_->put(obj), StatsType::KDS_PUT_DSTORE);
        return updateAnchorStatus;
    }
}

bool KDSep::SingleMergeInternal(const mempoolHandler_t& obj)
{
    if (KDSepRunningMode_ == kWithNoDeltaStore) {
        return lsmTreeInterface_.Merge(obj);
    } else if (enableLsmTreeDeltaMeta_ == true) {
        // Large enough, do separation
        size_t header_sz = sizeof(KvHeader);
        if (obj.valueSize_ >= deltaExtractSize_) {
            bool status;
            STAT_PROCESS(status = delta_store_->put(obj), StatsType::KDSep_MERGE_HASHSTORE);
            if (status == false) {
                debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", obj.keyPtr_, obj.valuePtr_);
                return false;
            }

            char lsm_buffer[header_sz];
            KvHeader header(false, true, obj.seq_num, obj.valueSize_);

            // encode header
            if (use_varint_kv_header == false) {
                memcpy(lsm_buffer, &header, header_sz);
            } else {
                header_sz = PutKVHeaderVarint(lsm_buffer, header);
            }
            return lsmTreeInterface_.Merge(obj.keyPtr_, obj.keySize_,
                    lsm_buffer, header_sz);
        } else { // do not do separation
            char lsm_buffer[header_sz + obj.valueSize_];
            KvHeader header(false, false, obj.seq_num, obj.valueSize_);

            // encode header
            if (use_varint_kv_header == false) {
                memcpy(lsm_buffer, &header, header_sz);
            } else {
                header_sz = PutKVHeaderVarint(lsm_buffer, header);
            }
            memcpy(lsm_buffer + header_sz, obj.valuePtr_, obj.valueSize_);
            return lsmTreeInterface_.Merge(obj.keyPtr_, obj.keySize_,
                    lsm_buffer, header_sz + obj.valueSize_);
        }
    } else {
        bool status;
        STAT_PROCESS(status = delta_store_->put(obj), StatsType::KDSep_MERGE_HASHSTORE);
        return status;
    }
}

bool KDSep::GetInternal(const string& key, string* value, bool writing_back) {
    // Do not use deltaStore
    if (KDSepRunningMode_ == kWithNoDeltaStore || KDSepRunningMode_ == kBatchedWithNoDeltaStore) {
        string lsm_value;
        bool ret = lsmTreeInterface_.Get(key, &lsm_value);
        if (ret == false) {
            debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
            exit(1);
        }

        size_t header_sz = sizeof(KvHeader);
        if (use_varint_kv_header == true) {
            header_sz = GetKVHeaderVarintSize(lsm_value.c_str());
        }
        // simply remove the header and return
        value->assign(lsm_value.substr(header_sz));
        return true;
    }

    tryTuneCache();

    if (enableLsmTreeDeltaMeta_ == true) {
        // Use deltaStore
        string lsm_value;
        bool ret;
        STAT_PROCESS(ret = lsmTreeInterface_.Get(key, &lsm_value), StatsType::LSM_INTERFACE_GET); 

        if (ret == false) {
            debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
            return false;
        } 

        // Extract header
        KvHeader header;
        size_t header_sz = sizeof(KvHeader);
        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz); 
        }
        string raw_value;

        if (header.valueSeparatedFlag_ == true) {
            debug_error("[ERROR] value separated but not retrieved %s\n", key.c_str());
            assert(0);
        }

        if (header.mergeFlag_ == true) {
            if (enableLsmTreeDeltaMeta_ == false) {
                debug_error("[ERROR] settings with no metadata but LSM-tree has metadata, key %s\n", key.c_str());
                exit(1);
            }

            // get deltas from the LSM-tree value 
            vector<pair<bool, string>> deltaInfoVec;
            STAT_PROCESS(extractDeltas(lsm_value, 
                        header_sz + header.rawValueSize_, deltaInfoVec),
                    StatsType::KDSep_GET_PROCESS_BUFFER);

            // get value from the LSM-tree value
            raw_value.assign(lsm_value.c_str() + header_sz,
                    header.rawValueSize_);

            bool isAnyDeltasAreExtratedFlag = false;
            for (auto& it : deltaInfoVec) {
                if (it.first == true) {
                    isAnyDeltasAreExtratedFlag = true;
                    break;
                }
            }

            if (isAnyDeltasAreExtratedFlag == true) {
                // should read external delta store
                vector<string> deltasFromDeltaStoreVec;
                bool ret;
                STAT_PROCESS(ret = delta_store_->get(key, deltasFromDeltaStoreVec), StatsType::DS_GET);
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

                    debug_trace("Start KDSep merge operation, raw_value = "
                            "%s, finalDeltaOperatorsVec.size = %lu\n",
                            raw_value.c_str(), finalDeltaOperatorsVec.size());

                    // merge the raw value with raw deltas
                    STAT_PROCESS(KDSepMergeOperatorPtr_->Merge(raw_value,
                                finalDeltaOperatorsVec, value),
                            StatsType::KDSep_GET_FULL_MERGE);
                    if (enable_write_back_ == true &&
                            deltaInfoVec.size() >
                            writeBackWhenReadDeltaNumerThreshold_ &&
                            writeBackWhenReadDeltaNumerThreshold_ != 0 &&
                            !writing_back) {
                        bool ret;
                        STAT_PROCESS(ret = PutImpl(key, *value),
                                StatsType::KDSep_GET_PUT_WRITE_BACK);
                        if (ret == false) {
                            debug_error("Write back failed key %s\n",
                                    key.c_str());
                            exit(1);
                        }
                    }
                    return true;
                }
            } else {
                // all deltas are stored internal
                vector<string> finalDeltaOperatorsVec;
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                }
                debug_trace("Start KDSep merge operation, raw_value = "
                        "%s, finalDeltaOperatorsVec.size = %lu\n",
                        raw_value.c_str(), finalDeltaOperatorsVec.size());
                STAT_PROCESS(KDSepMergeOperatorPtr_->Merge(
                            raw_value, finalDeltaOperatorsVec, value),
                        StatsType::KDSep_GET_FULL_MERGE);
                if (enable_write_back_ == true && !writing_back &&
                        deltaInfoVec.size() >
                        writeBackWhenReadDeltaNumerThreshold_ &&
                        writeBackWhenReadDeltaNumerThreshold_ != 0) {
                    writeBackObject* newPair = new writeBackObject(key, "", 0);
                    write_back_queue_->push(newPair);
                    write_back_cv_->notify_one();
                }
                return true;
            }
        } else {
            if (lsm_value.size() < header_sz + header.rawValueSize_) {
                debug_error("string size %lu raw value size %u\n", lsm_value.size(), header.rawValueSize_); 
                exit(1);
            }
            value->assign(string(lsm_value.c_str() + header_sz,
                        header.rawValueSize_));
            return true;
        }
    } else {
        // do not have metadata
        // Use deltaStore
        string lsm_value;
        bool ret;
        struct lsmInterfaceOperationStruct* op = nullptr;
       
        if (enableParallelLsmInterface == true) {
            op = new lsmInterfaceOperationStruct;
            op->key = key;
            op->value = &lsm_value;
            op->is_write = false;
            op->job_done = kNotDone;
            lsmInterfaceOperationsQueue_->push(op);
        } else {
            STAT_PROCESS(ret = lsmTreeInterface_.Get(key, &lsm_value), StatsType::LSM_INTERFACE_GET); 
            if (ret == false) {
                debug_error("[ERROR] Read LSM-tree fault, key = %s\n", key.c_str());
                return false;
            }
        }

        // get deltas from delta store
        vector<string> deltasFromDeltaStoreVec;
        ret = false;
        STAT_PROCESS(ret = delta_store_->get(key, deltasFromDeltaStoreVec), StatsType::DS_GET);
        if (ret != true) {
            debug_trace("Read external deltaStore fault, key = %s\n", key.c_str());
            return false;
        }

        if (op != nullptr) {
            while (op->job_done == kNotDone) {
                asm volatile("");
            }
            if (op->job_done == kError) {
                debug_error("lsmInterfaceOp error %s\n", ""); 
            }
            delete op;
        }

        KvHeader header;
        size_t header_sz = sizeof(KvHeader);
        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz);
        }

        if (header.valueSeparatedFlag_ == true) {
            debug_error("[ERROR] value separated but not retrieved %s\n", key.c_str());
            assert(0);
        }

        str_t raw_value(lsm_value.data() + header_sz, header.rawValueSize_);
        
        if (deltasFromDeltaStoreVec.empty() == true) { 
            value->assign(raw_value.data_, raw_value.size_);
            return true;
        }

        bool mergeOperationStatus;
        vector<str_t> deltaInStrT;
        int total_d_sz = 0;
        for (auto& it : deltasFromDeltaStoreVec) {
            deltaInStrT.push_back(str_t(it.data(), it.size()));
            total_d_sz += it.size();
        }
        STAT_PROCESS(mergeOperationStatus =
                KDSepMergeOperatorPtr_->Merge(raw_value, deltaInStrT
                    /*deltasFromDeltaStoreVec*/, value),
                StatsType::KDSep_GET_FULL_MERGE);
        if (mergeOperationStatus == false) { 
            debug_error("[ERROR] Perform merge operation fail, key = %s\n",
                    key.c_str());
            return false;
        }
        if (enable_write_back_ == true && !writing_back &&  
                ((deltasFromDeltaStoreVec.size() >
                  writeBackWhenReadDeltaNumerThreshold_ &&
                  writeBackWhenReadDeltaNumerThreshold_ != 0) ||
                (total_d_sz > writeBackWhenReadDeltaSizeThreshold_
                 && writeBackWhenReadDeltaSizeThreshold_ != 0))) {
            bool ret;
            STAT_PROCESS(ret = PutImpl(key, *value),
                    StatsType::KDSep_GET_PUT_WRITE_BACK);
            if (ret == false) {
                debug_error("write back failed key %s value %.*s\n", key.c_str(), 
                        (int)raw_value.size_, raw_value.data_);
                for (auto& it : deltaInStrT) {
                    debug_error("delta %.*s\n", (int)it.size_, it.data_);
                }
                exit(1);
            }
        }
        return true;
    }
}

bool KDSep::Put(const string& key, const string& value)
{
    // check write stall 
    if (write_stall_ != nullptr) {
        if (*write_stall_ == true) {
            debug_error("write stall key %s\n", key.c_str());
            while (*write_stall_ == true) {
                asm volatile("");
            }
            debug_error("write stall finish %s\n", key.c_str());
        }
    }

    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);

    bool ret = PutImpl(key, value);
    if (ret == false) {
        debug_error("write failed %s\n", key.c_str());
        exit(1);
    }
    return ret;
}

bool KDSep::PutImpl(const string& key, const string& value) {
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    mempoolHandler_t obj;
    bool insertMemPoolStatus;
    STAT_PROCESS(insertMemPoolStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, true, obj), StatsType::KDSep_INSERT_MEMPOOL);
    if (insertMemPoolStatus == false) {
        debug_error("insert to mempool failed, key %s value size %lu\n", key.c_str(), value.size());
        return false;
    }
    bool putOperationStatus = true;
    bool deleteMemPoolHandlerStatus = false;
    switch (KDSepRunningMode_) {
    case kBatchedWithNoDeltaStore:
    case kBatchedWithDeltaStore:
        putOperationStatus = PutWithWriteBatch(obj);
        break;
    case kWithDeltaStore:
    case kWithNoDeltaStore:
        putOperationStatus = SinglePutInternal(obj);
        deleteMemPoolHandlerStatus = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", KDSepRunningMode_);
        putOperationStatus = false;
        deleteMemPoolHandlerStatus = true;
        break;
    }
    if (deleteMemPoolHandlerStatus == true) {
        objectPairMemPool_->eraseContentFromMemPool(obj);
    }

    if (putOperationStatus == false) {
        debug_error("[ERROR] Could not put back current value, skip write back, key = %s, value = %s\n", key.c_str(), value.c_str());
        return false;
    }

    return true;
}

bool KDSep::Get(const string& key, string* value)
{
    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);
    // search first
    vector<str_t> buf_deltas;
    vector<string> buf_deltas_str;
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
        StatsRecorder::getInstance()->timeProcess(StatsType::KDS_GET_WAIT_BUFFER, tvAll);
        struct timeval tv, tvtmp;
        gettimeofday(&tv, 0);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        char keyBuffer[key.size()];
        memcpy(keyBuffer, key.c_str(), key.size());
        str_t currentKey(keyBuffer, key.length());
        auto mapIt = batch_map_[batch_in_use_]->find(currentKey);
        if (mapIt != batch_map_[batch_in_use_]->end()) {
            struct timeval tv0;
            gettimeofday(&tv0, 0);
            // the last item for the key is a put, directly return
            if (mapIt->second.size() != 0 && mapIt->second.back().first == kPutOp) {
                value->assign(mapIt->second.back().second.valuePtr_, mapIt->second.back().second.valueSize_);
                debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_BATCH_READ_GET_KEY, tv);
                return true;
            }
//            StatsRecorder::getInstance()->timeProcess(StatsType::KDS_GET_READ_BUFFER_PART1_LAST_PUT, tv0);
            gettimeofday(&tv0, 0);
            str_t newValueStr;
            bool findNewValueFlag = false;

            // the last item for the key is not a put, grap the related items 
            for (auto queueIt : mapIt->second) {
                if (queueIt.first == kPutOp) {
                    newValueStr = str_t(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    buf_deltas.clear();
                    findNewValueFlag = true;
                } else {
                    str_t currentValue(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    buf_deltas.push_back(currentValue);
                }
            }
//            StatsRecorder::getInstance()->timeProcess(StatsType::KDS_GET_READ_BUFFER_PART2, tv0);
            gettimeofday(&tvtmp, 0);
            if (findNewValueFlag == true) {
                STAT_PROCESS(KDSepMergeOperatorPtr_->Merge(newValueStr, buf_deltas, value), StatsType::KDS_GET_FULL_MERGE);
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), buf_deltas.size());
                StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_BATCH_READ_MERGE, tv0);
                return true;
            }
            if (buf_deltas.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), buf_deltas.size());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_BATCH_READ_MERGE_ALL, tv0);
        }

        struct timeval tv_merge;
        gettimeofday(&tv_merge, 0);

        if (buf_deltas.size() > 0) {
            str_t merged_delta;
            KDSepMergeOperatorPtr_->PartialMerge(buf_deltas, merged_delta);
            buf_deltas_str.push_back(string(merged_delta.data_, merged_delta.size_));
            delete[] merged_delta.data_;
//            for (auto& it : buf_deltas) {
//                buf_deltas_str.push_back(string(it.data_, it.size_));
//            }
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::KDS_GET_READ_BUFFER_P3_MERGE, tv_merge);
        StatsRecorder::getInstance()->timeProcess(StatsType::KDS_GET_READ_BUFFER, tv);
    }

    struct timeval tv;
    gettimeofday(&tv, 0);
    bool ret;

    // Read from deltastore (or no deltastore)
    ret = GetInternal(key, value, false);
    StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_BATCH_READ_STORE, tv);
    if (ret == false) {
        return false;
    } else {
        if (needMergeWithInBufferOperationsFlag == true) {
            string tempValueStr;
            tempValueStr.assign(*value);
            str_t tempValueStrT(tempValueStr.data(), tempValueStr.size());
            vector<str_t> tempVec;
            for (auto& it : buf_deltas_str) {
                tempVec.push_back(str_t(it.data(), it.size()));
            }
            value->clear();
            bool mergeStatus;
            STAT_PROCESS(mergeStatus = KDSepMergeOperatorPtr_->Merge(tempValueStrT, tempVec, value), StatsType::KDS_GET_FULL_MERGE);
            if (mergeStatus == false) {
                debug_error("[ERROR] merge failed: key %s number of deltas %lu raw value size %lu\n", key.c_str(), tempVec.size(), tempValueStr.size());
                exit(1);
            }
        }
        return true;
    }
}

// The correct procedure: (Not considering deleted keys)
// 1. Scan the RocksDB/vLog for keys and values
// 2. Scan the buffer to check whether some keys are updated
// 3. Scan delta store to find deltas.
bool KDSep::Scan(const string& startKey, int len, vector<string>& keys, vector<string>& values) 
{
    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);
    struct timeval tv;
    gettimeofday(&tv, 0);
    vector<string> lsm_values;

    // 1. Scan the RocksDB/vLog for keys and values
    STAT_PROCESS(lsmTreeInterface_.Scan(startKey, len, keys, lsm_values),
            StatsType::KDS_SCAN_LSM);

    if (KDSepRunningMode_ == kWithNoDeltaStore || 
	    KDSepRunningMode_ == kBatchedWithNoDeltaStore) {
        StatsRecorder::getInstance()->timeProcess(StatsType::SCAN, tv);
	return true;
    }

    // 2. Scan the delta store

    if (enableLsmTreeDeltaMeta_ == true) {
	debug_error("not implemented: key %lu\n", keys.size());
    }

    bool ret;
    vector<vector<string>> key_deltas;
//    debug_error("Start key %s len %d\n", startKey.c_str(), len);
    STAT_PROCESS(
    ret = delta_store_->multiGet(keys, key_deltas),
    StatsType::KDS_SCAN_DS);

    if (ret == false) {
	debug_error("scan in delta store failed: %lu\n", keys.size());
    }

    STAT_PROCESS(
    MultiGetFullMergeInternal(keys, lsm_values, key_deltas, values), 
    StatsType::KDS_SCAN_FULL_MERGE);

//    fprintf(stderr, "Start key %s len %d\n", startKey.c_str(), len);
//    fprintf(stderr, "keys.size() %lu values.size() %lu\n", keys.size(),
//            values.size());
//    for (int i = 0; i < (int)keys.size(); i++) {
//        fprintf(stderr, "%s %lu\n", keys[i].c_str(), values[i].size());
//    }
//    exit(1);

    StatsRecorder::getInstance()->timeProcess(StatsType::SCAN, tv);
    return true;
}

bool KDSep::MultiGetInternalForWriteBack(const vector<string>& keys, 
	vector<string>& values) 
{
//    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);
    struct timeval tv;
    gettimeofday(&tv, 0);
    vector<string> lsm_values;

    if (KDSepRunningMode_ == kWithNoDeltaStore || 
	    KDSepRunningMode_ == kBatchedWithNoDeltaStore) {
        StatsRecorder::getInstance()->timeProcess(StatsType::SCAN, tv);
	return true;
    }


    // 1. Scan the RocksDB/vLog for keys and values
    lsmInterfaceOperationStruct* op = nullptr;
    if (enableParallelLsmInterface == true) {
        op = new lsmInterfaceOperationStruct;
        op->keysPtr = &keys;
        op->valuesPtr = &lsm_values;
        op->is_write = false;
        op->job_done = kNotDone;
        lsmInterfaceOperationsQueue_->push(op);
    } else {
        STAT_PROCESS(lsmTreeInterface_.MultiGet(keys, lsm_values),
                StatsType::KDS_WRITE_BACK_GET_LSM);
    }

    // 2. Scan the delta store
    if (enableLsmTreeDeltaMeta_ == true) {
	debug_error("not implemented: key %lu\n", keys.size());
    }

    bool ret;
    vector<vector<string>> key_deltas;
//    debug_error("Start key %s len %d\n", startKey.c_str(), len);
    STAT_PROCESS(
    ret = delta_store_->multiGet(keys, key_deltas),
    StatsType::KDS_WRITE_BACK_GET_DS);

    if (ret == false) {
	debug_error("scan in delta store failed: %lu\n", keys.size());
    }

    if (op != nullptr) {
        while (op->job_done == kNotDone) {
            asm volatile("");
        }
        if (op->job_done == kError) {
            debug_e("lsmInterfaceOperationsQueue_ failed");
            exit(1);
        }
        delete op;
    }

    STAT_PROCESS(
    MultiGetFullMergeInternal(keys, lsm_values, key_deltas, values), 
    StatsType::KDS_WRITE_BACK_FULLMERGE);

//    fprintf(stderr, "Start key %s len %d\n", startKey.c_str(), len);
//    fprintf(stderr, "keys.size() %lu values.size() %lu\n", keys.size(),
//            values.size());
//    for (int i = 0; i < (int)keys.size(); i++) {
//        fprintf(stderr, "%s %lu\n", keys[i].c_str(), values[i].size());
//    }
//    exit(1);

    StatsRecorder::getInstance()->timeProcess(StatsType::SCAN, tv);
    return true;
}

bool KDSep::MultiGetFullMergeInternal(const vector<string>& keys,
	const vector<string>& lsm_values,
	const vector<vector<string>>& key_deltas,
	vector<string>& values) {

    values.resize(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
	const string& lsm_value = lsm_values[i];
	auto& key = keys[i];

	// extract header
        KvHeader header;
        size_t header_sz = sizeof(KvHeader);
        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz);
        }

        if (header.valueSeparatedFlag_ == true) {
            debug_error("[ERROR] value separated but not retrieved %s\n", key.c_str());
            assert(0);
        }

	// get the raw value
	str_t raw_value(const_cast<char*>(lsm_value.data()) + header_sz,
		header.rawValueSize_);

	// check the deltas
        if (key_deltas[i].empty() == true) { 
            values[i].assign(raw_value.data_, raw_value.size_);
	    continue;
        }

        bool mergeOperationStatus;
        vector<str_t> deltaInStrT;
        int total_d_sz = 0;
        for (auto& it : key_deltas[i]) {
            deltaInStrT.push_back(
		    str_t(const_cast<char*>(it.data()), it.size()));
            total_d_sz += it.size();
        }

        STAT_PROCESS(mergeOperationStatus =
		KDSepMergeOperatorPtr_->Merge(raw_value, deltaInStrT, 
		    &(values[i])),
                StatsType::KDSep_GET_FULL_MERGE);

        if (mergeOperationStatus == false) { 
            debug_error("[ERROR] Perform merge operation fail, key = %s\n",
                    key.c_str());
            return false;
        }
	// dont do write back
    }

    return true;
}

bool KDSep::Merge(const string& key, const string& value)
{
    // check write stall 
    if (write_stall_ != nullptr) {
        if (*write_stall_ == true) {
            debug_error("merge stall key %s\n", key.c_str());
            while (*write_stall_ == true) {
                asm volatile("");
            }
            debug_error("merge stall finish %s\n", key.c_str());
        }
    }

    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();

    mempoolHandler_t obj;
    ;
    bool insertStatus = false;
    STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, false, obj), StatsType::KDSep_INSERT_MEMPOOL);
    if (insertStatus == false) {
        debug_error("Insert error, size %lu %lu\n", key.size(), value.size());
        exit(1);
    }
    bool shouldDeleteMemPoolHandler = false;
    bool mergeOperationStatus = false;
    switch (KDSepRunningMode_) {
    case kBatchedWithNoDeltaStore:
    case kBatchedWithDeltaStore:
        mergeOperationStatus = MergeWithWriteBatch(obj); 
        break;
    case kWithDeltaStore:
    case kWithNoDeltaStore:
        mergeOperationStatus = SingleMergeInternal(obj);
        shouldDeleteMemPoolHandler = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", KDSepRunningMode_);
        mergeOperationStatus = false;
        shouldDeleteMemPoolHandler = true;
        break;
    }
    if (shouldDeleteMemPoolHandler == true) {
        objectPairMemPool_->eraseContentFromMemPool(obj);
    }
    if (mergeOperationStatus == true) {
        return true;
    } else {
        return false;
    }
}

bool KDSep::GetCurrentValueThenWriteBack(const string& key)
{
    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);

    vector<string> buf_deltas_str;
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
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_WAIT_BUFFER, tvAll);
        struct timeval tv;
        gettimeofday(&tv, 0);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        char keyBuffer[key.size()];
        memcpy(keyBuffer, key.c_str(), key.size());
        str_t currentKey(keyBuffer, key.length());
        auto mapIt = batch_map_[batch_in_use_]->find(currentKey);
        if (mapIt != batch_map_[batch_in_use_]->end()) {
            struct timeval tv0;
            gettimeofday(&tv0, 0);
            for (auto queueIt : mapIt->second) {
                if (queueIt.first == kPutOp) {
                    debug_info("Get current value in write buffer, skip write back, key = %s\n", key.c_str());
                    batchedBufferOperationMtx_.unlock();
                    StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_NO_WAIT_BUFFER, tv);
                    return true;
                } else {
                    buf_deltas_str.push_back(string(queueIt.second.valuePtr_, queueIt.second.valueSize_));
                }
            }
            if (buf_deltas_str.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), buf_deltas_str.size());
            }
        }
        batchedBufferOperationMtx_.unlock();
        StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_NO_WAIT_BUFFER, tv);
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_CHECK_BUFFER, tvAll);
    // get content from underlying DB
    string newValueStr;
    string tempRawValueStr;
    bool getNewValueStrSuccessFlag;
    STAT_PROCESS(getNewValueStrSuccessFlag = GetInternal(key, &tempRawValueStr, true), StatsType::KDSep_WRITE_BACK_GET);
    bool mergeStatus;
    if (getNewValueStrSuccessFlag) { 
        if (needMergeWithInBufferOperationsFlag == true) {
            STAT_PROCESS(mergeStatus = KDSepMergeOperatorPtr_->Merge(tempRawValueStr, buf_deltas_str, &newValueStr), StatsType::KDSep_WRITE_BACK_FULL_MERGE);
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
    STAT_PROCESS(ret = PutImpl(key, newValueStr), StatsType::KDSep_WRITE_BACK_PUT);
    if (ret == false) {
        debug_error("write back failed, key %s\n", key.c_str());
    }

    return ret;
}

bool KDSep::GetCurrentValuesThenWriteBack(const vector<string>& keys)
{
    scoped_lock<shared_mutex> w_lock(KDSepOperationsMtx_);

    vector<vector<string>> buf_deltas_strs;
    buf_deltas_strs.resize(keys.size());
    bool need_write_back[keys.size()];
    bool needMergeWithInBufferOperationsFlag[keys.size()];
    bool any_no_need = false;

    struct timeval tvAll, tv;
    gettimeofday(&tvAll, 0);
    tv = tvAll;

    // read from buffer
    for (int i = 0; i < keys.size(); i++) {
        gettimeofday(&tv, 0);
	auto& key = keys[i];
	need_write_back[i] = true;
	needMergeWithInBufferOperationsFlag[i] = false;

	if (isBatchedOperationsWithBufferInUse_ == false) {
	    break;
	}
	
	// try read from buffer first;
	if (oneBufferDuringProcessFlag_ == true) {
	    debug_trace("Wait for batched buffer process%s\n", "");
	    while (oneBufferDuringProcessFlag_ == true) {
		asm volatile("");
	    }
	}
	batchedBufferOperationMtx_.lock();
        StatsRecorder::staticProcess(StatsType::KDSep_WRITE_BACK_WAIT_BUFFER,
                tv);

	gettimeofday(&tv, 0);
	debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
	char keyBuffer[key.size()];
	memcpy(keyBuffer, key.c_str(), key.size());
	str_t currentKey(keyBuffer, key.length());
	auto mapIt = batch_map_[batch_in_use_]->find(currentKey);
	if (mapIt != batch_map_[batch_in_use_]->end()) {
	    struct timeval tv0;
	    gettimeofday(&tv0, 0);
	    for (auto queueIt : mapIt->second) {
		if (queueIt.first == kPutOp) {
		    debug_info("Get current value in write buffer, skip write back, key = %s\n", key.c_str());
		    batchedBufferOperationMtx_.unlock();
		    StatsRecorder::getInstance()->timeProcess(
			    StatsType::KDSep_WRITE_BACK_NO_WAIT_BUFFER,
			    tv);
		    need_write_back[i] = false;
		    any_no_need = true;
		} else {
		    buf_deltas_strs[i].push_back(string(queueIt.second.valuePtr_,
				queueIt.second.valueSize_));
		}
	    }
	    if (buf_deltas_strs[i].size() != 0) {
		needMergeWithInBufferOperationsFlag[i] = true;
		debug_info("get deltas from unflushed buffer, for key = "
			"%s, deltas number = %lu\n", key.c_str(),
			buf_deltas_strs[i].size());
	    }
	}
	batchedBufferOperationMtx_.unlock();
	StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_NO_WAIT_BUFFER, tv);
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK_CHECK_BUFFER, tvAll);

    vector<string> persist_values;
    bool ret = true;
    bool mergeStatus;

    if (any_no_need == false) {
        STAT_PROCESS(MultiGetInternalForWriteBack(keys, persist_values),
                KDSep_WRITE_BACK_GET);
	for (int i = 0; i < keys.size(); i++) {
	    auto& key = keys[i];
	    string& tempRawValueStr = persist_values[i];
	    string newValueStr;
	    // merge with existing deltas;

	    if (needMergeWithInBufferOperationsFlag[i] == true) {
		STAT_PROCESS(mergeStatus =
			KDSepMergeOperatorPtr_->Merge(tempRawValueStr,
			    buf_deltas_strs[i], &newValueStr),
			StatsType::KDSep_WRITE_BACK_FULL_MERGE);
		if (mergeStatus == false) {
		    debug_error("merge failed: key %s raw value size %lu\n",
			    key.c_str(), tempRawValueStr.size());
		    exit(1);
		}
	    } else {
		newValueStr.assign(tempRawValueStr);
	    }

	    // put
	    STAT_PROCESS(ret = PutImpl(key, newValueStr),
		    StatsType::KDSep_WRITE_BACK_PUT);
	    if (ret == false) {
		debug_error("write back failed, key %s\n", key.c_str());
	    }
	}
    } else {
	vector<string> new_keys;
	for (int i = 0; i < keys.size(); i++) {
	    if (need_write_back[i]) {
		new_keys.push_back(keys[i]);
	    }
	}
        STAT_PROCESS(MultiGetInternalForWriteBack(new_keys, persist_values),
                KDSep_WRITE_BACK_GET);
	int new_ki = 0;
	for (int i = 0; i < keys.size(); i++) {
	    if (need_write_back[i] == false) {
		continue;
	    }

	    auto& key = keys[i];
	    string& tempRawValueStr = persist_values[new_ki];
	    string newValueStr;

	    if (needMergeWithInBufferOperationsFlag[i] == true) {
		STAT_PROCESS(mergeStatus =
			KDSepMergeOperatorPtr_->Merge(tempRawValueStr,
			    buf_deltas_strs[i], &newValueStr),
			StatsType::KDSep_WRITE_BACK_FULL_MERGE);
		if (mergeStatus == false) {
		    debug_error("merge failed: key %s raw value size %lu\n",
			    key.c_str(), tempRawValueStr.size());
		    exit(1);
		}
	    } else {
		newValueStr.assign(tempRawValueStr);
	    }

	    // put
	    STAT_PROCESS(ret = PutImpl(key, newValueStr),
		    StatsType::KDSep_WRITE_BACK_PUT);
	    if (ret == false) {
		debug_error("write back failed, key %s\n", key.c_str());
	    }
	
	    new_ki++;
	}
    }

    return ret;
}

//bool KDSep::SingleDelete(const string& key)
//{
//    rocksdb::Status rocksDBStatus = pointerToRawRocksDB_->SingleDelete(internalWriteOption_, key);
//    if (!rocksDBStatus.ok()) {
//        debug_error("[ERROR] Deplete underlying rocksdb fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
//        return false;
//    } else {
//        return true;
//    }
//}

bool KDSep::PutWithWriteBatch(mempoolHandler_t obj)
{
//    static uint64_t cnt = 0;
    if (obj.isAnchorFlag_ == false) {
        debug_error("[ERROR] put operation should has an anchor flag%s\n", "");
        return false;
    }

    // cerr << "Key size = " << obj.keySize_ << endl;
    struct timeval tv;
    gettimeofday(&tv, 0);
    if (batch_sizes_[batch_in_use_] >=
            maxBatchOperationBeforeCommitSize_)
    {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::KDS_PUT_LOCK_1, tv);
    gettimeofday(&tv, 0);
    scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::getInstance()->timeProcess(StatsType::KDS_PUT_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", batch_in_use_, batch_nums_[batch_in_use_]);
    if (batch_sizes_[batch_in_use_] >=
            maxBatchOperationBeforeCommitSize_)
    {
        // flush old one
        notifyWriteBatchMQ_->push(batch_map_[batch_in_use_]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", batch_in_use_);
        // insert to another deque
        if (batch_in_use_ == 1) {
            batch_in_use_ = 0;
        } else {
            batch_in_use_ = 1;
        }
        batch_nums_[batch_in_use_] = 0;
        batch_sizes_[batch_in_use_] = 0;
    } 
    
    str_t currentKey(obj.keyPtr_, obj.keySize_);
    auto mapIt = batch_map_[batch_in_use_]->find(currentKey);
    if (mapIt != batch_map_[batch_in_use_]->end()) {
        // remove all the deltas
        for (auto it : mapIt->second) {
            objectPairMemPool_->eraseContentFromMemPool(it.second);
            batch_sizes_[batch_in_use_] -=
                it.second.keySize_ + it.second.valueSize_;
        }
        mapIt->second.clear();
        mapIt->second.push_back(make_pair(kPutOp, obj));
        batch_sizes_[batch_in_use_] += obj.keySize_ + obj.valueSize_;
        batch_nums_[batch_in_use_]++;
    } else {
        vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
        tempDeque.push_back(make_pair(kPutOp, obj));
        batch_map_[batch_in_use_]->insert(make_pair(currentKey, tempDeque));
        batch_sizes_[batch_in_use_] += obj.keySize_ + obj.valueSize_;
        batch_nums_[batch_in_use_]++;
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::KDS_PUT_APPEND_BUFFER, tv);
    return true;
}

bool KDSep::MergeWithWriteBatch(mempoolHandler_t obj)
{
    debug_info("[MergeOp] key = %s, sequence number = %u\n",
	    string(obj.keyPtr_, obj.keySize_).c_str(), obj.seq_num);
    if (obj.isAnchorFlag_ == true) {
        debug_error("[ERROR] merge operation should has no anchor flag%s\n", "");
    }
    struct timeval tv;
    gettimeofday(&tv, 0);
//    if (batch_nums_[batch_in_use_] == ) 
    if (batch_sizes_[batch_in_use_] >=
            maxBatchOperationBeforeCommitSize_)
    {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::staticProcess(StatsType::KDS_MERGE_LOCK_1, tv);
    gettimeofday(&tv, 0);
    scoped_lock<shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::staticProcess(StatsType::KDS_MERGE_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", batch_in_use_,
            batch_nums_[batch_in_use_]);
//    if (batch_nums_[batch_in_use_] == ) 
    if (batch_sizes_[batch_in_use_] >=
            maxBatchOperationBeforeCommitSize_) {
        // flush old one
        notifyWriteBatchMQ_->push(batch_map_[batch_in_use_]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", batch_in_use_);
        batch_sizes_[batch_in_use_] = 0;
        // insert to another deque
        if (batch_in_use_ == 1) {
            batch_in_use_ = 0;
        } else {
            batch_in_use_ = 1;
        }
        batch_nums_[batch_in_use_] = 0;
        batch_sizes_[batch_in_use_] = 0;
    }

    // only insert
    str_t currentKey(obj.keyPtr_, obj.keySize_);
    auto mapIt = batch_map_[batch_in_use_]->find(currentKey);
    if (mapIt != batch_map_[batch_in_use_]->end()) {
        // has the existing key
        auto& vec = mapIt->second;
        vec.push_back(make_pair(kMergeOp, obj));
        batch_nums_[batch_in_use_]++;
        batch_sizes_[batch_in_use_] += obj.keySize_ + obj.valueSize_;


        // remove some deltas in it.
        if (vec.size() > 10) {
            struct timeval tv_clean;
            gettimeofday(&tv_clean, 0);

            if (vec.back().first == kPutOp) {
                debug_error("more than 10 items, but the first is value: "
                        " %lu\n", vec.size());
            } else if (vec[0].first == kPutOp) {
                // the first is value and the left is delta
                str_t bv(vec[0].second.valuePtr_, vec[0].second.valueSize_);
                vector<str_t> bdeltas;
                bdeltas.resize(vec.size() - 1);
                for (auto vec_i = 1; vec_i < vec.size(); vec_i++) {
                    auto& tmp_obj = vec[vec_i].second;
                    if (vec[vec_i].first != kMergeOp) {
                        debug_error("buffer: id %d not merge\n", vec_i);
                    }
                    bdeltas[vec_i - 1] = str_t(tmp_obj.valuePtr_,
                            tmp_obj.valueSize_);
                }

                // merge a new value 
                string result;
                KDSepMergeOperatorPtr_->Merge(bv, bdeltas, &result);

                // remove all existing objects
                for (auto it : vec) {
                    objectPairMemPool_->eraseContentFromMemPool(it.second);
                    batch_sizes_[batch_in_use_] -= 
                        it.second.keySize_ + it.second.valueSize_;
                }

                // prepare new object
                mempoolHandler_t merged_obj;
                objectPairMemPool_->insertContentToMemPoolAndGetHandler(
                        string(obj.keyPtr_, obj.keySize_), result,
                        obj.seq_num, true, merged_obj); 
                vec.clear();
                vec.push_back(make_pair(kPutOp, merged_obj));
                batch_sizes_[batch_in_use_] +=
                    merged_obj.keySize_ + merged_obj.valueSize_;
            } else {
                // the first is merge, so the others are also merge
                vector<str_t> bdeltas;
                bdeltas.resize(vec.size());
                for (auto vec_i = 0; vec_i < vec.size(); vec_i++) {
                    auto& tmp_obj = vec[vec_i].second;
                    if (vec[vec_i].first != kMergeOp) {
                        debug_error("buffer: id %d not merge\n", vec_i);
                    }
                    bdeltas[vec_i] = str_t(tmp_obj.valuePtr_,
                            tmp_obj.valueSize_);
                }

                // merge a new delta
                str_t result;
                KDSepMergeOperatorPtr_->PartialMerge(bdeltas, result);

                // remove all existing objects
                for (auto it : vec) {
                    objectPairMemPool_->eraseContentFromMemPool(it.second);
                    batch_sizes_[batch_in_use_] -= it.second.keySize_ +
                        it.second.valueSize_;
                }

                // prepare new object
                mempoolHandler_t merged_obj;
                objectPairMemPool_->insertContentToMemPoolAndGetHandler(
                        string(obj.keyPtr_, obj.keySize_), 
                        string(result.data_, result.size_),
                        obj.seq_num, false, merged_obj); 
                delete[] result.data_;

                vec.clear();
                vec.push_back(make_pair(kMergeOp, merged_obj));
                batch_sizes_[batch_in_use_] +=
                    merged_obj.keySize_ + merged_obj.valueSize_;
            }

            StatsRecorder::getInstance()->timeProcess(
                    StatsType::KDS_MERGE_CLEAN_BUFFER, tv_clean);
        }

    } else {
        // do not have the key, add one
        vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
        tempDeque.push_back(make_pair(kMergeOp, obj));
        batch_map_[batch_in_use_]->insert(make_pair(currentKey, tempDeque));
        batch_nums_[batch_in_use_]++;
        batch_sizes_[batch_in_use_] += obj.keySize_ + obj.valueSize_;
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::KDS_MERGE_APPEND_BUFFER, tv);
    return true;
}

bool KDSep::performInBatchedBufferDeduplication(unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*& operationsMap)
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
            STAT_PROCESS(mergeStatus = KDSepMergeOperatorPtr_->Merge(firstValue, operandList, &finalValue), StatsType::KDS_DEDUP_FULL_MERGE);
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
            STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalValue, currentSequenceNumber, true, newHandler), StatsType::KDSep_INSERT_MEMPOOL);
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
            STAT_PROCESS(mergeStatus = KDSepMergeOperatorPtr_->PartialMerge(operandList, finalOperandList), StatsType::KDS_DEDUP_PARTIAL_MERGE);
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
            STAT_PROCESS(insertStatus = objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalOperandList[0], currentSequenceNumber, false, newHandler), StatsType::KDSep_INSERT_MEMPOOL);
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

void KDSep::processBatchedOperationsWorker()
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
//            if (KDSepRunningMode_ != kBatchedWithNoDeltaStore) {
                STAT_PROCESS(performInBatchedBufferDeduplication(currentHandler), StatsType::KDS_FLUSH_DEDUP);
                StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_PLAIN_ROCKSDB, tv);
//            }
            vector<mempoolHandler_t> pending_kvs, pending_kds;
            for (auto it = currentHandler->begin(); it != currentHandler->end(); it++) {
                for (auto dequeIt : it->second) {
                    if (dequeIt.first == kPutOp) {
                        pending_kvs.push_back(dequeIt.second);
                        if (dequeIt.second.isAnchorFlag_ == false) {
                            debug_error("[ERROR] Current key value pair not fit requirement, kPutOp should be anchor %s\n", "");
                        } else {
                            pending_kds.push_back(dequeIt.second);
                        }
                    } else {
                        if (dequeIt.second.isAnchorFlag_ == true) {
                            debug_error("[ERROR] Current key value pair not fit requirement, kMergeOp should not be anchor %s\n", "");
                        } else {
                            pending_kds.push_back(dequeIt.second);
                        }
                    }
                }
            }
            bool putToDeltaStoreStatus = false;
            switch (KDSepRunningMode_) {
            case kBatchedWithNoDeltaStore: 
             {
                struct timeval tv;
                gettimeofday(&tv, 0);
                rocksdb::Status rocksDBStatus;
                rocksdb::WriteBatch mergeBatch;
                for (auto index = 0; index < pending_kds.size(); index++) {
                    if (pending_kds[index].isAnchorFlag_ == false) {
                        auto& it = pending_kds[index];
                        KvHeader header(false, false, it.seq_num, it.valueSize_);
                        // reserve space
                        char buf[it.valueSize_ + sizeof(KvHeader)];
                        size_t header_sz = sizeof(KvHeader);

                        if (use_varint_kv_header == false) {
                            memcpy(buf, &header, header_sz);
                        } else {
                            header_sz = PutKVHeaderVarint(buf, header);
                        }
                        memcpy(buf + header_sz, it.valuePtr_, it.valueSize_);

                        rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
                        rocksdb::Slice newValue(buf, it.valueSize_ + header_sz);
                        mergeBatch.Merge(newKey, newValue);
                    }
                }

		bool placeholder = false;
		bool lsmTreeInterfaceStatus =
                    lsmTreeInterface_.MultiWriteWithBatch(
                            pending_kvs, &mergeBatch, placeholder);

                if (lsmTreeInterfaceStatus == false) {
                    debug_error("lsmTreeInterfaceStatus %d\n", (int)lsmTreeInterfaceStatus);
                }
                StatsRecorder::getInstance()->timeProcess(StatsType::KDS_FLUSH_WITH_NO_DSTORE, tv);
                break;
            }
            case kBatchedWithDeltaStore:
            {
                struct timeval tv;
                gettimeofday(&tv, 0);
                vector<bool> separateFlagVec;
                vector<mempoolHandler_t> notSeparatedDeltasVec;
                uint32_t spearateTrueCounter = 0, separateFalseCounter = 0;

                for (auto deltaIt = pending_kds.begin(); deltaIt != pending_kds.end(); deltaIt++) {
                    if (deltaIt->valueSize_ <= deltaExtractSize_ && deltaIt->isAnchorFlag_ == false) {
                        separateFlagVec.push_back(false);
                        notSeparatedDeltasVec.push_back(*deltaIt);
                        pending_kds.erase(deltaIt);
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
                                // write delta to rocksdb
                                char lsm_buffer[sizeof(KvHeader) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                KvHeader header(false, false,
                                        notSeparatedDeltasVec[notSeparatedID].seq_num,
                                        notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                size_t header_sz = sizeof(KvHeader);

                                // encode header
                                if (use_varint_kv_header == false) {
                                    memcpy(lsm_buffer, &header, header_sz);
                                } else {
                                    header_sz = PutKVHeaderVarint(lsm_buffer, header);
                                }
                                memcpy(lsm_buffer + header_sz,
                                        notSeparatedDeltasVec[notSeparatedID].valuePtr_,
                                        notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                rocksdb::Slice newKey(
                                        notSeparatedDeltasVec[notSeparatedID].keyPtr_,
                                        notSeparatedDeltasVec[notSeparatedID].keySize_);
                                rocksdb::Slice newValue(
                                        lsm_buffer, header_sz +
                                        notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), notSeparatedDeltasVec[notSeparatedID].seq_num);
                                mergeBatch.Merge(newKey, newValue);
                            } else {
                                // skip anchor
                                string newKey(
                                        notSeparatedDeltasVec[notSeparatedID].keyPtr_,
                                        notSeparatedDeltasVec[notSeparatedID].keySize_);
                                debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), notSeparatedDeltasVec[notSeparatedID].seq_num);
                            }
                            notSeparatedID++;
                        } else {
                            if (pending_kds[separatedID].isAnchorFlag_ == false) {
                                // write delta meta to rocksdb
                                char lsm_buffer[sizeof(KvHeader)];
                                KvHeader header(false, true,
                                        pending_kds[separatedID].seq_num,
                                        pending_kds[separatedID].valueSize_);
                                size_t header_sz = sizeof(KvHeader);

                                // encode header
                                if (use_varint_kv_header == false) {
                                    memcpy(lsm_buffer, &header, header_sz);
                                } else {
                                    header_sz = PutKVHeaderVarint(lsm_buffer, header);
                                }
                                rocksdb::Slice newKey(
                                        pending_kds[separatedID].keyPtr_,
                                        pending_kds[separatedID].keySize_);
                                rocksdb::Slice newValue(lsm_buffer, header_sz);
                                debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), pending_kds[separatedID].seq_num);
                                mergeBatch.Merge(newKey, newValue);
                            } else {
                                // skip anchor for rocksdb
                                string newKey(
                                        pending_kds[separatedID].keyPtr_,
                                        pending_kds[separatedID].keySize_);
                                debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), pending_kds[separatedID].seq_num);
                            }
                            separatedID++;
                        }
                    } 
                } else {
                    // don't do anything if we do not use metadata
                }

                // LSM interface
                struct lsmInterfaceOperationStruct* op = nullptr;
		bool vlog_need_post_update = false;
		bool ds_need_post_update = false;
                bool ds_need_flush = false;
                bool two_phase_write = 
                    enable_crash_consistency_ && !pending_kvs.empty();

                if (enableParallelLsmInterface == true) {
                    op = new lsmInterfaceOperationStruct;
                    op->mergeBatch = &mergeBatch; 
                    op->handlerToValueStoreVecPtr = &pending_kvs;
                    op->is_write = true;
                    op->job_done = kNotDone;
		    op->need_post_update_ptr = &vlog_need_post_update;
                    lsmInterfaceOperationsQueue_->push(op);
                } else {
                    STAT_PROCESS(lsmTreeInterface_.MultiWriteWithBatch(
				pending_kvs, &mergeBatch,
				vlog_need_post_update), 
                            StatsType::KDS_FLUSH_LSM_INTERFACE);
                }

                // DeltaStore interface

		// any value to write, then we need to 
		if (two_phase_write) {
                    putToDeltaStoreStatus = delta_store_->putCommitLog(pending_kds, ds_need_flush);
                    if (pending_kds.size() > 0) {
                        ds_need_post_update = true;
                    }
		} else {
                    // directly multiput
		    STAT_PROCESS(
		    putToDeltaStoreStatus =
		    delta_store_->multiPut(pending_kds, 
                        true /* arbitrary */, true), 
			StatsType::KDS_FLUSH_MUTIPUT_DSTORE);
		}
                if (putToDeltaStoreStatus == false) {
                    debug_error("[ERROR] could not put %zu object into delta store,"
                            " as well as not separated object number = %zu\n", 
                            pending_kds.size(), notSeparatedDeltasVec.size());
                    break;
                }

                // Check LSM interface
                if (op != nullptr) {
//                    lsm_interface_cv.notify_one();
                    while (op->job_done == kNotDone) {
			asm volatile("");
                    }
                    if (op->job_done == kError) {
                        debug_error("lsmInterfaceOp error %s\n", ""); 
                    }
                    delete op;
                }

                // Step 3. commit to commit log
                if (two_phase_write) {
                    delta_store_->commitToCommitLog();
                }

                // Step 4. update the lsm tree (step 4 and 5 can be parallel)
                if (vlog_need_post_update) {
                    lsmTreeInterface_.updateVlogLsmTree();
                }

                // Step 5. update the delta stop
                if (two_phase_write) {
                    bool s;
		    STAT_PROCESS(
                    s = delta_store_->multiPut(pending_kds, ds_need_flush,
                            false),
                    StatsType::KDS_FLUSH_MUTIPUT_DSTORE);
                    if (s == false) {
                        debug_error("multiput second phase error: %lu\n",
                                pending_kds.size());
                        exit(1);
                    }
                }
                
                StatsRecorder::getInstance()->timeProcess(StatsType::KDS_FLUSH_WITH_DSTORE, tv);
                break;
            }
            default:
                debug_error("[ERROR] unknown running mode = %d", KDSepRunningMode_);
                break;
            }
            // update write buffers
            debug_info("process batched contents done, start update write buffer's map, target update key number = %lu\n", pending_kds.size());
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
            StatsRecorder::getInstance()->timeProcess(StatsType::KDS_FLUSH, tv);
        }
    }
    writeBatchOperationWorkExitFlag = true;
    debug_info("Process batched operations done, exit thread%s\n", "");
    return;
}

void KDSep::processWriteBackOperationsWorker()
{
    uint64_t total_written_pairs = 0;
    int written_pairs = 0;
    int write_back_batch_size = 1000;
    vector<writeBackObject*> pairs;
    vector<string> keys;
    while (true) {
        if (write_back_queue_->done == true && write_back_queue_->isEmpty() == true) {
            break;
        }
        {
            unique_lock<mutex> lk(*write_back_mutex_);
            write_back_cv_->wait(lk);
        }
        writeBackObject* currentProcessPair;
        written_pairs = 0;
        struct timeval tv;
        gettimeofday(&tv, 0);
        while (write_back_queue_->pop(currentProcessPair)) {
            debug_warn("Target Write back key = %s\n", currentProcessPair->key.c_str());
	    pairs.push_back(currentProcessPair);
	    while (pairs.size() <= write_back_batch_size &&
		    write_back_queue_->pop(currentProcessPair)) {
		pairs.push_back(currentProcessPair);
		keys.push_back(currentProcessPair->key);
	    }

//            debug_error("(sta) Target Write back key = %s\n", currentProcessPair->key.c_str());
//            bool writeBackStatus = GetCurrentValueThenWriteBack(currentProcessPair->key);
            bool writeBackStatus = GetCurrentValuesThenWriteBack(keys);
//            debug_error("(fin) Target Write back key = %s\n", currentProcessPair->key.c_str());
            if (writeBackStatus == false) {
                debug_error("Could not write back target key = %s\n", currentProcessPair->key.c_str());
                exit(1);
            } else {
                debug_warn("Write back key = %s success\n", currentProcessPair->key.c_str());
            }
            written_pairs += keys.size();

	    for (auto& op : pairs) {
		delete op;
	    }

	    pairs.clear();
	    keys.clear();
        }

        if (written_pairs > 0) {
            wb_keys_mutex->lock();
            if (wb_keys->size() > 0) {
                uint64_t num_keys = wb_keys->size();
                debug_error("Use extra queue: size %lu\n", num_keys);
                std::queue<string> q(*wb_keys);
                std::queue<string> empty;
                std::swap(*wb_keys, empty); // clean the queue
                wb_keys_mutex->unlock();

                while (!q.empty()) {
//                    auto k = q.front(); // get the first element
		    keys.push_back(q.front());
                    q.pop();             // remove the first element

		    while (keys.size() <= write_back_batch_size && !q.empty())
		    {
			keys.push_back(q.front());
			q.pop();
		    }	

//                    bool wb = GetCurrentValueThenWriteBack(keys[0]);
                    bool wb = GetCurrentValuesThenWriteBack(keys);
                    if (wb == false) {
                        debug_error("Could not write back target keys %lu\n",
                                keys.size());
                        exit(1);
                    } else {
                    }

		    keys.clear();
                }
                written_pairs += num_keys;
            } else {
                wb_keys_mutex->unlock();
            }
        }

        if (written_pairs > 0) {
            total_written_pairs += written_pairs;
            debug_error("Write back: %d KD pairs (total %lu)\n", 
                    written_pairs, total_written_pairs);
            if (write_stall_ != nullptr) {
                *write_stall_ = false;
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::KDSep_WRITE_BACK, tv);
        }
    }
    return;
}

void KDSep::processLsmInterfaceOperationsWorker()
{
    lsmInterfaceOperationStruct* op;
    while (true) {
	if (lsmInterfaceOperationsQueue_->done == true &&
		lsmInterfaceOperationsQueue_->isEmpty() == true) {
            break;
        }

        while (lsmInterfaceOperationsQueue_->pop(op)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            if (op->is_write == false) {
                if (op->keysPtr == nullptr) {
                    STAT_PROCESS(lsmTreeInterface_.Get(op->key, op->value),
                            StatsType::KDS_LSM_INTERFACE_GET); 
                } else {
                    STAT_PROCESS(lsmTreeInterface_.MultiGet(*op->keysPtr,
                                *op->valuesPtr),
                            StatsType::KDS_WRITE_BACK_GET_LSM);
                }
            } else {
		STAT_PROCESS(lsmTreeInterface_.MultiWriteWithBatch(
			    *(op->handlerToValueStoreVecPtr),
			    op->mergeBatch, *op->need_post_update_ptr), 
                            StatsType::KDS_FLUSH_LSM_INTERFACE);
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::KDS_LSM_INTERFACE_OP, tv);
            op->job_done = kDone;
        }
    }
    return;
}

void KDSep::Recovery() {
    if (delta_store_ != nullptr) {
	delta_store_->Recovery();
    }
}

// TODO: upper functions are not complete

bool KDSep::deleteExistingThreads()
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

bool KDSep::extractDeltas(string lsm_value, uint64_t skipSize, 
        vector<pair<bool, string>>& mergeOperatorsVec) {
    uint64_t internalValueSize = lsm_value.size();
    uint64_t value_i = skipSize;
    size_t header_sz = sizeof(KvHeader);
    while (value_i != internalValueSize) {
        KvHeader header;
        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str() + value_i, sizeof(KvHeader));
        } else {
            header = GetKVHeaderVarint(lsm_value.c_str() + value_i, header_sz);
        }
        value_i += header_sz;
        if (header.mergeFlag_ == true) {
            debug_error("[ERROR] Find new value index in merge operand list,"
                    " this index refer to raw value size = %u\n",
                    header.rawValueSize_);
            assert(0);
        }
        if (header.valueSeparatedFlag_ != true) {
            assert(value_i + header.rawValueSize_ <= lsm_value.size());
            string value(lsm_value.c_str() + value_i, header.rawValueSize_);
            value_i += header.rawValueSize_;
            mergeOperatorsVec.push_back(make_pair(false, value));
        } else {
            mergeOperatorsVec.push_back(make_pair(true, ""));
        }
    }
    return true;
}

void KDSep::tryTuneCache() {
    if (kd_cache_ == nullptr || delta_store_ == nullptr || 
            extra_mem_threshold_ > memory_budget_ - min_block_cache_size_) {
        return;
    }
    struct timeval tv;
    gettimeofday(&tv, 0);
    if (tv.tv_sec - tv_tune_cache_.tv_sec >= 1) {
        tv_tune_cache_ = tv;
        // kd cache
        uint64_t kdcache_mem = min(kd_cache_->getUsage(), max_kd_cache_size_);
        // bucket table
        uint64_t bucket_mem = delta_store_->getNumOfBuckets() * 10 * 1024;

        if (kdcache_mem + bucket_mem > extra_mem_threshold_) {
            rocks_block_cache_->SetCapacity(memory_budget_ - extra_mem_threshold_);
            extra_mem_threshold_ += extra_mem_step_;
            debug_error("set rocksdb block cache capacity = %.2lf MiB, "
                    "extra %.2lf MiB (cache %.2lf + bucket table %.2lf)\n", 
                    (memory_budget_ - extra_mem_threshold_) / 1024.0 / 1024, 
                    (kdcache_mem + bucket_mem) / 1024.0 / 1024, 
                    kdcache_mem / 1024.0 / 1024, 
                    bucket_mem / 1024.0 / 1024);
            uint64_t rss = getRss();
            debug_error("rss = %.2lf (-block: %.2lf) MiB, bucket num %lu\n", 
                rss / 1024.0 / 1024, 
                (rss * 1024 - rocks_block_cache_->GetUsage()) / 1024.0 / 1024,
                bucket_mem / 10 / 1024);
        }
    }
}

void KDSep::GetRocksDBProperty(const string& property, string* str) {
    lsmTreeInterface_.GetRocksDBProperty(property, str);
}

} // namespace KDSEP_NAMESPACE
