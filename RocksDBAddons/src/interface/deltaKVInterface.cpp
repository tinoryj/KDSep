#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // request merge operation when the value is found
    debug_trace("Full merge for key = %s, value size = %lu, content = %s\n", key.ToString().c_str(), existing_value->size(), existing_value->ToString().c_str());
    string newValueIndexStr;
    string filteredOperandStr;
    int headerSize = sizeof(internalValueType), valueIndexSize = sizeof(externalIndexInfo);

    internalValueType existingValueType;
    internalValueType outputValueType;
    memcpy(&existingValueType, existing_value->ToString().c_str(), headerSize);

    int operandIndex = 0;
    bool findUpdatedValueIndex = false;
    vector<string> leadingRawDeltas;
    string operand;

    // Output format:
    // If value is separated:    [internalValueType] [externalIndexInfo] [appended deltas if any]
    // If value is not separated:[internalValueType] [   raw   value   ] [appended deltas if any]

    // Step 1. Scan the operand list
    for (auto operandListIt : operand_list) {
        uint64_t deltaOffset = 0;

        while (deltaOffset < operandListIt.size()) {
            internalValueType tempInternalValueType;
            memcpy(&tempInternalValueType, operandListIt.c_str() + deltaOffset, headerSize);

            // extract the oprand
            if (tempInternalValueType.mergeFlag_ == true) {
                // index update
                assert(tempInternalValueType.valueSeparatedFlag_ == true && deltaOffset + headerSize + valueIndexSize <= operandListIt.size());
                operand.assign(operandListIt.c_str() + deltaOffset, headerSize + valueIndexSize);
                deltaOffset += headerSize + valueIndexSize;
            } else {
                if (tempInternalValueType.valueSeparatedFlag_ == false) {
                    // raw delta
                    assert(deltaOffset + headerSize + tempInternalValueType.rawValueSize_ <= operandListIt.size());
                    operand.assign(operandListIt.c_str() + deltaOffset, headerSize + tempInternalValueType.rawValueSize_);
                    deltaOffset += headerSize + tempInternalValueType.rawValueSize_;
                } else {
                    // separated delta
                    assert(deltaOffset + headerSize <= operandListIt.size());
                    operand.assign(operandListIt.c_str() + deltaOffset, headerSize);
                    deltaOffset += headerSize;
                }
            }

            // Find a delta from normal merge operator
            if (tempInternalValueType.mergeFlag_ == false) {
                // Check whether we need to collect the raw deltas for immediate merging.
                // 1. The value should be not separated (i.e., should be raw value)
                // 2. The previous deltas (if exists) should also be raw deltas
                // 3. The current deltas should be a raw delta
                if (existingValueType.valueSeparatedFlag_ == false && (int)leadingRawDeltas.size() == operandIndex && tempInternalValueType.valueSeparatedFlag_ == false) {
                    // Extract the raw delta, prepare for field updates
                    leadingRawDeltas.push_back(operand.substr(headerSize));
                } else {
                    // Append to the string
                    filteredOperandStr.append(operand);
                }
            } else { // Find a delta from vLog GC
                if (existingValueType.valueSeparatedFlag_ == false) {
                    debug_error("[ERROR] updating a value index but the value is not separated! key [%s]\n", key.ToString().c_str());
                    assert(0);
                }
                findUpdatedValueIndex = true;
                newValueIndexStr.assign(operand);
            }
            operandIndex++;
        }
    }

    // Step 2. Check index updates and output
    //         output format     [internalValueType] [externalIndexInfo] [appended deltas]
    if (findUpdatedValueIndex == true) {
        memcpy(&outputValueType, newValueIndexStr.c_str(), headerSize);
        if (filteredOperandStr.empty()) {
            outputValueType.mergeFlag_ = false;
            new_value->assign(std::string((char*)(&outputValueType), headerSize)); // internalValueType
            new_value->append(newValueIndexStr.substr(headerSize)); // externalIndexInfo
        } else {
            new_value->assign(newValueIndexStr); // internalValueType + externalIndexInfo
        }
        new_value->append(filteredOperandStr);
        return true;
    }

    // Step 3.1 Prepare the header
    outputValueType = existingValueType;
    if (!filteredOperandStr.empty()) {
        outputValueType.mergeFlag_ = true;
    }

    // Step 3.2 Prepare the value, if some merges on raw deltas can be performed
    string mergedValueWithoutValueType;
    string rawValue(existing_value->data_ + headerSize, existing_value->size_ - headerSize);
    if (!leadingRawDeltas.empty()) {
        FullMergeFieldUpdates(rawValue, leadingRawDeltas, &mergedValueWithoutValueType);
        if (mergedValueWithoutValueType.size() != existingValueType.rawValueSize_) {
            debug_error("[ERROR] value size differs after merging: %lu v.rocksDBStatus. %u\n", mergedValueWithoutValueType.size(), existingValueType.rawValueSize_);
        }
    } else {
        mergedValueWithoutValueType.assign(rawValue);
    }

    // Step 3.3 Prepare the following deltas (whether raw or not raw)
    //          Already prepared, don't need to do anything

    // Step 3.4 Append everything

    new_value->assign(string((char*)&outputValueType, headerSize));
    new_value->append(mergedValueWithoutValueType);
    new_value->append(filteredOperandStr);

    return true;
};

bool RocksDBInternalMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    string operandStr;
    operandStr.assign(left_operand.ToString());
    operandStr.append(right_operand.ToString());
    auto deltaOffset = 0;
    string newValueIndexStr = "";
    vector<pair<internalValueType, string>> batchedOperandVec;
    bool findRawDeltaFlag = false;
    while (deltaOffset < operandStr.size()) {
        internalValueType tempInternalValueType;
        memcpy(&tempInternalValueType, operandStr.c_str() + deltaOffset, sizeof(internalValueType));
        // extract the oprand
        if (tempInternalValueType.mergeFlag_ == true) {
            // index update
            assert(tempInternalValueType.valueSeparatedFlag_ == true && (deltaOffset + sizeof(internalValueType) + sizeof(externalIndexInfo)) <= operandStr.size());
            newValueIndexStr.assign(operandStr.substr(deltaOffset, sizeof(internalValueType) + sizeof(externalIndexInfo)));
            deltaOffset += (sizeof(internalValueType) + sizeof(externalIndexInfo));
            batchedOperandVec.clear(); // clear since new value
        } else {
            if (tempInternalValueType.valueSeparatedFlag_ == false) {
                // raw delta
                assert(deltaOffset + sizeof(internalValueType) + tempInternalValueType.rawValueSize_ <= operandStr.size());
                batchedOperandVec.push_back(make_pair(tempInternalValueType, operandStr.substr(deltaOffset + sizeof(internalValueType), tempInternalValueType.rawValueSize_)));
                deltaOffset += (sizeof(internalValueType) + tempInternalValueType.rawValueSize_);
                findRawDeltaFlag = true;
            } else {
                // separated delta
                assert(deltaOffset + sizeof(internalValueType) <= operandStr.size());
                batchedOperandVec.push_back(make_pair(tempInternalValueType, ""));
                deltaOffset += sizeof(internalValueType);
            }
        }
    }
    if (findRawDeltaFlag == true) {
        string finalDeltaListStr = "";
        PartialMergeFieldUpdates(batchedOperandVec, finalDeltaListStr);
        if (newValueIndexStr.size() > 0) {
            new_value->assign(newValueIndexStr);
            new_value->append(finalDeltaListStr);
        } else {
            new_value->assign(finalDeltaListStr);
        }
    } else {
        string finalDeltaListStr;
        for (auto i = 0; i < batchedOperandVec.size(); i++) {
            if (batchedOperandVec[i].first.valueSeparatedFlag_ == true) {
                char buffer[sizeof(internalValueType)];
                memcpy(buffer, &batchedOperandVec[i].first, sizeof(internalValueType));
                string headerStr(buffer, sizeof(internalValueType));
                finalDeltaListStr.append(headerStr);
            } else {
                char buffer[sizeof(internalValueType) + batchedOperandVec[i].first.rawValueSize_];
                memcpy(buffer, &batchedOperandVec[i].first, sizeof(internalValueType));
                memcpy(buffer + sizeof(internalValueType), batchedOperandVec[i].second.c_str(), batchedOperandVec[i].first.rawValueSize_);
                string contentStr(buffer, sizeof(internalValueType) + batchedOperandVec[i].first.rawValueSize_);
                finalDeltaListStr.append(contentStr);
            }
        }
        if (newValueIndexStr.size() > 0) {
            new_value->assign(newValueIndexStr);
            new_value->append(finalDeltaListStr);
        } else {
            new_value->assign(finalDeltaListStr);
        }
    }
    return true;
};

bool RocksDBInternalMergeOperator::PartialMergeFieldUpdates(vector<pair<internalValueType, string>> batchedOperandVec, string& finalDeltaListStr) const
{
    unordered_set<int> findIndexSet;
    stack<pair<internalValueType, string>> finalResultStack;
    for (auto i = batchedOperandVec.size() - 1; i != 0; i--) {
        if (batchedOperandVec[i].first.valueSeparatedFlag_ == false) {
            int index = stoi(batchedOperandVec[i].second.substr(0, batchedOperandVec[i].second.find(",")));
            if (findIndexSet.find(index) == findIndexSet.end()) {
                findIndexSet.insert(index);
                finalResultStack.push(batchedOperandVec[i]);
            }
        } else {
            finalResultStack.push(batchedOperandVec[i]);
        }
    }
    debug_info("PartialMerge raw delta number = %lu, valid delta number = %lu", batchedOperandVec.size(), finalResultStack.size());
    while (finalResultStack.empty() == false) {
        if (finalResultStack.top().first.valueSeparatedFlag_ == true) {
            char buffer[sizeof(internalValueType)];
            memcpy(buffer, &finalResultStack.top().first, sizeof(internalValueType));
            string headerStr(buffer, sizeof(internalValueType));
            finalDeltaListStr.append(headerStr);
        } else {
            char buffer[sizeof(internalValueType) + finalResultStack.top().first.rawValueSize_];
            memcpy(buffer, &finalResultStack.top().first, sizeof(internalValueType));
            memcpy(buffer + sizeof(internalValueType), finalResultStack.top().second.c_str(), finalResultStack.top().first.rawValueSize_);
            string contentStr(buffer, sizeof(internalValueType) + finalResultStack.top().first.rawValueSize_);
            finalDeltaListStr.append(contentStr);
        }
        finalResultStack.pop();
    }
    return true;
}

bool RocksDBInternalMergeOperator::FullMergeFieldUpdates(string rawValue, vector<string>& operandList, string* finalValue) const
{
    vector<string> rawValueFieldsVec;

    size_t pos = 0;
    string token;
    string delimiter = ",";
    while ((pos = rawValue.find(delimiter)) != std::string::npos) {
        token = rawValue.substr(0, pos);
        rawValueFieldsVec.push_back(token);
        rawValue.erase(0, pos + delimiter.length());
    }
    rawValueFieldsVec.push_back(token);

    for (auto& q : operandList) {
        string indexStr = q.substr(0, q.find(","));
        int index = stoi(indexStr);
        string updateContentStr = q.substr(q.find(",") + 1, q.size());
        debug_trace("merge operand = %s, current index =  %d, content = %s, rawValue at indx = %s\n", q.c_str(), index, updateContentStr.c_str(), rawValueFieldsVec[index].c_str());
        rawValueFieldsVec[index].assign(updateContentStr);
    }

    string temp;
    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        finalValue->append(rawValueFieldsVec[i]);
        finalValue->append(",");
    }
    finalValue->append(rawValueFieldsVec[rawValueFieldsVec.size() - 1]);
    return true;
}

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
    cerr << "[DeltaKV Interface] Try delete Read Cache" << endl;
    if (enableKeyValueCache_ == true) {
        delete keyToValueListCache_;
    }
    cerr << "[DeltaKV Interface] Try delete HashStore" << endl;
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        delete HashStoreInterfaceObjPtr_;
        // delete related object pointers
        delete hashStoreFileManagerPtr_;
        delete hashStoreFileOperatorPtr_;
    }
    cerr << "[DeltaKV Interface] Try delete IndexStore" << endl;
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        delete IndexStoreInterfaceObjPtr_;
        // delete related object pointers
    }
    delete objectPairMemPool_;
    cerr << "[DeltaKV Interface] Try delete RocksDB" << endl;
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
    // object mem pool
    objectPairMemPool_ = new KeyValueMemPool(options.deltaStore_mem_pool_object_number_, options.deltaStore_mem_pool_object_size_);
    // Rest merge function if delta/value separation enabled
    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocksdbRawOptions_.merge_operator.reset(new RocksDBInternalMergeOperator); // reset
    }
    deltaKVMergeOperatorPtr_ = options.deltaKV_merge_operation_ptr;
    if (options.rocksdb_sync_put) {
        internalWriteOption_.sync = true;
    } else {
        internalWriteOption_.sync = false;
    }
    if (options.rocksdb_sync_merge) {
        internalMergeOption_.sync = true;
    } else {
        internalMergeOption_.sync = false;
    }
    rocksdb::Status rocksDBStatus = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Can't open underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }
    if (options.enable_key_value_cache_ == true && options.key_value_cache_object_number_ != 0) {
        enableKeyValueCache_ = true;
        keyToValueListCache_ = new AppendAbleLRUCache<string, string>(options.key_value_cache_object_number_);
    } else {
        enableKeyValueCache_ = false;
    }
    // Create objects
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        isValueStoreInUseFlag_ = true;
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
        valueExtractSize_ = IndexStoreInterfaceObjPtr_->getExtractSizeThreshold();
    }
    if (options.enable_batched_operations_ == true) {
        writeBatchMapForSearch_[0] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        writeBatchMapForSearch_[1] = new unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>;
        notifyWriteBatchMQ_ = new messageQueue<unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        thList_.push_back(th);
        isBatchedOperationsWithBufferInUse_ = true;
        maxBatchOperationBeforeCommitNumber_ = options.batched_operations_number_;
        if (options.internalRocksDBBatchedOperation_ == true) {
            useInternalRocksDBBatchOperationsFlag_ = true;
        } else {
            useInternalRocksDBBatchOperationsFlag_ = false;
        }
    }

    if (options.enable_write_back_optimization_ == true) {
        enableWriteBackOperationsFlag_ = true;
        writeBackWhenReadDeltaNumerThreshold_ = options.deltaStore_write_back_during_reads_threshold;
        writeBackOperationsQueue_ = new messageQueue<writeBackObjectStruct*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processWriteBackOperationsWorker, this));
        thList_.push_back(th);
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
            th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::notifyOperationWorkerThread, hashStoreFileOperatorPtr_));
            thList_.push_back(th);
            for (auto threadID = 0; threadID < options.deltaStore_op_worker_thread_number_limit_; threadID++) {
                th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_, threadID));
                thList_.push_back(th);
            }
        } else {
            debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
        }
    }

    // process runnning mode
    if (options.enable_valueStore && options.enable_deltaStore) {
        deltaKVRunningMode_ = kBothValueAndDeltaLog;
        cerr << "deltaKVRunningMode_ = kBothValueAndDeltaLog" << endl;
    } else if (options.enable_valueStore && !options.enable_deltaStore) {
        deltaKVRunningMode_ = kOnlyValueLog;
        cerr << "deltaKVRunningMode_ = kOnlyValueLog" << endl;
    } else if (!options.enable_valueStore && options.enable_deltaStore) {
        deltaKVRunningMode_ = kOnlyDeltaLog;
        cerr << "deltaKVRunningMode_ = kOnlyDeltaLog" << endl;
    } else if (!options.enable_valueStore && !options.enable_deltaStore) {
        deltaKVRunningMode_ = kPlainRocksDB;
        cerr << "deltaKVRunningMode_ = kPlainRocksDB" << endl;
    }

    if (options.enable_batched_operations_) {
        switch (deltaKVRunningMode_) {
        case kBothValueAndDeltaLog:
            deltaKVRunningMode_ = kBatchedWithBothValueAndDeltaLog;
            cerr << "deltaKVRunningMode_ = kBatchedWithBothValueAndDeltaLog" << endl;
            break;
        case kOnlyDeltaLog:
            deltaKVRunningMode_ = kBatchedWithOnlyDeltaLog;
            cerr << "deltaKVRunningMode_ = kBatchedWithOnlyDeltaLog" << endl;
            break;
        case kOnlyValueLog:
            deltaKVRunningMode_ = kBatchedWithOnlyValueLog;
            cerr << "deltaKVRunningMode_ = kBatchedWithOnlyValueLog" << endl;
            break;
        case kPlainRocksDB:
            deltaKVRunningMode_ = kBatchedWithPlainRocksDB;
            cerr << "deltaKVRunningMode_ = kBatchedWithPlainRocksDB" << endl;
            break;
        default:
            debug_error("Unsupported DeltaKV running mode = %d\n", deltaKVRunningMode_);
            break;
        }
    }
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
        writeBackOperationsQueue_->done_ = true;
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
        notifyWriteBatchMQ_->done_ = true;
        while (writeBatchOperationWorkExitFlag == false) {
            asm volatile("");
        }
        cerr << "\tFlush write batch done" << endl;
    }
    cerr << "[DeltaKV Close DB] Set job done" << endl;
    if (isDeltaStoreInUseFlag_ == true) {
        HashStoreInterfaceObjPtr_->setJobDone();
        cerr << "\tHashStore set job done" << endl;
    }
    cerr << "[DeltaKV Close DB] Delete existing threads" << endl;
    deleteExistingThreads();
    cerr << "\tJoin all existing threads done" << endl;
    return true;
}

bool DeltaKV::PutWithPlainRocksDB(mempoolHandler_t objectPairMemPoolHandler)
{
    rocksdb::Status rocksDBStatus;
    rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
    rocksdb::Slice newValue(objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::MergeWithPlainRocksDB(mempoolHandler_t objectPairMemPoolHandler)
{
    rocksdb::Status rocksDBStatus;
    rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
    rocksdb::Slice newValue(objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with merge value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::GetWithPlainRocksDB(const string& key, string* value)
{
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, value), StatsType::DELTAKV_GET_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::PutWithOnlyValueStore(mempoolHandler_t objectPairMemPoolHandler)
{
    if (objectPairMemPoolHandler.valueSize_ >= valueExtractSize_) {
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(objectPairMemPoolHandler, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            return true;
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
        currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
        currentInternalValueType.valueSeparatedFlag_ = false;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
        rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::MergeWithOnlyValueStore(mempoolHandler_t objectPairMemPoolHandler)
{
    // also need internal value header since value store GC may update fake header as merge
    char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
    currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
    currentInternalValueType.valueSeparatedFlag_ = false;
    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
    memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
    rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR]  Merge underlying rocksdb with interanl value header fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::GetWithOnlyValueStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    string internalValueStr;
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr), StatsType::DELTAKV_GET_ROCKSDB);
    // check value status
    internalValueType tempInternalValueHeader;
    memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
    string rawValueStr;
    if (tempInternalValueHeader.mergeFlag_ == true) {
        // get deltas from delta store
        vector<pair<bool, string>> deltaInfoVec;
        externalIndexInfo newExternalIndexInfo;
        bool findNewValueIndexFlag = false;
        if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo, maxSequenceNumber);
        } else {
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo, maxSequenceNumber);
        }
        if (findNewValueIndexFlag == true) {
            string tempReadValueStr;
            IndexStoreInterfaceObjPtr_->get(key, newExternalIndexInfo, &tempReadValueStr);
            rawValueStr.assign(tempReadValueStr);
            debug_error("[ERROR] Assigned new value by new external index, value = %s\n", rawValueStr.c_str());
            assert(0);
        } else {
            if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
                // get value from value store first
                externalIndexInfo tempReadExternalStorageInfo;
                memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
                string tempReadValueStr;
                STAT_PROCESS(IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr), StatsType::DELTAKV_GET_INDEXSTORE);
                rawValueStr.assign(tempReadValueStr);
            } else {
                char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
                memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
                string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
                rawValueStr.assign(internalRawValueStr);
            }
        }
        vector<string> finalDeltaOperatorsVec;
        for (auto i = 0; i < deltaInfoVec.size(); i++) {
            if (deltaInfoVec[i].first == true) {
                debug_error("[ERROR] Request external deltaStore when no KD separation enabled (Internal value error), index = %d\n", i);
                return false;
            } else {
                finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
            }
        }
        if (deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value) != true) {
            debug_error("[ERROR] DeltaKV merge operation fault, rawValueStr = %s, operand number = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
            return false;
        } else {
            return true;
        }
    } else {
        maxSequenceNumber = tempInternalValueHeader.sequenceNumber_;
        if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
            // get value from value store first
            externalIndexInfo tempReadExternalStorageInfo;
            memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
            string tempReadValueStr;
            STAT_PROCESS(IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr), StatsType::DELTAKV_GET_INDEXSTORE);
            rawValueStr.assign(tempReadValueStr);
        } else {
            char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
            memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
            string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
            rawValueStr.assign(internalRawValueStr);
        }
        value->assign(rawValueStr);
        return true;
    }
}

bool DeltaKV::PutWithOnlyDeltaStore(mempoolHandler_t objectPairMemPoolHandler)
{
    // for merge flag check, add internal value header to raw value
    char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.valueSeparatedFlag_ = false;
    currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
    currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
    memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
    string newWriteValueStr(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);

    rocksdb::Status rocksDBStatus;
    string keyStr(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, keyStr, newWriteValueStr), StatsType::DELTAKV_PUT_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with added value header fault, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
        return false;
    } else {
        // need to append anchor to delta store
        bool addAnchorStatus;
        STAT_PROCESS(addAnchorStatus = HashStoreInterfaceObjPtr_->put(objectPairMemPoolHandler), StatsType::DELTAKV_PUT_HASHSTORE);
        if (addAnchorStatus == true) {
            return true;
        } else {
            debug_error("[ERROR]  Append anchor to delta store fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    }
}

bool DeltaKV::MergeWithOnlyDeltaStore(mempoolHandler_t objectPairMemPoolHandler)
{
    if (objectPairMemPoolHandler.valueSize_ >= deltaExtractSize_) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(objectPairMemPoolHandler), StatsType::DELTAKV_MERGE_HASHSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
            currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            rocksdb::Status rocksDBStatus;
            rocksdb::Slice keySlice(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
            rocksdb::Slice newWriteValueSlice(writeInternalValueBuffer, sizeof(internalValueType));
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, keySlice, newWriteValueSlice), StatsType::DELTAKV_MERGE_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external value type info, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
                return false;
            } else {
                return true;
            }
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.valueSeparatedFlag_ = false;
        currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
        currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
        rocksdb::Status rocksDBStatus;
        rocksdb::Slice keySlice(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        rocksdb::Slice newWriteValueSlice(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, keySlice, newWriteValueSlice), StatsType::DELTAKV_MERGE_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::GetWithOnlyDeltaStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    string internalValueStr;
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr), StatsType::DELTAKV_GET_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Read underlying rocksdb fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        internalValueType tempInternalValueHeader;
        memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
        char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
        memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
        string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
        if (tempInternalValueHeader.mergeFlag_ == true) {
            // get deltas from delta store
            vector<pair<bool, string>> deltaInfoVec;
            vector<internalValueType> deltaInfoHeaderVec;
            externalIndexInfo newExternalIndexInfo;
            bool findNewValueIndexFlag = false;
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, deltaInfoHeaderVec, findNewValueIndexFlag, newExternalIndexInfo, maxSequenceNumber);
            if (findNewValueIndexFlag == true) {
                debug_error("[ERROR] In only delta store, should not extract exteranl index, flag = %d\n", findNewValueIndexFlag);
            }
            debug_trace("read deltaInfoVec from LSM-tree size = %lu\n", deltaInfoVec.size());
            vector<string>* deltaValueFromExternalStoreVec = new vector<string>;
            vector<hashStoreRecordHeader>* deltaRecordFromExternalStoreVec = new vector<hashStoreRecordHeader>;
            bool ret;
            STAT_PROCESS(ret = HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec, deltaRecordFromExternalStoreVec), StatsType::DELTAKV_GET_HASHSTORE);
            if (ret != true) {
                debug_trace("Read external deltaStore fault, key = %s\n", key.c_str());
                delete deltaValueFromExternalStoreVec;
                delete deltaRecordFromExternalStoreVec;
                return false;
            } else {
                vector<string> finalDeltaOperatorsVec;
                auto index = 0;
                debug_trace("Read from deltaStore object number = %lu, target delta number in RocksDB = %lu\n", deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    if (deltaInfoVec[i].first == true) { // separated
                        if (index >= deltaValueFromExternalStoreVec->size()) {
                            debug_error("[ERROR] Read external deltaStore number mismatch with requested number (may overflow), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                            delete deltaValueFromExternalStoreVec;
                            delete deltaRecordFromExternalStoreVec;
                            exit(1);
                            return false;
                        }
                        finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                        index++;
                    } else {
                        finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                    }
                }

                if (index != deltaValueFromExternalStoreVec->size()) {
                    debug_error("[ERROR] Read external deltaStore number mismatch with requested number (Inconsistent), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                    for (auto internalIt = deltaRecordFromExternalStoreVec->begin(); internalIt != deltaRecordFromExternalStoreVec->end(); internalIt++) {
                        debug_error("[ERROR] sequence number in hash store = %u\n", internalIt->sequence_number_);
                    }
                    for (auto internalIt = deltaInfoHeaderVec.begin(); internalIt != deltaInfoHeaderVec.end(); internalIt++) {
                        debug_error("[ERROR] sequence number in rocksdb = %u\n", internalIt->sequenceNumber_);
                    }
                    delete deltaValueFromExternalStoreVec;
                    delete deltaRecordFromExternalStoreVec;
                    exit(1);
                    return false;
                } else {
                    debug_trace("Start DeltaKV merge operation, internalRawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", internalRawValueStr.c_str(), finalDeltaOperatorsVec.size());
                    bool mergeOperationStatus = deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value);
                    if (mergeOperationStatus == true) {
                        if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                            writeBackObjectStruct* newPair = new writeBackObjectStruct(key, "", 0);
                            writeBackOperationsQueue_->push(newPair);
                        }
                        delete deltaValueFromExternalStoreVec;
                        delete deltaRecordFromExternalStoreVec;
                        return true;
                    } else {
                        debug_error("[ERROR] Perform merge operation fail, key = %s\n", key.c_str());
                        delete deltaValueFromExternalStoreVec;
                        delete deltaRecordFromExternalStoreVec;
                        return false;
                    }
                }
            }
        } else {
            maxSequenceNumber = tempInternalValueHeader.sequenceNumber_;
            value->assign(internalRawValueStr);
            return true;
        }
    }
}

bool DeltaKV::PutWithValueAndDeltaStore(mempoolHandler_t objectPairMemPoolHandler)
{
    if (objectPairMemPoolHandler.valueSize_ >= valueExtractSize_) {
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(objectPairMemPoolHandler, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            bool updateDeltaStoreWithAnchorFlagstatus;
            STAT_PROCESS(updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(objectPairMemPoolHandler), StatsType::DELTAKV_PUT_HASHSTORE);
            if (updateDeltaStoreWithAnchorFlagstatus == true) {
                return true;
            } else {
                debug_error("[ERROR] Update anchor to current key fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
                return false;
            }
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
        currentInternalValueType.valueSeparatedFlag_ = true;
        currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
        rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
            return false;
        } else {
            bool updateDeltaStoreWithAnchorFlagStatus;
            STAT_PROCESS(updateDeltaStoreWithAnchorFlagStatus = HashStoreInterfaceObjPtr_->put(objectPairMemPoolHandler), StatsType::DELTAKV_PUT_HASHSTORE);
            if (updateDeltaStoreWithAnchorFlagStatus == true) {
                return true;
            } else {
                debug_error("[ERROR] Update anchor to current key fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
                return false;
            }
        }
    }
}

bool DeltaKV::MergeWithValueAndDeltaStore(mempoolHandler_t objectPairMemPoolHandler)
{
    if (objectPairMemPoolHandler.valueSize_ >= deltaExtractSize_) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(objectPairMemPoolHandler), StatsType::DELTAKV_MERGE_HASHSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
            currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
            rocksdb::Status rocksDBStatus;
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
                return false;
            } else {
                return true;
            }
        } else {
            debug_error("[ERROR] Write value to external storage faul, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.valueSeparatedFlag_ = false;
        currentInternalValueType.rawValueSize_ = objectPairMemPoolHandler.valueSize_;
        currentInternalValueType.sequenceNumber_ = objectPairMemPoolHandler.sequenceNumber_;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
        rocksdb::Slice newKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + objectPairMemPoolHandler.valueSize_);
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_, rocksDBStatus.ToString().c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::GetWithValueAndDeltaStore(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    string internalValueStr;
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr), StatsType::DELTAKV_GET_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Read underlying rocksdb fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        internalValueType tempInternalValueHeader;
        memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
        string rawValueStr;
        if (tempInternalValueHeader.mergeFlag_ == true) {
            // get deltas from delta store
            vector<pair<bool, string>> deltaInfoVec;
            externalIndexInfo newExternalIndexInfo;
            bool findNewValueIndexFlag = false;
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo, maxSequenceNumber);
            if (findNewValueIndexFlag == true) {
                string tempReadValueStr;
                IndexStoreInterfaceObjPtr_->get(key, newExternalIndexInfo, &tempReadValueStr);
                rawValueStr.assign(tempReadValueStr);
                debug_error("[ERROR] Assigned new value by new external index, value = %s\n", rawValueStr.c_str());
                assert(0);
            } else {
                if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
                    // read value from value store
                    externalIndexInfo tempReadExternalStorageInfo;
                    memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
                    string tempReadValueStr;
                    STAT_PROCESS(IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr), StatsType::DELTAKV_GET_INDEXSTORE);
                    rawValueStr.assign(tempReadValueStr);
                } else {
                    char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
                    memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
                    string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
                    rawValueStr.assign(internalRawValueStr);
                }
            }
            bool isAnyDeltasAreExtratedFlag = false;
            for (auto it : deltaInfoVec) {
                if (it.first == true) {
                    isAnyDeltasAreExtratedFlag = true;
                }
            }
            if (isAnyDeltasAreExtratedFlag == true) {
                // should read external delta store
                vector<string>* deltaValueFromExternalStoreVec = new vector<string>;
                bool ret;
                STAT_PROCESS(ret = HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec), StatsType::DELTAKV_GET_HASHSTORE);
                if (ret != true) {
                    debug_error("[ERROR] Read external deltaStore fault, key = %s\n", key.c_str());
                    delete deltaValueFromExternalStoreVec;
                    return false;
                } else {
                    vector<string> finalDeltaOperatorsVec;
                    auto index = 0;
                    debug_trace("Read from deltaStore object number = %lu, target delta number in RocksDB = %lu\n", deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                    for (auto i = 0; i < deltaInfoVec.size(); i++) {
                        if (deltaInfoVec[i].first == true) {
                            if (index >= deltaValueFromExternalStoreVec->size()) {
                                debug_error("[ERROR] Read external deltaStore number mismatch with requested number (may overflow), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                                delete deltaValueFromExternalStoreVec;
                                exit(1);
                                return false;
                            }
                            finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                            index++;
                        } else {
                            finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                        }
                    }

                    if (index != deltaValueFromExternalStoreVec->size()) {
                        debug_error("[ERROR] Read external deltaStore number mismatch with requested number (Inconsistent), key = %s, request delta number in HashStore = %d, delta number get from HashStore = %lu, total read delta number from RocksDB = %lu\n", key.c_str(), index, deltaValueFromExternalStoreVec->size(), deltaInfoVec.size());
                        delete deltaValueFromExternalStoreVec;
                        exit(1);
                        return false;
                    } else {
                        debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                        deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value);
                        if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                            writeBackObjectStruct* newPair = new writeBackObjectStruct(key, "", 0);
                            writeBackOperationsQueue_->push(newPair);
                        }
                        delete deltaValueFromExternalStoreVec;
                        return true;
                    }
                }
            } else {
                // all deltas are stored internal
                vector<string> finalDeltaOperatorsVec;
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                }
                debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value);
                if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                    writeBackObjectStruct* newPair = new writeBackObjectStruct(key, "", 0);
                    writeBackOperationsQueue_->push(newPair);
                }
                return true;
            }
        } else {
            maxSequenceNumber = tempInternalValueHeader.sequenceNumber_;
            if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
                // read value from value store
                externalIndexInfo tempReadExternalStorageInfo;
                memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
                string tempReadValueStr;
                STAT_PROCESS(IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr), StatsType::DELTAKV_GET_INDEXSTORE);
                rawValueStr.assign(tempReadValueStr);
            } else {
                char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
                memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
                string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
                rawValueStr.assign(internalRawValueStr);
            }
            value->assign(rawValueStr);
            return true;
        }
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
    // insert to cache if is value update
    if (enableKeyValueCache_ == true) {
        string cacheKey = key;
        if (keyToValueListCache_->existsInCache(cacheKey) == true) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            keyToValueListCache_->getFromCache(cacheKey).assign(value);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_NEW, tv);
        }
    }
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    mempoolHandler_t objectPairMemPoolHandler;
    objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, true, objectPairMemPoolHandler);
    bool putOperationStatus = true;
    bool shouldDeleteMemPoolHandler = false;
    switch (deltaKVRunningMode_) {
    case kBatchedWithPlainRocksDB:
    case kBatchedWithOnlyValueLog:
    case kBatchedWithOnlyDeltaLog:
    case kBatchedWithBothValueAndDeltaLog:
        if (PutWithWriteBatch(objectPairMemPoolHandler) == false) {
            putOperationStatus = false;
        } else {
            putOperationStatus = true;
        }
        break;
    case kBothValueAndDeltaLog:
        if (PutWithValueAndDeltaStore(objectPairMemPoolHandler) == false) {
            putOperationStatus = false;
        } else {
            putOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kOnlyValueLog:
        if (PutWithOnlyValueStore(objectPairMemPoolHandler) == false) {
            putOperationStatus = false;
        } else {
            putOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kOnlyDeltaLog:
        if (PutWithOnlyDeltaStore(objectPairMemPoolHandler) == false) {
            putOperationStatus = false;
        } else {
            putOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kPlainRocksDB:
        if (PutWithPlainRocksDB(objectPairMemPoolHandler) == false) {
            putOperationStatus = false;
        } else {
            putOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        putOperationStatus = false;
        shouldDeleteMemPoolHandler = true;
        break;
    }
    if (shouldDeleteMemPoolHandler == true) {
        objectPairMemPool_->eraseContentFromMemPool(objectPairMemPoolHandler);
    }
    if (putOperationStatus == true) {
        return true;
    } else {
        return false;
    }
}

bool DeltaKV::Get(const string& key, string* value)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
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
        std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_WAIT_BUFFER, tvAll);
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
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
                return true;
            }
            struct timeval tv0;
            gettimeofday(&tv0, 0);
            string newValueStr;
            bool findNewValueFlag = false;
            for (auto queueIt : mapIt->second) {
                if (queueIt.first == kPutOp) {
                    newValueStr.assign(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    tempNewMergeOperatorsVec.clear();
                    findNewValueFlag = true;
                } else {
                    string currentValue(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    tempNewMergeOperatorsVec.push_back(currentValue);
                }
            }
            if (findNewValueFlag == true) {
                deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_MERGE, tv0);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
                return true;
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_MERGE_ALL, tv0);
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_NO_WAIT_BUFFER, tv);
    }
    struct timeval tv;
    gettimeofday(&tv, 0);
    uint32_t maxSequenceNumberPlaceHolder;
    bool ret;
    switch (deltaKVRunningMode_) {
    case kBatchedWithBothValueAndDeltaLog:
    case kBothValueAndDeltaLog:
        ret = GetWithValueAndDeltaStore(key, value, maxSequenceNumberPlaceHolder, false);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_STORE, tv);
        if (ret == false) {
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
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
        break;
    case kBatchedWithOnlyValueLog:
    case kOnlyValueLog:
        ret = GetWithOnlyValueStore(key, value, maxSequenceNumberPlaceHolder, false);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_STORE, tv);
        if (ret == false) {
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
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
        break;
    case kBatchedWithOnlyDeltaLog:
    case kOnlyDeltaLog:
        ret = GetWithOnlyDeltaStore(key, value, maxSequenceNumberPlaceHolder, false);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_STORE, tv);
        if (ret == false) {
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
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
        break;
    case kBatchedWithPlainRocksDB:
    case kPlainRocksDB:
        ret = GetWithPlainRocksDB(key, value);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_STORE, tv);
        if (ret == true) {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
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
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ, tvAll);
        return ret;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        return false;
        break;
    }
    return false;
}

bool DeltaKV::Merge(const string& key, const string& value)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
    if (enableKeyValueCache_ == true) {
        string cacheKey = key;
        if (keyToValueListCache_->existsInCache(cacheKey) == true) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            string oldValue = keyToValueListCache_->getFromCache(cacheKey);
            string finalValue;
            vector<string> operandListForCacheUpdate;
            operandListForCacheUpdate.push_back(value);
            deltaKVMergeOperatorPtr_->Merge(oldValue, operandListForCacheUpdate, &finalValue);
            keyToValueListCache_->getFromCache(cacheKey).assign(finalValue);
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_MERGE, tv);
        }
    }
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    mempoolHandler_t objectPairMemPoolHandler;
    objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, value, currentSequenceNumber, false, objectPairMemPoolHandler);
    bool shouldDeleteMemPoolHandler = false;
    bool mergeOperationStatus = false;
    switch (deltaKVRunningMode_) {
    case kBatchedWithPlainRocksDB:
    case kBatchedWithOnlyValueLog:
    case kBatchedWithOnlyDeltaLog:
    case kBatchedWithBothValueAndDeltaLog:
        if (MergeWithWriteBatch(objectPairMemPoolHandler) == false) {
            mergeOperationStatus = false;
        } else {
            mergeOperationStatus = true;
        }
        break;
    case kBothValueAndDeltaLog:
        if (MergeWithValueAndDeltaStore(objectPairMemPoolHandler) == false) {
            mergeOperationStatus = false;
        } else {
            mergeOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kOnlyValueLog:
        if (MergeWithOnlyValueStore(objectPairMemPoolHandler) == false) {
            mergeOperationStatus = false;
        } else {
            mergeOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kOnlyDeltaLog:
        if (MergeWithOnlyDeltaStore(objectPairMemPoolHandler) == false) {
            mergeOperationStatus = false;
        } else {
            mergeOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    case kPlainRocksDB:
        if (MergeWithPlainRocksDB(objectPairMemPoolHandler) == false) {
            mergeOperationStatus = false;
        } else {
            mergeOperationStatus = true;
        }
        shouldDeleteMemPoolHandler = true;
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        mergeOperationStatus = false;
        shouldDeleteMemPoolHandler = true;
        break;
    }
    if (shouldDeleteMemPoolHandler == true) {
        objectPairMemPool_->eraseContentFromMemPool(objectPairMemPoolHandler);
    }
    if (mergeOperationStatus == true) {
        return true;
    } else {
        return false;
    }
}

bool DeltaKV::GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
    switch (deltaKVRunningMode_) {
    case kBatchedWithBothValueAndDeltaLog:
    case kBothValueAndDeltaLog:
        if (GetWithValueAndDeltaStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBatchedWithOnlyValueLog:
    case kOnlyValueLog:
        if (GetWithOnlyValueStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBatchedWithOnlyDeltaLog:
    case kOnlyDeltaLog:
        if (GetWithOnlyDeltaStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBatchedWithPlainRocksDB:
    case kPlainRocksDB:
        if (GetWithPlainRocksDB(key, value) == false) {
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

bool DeltaKV::GetCurrentValueThenWriteBack(const string& key)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
    vector<string> tempNewMergeOperatorsVec;
    bool needMergeWithInBufferOperationsFlag = false;
    string newValueStr;
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
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_WAIT_BUFFER, tvAll);
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
                    return true;
                } else {
                    string currentValue(queueIt.second.valuePtr_, queueIt.second.valueSize_);
                    tempNewMergeOperatorsVec.push_back(currentValue);
                }
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_MERGE_ALL, tv0);
        }
        batchedBufferOperationMtx_.unlock();
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_BATCH_READ_NO_WAIT_BUFFER, tv);
    }
    // get content from underlying DB
    uint32_t maxSequenceNumber = 0;
    string tempRawValueStr;
    bool getNewValueStrSuccessFlag = false;
    switch (deltaKVRunningMode_) {
    case kBatchedWithBothValueAndDeltaLog:
    case kBothValueAndDeltaLog:
        if (GetWithValueAndDeltaStore(key, &tempRawValueStr, maxSequenceNumber, true) == false) {
            getNewValueStrSuccessFlag = false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                newValueStr.clear();
                deltaKVMergeOperatorPtr_->Merge(tempRawValueStr, tempNewMergeOperatorsVec, &newValueStr);
                getNewValueStrSuccessFlag = true;
            } else {
                newValueStr.clear();
                newValueStr.assign(tempRawValueStr);
                getNewValueStrSuccessFlag = true;
            }
        }
        break;
    case kBatchedWithOnlyValueLog:
    case kOnlyValueLog:
        if (GetWithOnlyValueStore(key, &tempRawValueStr, maxSequenceNumber, true) == false) {
            getNewValueStrSuccessFlag = false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                newValueStr.clear();
                deltaKVMergeOperatorPtr_->Merge(tempRawValueStr, tempNewMergeOperatorsVec, &newValueStr);
                getNewValueStrSuccessFlag = true;
            } else {
                newValueStr.clear();
                newValueStr.assign(tempRawValueStr);
                getNewValueStrSuccessFlag = true;
            }
        }
        break;
    case kBatchedWithOnlyDeltaLog:
    case kOnlyDeltaLog:
        if (GetWithOnlyDeltaStore(key, &tempRawValueStr, maxSequenceNumber, true) == false) {
            getNewValueStrSuccessFlag = false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                newValueStr.clear();
                deltaKVMergeOperatorPtr_->Merge(tempRawValueStr, tempNewMergeOperatorsVec, &newValueStr);
                getNewValueStrSuccessFlag = true;
            } else {
                newValueStr.clear();
                newValueStr.assign(tempRawValueStr);
                getNewValueStrSuccessFlag = true;
            }
        }
        break;
    case kBatchedWithPlainRocksDB:
    case kPlainRocksDB:
        if (GetWithPlainRocksDB(key, &tempRawValueStr) == false) {
            getNewValueStrSuccessFlag = false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                newValueStr.clear();
                deltaKVMergeOperatorPtr_->Merge(tempRawValueStr, tempNewMergeOperatorsVec, &newValueStr);
                getNewValueStrSuccessFlag = true;
            } else {
                newValueStr.clear();
                newValueStr.assign(tempRawValueStr);
                getNewValueStrSuccessFlag = true;
            }
        }
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        getNewValueStrSuccessFlag = false;
        break;
    }
    if (getNewValueStrSuccessFlag == true) {
        debug_info("Get current value done, start write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
        globalSequenceNumberGeneratorMtx_.lock();
        uint32_t currentSequenceNumber = globalSequenceNumber_++;
        globalSequenceNumberGeneratorMtx_.unlock();
        mempoolHandler_t objectPairMemPoolHandler;
        objectPairMemPool_->insertContentToMemPoolAndGetHandler(key, newValueStr, currentSequenceNumber, true, objectPairMemPoolHandler);
        bool putOperationStatus = true;
        bool deleteMemPoolHandlerStatus = false;
        switch (deltaKVRunningMode_) {
        case kBatchedWithPlainRocksDB:
        case kBatchedWithOnlyValueLog:
        case kBatchedWithOnlyDeltaLog:
        case kBatchedWithBothValueAndDeltaLog:
            if (PutWithWriteBatch(objectPairMemPoolHandler) == false) {
                putOperationStatus = false;
            } else {
                putOperationStatus = true;
            }
            break;
        case kBothValueAndDeltaLog:
            if (PutWithValueAndDeltaStore(objectPairMemPoolHandler) == false) {
                putOperationStatus = false;
            } else {
                putOperationStatus = true;
            }
            deleteMemPoolHandlerStatus = true;
            break;
        case kOnlyValueLog:
            if (PutWithOnlyValueStore(objectPairMemPoolHandler) == false) {
                putOperationStatus = false;
            } else {
                putOperationStatus = true;
            }
            deleteMemPoolHandlerStatus = true;
            break;
        case kOnlyDeltaLog:
            if (PutWithOnlyDeltaStore(objectPairMemPoolHandler) == false) {
                putOperationStatus = false;
            } else {
                putOperationStatus = true;
            }
            deleteMemPoolHandlerStatus = true;
            break;
        case kPlainRocksDB:
            if (PutWithPlainRocksDB(objectPairMemPoolHandler) == false) {
                putOperationStatus = false;
            } else {
                putOperationStatus = true;
            }
            deleteMemPoolHandlerStatus = true;
            break;
        default:
            debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
            putOperationStatus = false;
            deleteMemPoolHandlerStatus = true;
            break;
        }
        if (deleteMemPoolHandlerStatus == true) {
            objectPairMemPool_->eraseContentFromMemPool(objectPairMemPoolHandler);
        }
        if (putOperationStatus == true) {
            if (enableKeyValueCache_ == true) {
                string cacheKey = key;
                struct timeval tv;
                gettimeofday(&tv, 0);
                keyToValueListCache_->insertToCache(cacheKey, newValueStr);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_NEW, tv);
            }
            return true;
        } else {
            debug_error("[ERROR] Could not put back current value, skip write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
            return false;
        }
    } else {
        debug_error("Could not get current value, skip write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
        return false;
    }
}

// TODO: following functions are not complete

vector<bool> DeltaKV::GetKeysByTargetNumber(const string& targetStartKey, const uint64_t& targetGetNumber, vector<string>& keys, vector<string>& values)
{
    vector<bool> queryStatus;
    rocksdb::Iterator* it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    it->Seek(targetStartKey);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        keys.push_back(it->key().ToString());
        values.push_back(it->value().ToString());
        queryStatus.push_back(true);
    }
    if (queryStatus.size() < targetGetNumber) {
        for (int i = 0; i < targetGetNumber - queryStatus.size(); i++) {
            queryStatus.push_back(false);
        }
    }
    delete it;
    return queryStatus;
}

bool DeltaKV::SingleDelete(const string& key)
{
    rocksdb::Status rocksDBStatus = pointerToRawRocksDB_->SingleDelete(internalWriteOption_, key);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Deplete underlying rocksdb fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::PutWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler)
{
    if (objectPairMemPoolHandler.isAnchorFlag_ == false) {
        debug_error("[ERROR] put operation should has an anchor flag%s\n", "");
    }
    // cerr << "Key size = " << objectPairMemPoolHandler.keySize_ << endl;
    struct timeval tv;
    gettimeofday(&tv, 0);
    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == maxBatchOperationBeforeCommitNumber_) {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_LOCK_1, tv);
    gettimeofday(&tv, 0);
    std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, batchedOperationsCounter[currentWriteBatchDequeInUse]);
    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == maxBatchOperationBeforeCommitNumber_) {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchMapForSearch_[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        batchedOperationsCounter[currentWriteBatchDequeInUse] = 0;
        str_t currentKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        // cerr << "Key in pool = " << objectPairMemPoolHandler.keyPtr_ << endl;
        // cerr << "Key in str_t = " << currentKey.data_ << endl;
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            for (auto it : mapIt->second) {
                objectPairMemPool_->eraseContentFromMemPool(it.second);
                batchedOperationsCounter[currentWriteBatchDequeInUse]--;
            }
            mapIt->second.clear();
            mapIt->second.push_back(make_pair(kPutOp, objectPairMemPoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kPutOp, objectPairMemPoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_AFTER_LOCK_FULL, tv);
        return true;
    } else {
        // only insert
        str_t currentKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        // cerr << "Key in pool = " << objectPairMemPoolHandler.keyPtr_ << endl;
        // cerr << "Key in str_t = " << currentKey.data_ << endl;
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            for (auto it : mapIt->second) {
                objectPairMemPool_->eraseContentFromMemPool(it.second);
                batchedOperationsCounter[currentWriteBatchDequeInUse]--;
            }
            mapIt->second.clear();
            mapIt->second.push_back(make_pair(kPutOp, objectPairMemPoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kPutOp, objectPairMemPoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_AFTER_LOCK_FULL, tv);
        return true;
    }
}

bool DeltaKV::MergeWithWriteBatch(mempoolHandler_t objectPairMemPoolHandler)
{
    debug_info("[MergeOp] key = %s, sequence number = %u\n", string(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_).c_str(), objectPairMemPoolHandler.sequenceNumber_);
    if (objectPairMemPoolHandler.isAnchorFlag_ == true) {
        debug_error("[ERROR] merge operation should has no anchor flag%s\n", "");
    }
    struct timeval tv;
    gettimeofday(&tv, 0);
    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == maxBatchOperationBeforeCommitNumber_) {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_LOCK_1, tv);
    gettimeofday(&tv, 0);
    std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
    StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_LOCK_2, tv);
    gettimeofday(&tv, 0);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, batchedOperationsCounter[currentWriteBatchDequeInUse]);
    if (batchedOperationsCounter[currentWriteBatchDequeInUse] == maxBatchOperationBeforeCommitNumber_) {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchMapForSearch_[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        batchedOperationsCounter[currentWriteBatchDequeInUse] = 0;
        str_t currentKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            mapIt->second.push_back(make_pair(kMergeOp, objectPairMemPoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kMergeOp, objectPairMemPoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_AFTER_LOCK_FULL, tv);
        return true;
    } else {
        // only insert
        str_t currentKey(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_);
        auto mapIt = writeBatchMapForSearch_[currentWriteBatchDequeInUse]->find(currentKey);
        if (mapIt != writeBatchMapForSearch_[currentWriteBatchDequeInUse]->end()) {
            mapIt->second.push_back(make_pair(kMergeOp, objectPairMemPoolHandler));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        } else {
            vector<pair<DBOperationType, mempoolHandler_t>> tempDeque;
            tempDeque.push_back(make_pair(kMergeOp, objectPairMemPoolHandler));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse]->insert(make_pair(currentKey, tempDeque));
            batchedOperationsCounter[currentWriteBatchDequeInUse]++;
        }
        StatsRecorder::getInstance()->timeProcess(StatsType::MERGE_AFTER_LOCK_FULL, tv);
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
            bool mergeStatus = deltaKVMergeOperatorPtr_->Merge(firstValue, operandList, &finalValue);
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
            objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalValue, currentSequenceNumber, true, newHandler);
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
            bool mergeStatus = deltaKVMergeOperatorPtr_->PartialMerge(operandList, finalOperandList);
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
            objectPairMemPool_->insertContentToMemPoolAndGetHandler(newKeyStr, finalOperandList[0], currentSequenceNumber, false, newHandler);
            it->second.push_back(make_pair(kMergeOp, newHandler));
            validObjectNumber++;
        }
    }
    // uint32_t counter = 0;
    // for (auto it = operationsMap->begin(); it != operationsMap->end(); it++) {
    //     counter += it->second.size();
    // }
    // cerr << "Total object number = " << totalObjectNumber << ", valid object number = " << validObjectNumber << ", map size = " << operationsMap->size() << ", object number in map = " << counter << endl;
    return true;
}

void DeltaKV::processBatchedOperationsWorker()
{
    while (true) {
        if (notifyWriteBatchMQ_->done_ == true && notifyWriteBatchMQ_->isEmpty() == true) {
            break;
        }
        unordered_map<str_t, vector<pair<DBOperationType, mempoolHandler_t>>, mapHashKeyForStr_t, mapEqualKeForStr_t>* currentHandler;
        if (notifyWriteBatchMQ_->pop(currentHandler)) {
            std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
            oneBufferDuringProcessFlag_ = true;
            debug_info("process batched contents for object number = %lu\n", currentHandler->size());
            if (deltaKVRunningMode_ != kBatchedWithPlainRocksDB) {
                performInBatchedBufferDeduplication(currentHandler);
            }
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
            bool putToValueStoreStatus = false;
            switch (deltaKVRunningMode_) {
            case kBatchedWithPlainRocksDB: {
                struct timeval tv;
                gettimeofday(&tv, 0);
                rocksdb::Status rocksDBStatus;
                rocksdb::WriteOptions batchedWriteOperation;
                batchedWriteOperation.sync = false;
                if (useInternalRocksDBBatchOperationsFlag_ == false) {
                    for (auto index = 0; index < handlerToValueStoreVec.size(); index++) {
                        rocksdb::Slice newKey(handlerToValueStoreVec[index].keyPtr_, handlerToValueStoreVec[index].keySize_);
                        rocksdb::Slice newValue(handlerToValueStoreVec[index].valuePtr_, handlerToValueStoreVec[index].valueSize_);
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
                        if (!rocksDBStatus.ok()) {
                            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                        }
                    }
                    for (auto index = 0; index < handlerToDeltaStoreVec.size(); index++) {
                        if (handlerToDeltaStoreVec[index].isAnchorFlag_ == false) {
                            rocksdb::Slice newKey(handlerToDeltaStoreVec[index].keyPtr_, handlerToDeltaStoreVec[index].keySize_);
                            rocksdb::Slice newValue(handlerToDeltaStoreVec[index].valuePtr_, handlerToDeltaStoreVec[index].valueSize_);
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                            }
                        }
                    }
                } else {
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    rocksdb::WriteBatch batch;
                    for (auto index = 0; index < handlerToValueStoreVec.size(); index++) {
                        rocksdb::Slice newKey(handlerToValueStoreVec[index].keyPtr_, handlerToValueStoreVec[index].keySize_);
                        rocksdb::Slice newValue(handlerToValueStoreVec[index].valuePtr_, handlerToValueStoreVec[index].valueSize_);
                        rocksDBStatus = batch.Put(newKey, newValue);
                    }
                    for (auto index = 0; index < handlerToDeltaStoreVec.size(); index++) {
                        if (handlerToDeltaStoreVec[index].isAnchorFlag_ == false) {
                            rocksdb::Slice newKey(handlerToDeltaStoreVec[index].keyPtr_, handlerToDeltaStoreVec[index].keySize_);
                            rocksdb::Slice newValue(handlerToDeltaStoreVec[index].valuePtr_, handlerToDeltaStoreVec[index].valueSize_);
                            rocksDBStatus = batch.Merge(newKey, newValue);
                        }
                    }
                    STAT_PROCESS(pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_MERGE_ROCKSDB);
                }
                STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
                StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_PLAIN_ROCKSDB, tv);
                break;
            }
            case kBatchedWithBothValueAndDeltaLog: {
                struct timeval tv;
                gettimeofday(&tv, 0);
                rocksdb::WriteOptions batchedWriteOperation;
                batchedWriteOperation.sync = false;
                // process value
                if (useInternalRocksDBBatchOperationsFlag_ == false) {
                    for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                        if (valueIt->valueSize_ <= valueExtractSize_) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                            } else {
                                debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                            }
                            handlerToValueStoreVec.erase(valueIt);
                        }
                    }
                } else {
                    rocksdb::WriteBatch batch;
                    for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                        if (valueIt->valueSize_ <= valueExtractSize_) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            batch.Put(newKey, newValue);
                            handlerToValueStoreVec.erase(valueIt);
                        }
                    }
                    STAT_PROCESS(pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_ROCKSDB);
                }
                STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
                // process deltas
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
                cerr << "handlerToDeltaStoreVec size = " << handlerToDeltaStoreVec.size() << ", notSeparatedDeltasVec size = " << notSeparatedDeltasVec.size() << ", separate flag number = " << separateFlagVec.size() << ", separated counter = " << spearateTrueCounter << ", not separated counter = " << separateFalseCounter << endl;
                STAT_PROCESS(putToValueStoreStatus = IndexStoreInterfaceObjPtr_->multiPut(handlerToValueStoreVec), StatsType::DELTAKV_PUT_INDEXSTORE);
                if (putToValueStoreStatus == true) {
                    STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(handlerToDeltaStoreVec), StatsType::DELTAKV_PUT_HASHSTORE);
                    if (putToDeltaStoreStatus == true) {
                        if (useInternalRocksDBBatchOperationsFlag_ == false) {
                            auto separatedID = 0, notSeparatedID = 0;
                            for (auto separatedDeltaFlagIndex = 0; separatedDeltaFlagIndex < separateFlagVec.size(); separatedDeltaFlagIndex++) {
                                if (separateFlagVec[separatedDeltaFlagIndex] == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = notSeparatedDeltasVec[notSeparatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = false;
                                    currentInternalValueType.sequenceNumber_ = notSeparatedDeltasVec[notSeparatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    memcpy(writeInternalValueBuffer + sizeof(internalValueType), notSeparatedDeltasVec[notSeparatedID].valuePtr_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Slice newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Status rocksDBStatus;
                                    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                    if (!rocksDBStatus.ok()) {
                                        debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                    } else {
                                        debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                                    }
                                    notSeparatedID++;
                                } else {
                                    char writeInternalValueBuffer[sizeof(internalValueType)];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[separatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = true;
                                    currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[separatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    rocksdb::Slice newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
                                    rocksdb::Status rocksDBStatus;
                                    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                    if (!rocksDBStatus.ok()) {
                                        debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                    } else {
                                        debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                                    }
                                    separatedID++;
                                }
                            }
                        } else {
                            // use rocksdb batch
                            rocksdb::WriteBatch batch;
                            auto separatedID = 0, notSeparatedID = 0;
                            for (auto separatedDeltaFlagIndex = 0; separatedDeltaFlagIndex < separateFlagVec.size(); separatedDeltaFlagIndex++) {
                                if (separateFlagVec[separatedDeltaFlagIndex] == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = notSeparatedDeltasVec[notSeparatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = false;
                                    currentInternalValueType.sequenceNumber_ = notSeparatedDeltasVec[notSeparatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    memcpy(writeInternalValueBuffer + sizeof(internalValueType), notSeparatedDeltasVec[notSeparatedID].valuePtr_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Slice newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Status rocksDBStatus;
                                    batch.Merge(newKey, newValue);
                                    notSeparatedID++;
                                } else {
                                    char writeInternalValueBuffer[sizeof(internalValueType)];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[separatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = true;
                                    currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[separatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    rocksdb::Slice newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
                                    rocksdb::Status rocksDBStatus;
                                    batch.Merge(newKey, newValue);
                                    separatedID++;
                                }
                            }
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_MERGE_ROCKSDB);
                            debug_info("Write underlying rocksdb for %lu keys done \n", handlerToDeltaStoreVec.size());
                        }
                        STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
                        StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_KV_KD, tv);
                    } else {
                        debug_error("[ERROR] Write underlying value store error, target separated value number = %lu\n", handlerToValueStoreVec.size());
                    }
                } else {
                    debug_error("[ERROR] Write underlying delta store error, target separated delta number = %lu, not separated delta number = %lu\n", handlerToValueStoreVec.size(), notSeparatedDeltasVec.size());
                }
                break;
            }
            case kBatchedWithOnlyValueLog: {
                struct timeval tv;
                gettimeofday(&tv, 0);
                rocksdb::WriteOptions batchedWriteOperation;
                batchedWriteOperation.sync = false;
                // process value
                if (useInternalRocksDBBatchOperationsFlag_ == false) {
                    for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                        if (valueIt->valueSize_ <= valueExtractSize_) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                            } else {
                                debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                            }
                            handlerToValueStoreVec.erase(valueIt);
                        }
                    }
                } else {
                    rocksdb::WriteBatch batch;
                    for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                        if (valueIt->valueSize_ <= valueExtractSize_) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            batch.Put(newKey, newValue);
                            handlerToValueStoreVec.erase(valueIt);
                        }
                    }
                    STAT_PROCESS(pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_ROCKSDB);
                }
                STAT_PROCESS(putToValueStoreStatus = IndexStoreInterfaceObjPtr_->multiPut(handlerToValueStoreVec), StatsType::DELTAKV_PUT_INDEXSTORE);
                if (putToValueStoreStatus == true) {
                    rocksdb::Status rocksDBStatus;
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    if (useInternalRocksDBBatchOperationsFlag_ == false) {
                        for (auto index = 0; index < handlerToDeltaStoreVec.size(); index++) {
                            if (handlerToDeltaStoreVec[index].isAnchorFlag_ == false) {
                                char writeInternalValueBuffer[sizeof(internalValueType) + handlerToDeltaStoreVec[index].valueSize_];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[index].valueSize_;
                                currentInternalValueType.valueSeparatedFlag_ = false;
                                currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[index].sequenceNumber_;
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                memcpy(writeInternalValueBuffer + sizeof(internalValueType), handlerToDeltaStoreVec[index].valuePtr_, handlerToDeltaStoreVec[index].valueSize_);
                                rocksdb::Slice newKey(handlerToDeltaStoreVec[index].keyPtr_, handlerToDeltaStoreVec[index].keySize_);
                                rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + handlerToDeltaStoreVec[index].valueSize_);
                                STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                if (!rocksDBStatus.ok()) {
                                    debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                }
                            }
                        }
                    } else {
                        rocksdb::WriteBatch batch;
                        for (auto index = 0; index < handlerToValueStoreVec.size(); index++) {
                            if (handlerToDeltaStoreVec[index].isAnchorFlag_ == false) {
                                char writeInternalValueBuffer[sizeof(internalValueType) + handlerToDeltaStoreVec[index].valueSize_];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[index].valueSize_;
                                currentInternalValueType.valueSeparatedFlag_ = false;
                                currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[index].sequenceNumber_;
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                memcpy(writeInternalValueBuffer + sizeof(internalValueType), handlerToDeltaStoreVec[index].valuePtr_, handlerToDeltaStoreVec[index].valueSize_);
                                rocksdb::Slice newKey(handlerToDeltaStoreVec[index].keyPtr_, handlerToDeltaStoreVec[index].keySize_);
                                rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + handlerToDeltaStoreVec[index].valueSize_);
                                batch.Merge(newKey, newValue);
                            }
                        }
                        STAT_PROCESS(pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_MERGE_ROCKSDB);
                    }
                } else {
                    debug_error("[ERROR] Could not put into value store via multiput, operations number = %lu\n", handlerToValueStoreVec.size());
                }
                STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
                StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_KV, tv);
                break;
            }
            case kBatchedWithOnlyDeltaLog: {
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
                cerr << "handlerToDeltaStoreVec size = " << handlerToDeltaStoreVec.size() << ", notSeparatedDeltasVec size = " << notSeparatedDeltasVec.size() << ", separate flag number = " << separateFlagVec.size() << ", separated counter = " << spearateTrueCounter << ", not separated counter = " << separateFalseCounter << endl;
                STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(handlerToDeltaStoreVec), StatsType::DELTAKV_PUT_HASHSTORE);
                if (putToDeltaStoreStatus == true) {
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    if (useInternalRocksDBBatchOperationsFlag_ == false) {
                        for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                            } else {
                                debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                            }
                        }
                        auto separatedID = 0, notSeparatedID = 0;
                        for (auto separatedDeltaFlagIndex = 0; separatedDeltaFlagIndex < separateFlagVec.size(); separatedDeltaFlagIndex++) {
                            if (separateFlagVec[separatedDeltaFlagIndex] == false) {
                                if (notSeparatedDeltasVec[notSeparatedID].isAnchorFlag_ == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = notSeparatedDeltasVec[notSeparatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = false;
                                    currentInternalValueType.sequenceNumber_ = notSeparatedDeltasVec[notSeparatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    memcpy(writeInternalValueBuffer + sizeof(internalValueType), notSeparatedDeltasVec[notSeparatedID].valuePtr_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Slice newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Status rocksDBStatus;
                                    debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                                    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                    if (!rocksDBStatus.ok()) {
                                        debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                    } else {
                                        debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                                    }
                                    notSeparatedID++;
                                } else {
                                    string newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                                    notSeparatedID++;
                                }
                            } else {
                                if (handlerToDeltaStoreVec[separatedID].isAnchorFlag_ == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType)];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[separatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = true;
                                    currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[separatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    rocksdb::Slice newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
                                    rocksdb::Status rocksDBStatus;
                                    debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                                    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                    if (!rocksDBStatus.ok()) {
                                        debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                    } else {
                                        debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", newKey.ToString().c_str());
                                    }
                                    separatedID++;
                                } else {
                                    string newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                                    separatedID++;
                                }
                            }
                        }
                    } else {
                        // use rocksdb batch
                        rocksdb::WriteBatch batch;
                        for (auto valueIt = handlerToValueStoreVec.begin(); valueIt != handlerToValueStoreVec.end(); valueIt++) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueIt->valueSize_];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueIt->valueSize_;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = valueIt->sequenceNumber_;
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueIt->valuePtr_, valueIt->valueSize_);
                            rocksdb::Slice newKey(valueIt->keyPtr_, valueIt->keySize_);
                            rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + valueIt->valueSize_);
                            rocksdb::Status rocksDBStatus;
                            batch.Put(newKey, newValue);
                        }
                        auto separatedID = 0, notSeparatedID = 0;
                        for (auto separatedDeltaFlagIndex = 0; separatedDeltaFlagIndex < separateFlagVec.size(); separatedDeltaFlagIndex++) {
                            if (separateFlagVec[separatedDeltaFlagIndex] == false) {
                                if (notSeparatedDeltasVec[notSeparatedID].isAnchorFlag_ == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = notSeparatedDeltasVec[notSeparatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = false;
                                    currentInternalValueType.sequenceNumber_ = notSeparatedDeltasVec[notSeparatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    memcpy(writeInternalValueBuffer + sizeof(internalValueType), notSeparatedDeltasVec[notSeparatedID].valuePtr_, notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Slice newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType) + notSeparatedDeltasVec[notSeparatedID].valueSize_);
                                    rocksdb::Status rocksDBStatus;
                                    debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                                    batch.Merge(newKey, newValue);
                                    notSeparatedID++;
                                } else {
                                    string newKey(notSeparatedDeltasVec[notSeparatedID].keyPtr_, notSeparatedDeltasVec[notSeparatedID].keySize_);
                                    debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), notSeparatedDeltasVec[notSeparatedID].sequenceNumber_);
                                    notSeparatedID++;
                                }
                            } else {
                                if (handlerToDeltaStoreVec[separatedID].isAnchorFlag_ == false) {
                                    char writeInternalValueBuffer[sizeof(internalValueType)];
                                    internalValueType currentInternalValueType;
                                    currentInternalValueType.mergeFlag_ = false;
                                    currentInternalValueType.rawValueSize_ = handlerToDeltaStoreVec[separatedID].valueSize_;
                                    currentInternalValueType.valueSeparatedFlag_ = true;
                                    currentInternalValueType.sequenceNumber_ = handlerToDeltaStoreVec[separatedID].sequenceNumber_;
                                    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                    rocksdb::Slice newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    rocksdb::Slice newValue(writeInternalValueBuffer, sizeof(internalValueType));
                                    rocksdb::Status rocksDBStatus;
                                    debug_info("[MergeOp-rocks] key = %s, sequence number = %u\n", newKey.ToString().c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                                    batch.Merge(newKey, newValue);
                                    separatedID++;
                                } else {
                                    string newKey(handlerToDeltaStoreVec[separatedID].keyPtr_, handlerToDeltaStoreVec[separatedID].keySize_);
                                    debug_info("[MergeOp-rocks] skip anchor key = %s, sequence number = %u\n", newKey.c_str(), handlerToDeltaStoreVec[separatedID].sequenceNumber_);
                                    separatedID++;
                                }
                            }
                        }
                        rocksdb::Status rocksDBStatus;
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_MERGE_ROCKSDB);
                        debug_info("Write underlying rocksdb for %lu keys done \n", handlerToDeltaStoreVec.size());
                    }
                    STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
                    StatsRecorder::getInstance()->timeProcess(StatsType::BATCH_KD, tv);
                } else {
                    debug_error("[ERROR] could not put %zu object into delta store, as well as not separated object number = %zu\n", handlerToDeltaStoreVec.size(), notSeparatedDeltasVec.size());
                }
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
        }
    }
    writeBatchOperationWorkExitFlag = true;
    debug_info("Process batched operations done, exit thread%s\n", "");
    return;
}

void DeltaKV::processWriteBackOperationsWorker()
{
    while (true) {
        if (writeBackOperationsQueue_->done_ == true && writeBackOperationsQueue_->isEmpty() == true) {
            break;
        }
        writeBackObjectStruct* currentProcessPair;
        if (writeBackOperationsQueue_->pop(currentProcessPair)) {
            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_warn("Target Write back key = %s\n", currentProcessPair->key.c_str());
            bool writeBackStatus = GetCurrentValueThenWriteBack(currentProcessPair->key);
            if (writeBackStatus == false) {
                debug_error("Could not write back target key = %s\n", currentProcessPair->key.c_str());
            } else {
                debug_warn("Write back key = %s success\n", currentProcessPair->key.c_str());
            }
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_GC_WRITE_BACK, tv);
            delete currentProcessPair;
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

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, bool& findNewValueIndex, externalIndexInfo& newExternalIndexInfo, uint32_t& maxSequenceNumber)
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
            memcpy(&newExternalIndexInfo, internalValue.c_str() + currentProcessLocationIndex, sizeof(externalIndexInfo));
            currentProcessLocationIndex += sizeof(externalIndexInfo);
            findNewValueIndex = true;
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

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, vector<internalValueType>& mergeOperatorsRecordVec, bool& findNewValueIndex, externalIndexInfo& newExternalIndexInfo, uint32_t& maxSequenceNumber)
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
            memcpy(&newExternalIndexInfo, internalValue.c_str() + currentProcessLocationIndex, sizeof(externalIndexInfo));
            currentProcessLocationIndex += sizeof(externalIndexInfo);
            findNewValueIndex = true;
        }
        if (currentInternalValueTypeHeader.valueSeparatedFlag_ != true) {
            assert(currentProcessLocationIndex + currentInternalValueTypeHeader.rawValueSize_ <= internalValue.size());
            string currentValue(internalValue.c_str() + currentProcessLocationIndex, currentInternalValueTypeHeader.rawValueSize_);
            currentProcessLocationIndex += currentInternalValueTypeHeader.rawValueSize_;
            mergeOperatorsVec.push_back(make_pair(false, currentValue));
            mergeOperatorsRecordVec.push_back(currentInternalValueTypeHeader);
        } else {
            mergeOperatorsVec.push_back(make_pair(true, ""));
            mergeOperatorsRecordVec.push_back(currentInternalValueTypeHeader);
        }
    }
    return true;
}

} // namespace DELTAKV_NAMESPACE
