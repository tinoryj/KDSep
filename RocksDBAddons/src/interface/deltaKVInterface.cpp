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
            memcpy(&tempInternalValueType, operandListIt.substr(deltaOffset).c_str(), headerSize);

            // extract the oprand
            if (tempInternalValueType.mergeFlag_ == true) {
                // index update
                assert(tempInternalValueType.valueSeparatedFlag_ == true && deltaOffset + headerSize + valueIndexSize <= operandListIt.size());
                operand.assign(operandListIt.substr(deltaOffset, headerSize + valueIndexSize));
                deltaOffset += headerSize + valueIndexSize;
            } else {
                if (tempInternalValueType.valueSeparatedFlag_ == false) {
                    // raw delta
                    assert(deltaOffset + headerSize + tempInternalValueType.rawValueSize_ <= operandListIt.size());
                    operand.assign(operandListIt.substr(deltaOffset, headerSize + tempInternalValueType.rawValueSize_));
                    deltaOffset += headerSize + tempInternalValueType.rawValueSize_;
                } else {
                    // separated delta
                    assert(deltaOffset + headerSize <= operandListIt.size());
                    operand.assign(operandListIt.substr(deltaOffset, headerSize));
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
    if (!leadingRawDeltas.empty()) {
        FullMergeFieldUpdates(existing_value->ToString().substr(headerSize), leadingRawDeltas, &mergedValueWithoutValueType);
        if (mergedValueWithoutValueType.size() != existingValueType.rawValueSize_) {
            debug_error("[ERROR] value size differs after merging: %lu v.rocksDBStatus. %u\n", mergedValueWithoutValueType.size(), existingValueType.rawValueSize_);
        }
    } else {
        mergedValueWithoutValueType.assign(existing_value->ToString().substr(headerSize));
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
        delete writeBatchDeque[0];
        delete writeBatchDeque[1];
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
        // delete related object pointers
        delete IndexStoreInterfaceObjPtr_;
    }
    cerr << "[DeltaKV Interface] Try delete RocksDB" << endl;
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
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
    if (options.enable_key_value_cache_ == true) {
        enableKeyValueCache_ = true;
        keyToValueListCache_ = new AppendAbleLRUCache<string, string>(options.key_value_cache_object_number_);
    }
    // Create objects
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        isValueStoreInUseFlag_ = true;
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
    }
    if (options.enable_batched_operations_ == true) {
        writeBatchDeque[0] = new deque<tuple<DBOperationType, string, string, uint32_t>>;
        writeBatchDeque[1] = new deque<tuple<DBOperationType, string, string, uint32_t>>;
        notifyWriteBatchMQ_ = new messageQueue<deque<tuple<DBOperationType, string, string, uint32_t>>*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        thList_.push_back(th);
        isBatchedOperationsWithBufferInUse_ = true;
        maxBatchOperationBeforeCommitNumber_ = options.batched_operations_number_;
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
            for (auto threadID = 0; threadID < options.deltaStore_op_worker_thread_number_limit_ - 1; threadID++) {
                th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_, threadID));
                thList_.push_back(th);
            }
            th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::notifyOperationWorkerThread, hashStoreFileOperatorPtr_));
            thList_.push_back(th);
        } else {
            debug_info("Total thread number for operationWorker < 2, use direct operation instead%s\n", "");
        }
    }

    // process runnning mode
    if (options.enable_valueStore && options.enable_deltaStore) {
        deltaKVRunningMode_ = kBothValueAndDeltaLog;
    } else if (options.enable_valueStore && !options.enable_deltaStore) {
        deltaKVRunningMode_ = kOnlyValueLog;
    } else if (!options.enable_valueStore && options.enable_deltaStore) {
        deltaKVRunningMode_ = kOnlyDeltaLog;
    } else if (!options.enable_valueStore && !options.enable_deltaStore) {
        deltaKVRunningMode_ = kPlainRocksDB;
    }

    if (options.enable_batched_operations_) {
        switch (deltaKVRunningMode_) {
        case kBothValueAndDeltaLog:
            deltaKVRunningMode_ = kBatchedWithBothValueAndDeltaLog;
            break;
        case kOnlyDeltaLog:
            deltaKVRunningMode_ = kBatchedWithOnlyDeltaLog;
            break;
        case kOnlyValueLog:
            deltaKVRunningMode_ = kBatchedWithOnlyValueLog;
            break;
        case kPlainRocksDB:
            deltaKVRunningMode_ = kBatchedWithPlainRocksDB;
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
    if (isDeltaStoreInUseFlag_ == true) {
        if (enableDeltaStoreWithBackgroundGCFlag_ == true) {
            HashStoreInterfaceObjPtr_->forcedManualGarbageCollection();
            cerr << "DeltaStore forced GC done" << endl;
        }
    }
    if (enableWriteBackOperationsFlag_ == true) {
        writeBackOperationsQueue_->done_ = true;
        while (writeBackOperationsQueue_->isEmpty() == false) {
            asm volatile("");
        }
        cerr << "Write back done" << endl;
    }
    if (isBatchedOperationsWithBufferInUse_ == true) {
        for (auto i = 0; i < 2; i++) {
            if (writeBatchDeque[i]->size() != 0) {
                notifyWriteBatchMQ_->push(writeBatchDeque[i]);
            }
        }
        notifyWriteBatchMQ_->done_ = true;
        cerr << "Flush write back" << endl;
        while (writeBatchOperationWorkExitFlag == false) {
            asm volatile("");
        }
        cerr << "Flush write back done" << endl;
    }
    if (isDeltaStoreInUseFlag_ == true) {
        HashStoreInterfaceObjPtr_->setJobDone();
        cerr << "HashStore set job done" << endl;
    }
    deleteExistingThreads();
    return true;
}

bool DeltaKV::PutWithPlainRocksDB(const string& key, const string& value)
{
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, value), StatsType::DELTAKV_PUT_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::MergeWithPlainRocksDB(const string& key, const string& value)
{
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, value), StatsType::DELTAKV_MERGE_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with merge value fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
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

bool DeltaKV::PutWithOnlyValueStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    if (value.size() >= IndexStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        externalIndexInfo currentExternalIndexInfo;
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo, currentSequenceNumber, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            return true;
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", key.c_str(), value.c_str());
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.rawValueSize_ = value.size();
        currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
        currentInternalValueType.valueSeparatedFlag_ = false;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::MergeWithOnlyValueStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    // also need internal value header since value store GC may update fake header as merge
    char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.valueSeparatedFlag_ = false;
    currentInternalValueType.rawValueSize_ = value.size();
    currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
    memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
    string newWriteValueStr(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, newWriteValueStr), StatsType::DELTAKV_MERGE_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Merge underlying rocksdb with interanl value header fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
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
        auto index = 0;
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

bool DeltaKV::PutWithOnlyDeltaStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    // for merge flag check, add internal value header to raw value
    char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.valueSeparatedFlag_ = false;
    currentInternalValueType.rawValueSize_ = value.size();
    currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
    memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
    string newWriteValueStr(writeInternalValueBuffer, sizeof(internalValueType) + value.size());

    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValueStr), StatsType::DELTAKV_PUT_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with added value header fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        // need to append anchor to delta store
        bool addAnchorStatus;
        STAT_PROCESS(addAnchorStatus = HashStoreInterfaceObjPtr_->put(key, value, currentSequenceNumber, true), StatsType::DELTAKV_PUT_HASHSTORE);
        if (addAnchorStatus == true) {
            return true;
        } else {
            debug_error("[ERROR]  Append anchor to delta store fault, key = %s, value = %s\n", key.c_str(), value.c_str());
            return false;
        }
    }
}

bool DeltaKV::MergeWithOnlyDeltaStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(key, value, currentSequenceNumber, false), StatsType::DELTAKV_MERGE_HASHSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
            rocksdb::Status rocksDBStatus;
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external value type info, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
                return false;
            } else {
                return true;
            }
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", key.c_str(), value.c_str());
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.valueSeparatedFlag_ = false;
        currentInternalValueType.rawValueSize_ = value.size();
        currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + value.size());

        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
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
            externalIndexInfo newExternalIndexInfo;
            bool findNewValueIndexFlag = false;
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo, maxSequenceNumber);
            if (findNewValueIndexFlag == true) {
                debug_error("[ERROR] In only delta store, should not extract exteranl index, flag = %d\n", findNewValueIndexFlag);
            }
            debug_trace("read deltaInfoVec from LSM-tree size = %lu\n", deltaInfoVec.size());
            vector<string>* deltaValueFromExternalStoreVec = new vector<string>;
            bool ret;
            STAT_PROCESS(ret = HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec), StatsType::DELTAKV_GET_HASHSTORE);
            if (ret != true) {
                debug_trace("Read external deltaStore fault, key = %s\n", key.c_str());
                delete deltaValueFromExternalStoreVec;
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
                    debug_trace("Start DeltaKV merge operation, internalRawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", internalRawValueStr.c_str(), finalDeltaOperatorsVec.size());
                    bool mergeOperationStatus = deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value);
                    if (mergeOperationStatus == true) {
                        if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_ && writeBackWhenReadDeltaNumerThreshold_ != 0 && !getByWriteBackFlag) {
                            writeBackObjectStruct* newPair = new writeBackObjectStruct(key, "", 0);
                            writeBackOperationsQueue_->push(newPair);
                        }
                        delete deltaValueFromExternalStoreVec;
                        return true;
                    } else {
                        debug_error("[ERROR] Perform merge operation fail, key = %s\n", key.c_str());
                        delete deltaValueFromExternalStoreVec;
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

bool DeltaKV::PutWithValueAndDeltaStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    if (value.size() >= IndexStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        externalIndexInfo currentExternalIndexInfo;
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo, currentSequenceNumber, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            bool updateDeltaStoreWithAnchorFlagstatus;
            STAT_PROCESS(updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, currentSequenceNumber, true), StatsType::DELTAKV_PUT_HASHSTORE);
            if (updateDeltaStoreWithAnchorFlagstatus == true) {
                return true;
            } else {
                debug_error("[ERROR] Update anchor to current key fault, key = %s, value = %s\n", key.c_str(), value.c_str());
                return false;
            }
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", key.c_str(), value.c_str());
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.rawValueSize_ = value.size();
        currentInternalValueType.valueSeparatedFlag_ = true;
        currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        } else {
            bool updateDeltaStoreWithAnchorFlagStatus;
            STAT_PROCESS(updateDeltaStoreWithAnchorFlagStatus = HashStoreInterfaceObjPtr_->put(key, value, currentSequenceNumber, true), StatsType::DELTAKV_PUT_HASHSTORE);
            if (updateDeltaStoreWithAnchorFlagStatus == true) {
                return true;
            } else {
                debug_error("[ERROR] Update anchor to current key fault, key = %s, value = %s\n", key.c_str(), value.c_str());
                return false;
            }
        }
    }
}

bool DeltaKV::MergeWithValueAndDeltaStore(const string& key, const string& value)
{
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(key, value, currentSequenceNumber, false), StatsType::DELTAKV_MERGE_HASHSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
            rocksdb::Status rocksDBStatus;
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
                return false;
            } else {
                return true;
            }
        } else {
            debug_error("[ERROR] Write value to external storage faul, key = %s, value = %s\n", key.c_str(), value.c_str());
            return false;
        }
    } else {
        char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
        internalValueType currentInternalValueType;
        currentInternalValueType.mergeFlag_ = false;
        currentInternalValueType.valueSeparatedFlag_ = false;
        currentInternalValueType.rawValueSize_ = value.size();
        currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, key, newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
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
    switch (deltaKVRunningMode_) {
    case kBatchedWithPlainRocksDB:
    case kBatchedWithOnlyValueLog:
    case kBatchedWithOnlyDeltaLog:
    case kBatchedWithBothValueAndDeltaLog:
        if (PutWithWriteBatch(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBothValueAndDeltaLog:
        if (PutWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kOnlyValueLog:
        if (PutWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kOnlyDeltaLog:
        if (PutWithOnlyDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kPlainRocksDB:
        if (PutWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        return false;
    }
    return false;
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
    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
        std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WAIT_BUFFER, tv);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).size() != 0) {
                if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().op_ == kPutOp) {
                    value->assign(writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().value_);
                    debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                    return true;
                }
            }
            string newValueStr;
            bool findNewValueFlag = false;
            for (auto it : writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key)) {
                if (it.op_ == kPutOp) {
                    newValueStr.assign(it.value_);
                    tempNewMergeOperatorsVec.clear();
                    findNewValueFlag = true;
                } else {
                    tempNewMergeOperatorsVec.push_back(it.value_);
                }
            }
            if (findNewValueFlag == true) {
                deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
                return true;
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
        }
    }
    uint32_t maxSequenceNumberPlaceHolder;
    switch (deltaKVRunningMode_) {
    case kBatchedWithBothValueAndDeltaLog:
    case kBothValueAndDeltaLog:
        if (GetWithValueAndDeltaStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
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
    case kBatchedWithOnlyValueLog:
    case kOnlyValueLog:
        if (GetWithOnlyValueStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
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
        if (GetWithOnlyDeltaStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
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
        if (GetWithPlainRocksDB(key, value) == false) {
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
    switch (deltaKVRunningMode_) {
    case kBatchedWithPlainRocksDB:
    case kBatchedWithOnlyValueLog:
    case kBatchedWithOnlyDeltaLog:
    case kBatchedWithBothValueAndDeltaLog:
        if (MergeWithWriteBatch(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kBothValueAndDeltaLog:
        if (MergeWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kOnlyValueLog:
        if (MergeWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    case kOnlyDeltaLog:
        if (MergeWithOnlyDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    case kPlainRocksDB:
        if (MergeWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            return true;
        }
        break;
    default:
        debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
        return false;
    }
    return false;
}

bool DeltaKV::GetWithMaxSequenceNumber(const string& key, string* value, uint32_t& maxSequenceNumber, bool getByWriteBackFlag)
{
    std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
    vector<string> tempNewMergeOperatorsVec;
    bool needMergeWithInBufferOperationsFlag = false;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
        std::scoped_lock<std::shared_mutex> r_lock(batchedBufferOperationMtx_);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().op_ == kPutOp) {
                value->assign(writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().value_);
                debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                maxSequenceNumber = writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().sequenceNumber_;
                return true;
            }
            string newValueStr;
            bool findNewValueFlag = false;
            maxSequenceNumber = writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().sequenceNumber_;
            // queue 1 during process, should search second(1) first
            for (auto it : writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key)) {
                if (it.op_ == kPutOp) {
                    newValueStr.assign(it.value_);
                    tempNewMergeOperatorsVec.clear();
                    findNewValueFlag = true;
                } else {
                    tempNewMergeOperatorsVec.push_back(it.value_);
                }
            }
            if (findNewValueFlag == true) {
                deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
                return true;
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
        }
    }
    switch (deltaKVRunningMode_) {
    case kBatchedWithBothValueAndDeltaLog:
    case kBothValueAndDeltaLog:
        if (GetWithValueAndDeltaStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
                return true;
            } else {
                return true;
            }
        }
        break;
    case kBatchedWithOnlyValueLog:
    case kOnlyValueLog:
        if (GetWithOnlyValueStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
                return true;
            } else {
                return true;
            }
        }
        break;
    case kBatchedWithOnlyDeltaLog:
    case kOnlyDeltaLog:
        if (GetWithOnlyDeltaStore(key, value, maxSequenceNumber, getByWriteBackFlag) == false) {
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
                return true;
            } else {
                return true;
            }
        }
        break;
    case kBatchedWithPlainRocksDB:
    case kPlainRocksDB:
        if (GetWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            if (needMergeWithInBufferOperationsFlag == true) {
                string tempValueStr;
                tempValueStr.assign(*value);
                value->clear();
                deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
                return true;
            } else {
                return true;
            }
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
    bool findNewValueInBatchedBuffer = false;
    string newValueStr;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        struct timeval tv;
        gettimeofday(&tv, 0);
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
        std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
        StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_HASHSTORE_WAIT_BUFFER, tv);
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().op_ == kPutOp) {
                newValueStr.assign(writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().value_);
                debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                findNewValueInBatchedBuffer = true;
            }
            bool findNewValueFlag = false;
            for (auto it : writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key)) {
                if (it.op_ == kPutOp) {
                    newValueStr.assign(it.value_);
                    tempNewMergeOperatorsVec.clear();
                    findNewValueFlag = true;
                } else {
                    tempNewMergeOperatorsVec.push_back(it.value_);
                }
            }

            if (findNewValueFlag == true) {
                string tempValue;
                deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, &tempValue);
                newValueStr = tempValue;
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
                findNewValueInBatchedBuffer = true;
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
        }
    }
    if (findNewValueInBatchedBuffer == true) {
        debug_info("Get current value in write buffer, skip write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
        return true;
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
        if (enableKeyValueCache_ == true) {
            string cacheKey = key;
            if (keyToValueListCache_->existsInCache(cacheKey) == false) {
                struct timeval tv;
                gettimeofday(&tv, 0);
                keyToValueListCache_->insertToCache(cacheKey, newValueStr);
                StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_CACHE_INSERT_NEW, tv);
            }
        }
        switch (deltaKVRunningMode_) {
        case kBatchedWithPlainRocksDB:
        case kBatchedWithOnlyValueLog:
        case kBatchedWithOnlyDeltaLog:
        case kBatchedWithBothValueAndDeltaLog:
            if (PutWithWriteBatch(key, newValueStr) == false) {
                return false;
            } else {
                return true;
            }
            break;
        case kBothValueAndDeltaLog:
            if (PutWithValueAndDeltaStore(key, newValueStr) == false) {
                return false;
            } else {
                return true;
            }
            break;
        case kOnlyValueLog:
            if (PutWithOnlyValueStore(key, newValueStr) == false) {
                return false;
            } else {
                return true;
            }
            break;
        case kOnlyDeltaLog:
            if (PutWithOnlyDeltaStore(key, newValueStr) == false) {
                return false;
            } else {
                return true;
            }
            break;
        case kPlainRocksDB:
            if (PutWithPlainRocksDB(key, newValueStr) == false) {
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
    } else {
        debug_error("Could not get current value, skip write back, key = %s, value = %s\n", key.c_str(), newValueStr.c_str());
        return false;
    }
}

// TODO: following functions are not complete

// bool DeltaKV::RangeScan(const string& startKey, uint64_t targetScanNumber, vector<string*> valueVec)
// {
//     std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
//     vector<bool> getKsysStatusVec;
//     vector<string> getKeysVec, getValuesVec;
//     getKsysStatusVec = GetKeysByTargetNumber(startKey, targetScanNumber, getKeysVec, getValuesVec);
//     string endKey = getKeysVec.end();
//     // search write buffer if need =================================================================
//     vector<string> tempNewMergeOperatorsVec;
//     bool needMergeWithInBufferOperationsFlag = false;
//     if (isBatchedOperationsWithBufferInUse_ == true) {
//         // try read from buffer first;
//         if (oneBufferDuringProcessFlag_ == true) {
//             debug_trace("Wait for batched buffer process%s\n", "");
//             while (oneBufferDuringProcessFlag_ == true) {
//                 asm volatile("");
//             }
//         }
//         std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
//         debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
//         if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
//             if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).size() != 0) {
//                 if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().op_ == kPutOp) {
//                     value->assign(writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().value_);
//                     debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
//                     return true;
//                 }
//             }
//             string newValueStr;
//             bool findNewValueFlag = false;
//             for (auto it : writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key)) {
//                 if (it.op_ == kPutOp) {
//                     newValueStr.assign(it.value_);
//                     tempNewMergeOperatorsVec.clear();
//                     findNewValueFlag = true;
//                 } else {
//                     tempNewMergeOperatorsVec.push_back(it.value_);
//                 }
//             }
//             if (findNewValueFlag == true) {
//                 deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
//                 debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
//                 return true;
//             }
//             if (tempNewMergeOperatorsVec.size() != 0) {
//                 needMergeWithInBufferOperationsFlag = true;
//                 debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
//             }
//         }
//     }
//     uint32_t maxSequenceNumberPlaceHolder;
//     switch (deltaKVRunningMode_) {
//     case kBatchedWithBothValueAndDeltaLog:
//     case kBothValueAndDeltaLog:
//         if (GetWithValueAndDeltaStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
//             return false;
//         } else {
//             if (needMergeWithInBufferOperationsFlag == true) {
//                 string tempValueStr;
//                 tempValueStr.assign(*value);
//                 value->clear();
//                 deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
//                 return true;
//             } else {
//                 return true;
//             }
//         }
//         break;
//     case kBatchedWithOnlyValueLog:
//     case kOnlyValueLog:
//         if (GetWithOnlyValueStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
//             return false;
//         } else {
//             if (needMergeWithInBufferOperationsFlag == true) {
//                 string tempValueStr;
//                 tempValueStr.assign(*value);
//                 value->clear();
//                 deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
//                 return true;
//             } else {
//                 return true;
//             }
//         }
//         break;
//     case kBatchedWithOnlyDeltaLog:
//     case kOnlyDeltaLog:
//         if (GetWithOnlyDeltaStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
//             return false;
//         } else {
//             if (needMergeWithInBufferOperationsFlag == true) {
//                 string tempValueStr;
//                 tempValueStr.assign(*value);
//                 value->clear();
//                 deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
//                 return true;
//             } else {
//                 return true;
//             }
//         }
//         break;
//     case kBatchedWithPlainRocksDB:
//     case kPlainRocksDB:
//         if (GetWithPlainRocksDB(key, value) == false) {
//             return false;
//         } else {
//             if (needMergeWithInBufferOperationsFlag == true) {
//                 string tempValueStr;
//                 tempValueStr.assign(*value);
//                 value->clear();
//                 deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
//                 return true;
//             }
//             return true;
//         }
//         break;
//     default:
//         debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
//         return false;
//         break;
//     }
//     return false;
// }

// vector<bool> DeltaKV::MultiGetWithBothValueAndDeltaStore(const vector<string>& keys, vector<string>& values)
// {
//     std::scoped_lock<std::shared_mutex> w_lock(DeltaKVOperationsMtx_);
//     vector<string> tempNewMergeOperatorsVec;
//     bool needMergeWithInBufferOperationsFlag = false;
//     if (isBatchedOperationsWithBufferInUse_ == true) {
//         // try read from buffer first;
//         if (oneBufferDuringProcessFlag_ == true) {
//             debug_trace("Wait for batched buffer process%s\n", "");
//             while (oneBufferDuringProcessFlag_ == true) {
//                 asm volatile("");
//             }
//         }
//         std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
//         debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
//         if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
//             if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).size() != 0) {
//                 if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().op_ == kPutOp) {
//                     value->assign(writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).back().value_);
//                     debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
//                     return true;
//                 }
//             }
//             string newValueStr;
//             bool findNewValueFlag = false;
//             for (auto it : writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key)) {
//                 if (it.op_ == kPutOp) {
//                     newValueStr.assign(it.value_);
//                     tempNewMergeOperatorsVec.clear();
//                     findNewValueFlag = true;
//                 } else {
//                     tempNewMergeOperatorsVec.push_back(it.value_);
//                 }
//             }
//             if (findNewValueFlag == true) {
//                 deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
//                 debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
//                 return true;
//             }
//             if (tempNewMergeOperatorsVec.size() != 0) {
//                 needMergeWithInBufferOperationsFlag = true;
//                 debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
//             }
//         }
//     }
//     uint32_t maxSequenceNumberPlaceHolder;
//     if (GetWithValueAndDeltaStore(key, value, maxSequenceNumberPlaceHolder, false) == false) {
//         return false;
//     } else {
//         if (needMergeWithInBufferOperationsFlag == true) {
//             string tempValueStr;
//             tempValueStr.assign(*value);
//             value->clear();
//             deltaKVMergeOperatorPtr_->Merge(tempValueStr, tempNewMergeOperatorsVec, value);
//             return true;
//         } else {
//             return true;
//         }
//     }
//     return true;
// }

// vector<bool> DeltaKV::GetByPrefix(const string& targetKeyPrefix, vector<string>& keys, vector<string>& values)
// {
//     auto it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
//     it->Seek(targetKeyPrefix);
//     vector<bool> queryStatus;
//     for (int i = 0; it->Valid(); i++) {
//         string tempKey, tempValue;
//         tempKey = it->key().ToString();
//         tempValue = it->value().ToString();
//         it->Next();
//         keys->push_back(tempKey);
//         values->push_back(tempValue);
//         if (tempValue.empty()) {
//             queryStatus.push_back(false);
//         } else {
//             queryStatus.push_back(true);
//         }
//     }
//     delete it;
//     return queryStatus;
// }

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

bool DeltaKV::PutWithWriteBatch(const string& key, const string& value)
{
    if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber_) {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, writeBatchDeque[currentWriteBatchDequeInUse]->size());
    if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber_) {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchDeque[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kPutOp, key, value, currentSequenceNumber));
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).push_back(writeBatchSearch_t(kPutOp, value, currentSequenceNumber));
        } else {
            deque<writeBatchSearch_t> tempDeque;
            tempDeque.push_back(writeBatchSearch_t(kPutOp, value, currentSequenceNumber));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].insert(make_pair(key, tempDeque));
        }
        return true;
    } else {
        // only insert
        writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kPutOp, key, value, currentSequenceNumber));
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).push_back(writeBatchSearch_t(kPutOp, value, currentSequenceNumber));
        } else {
            deque<writeBatchSearch_t> tempDeque;
            tempDeque.push_back(writeBatchSearch_t(kPutOp, value, currentSequenceNumber));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].insert(make_pair(key, tempDeque));
        }
        return true;
    }
}

bool DeltaKV::MergeWithWriteBatch(const string& key, const string& value)
{
    if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber_) {
        if (oneBufferDuringProcessFlag_ == true) {
            debug_trace("Wait for batched buffer process%s\n", "");
            while (oneBufferDuringProcessFlag_ == true) {
                asm volatile("");
            }
        }
    }
    globalSequenceNumberGeneratorMtx_.lock();
    uint32_t currentSequenceNumber = globalSequenceNumber_++;
    globalSequenceNumberGeneratorMtx_.unlock();
    std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
    debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, writeBatchDeque[currentWriteBatchDequeInUse]->size());
    if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber_) {
        // flush old one
        notifyWriteBatchMQ_->push(writeBatchDeque[currentWriteBatchDequeInUse]);
        debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
        // insert to another deque
        if (currentWriteBatchDequeInUse == 1) {
            currentWriteBatchDequeInUse = 0;
        } else {
            currentWriteBatchDequeInUse = 1;
        }
        writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kMergeOp, key, value, currentSequenceNumber));
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).push_back(writeBatchSearch_t(kMergeOp, value, currentSequenceNumber));
        } else {
            deque<writeBatchSearch_t> tempDeque;
            tempDeque.push_back(writeBatchSearch_t(kMergeOp, value, currentSequenceNumber));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].insert(make_pair(key, tempDeque));
        }
        return true;
    } else {
        // only insert
        writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kMergeOp, key, value, currentSequenceNumber));
        if (writeBatchMapForSearch_[currentWriteBatchDequeInUse].find(key) != writeBatchMapForSearch_[currentWriteBatchDequeInUse].end()) {
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].at(key).push_back(writeBatchSearch_t(kMergeOp, value, currentSequenceNumber));
        } else {
            deque<writeBatchSearch_t> tempDeque;
            tempDeque.push_back(writeBatchSearch_t(kMergeOp, value, currentSequenceNumber));
            writeBatchMapForSearch_[currentWriteBatchDequeInUse].insert(make_pair(key, tempDeque));
        }
        return true;
    }
}

bool DeltaKV::performInBatchedBufferPartialMerge(deque<tuple<DBOperationType, string, string, uint32_t>>*& operationsQueue)
{
    uint32_t sequenceNumberBegin = std::get<3>(operationsQueue->front());
    debug_info("PreMerge operations, current queue size = %lu, sequence number at begin = %u\n", operationsQueue->size(), sequenceNumberBegin);
    unordered_map<string, pair<string, vector<string>>> performPreMergeMap;
    for (auto it : *operationsQueue) {
        DBOperationType currentOpType = std::get<0>(it);
        string keyStr = std::get<1>(it);
        string valueStr = std::get<2>(it);
        switch (currentOpType) {
        case kPutOp:
            if (performPreMergeMap.find(keyStr) != performPreMergeMap.end()) {
                performPreMergeMap.at(keyStr).second.clear(); // find new value, existing deltas are invalid
                performPreMergeMap.at(keyStr).first = valueStr;
            } else {
                vector<string> deltas;
                performPreMergeMap.insert(make_pair(keyStr, make_pair(valueStr, deltas)));
            }
            break;
        case kMergeOp:
            if (performPreMergeMap.find(keyStr) != performPreMergeMap.end()) {
                performPreMergeMap.at(keyStr).second.push_back(valueStr);
            } else {
                vector<string> deltas;
                deltas.push_back(valueStr);
                performPreMergeMap.insert(make_pair(keyStr, make_pair("", deltas)));
            }
            break;
        default:
            debug_error("[ERROR] get unknown operation type in operation queue, operation type = %d\n", currentOpType);
            break;
        }
    }
    debug_info("PerformPreMergeMap include key number = %lu\n", performPreMergeMap.size());
    operationsQueue->clear(); // clear for further usage
    for (auto it : performPreMergeMap) {
        if (it.second.first.size() > 0 && it.second.second.size() > 0) {
            // find new value, and new delta
            string finalValue;
            bool mergeStatus = deltaKVMergeOperatorPtr_->Merge(it.second.first, it.second.second, &finalValue);
            if (mergeStatus == false) {
                debug_error("[ERROR] COuld not merge for key = %s, delta number = %lu\n", it.first.c_str(), it.second.second.size());
            }
            it.second.first = finalValue;
            it.second.second.clear(); // merged, clean up
        }
        debug_trace("PerformPreMergeMap include key = %s, value size = %lu, delta number = %lu\n", it.first.c_str(), it.second.first.size(), it.second.second.size());
    }
    // push back to operation queue
    uint32_t currentSequenceNumber = sequenceNumberBegin;
    for (auto it : performPreMergeMap) {
        if (it.second.first.size() > 0) {
            operationsQueue->push_back(make_tuple(kPutOp, it.first, it.second.first, currentSequenceNumber));
            currentSequenceNumber++;
        }
        for (auto mergeIt : it.second.second) {
            operationsQueue->push_back(make_tuple(kMergeOp, it.first, mergeIt, currentSequenceNumber));
            currentSequenceNumber++;
        }
    }
    debug_info("After preMerge operations, current queue size = %lu, sequence number at last = %u\n", operationsQueue->size(), currentSequenceNumber);
    return true;
}

void DeltaKV::processBatchedOperationsWorker()
{
    while (true) {
        if (notifyWriteBatchMQ_->done_ == true && notifyWriteBatchMQ_->isEmpty() == true) {
            break;
        }
        deque<tuple<DBOperationType, string, string, uint32_t>>* currentHandler;
        if (notifyWriteBatchMQ_->pop(currentHandler)) {
            std::scoped_lock<std::shared_mutex> w_lock(batchedBufferOperationMtx_);
            oneBufferDuringProcessFlag_ = true;
            debug_info("process batched contents for object number = %lu\n", currentHandler->size());
            bool preMergeStatus = performInBatchedBufferPartialMerge(currentHandler);
            vector<string> keyToValueStoreVec, valueToValueStoreVec, keyToDeltaStoreVec, valueToDeltaStoreVec;
            vector<uint32_t> sequenceNumberVec, valueStoreSequenceNumberVec;
            vector<bool> isAnchorFlagToDeltaStoreVec;
            vector<externalIndexInfo*> storageInfoVec;
            for (auto it = currentHandler->begin(); it != currentHandler->end(); it++) {
                keyToDeltaStoreVec.push_back(std::get<1>(*it));
                valueToDeltaStoreVec.push_back(std::get<2>(*it));
                sequenceNumberVec.push_back(std::get<3>(*it));
                if (std::get<0>(*it) == kPutOp) {
                    keyToValueStoreVec.push_back(std::get<1>(*it));
                    valueToValueStoreVec.push_back(std::get<2>(*it));
                    valueStoreSequenceNumberVec.push_back(std::get<3>(*it));
                    isAnchorFlagToDeltaStoreVec.push_back(true);
                    externalIndexInfo externalInfoPlaceHolder;
                    storageInfoVec.push_back(&externalInfoPlaceHolder);
                } else if (std::get<0>(*it) == kMergeOp) {
                    isAnchorFlagToDeltaStoreVec.push_back(false);
                }
            }
            // bool putToIndexStoreStatus = false;
            bool putToDeltaStoreStatus = false;
            bool putToValueStoreStatus = false;
            switch (deltaKVRunningMode_) {
            case kBatchedWithPlainRocksDB: {
                rocksdb::Status rocksDBStatus;
                rocksdb::WriteOptions batchedWriteOperation;
                batchedWriteOperation.sync = false;
                auto valueIndex = 0;
                for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                    debug_info("Try write underlying rocksdb for key = %s\n", keyToDeltaStoreVec[index].c_str());
                    if (isAnchorFlagToDeltaStoreVec[index] == true) {
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, keyToValueStoreVec[valueIndex], valueToValueStoreVec[valueIndex]), StatsType::DELTAKV_PUT_ROCKSDB);
                        if (!rocksDBStatus.ok()) {
                            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                        }
                        valueIndex++;
                    } else {
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], valueToDeltaStoreVec[index]), StatsType::DELTAKV_MERGE_ROCKSDB);
                        if (!rocksDBStatus.ok()) {
                            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                        }
                    }
                }
                pointerToRawRocksDB_->FlushWAL(true);
                break;
            }
            case kBatchedWithBothValueAndDeltaLog:
                STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(keyToDeltaStoreVec, valueToDeltaStoreVec, sequenceNumberVec, isAnchorFlagToDeltaStoreVec), StatsType::DELTAKV_PUT_HASHSTORE);
                STAT_PROCESS(putToValueStoreStatus = IndexStoreInterfaceObjPtr_->multiPut(keyToValueStoreVec, valueToValueStoreVec, storageInfoVec, valueStoreSequenceNumberVec), StatsType::DELTAKV_PUT_INDEXSTORE);
                if (putToDeltaStoreStatus == true && putToValueStoreStatus == true) {
                    debug_info("Try write underlying rocksdb for %lu keys\n", keyToDeltaStoreVec.size());
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                        debug_info("Try write underlying rocksdb for key = %s\n", keyToDeltaStoreVec[index].c_str());
                        if (isAnchorFlagToDeltaStoreVec[index] != true) {
                            if (valueToDeltaStoreVec[index].size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
                                char writeInternalValueBuffer[sizeof(internalValueType)];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                                currentInternalValueType.valueSeparatedFlag_ = true;
                                currentInternalValueType.sequenceNumber_ = sequenceNumberVec[index];
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
                                rocksdb::Status rocksDBStatus;
                                STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                if (!rocksDBStatus.ok()) {
                                    debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                } else {
                                    debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", keyToDeltaStoreVec[index].c_str());
                                }
                            } else {
                                // not separate
                                char writeInternalValueBuffer[sizeof(internalValueType) + valueToDeltaStoreVec[index].size()];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                                currentInternalValueType.valueSeparatedFlag_ = false;
                                currentInternalValueType.sequenceNumber_ = sequenceNumberVec[index];
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueToDeltaStoreVec[index].c_str(), valueToDeltaStoreVec[index].size());
                                string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + valueToDeltaStoreVec[index].size());
                                rocksdb::Status rocksDBStatus;
                                STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                if (!rocksDBStatus.ok()) {
                                    debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                } else {
                                    debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", keyToDeltaStoreVec[index].c_str());
                                }
                            }
                        }
                    }
                    debug_info("Write underlying rocksdb for %lu keys done \n", keyToDeltaStoreVec.size());
                } else {
                    debug_error("[ERROR] Could not put into delta store via multiput, operations number = %lu\n", keyToDeltaStoreVec.size());
                }
                break;
            case kBatchedWithOnlyValueLog: {
                STAT_PROCESS(putToValueStoreStatus = IndexStoreInterfaceObjPtr_->multiPut(keyToValueStoreVec, valueToValueStoreVec, storageInfoVec, valueStoreSequenceNumberVec), StatsType::DELTAKV_PUT_INDEXSTORE);
                if (putToValueStoreStatus == true) {
                    rocksdb::Status rocksDBStatus;
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    auto valueIndex = 0;
                    for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                        debug_info("Try write underlying rocksdb for key = %s\n", keyToDeltaStoreVec[index].c_str());
                        if (isAnchorFlagToDeltaStoreVec[index] == false) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueToDeltaStoreVec[index].size()];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.sequenceNumber_ = sequenceNumberVec[index];
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueToDeltaStoreVec[index].c_str(), valueToDeltaStoreVec[index].size());
                            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + valueToDeltaStoreVec[index].size());
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                            } else {
                                debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", keyToDeltaStoreVec[index].c_str());
                            }
                        }
                    }
                    pointerToRawRocksDB_->FlushWAL(true);
                } else {
                    debug_error("[ERROR] Could not put into value store via multiput, operations number = %lu\n", keyToValueStoreVec.size());
                }
                break;
            }
            case kBatchedWithOnlyDeltaLog:
                STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(keyToDeltaStoreVec, valueToDeltaStoreVec, sequenceNumberVec, isAnchorFlagToDeltaStoreVec), StatsType::DELTAKV_PUT_HASHSTORE);
                if (putToDeltaStoreStatus == true) {
                    debug_info("Try write underlying rocksdb for %lu keys\n", keyToDeltaStoreVec.size());
                    rocksdb::WriteOptions batchedWriteOperation;
                    batchedWriteOperation.sync = false;
                    auto valueIndex = 0;
                    for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                        debug_info("Try write underlying rocksdb for key = %s\n", keyToDeltaStoreVec[index].c_str());
                        if (isAnchorFlagToDeltaStoreVec[index] == true) {
                            char writeInternalValueBuffer[sizeof(internalValueType) + valueToValueStoreVec[valueIndex].size()];
                            internalValueType currentInternalValueType;
                            currentInternalValueType.mergeFlag_ = false;
                            currentInternalValueType.valueSeparatedFlag_ = false;
                            currentInternalValueType.rawValueSize_ = valueToValueStoreVec[valueIndex].size();
                            currentInternalValueType.sequenceNumber_ = valueStoreSequenceNumberVec[valueIndex];
                            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                            memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueToValueStoreVec[valueIndex].c_str(), valueToValueStoreVec[valueIndex].size());
                            string newWriteValueStr(writeInternalValueBuffer, sizeof(internalValueType) + valueToValueStoreVec[valueIndex].size());
                            rocksdb::Status rocksDBStatus;
                            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(batchedWriteOperation, keyToValueStoreVec[valueIndex], newWriteValueStr), StatsType::DELTAKV_PUT_ROCKSDB);
                            if (!rocksDBStatus.ok()) {
                                debug_error("[ERROR] Write underlying rocksdb with added value header fault, key = %s, value = %s, status = %s\n", keyToValueStoreVec[valueIndex].c_str(), valueToValueStoreVec[valueIndex].c_str(), rocksDBStatus.ToString().c_str());
                            } else {
                                debug_trace("Write underlying rocksdb with added value header succes, key = %s\n", keyToValueStoreVec[valueIndex].c_str());
                            }
                            valueIndex++;
                        } else {
                            if (valueToDeltaStoreVec[index].size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
                                char writeInternalValueBuffer[sizeof(internalValueType)];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                                currentInternalValueType.valueSeparatedFlag_ = true;
                                currentInternalValueType.sequenceNumber_ = sequenceNumberVec[index];
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
                                rocksdb::Status rocksDBStatus;
                                STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                if (!rocksDBStatus.ok()) {
                                    debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                } else {
                                    debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", keyToDeltaStoreVec[index].c_str());
                                }
                            } else {
                                // not separate
                                char writeInternalValueBuffer[sizeof(internalValueType) + valueToDeltaStoreVec[index].size()];
                                internalValueType currentInternalValueType;
                                currentInternalValueType.mergeFlag_ = false;
                                currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                                currentInternalValueType.valueSeparatedFlag_ = false;
                                currentInternalValueType.sequenceNumber_ = sequenceNumberVec[index];
                                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                                memcpy(writeInternalValueBuffer + sizeof(internalValueType), valueToDeltaStoreVec[index].c_str(), valueToDeltaStoreVec[index].size());
                                string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + valueToDeltaStoreVec[index].size());
                                rocksdb::Status rocksDBStatus;
                                STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(batchedWriteOperation, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                                if (!rocksDBStatus.ok()) {
                                    debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                                } else {
                                    debug_trace("Merge underlying rocksdb with added value header succes, key = %s\n", keyToDeltaStoreVec[index].c_str());
                                }
                            }
                        }
                    }
                    debug_info("Write underlying rocksdb for %lu keys done \n", keyToDeltaStoreVec.size());
                } else {
                    debug_error("[ERROR] Could not put into delta store via multiput, operations number = %lu\n", keyToDeltaStoreVec.size());
                }
                break;
            default:
                debug_error("[ERROR] unknown running mode = %d", deltaKVRunningMode_);
                break;
            }
            // update write buffers
            debug_info("process batched contents done, start update write buffer's map, target update key number = %lu\n", keyToDeltaStoreVec.size());
            if (currentWriteBatchDequeInUse == 0) {
                writeBatchMapForSearch_[1].clear();
            } else {
                writeBatchMapForSearch_[0].clear();
            }
            currentHandler->clear();
            oneBufferDuringProcessFlag_ = false;
            debug_info("process batched contents done, not cleaned object number = %lu\n", currentHandler->size());
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
} // namespace DELTAKV_NAMESPACE
