#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // request merge operation when the value is found
    debug_trace("Full merge value size = %lu, content = %s\n", existing_value->size(), existing_value->ToString().c_str());
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
    string leftOpStr(left_operand.data(), left_operand.size());
    string rightOpStr(right_operand.data(), right_operand.size());
    new_value->assign(leftOpStr);
    new_value->append(rightOpStr);
    return true;
};

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
    if (isBatchedOperationsWithBufferInUse_ == true) {
        delete notifyWriteBatchMQ_;
        delete writeBatchDeque[0];
        delete writeBatchDeque[1];
    }
    if (notifyWriteBackMQ_) {
        string* str;
        while (notifyWriteBackMQ_->pop(str)) {
            delete str;
        }
        delete notifyWriteBackMQ_;
    }
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    boost::thread::attributes attrs;
    attrs.set_stack_size(1000 * 1024 * 1024);
    // Rest merge function if delta/value separation enabled
    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocksdbRawOptions_.merge_operator.reset(new RocksDBInternalMergeOperator); // reset internal merge operator
        deltaKVMergeOperatorPtr_ = options.deltaKV_merge_operation_ptr;
    }
    if (options.rocksdb_sync_put) {
        internalWriteOption_.sync = true;
    }
    if (options.rocksdb_sync_merge) {
        internalMergeOption_.sync = true;
    }
    rocksdb::Status rocksDBStatus = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Can't open underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }
    // Create objects
    if (options.enable_batched_operations_ == true) {
        writeBatchDeque[0] = new deque<tuple<DBOperationType, string, string>>;
        writeBatchDeque[1] = new deque<tuple<DBOperationType, string, string>>;
        notifyWriteBatchMQ_ = new messageQueue<deque<tuple<DBOperationType, string, string>>*>;
        // boost::asio::post(*threadpool_, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        thList_.push_back(th);
        isBatchedOperationsWithBufferInUse_ = true;
        cerr << "Enabled batch operations" << endl;
    }

    if (options.enable_write_back_optimization_ == true) {
        enableWriteBackOperationsFlag_ = true;
        writeBackWhenReadDeltaNumerThreshold_ = options.deltaStore_write_back_during_reads_threshold;
        writeBackOperationsQueue_ = new messageQueue<writeBackObjectPair*>;
        boost::thread* th = new boost::thread(attrs, boost::bind(&DeltaKV::processWriteBackOperationsWorker, this));
        thList_.push_back(th);
    }

    if (options.enable_deltaStore == true && HashStoreInterfaceObjPtr_ == nullptr) {
        isDeltaStoreInUseFlag_ = true;
        notifyWriteBackMQ_ = new messageQueue<string*>;

        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, notifyWriteBackMQ_);
        // create deltaStore related threads
        // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::scheduleMetadataUpdateWorker, hashStoreFileManagerPtr_));
        boost::thread* th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::scheduleMetadataUpdateWorker, hashStoreFileManagerPtr_));
        thList_.push_back(th);
        uint64_t totalNumberOfThreadsAllowed = options.deltaStore_thread_number_limit - 1;
        if (totalNumberOfThreadsAllowed > 2) {
            if (options.enable_deltaStore_garbage_collection == true) {
                enableDeltaStoreWithBackgroundGCFlag_ = true;
                uint64_t totalNumberOfThreadsForOperationAllowed = totalNumberOfThreadsAllowed / 2 + 1;
                uint64_t totalNumberOfThreadsForGCAllowed = totalNumberOfThreadsAllowed - totalNumberOfThreadsForOperationAllowed;
                for (auto threadID = 0; threadID < totalNumberOfThreadsForGCAllowed; threadID++) {
                    // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
                    th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
                    thList_.push_back(th);
                }
                for (auto threadID = 0; threadID < totalNumberOfThreadsForOperationAllowed; threadID++) {
                    // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
                    th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
                    thList_.push_back(th);
                }
            } else {
                enableDeltaStoreWithBackgroundGCFlag_ = false;
                for (auto threadID = 0; threadID < totalNumberOfThreadsAllowed; threadID++) {
                    // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
                    th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
                    thList_.push_back(th);
                }
            }
        } else {
            // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
            // boost::asio::post(*threadpool_, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
            if (options.enable_deltaStore_garbage_collection == true) {
                enableDeltaStoreWithBackgroundGCFlag_ = true;
                th = new boost::thread(attrs, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
                thList_.push_back(th);
            } else {
                enableDeltaStoreWithBackgroundGCFlag_ = false;
            }
            th = new boost::thread(attrs, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
            thList_.push_back(th);
        }
    }
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        isValueStoreInUseFlag_ = true;
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
    }
    return true;
}

bool DeltaKV::Close()
{
    if (enableWriteBackOperationsFlag_ == true) {
        writeBackOperationsQueue_->done_ = true;
    }
    if (isBatchedOperationsWithBufferInUse_ == true) {
        for (auto i = 0; i < 2; i++) {
            if (writeBatchDeque[i]->size() != 0) {
                notifyWriteBatchMQ_->push(writeBatchDeque[i]);
            }
        }
        notifyWriteBatchMQ_->done_ = true;
    }
    if (isDeltaStoreInUseFlag_ == true) {
        if (enableDeltaStoreWithBackgroundGCFlag_ == true) {
            HashStoreInterfaceObjPtr_->forcedManualGarbageCollection();
        }
        HashStoreInterfaceObjPtr_->setJobDone();
    }
    deleteExistingThreads();
    if (enableWriteBackOperationsFlag_ == true) {
        delete writeBackOperationsQueue_;
    }
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        delete HashStoreInterfaceObjPtr_;
        // delete related object pointers
        delete hashStoreFileManagerPtr_;
        delete hashStoreFileOperatorPtr_;
    }
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        delete IndexStoreInterfaceObjPtr_;
        // delete related object pointers
    }
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
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
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType) + sizeof(externalIndexInfo)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.sequenceNumber_ = currentSequenceNumber;
            currentInternalValueType.valueSeparatedFlag_ = true;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            memcpy(writeInternalValueBuffer + sizeof(internalValueType), &currentExternalIndexInfo, sizeof(externalIndexInfo));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + sizeof(externalIndexInfo));
            rocksdb::Status rocksDBStatus;
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
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
        currentInternalValueType.rawValueSize_ = value.size();
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
    // also need internal value header since value store GC may update fake header as merge
    char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.valueSeparatedFlag_ = false;
    currentInternalValueType.rawValueSize_ = value.size();
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

bool DeltaKV::GetWithOnlyValueStore(const string& key, string* value)
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
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo);
        } else {
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo);
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
        STAT_PROCESS(addAnchorStatus = HashStoreInterfaceObjPtr_->put(key, value, true), StatsType::DELTAKV_PUT_HASHSTORE);
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
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(key, value, false), StatsType::DELTAKV_HASHSTORE_PUT);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
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

bool DeltaKV::GetWithOnlyDeltaStore(const string& key, string* value)
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
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo);
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
                debug_trace("Read from deltaStore object number = %lu\n", deltaValueFromExternalStoreVec->size());
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    if (deltaInfoVec[i].first == true) { // separated
                        finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                        index++;
                    } else {
                        finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                    }
                }

                if (index != deltaValueFromExternalStoreVec->size()) {
                    debug_error("[ERROR] Read external deltaStore number mismatch with requested number (Inconsistent), key = %s, index = %d, deltaValueFromExternalStoreVec.size = %lu\n", key.c_str(), index, deltaValueFromExternalStoreVec->size());
                    delete deltaValueFromExternalStoreVec;
                    return false;
                } else {
                    debug_trace("Start DeltaKV merge operation, internalRawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", internalRawValueStr.c_str(), finalDeltaOperatorsVec.size());
                    bool mergeOperationStatus = deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value);
                    if (mergeOperationStatus == true) {
                        if (enableWriteBackOperationsFlag_ == true && index > writeBackWhenReadDeltaNumerThreshold_) {
                            globalSequenceNumberGeneratorMtx_.lock();
                            uint32_t currentSequenceNumber = globalSequenceNumber_++;
                            globalSequenceNumberGeneratorMtx_.unlock();
                            writeBackObjectPair* newPair = new writeBackObjectPair(key, *value, currentSequenceNumber);
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
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType) + sizeof(externalIndexInfo)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.valueSeparatedFlag_ = true;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            memcpy(writeInternalValueBuffer + sizeof(internalValueType), &currentExternalIndexInfo, sizeof(externalIndexInfo));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + sizeof(externalIndexInfo));
            rocksdb::Status rocksDBStatus;
            // the write option should be not sync here. not touch
            STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
            if (!rocksDBStatus.ok()) {
                debug_error("[ERROR] Write underlying rocksdb with external storage index fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
                return false;
            } else {
                bool updateDeltaStoreWithAnchorFlagstatus;
                STAT_PROCESS(updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, true), StatsType::DELTAKV_PUT_HASHSTORE);
                if (updateDeltaStoreWithAnchorFlagstatus == true) {
                    return true;
                } else {
                    debug_error("[ERROR] Update anchor to current key fault, key = %s, value = %s\n", key.c_str(), value.c_str());
                    return false;
                }
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
        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
        memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, key, newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", key.c_str(), value.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        } else {
            bool updateDeltaStoreWithAnchorFlagstatus;
            STAT_PROCESS(updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, true), StatsType::DELTAKV_PUT_HASHSTORE);
            if (updateDeltaStoreWithAnchorFlagstatus == true) {
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
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status;
        STAT_PROCESS(status = HashStoreInterfaceObjPtr_->put(key, value, false), StatsType::DELTAKV_MERGE_HASHSTORE);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
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

bool DeltaKV::GetWithValueAndDeltaStore(const string& key, string* value)
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
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec, findNewValueIndexFlag, newExternalIndexInfo);
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
                    for (auto i = 0; i < deltaInfoVec.size(); i++) {
                        if (deltaInfoVec[i].first == true) {
                            finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                            index++;
                        } else {
                            finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                        }
                    }

                    if (index != deltaValueFromExternalStoreVec->size()) {
                        debug_error("[ERROR] Read external deltaStore number mismatch with requested number (Inconsistent), key %s, deltaValueFromExternalStoreVec.size = %lu, index = %d\n", key.c_str(), deltaValueFromExternalStoreVec->size(), index);
                        delete deltaValueFromExternalStoreVec;
                        return false;
                    } else {
                        debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                        deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value);
                        if (enableWriteBackOperationsFlag_ == true && index > writeBackWhenReadDeltaNumerThreshold_) {
                            globalSequenceNumberGeneratorMtx_.lock();
                            uint32_t currentSequenceNumber = globalSequenceNumber_++;
                            globalSequenceNumberGeneratorMtx_.unlock();
                            writeBackObjectPair* newPair = new writeBackObjectPair(key, *value, currentSequenceNumber);
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
                if (enableWriteBackOperationsFlag_ == true && deltaInfoVec.size() > writeBackWhenReadDeltaNumerThreshold_) {
                    globalSequenceNumberGeneratorMtx_.lock();
                    uint32_t currentSequenceNumber = globalSequenceNumber_++;
                    globalSequenceNumberGeneratorMtx_.unlock();
                    writeBackObjectPair* newPair = new writeBackObjectPair(key, *value, currentSequenceNumber);
                    writeBackOperationsQueue_->push(newPair);
                }
                return true;
            }
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
            value->assign(rawValueStr);
            return true;
        }
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == true) {
        // TODO not a good position... Think about it later
        tryWriteBack();
        if (PutWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag_ == false && isValueStoreInUseFlag_ == true) {
        if (PutWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == false) {
        // TODO not a good position... Think about it later
        tryWriteBack();
        if (PutWithOnlyDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else {
        if (PutWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::Get(const string& key, string* value)
{
    vector<string> tempNewMergeOperatorsVec;
    bool needMergeWithInBufferOperationsFlag = false;
    if (isBatchedOperationsWithBufferInUse_ == true) {
        // try read from buffer first;
        debug_info("try read from unflushed buffer for key = %s\n", key.c_str());
        batchedBufferOperationMtx_.lock();
        if (writeBatchMapForSearch_.find(key) != writeBatchMapForSearch_.end()) {
            if (writeBatchMapForSearch_.at(key).back().first == kPutOp) {
                value->assign(writeBatchMapForSearch_.at(key).back().second);
                batchedBufferOperationMtx_.unlock();
                debug_info("get value from unflushed buffer for key = %s\n", key.c_str());
                return true;
            }
            string newValueStr;
            bool findNewValueFlag = false;
            for (auto it : writeBatchMapForSearch_.at(key)) {
                if (it.first == kPutOp) {
                    newValueStr.assign(it.second);
                    tempNewMergeOperatorsVec.clear();
                    findNewValueFlag = true;
                } else {
                    tempNewMergeOperatorsVec.push_back(it.second);
                }
            }
            if (findNewValueFlag == true) {
                deltaKVMergeOperatorPtr_->Merge(newValueStr, tempNewMergeOperatorsVec, value);
                batchedBufferOperationMtx_.unlock();
                debug_info("get raw value and deltas from unflushed buffer, for key = %s, value = %s, deltas number = %lu\n", key.c_str(), newValueStr.c_str(), tempNewMergeOperatorsVec.size());
                return true;
            }
            if (tempNewMergeOperatorsVec.size() != 0) {
                needMergeWithInBufferOperationsFlag = true;
                debug_info("get deltas from unflushed buffer, for key = %s, deltas number = %lu\n", key.c_str(), tempNewMergeOperatorsVec.size());
            }
        }
        batchedBufferOperationMtx_.unlock();
    }
    if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == true) {
        // TODO not a good position... Think about it later
        tryWriteBack();
        if (GetWithValueAndDeltaStore(key, value) == false) {
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
    } else if (isDeltaStoreInUseFlag_ == false && isValueStoreInUseFlag_ == true) {
        if (GetWithOnlyValueStore(key, value) == false) {
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
    } else if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == false) {
        // TODO not a good position... Think about it later
        tryWriteBack();
        if (GetWithOnlyDeltaStore(key, value) == false) {
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
    } else {
        if (GetWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            return true;
        }
    }
    return false;
}

bool DeltaKV::Merge(const string& key, const string& value)
{
    if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == true) {
        if (MergeWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag_ == false && isValueStoreInUseFlag_ == true) {
        if (MergeWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag_ == true && isValueStoreInUseFlag_ == false) {
        if (MergeWithOnlyDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else {
        if (MergeWithPlainRocksDB(key, value) == false) {
            return false;
        } else {
            return true;
        }
    }
}

// TODO: following functions are not complete

vector<bool> DeltaKV::MultiGet(const vector<string>& keys, vector<string>* values)
{
    vector<bool> queryStatus;
    for (auto currentKey : keys) {
        string tempValue;
        rocksdb::Status rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), currentKey, &tempValue);
        values->push_back(tempValue);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb fault, key = %s, status = %s\n", currentKey.c_str(), rocksDBStatus.ToString().c_str());
            queryStatus.push_back(false);
        } else {
            queryStatus.push_back(true);
        }
    }
    return queryStatus;
}

vector<bool> DeltaKV::GetByPrefix(const string& targetKeyPrefix, vector<string>* keys, vector<string>* values)
{
    auto it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    it->Seek(targetKeyPrefix);
    vector<bool> queryStatus;
    for (int i = 0; it->Valid(); i++) {
        string tempKey, tempValue;
        tempKey = it->key().ToString();
        tempValue = it->value().ToString();
        it->Next();
        keys->push_back(tempKey);
        values->push_back(tempValue);
        if (tempValue.empty()) {
            queryStatus.push_back(false);
        } else {
            queryStatus.push_back(true);
        }
    }
    delete it;
    return queryStatus;
}

vector<bool> DeltaKV::GetByTargetNumber(const uint64_t& targetGetNumber, vector<string>* keys, vector<string>* values)
{
    vector<bool> queryStatus;
    rocksdb::Iterator* it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        keys->push_back(it->key().ToString());
        values->push_back(it->value().ToString());
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
    if (isBatchedOperationsWithBufferInUse_ == true) {
        debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, writeBatchDeque[currentWriteBatchDequeInUse]->size());
        if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber) {
            // flush old one
            notifyWriteBatchMQ_->push(writeBatchDeque[currentWriteBatchDequeInUse]);
            debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
            // insert to another deque
            if (currentWriteBatchDequeInUse == 1) {
                currentWriteBatchDequeInUse = 0;
            } else {
                currentWriteBatchDequeInUse = 1;
            }
            writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kPutOp, key, value));
            batchedBufferOperationMtx_.lock();
            if (writeBatchMapForSearch_.find(key) != writeBatchMapForSearch_.end()) {
                writeBatchMapForSearch_.at(key).push_back(make_pair(kPutOp, value));
            } else {
                deque<pair<DBOperationType, string>> tempDeque;
                tempDeque.push_back(make_pair(kPutOp, value));
                writeBatchMapForSearch_.insert(make_pair(key, tempDeque));
            }
            batchedBufferOperationMtx_.unlock();
            return true;
        } else {
            // only insert
            writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kPutOp, key, value));
            batchedBufferOperationMtx_.lock();
            if (writeBatchMapForSearch_.find(key) != writeBatchMapForSearch_.end()) {
                writeBatchMapForSearch_.at(key).push_back(make_pair(kPutOp, value));
            } else {
                deque<pair<DBOperationType, string>> tempDeque;
                tempDeque.push_back(make_pair(kPutOp, value));
                writeBatchMapForSearch_.insert(make_pair(key, tempDeque));
            }
            batchedBufferOperationMtx_.unlock();
            return true;
        }
    } else {
        debug_error("[ERROR] Batched operation with buffer not enabled, key = %s\n", key.c_str());
        return false;
    }
}

bool DeltaKV::MergeWithWriteBatch(const string& key, const string& value)
{
    if (isBatchedOperationsWithBufferInUse_ == true) {
        debug_info("Current buffer id = %lu, used size = %lu\n", currentWriteBatchDequeInUse, writeBatchDeque[currentWriteBatchDequeInUse]->size());
        if (writeBatchDeque[currentWriteBatchDequeInUse]->size() == maxBatchOperationBeforeCommitNumber) {
            // flush old one
            notifyWriteBatchMQ_->push(writeBatchDeque[currentWriteBatchDequeInUse]);
            debug_info("put batched contents into job worker, current buffer in use = %lu\n", currentWriteBatchDequeInUse);
            // insert to another deque
            if (currentWriteBatchDequeInUse == 1) {
                currentWriteBatchDequeInUse = 0;
            } else {
                currentWriteBatchDequeInUse = 1;
            }
            writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kMergeOp, key, value));
            batchedBufferOperationMtx_.lock();
            if (writeBatchMapForSearch_.find(key) != writeBatchMapForSearch_.end()) {
                writeBatchMapForSearch_.at(key).push_back(make_pair(kMergeOp, value));
            } else {
                deque<pair<DBOperationType, string>> tempDeque;
                tempDeque.push_back(make_pair(kMergeOp, value));
                writeBatchMapForSearch_.insert(make_pair(key, tempDeque));
            }
            batchedBufferOperationMtx_.unlock();
            return true;
        } else {
            // only insert
            writeBatchDeque[currentWriteBatchDequeInUse]->push_back(make_tuple(kMergeOp, key, value));
            batchedBufferOperationMtx_.lock();
            if (writeBatchMapForSearch_.find(key) != writeBatchMapForSearch_.end()) {
                writeBatchMapForSearch_.at(key).push_back(make_pair(kMergeOp, value));
            } else {
                deque<pair<DBOperationType, string>> tempDeque;
                tempDeque.push_back(make_pair(kMergeOp, value));
                writeBatchMapForSearch_.insert(make_pair(key, tempDeque));
            }
            batchedBufferOperationMtx_.unlock();
            return true;
        }
    } else {
        debug_error("[ERROR] Batched operation with buffer not enabled, key = %s\n", key.c_str());
        return false;
    }
}

void DeltaKV::processBatchedOperationsWorker()
{
    while (true) {
        deque<tuple<DBOperationType, string, string>>* currentHandler;
        if (notifyWriteBatchMQ_->pop(currentHandler)) {
            debug_info("process batched contents for object number = %lu\n", currentHandler->size());
            vector<string> keyToValueStoreVec, valueToValueStoreVec, keyToDeltaStoreVec, valueToDeltaStoreVec;
            vector<bool> isAnchorFlagToDeltaStoreVec;
            for (auto it = currentHandler->begin(); it != currentHandler->end(); it++) {
                if (std::get<0>(*it) == kPutOp) {
                    keyToValueStoreVec.push_back(std::get<1>(*it));
                    valueToValueStoreVec.push_back(std::get<2>(*it));
                    keyToDeltaStoreVec.push_back(std::get<1>(*it));
                    valueToDeltaStoreVec.push_back(std::get<2>(*it));
                    isAnchorFlagToDeltaStoreVec.push_back(true);
                } else if (std::get<0>(*it) == kMergeOp) {
                    keyToDeltaStoreVec.push_back(std::get<1>(*it));
                    valueToDeltaStoreVec.push_back(std::get<2>(*it));
                    isAnchorFlagToDeltaStoreVec.push_back(false);
                }
            }
            // commit to value store
            bool putToIndexStoreStatus;
            vector<externalIndexInfo*> storageInfoVecPtr;
            for (int i = 0; i < (int)keyToValueStoreVec.size(); i++) {
                storageInfoVecPtr.push_back(new externalIndexInfo);
            }

            STAT_PROCESS(putToIndexStoreStatus = IndexStoreInterfaceObjPtr_->multiPut(keyToValueStoreVec, valueToValueStoreVec, storageInfoVecPtr), StatsType::DELTAKV_PUT_INDEXSTORE);

            // commit to delta store
            bool putToDeltaStoreStatus;
            STAT_PROCESS(putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(keyToDeltaStoreVec, valueToDeltaStoreVec, isAnchorFlagToDeltaStoreVec), StatsType::DELTAKV_PUT_HASHSTORE);
            if (putToDeltaStoreStatus == false) {
                debug_error("[ERROR] Write batched objects to underlying DeltaStore fault, keyToDeltaStoreVec size = %lu\n", keyToDeltaStoreVec.size());
            } else if (putToIndexStoreStatus == false) {
                debug_error("[ERROR] Write batched objects to underlying IndexStore fault, keyToValueStoreVec size = %lu\n", keyToValueStoreVec.size());
            } else {
                uint64_t valueIndex = 0;
                for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                    if (isAnchorFlagToDeltaStoreVec[index] == true) {
                        char writeInternalValueBuffer[sizeof(internalValueType) + sizeof(externalIndexInfo)];
                        internalValueType currentInternalValueType;
                        currentInternalValueType.mergeFlag_ = false;
                        currentInternalValueType.rawValueSize_ = valueToValueStoreVec[valueIndex].size();
                        currentInternalValueType.valueSeparatedFlag_ = true;
                        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                        memcpy(writeInternalValueBuffer + sizeof(internalValueType), storageInfoVecPtr.at(valueIndex), sizeof(externalIndexInfo));
                        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + sizeof(externalIndexInfo));
                        rocksdb::Status rocksDBStatus;
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, keyToValueStoreVec[valueIndex], newWriteValue), StatsType::DELTAKV_PUT_ROCKSDB);
                        valueIndex++;
                    } else {
                        char writeInternalValueBuffer[sizeof(internalValueType)];
                        internalValueType currentInternalValueType;
                        currentInternalValueType.mergeFlag_ = false;
                        currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                        currentInternalValueType.valueSeparatedFlag_ = true;
                        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
                        rocksdb::Status rocksDBStatus;
                        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, keyToDeltaStoreVec[index], newWriteValue), StatsType::DELTAKV_MERGE_ROCKSDB);
                        if (!rocksDBStatus.ok()) {
                            debug_error("[ERROR] Write underlying rocksdb with external storage index fault, status = %s\n", rocksDBStatus.ToString().c_str());
                        }
                    }
                }
            }
            batchedBufferOperationMtx_.lock();
            for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                writeBatchMapForSearch_.at(keyToDeltaStoreVec[index]).pop_front();
                if (writeBatchMapForSearch_.at(keyToDeltaStoreVec[index]).size() == 0) {
                    writeBatchMapForSearch_.erase(keyToDeltaStoreVec[index]);
                }
            }
            batchedBufferOperationMtx_.unlock();
            currentHandler->clear();
            debug_info("process batched contents done, not cleaned object number = %lu\n", currentHandler->size());
        }
        if (notifyWriteBatchMQ_->done_ == true) {
            break;
        }
    }
    return;
}

void DeltaKV::processWriteBackOperationsWorker()
{
    while (true) {
        if (writeBackOperationsQueue_->done_ == true && writeBackOperationsQueue_->isEmpty() == true) {
            break;
        }
        writeBackObjectPair* currentProcessPair;
        if (writeBackOperationsQueue_->pop(currentProcessPair)) {
            debug_trace("Process write back content, key = %s, value =s %s\n", currentProcessPair->key.c_str(), currentProcessPair->value.c_str());
            bool ret;
            struct timeval tv;
            gettimeofday(&tv, 0);
            debug_info("Write back key %s\n", currentProcessPair->key.c_str());
            StatsRecorder::getInstance()->timeProcess(StatsType::DELTAKV_WRITE_BACK, tv);
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

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec, bool& findNewValueIndex, externalIndexInfo& newExternalIndexInfo)
{
    uint64_t internalValueSize = internalValue.size();
    debug_trace("internalValueSize = %lu, skipSize = %lu\n", internalValueSize, skipSize);
    uint64_t currentProcessLocationIndex = skipSize;
    while (currentProcessLocationIndex != internalValueSize) {
        internalValueType currentInternalValueTypeHeader;
        memcpy(&currentInternalValueTypeHeader, internalValue.c_str() + currentProcessLocationIndex, sizeof(internalValueType));
        currentProcessLocationIndex += sizeof(internalValueType);
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
