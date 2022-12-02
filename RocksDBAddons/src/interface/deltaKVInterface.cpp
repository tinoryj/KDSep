#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // request merge operation when the value is found
    cerr << "Full merge value = " << existing_value << endl;
    string filteredOperandStr;
    string newValueIndexStr;
    bool findUpdatedValueIndex = false;
    for (auto operandListIt : operand_list) {
        internalValueType tempInternalValueTypeStructForCheck;
        memcpy(&tempInternalValueTypeStructForCheck, operandListIt.c_str(), sizeof(internalValueType));
        if (tempInternalValueTypeStructForCheck.mergeFlag_ == false) {
            filteredOperandStr.append(operandListIt);
        } else {
            findUpdatedValueIndex = true;
            newValueIndexStr.assign(operandListIt);
        }
    }
    if (findUpdatedValueIndex == true) {
        new_value->assign(newValueIndexStr);
        new_value->append(filteredOperandStr);
    } else {
        char contentBuffer[existing_value->size()];
        memcpy(contentBuffer, existing_value->data(), existing_value->size());
        internalValueType tempInternalValueTypeStructForCheck;
        memcpy(&tempInternalValueTypeStructForCheck, existing_value->data(), sizeof(internalValueType));
        tempInternalValueTypeStructForCheck.mergeFlag_ = true;
        memcpy(contentBuffer, &tempInternalValueTypeStructForCheck, sizeof(internalValueType));
        string newValueStr(contentBuffer, existing_value->size());
        new_value->assign(newValueStr);
        new_value->append(filteredOperandStr);
    }
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

DeltaKV::DeltaKV()
{
}

DeltaKV::~DeltaKV()
{
    if (isBatchedOperationsWithBuffer_ == true) {
        delete notifyWriteBatchMQ_;
        delete writeBatchDeque[0];
        delete writeBatchDeque[1];
    }
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    // start threadPool, memPool, etc.
    launchThreadPool(options.deltaKV_thread_number_limit);
    // Rest merge function if delta/value separation enabled
    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocksdbRawOptions_.merge_operator.reset(new RocksDBInternalMergeOperator); // reset internal merge operator
        deltaKVMergeOperatorPtr_ = options.deltaKV_merge_operation_ptr;
    }
    rocksdb::Status s = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!s.ok()) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Can't open underlying rocksdb" << RESET << endl;
        return false;
    }
    // Create objects
    if (options.enable_batched_operations_ == true) {
        writeBatchDeque[0] = new deque<tuple<DBOperationType, string, string>>;
        writeBatchDeque[1] = new deque<tuple<DBOperationType, string, string>>;
        notifyWriteBatchMQ_ = new messageQueue<deque<tuple<DBOperationType, string, string>>*>;
        boost::asio::post(*threadpool_, boost::bind(&DeltaKV::processBatchedOperationsWorker, this));
        isBatchedOperationsWithBuffer_ = true;
    }

    if (options.enable_deltaStore == true && HashStoreInterfaceObjPtr_ == nullptr) {
        isDeltaStoreInUseFlag = true;
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_);
        // create deltaStore related threads
        boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::scheduleMetadataUpdateWorker, hashStoreFileManagerPtr_));
        uint64_t totalNumberOfThreadsAllowed = options.deltaStore_thread_number_limit - 1;
        if (totalNumberOfThreadsAllowed > 2) {
            uint64_t totalNumberOfThreadsForOperationAllowed = totalNumberOfThreadsAllowed / 2 + 1;
            uint64_t totalNumberOfThreadsForGCAllowed = totalNumberOfThreadsAllowed - totalNumberOfThreadsForOperationAllowed;
            for (auto threadID = 0; threadID < totalNumberOfThreadsForGCAllowed; threadID++) {
                boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
            }
            for (auto threadID = 0; threadID < totalNumberOfThreadsForOperationAllowed; threadID++) {
                boost::asio::post(*threadpool_, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
            }
        } else {
            boost::asio::post(*threadpool_, boost::bind(&HashStoreFileManager::processGCRequestWorker, hashStoreFileManagerPtr_));
            boost::asio::post(*threadpool_, boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
        }
    }
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        isValueStoreInUseFlag = true;
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
    }
    return true;
}

bool DeltaKV::Close()
{
    if (isBatchedOperationsWithBuffer_ == true) {
        for (auto i = 0; i < 2; i++) {
            if (writeBatchDeque[i]->size() != 0) {
                notifyWriteBatchMQ_->push(writeBatchDeque[i]);
            }
        }
        notifyWriteBatchMQ_->done_ = true;
    }
    if (isDeltaStoreInUseFlag == true) {
        HashStoreInterfaceObjPtr_->forcedManualGarbageCollection();
        HashStoreInterfaceObjPtr_->setJobDone();
    }
    deleteThreadPool();
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        delete IndexStoreInterfaceObjPtr_;
        // delete related object pointers
    }
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        delete HashStoreInterfaceObjPtr_;
        // delete related object pointers
        delete hashStoreFileManagerPtr_;
        delete hashStoreFileOperatorPtr_;
    }
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
    return true;
}

bool DeltaKV::PutWithPlainRocksDB(const string& key, const string& value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with raw value fault" << RESET << endl;
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::MergeWithPlainRocksDB(const string& key, const string& value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with raw value fault" << RESET << endl;

        return false;
    } else {
        return true;
    }
}

bool DeltaKV::GetWithPlainRocksDB(const string& key, string* value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, value);
    if (!s.ok()) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read underlying rocksdb fault" << RESET << endl;

        return false;
    } else {
        return true;
    }
}

bool DeltaKV::PutWithOnlyValueStore(const string& key, const string& value)
{
    if (value.size() >= IndexStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        externalIndexInfo currentExternalIndexInfo;
        bool status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType) + sizeof(externalIndexInfo)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.valueSeparatedFlag_ = true;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            memcpy(writeInternalValueBuffer + sizeof(internalValueType), &currentExternalIndexInfo, sizeof(externalIndexInfo));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + sizeof(externalIndexInfo));
            rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, newWriteValue);
            if (!s.ok()) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

                return false;
            } else {
                return true;
            }
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write value to external storage fault" << RESET << endl;

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
        rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, newWriteValue);
        if (!s.ok()) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with raw value fault" << RESET << endl;

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
    rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValueStr);
    if (!s.ok()) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Merge underlying rocksdb with interanl value header fault" << RESET << endl;

        return false;
    } else {
        return true;
    }
}

bool DeltaKV::GetWithOnlyValueStore(const string& key, string* value)
{
    string internalValueStr;
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr);
    // check value status
    internalValueType tempInternalValueHeader;
    memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
    string rawValueStr;
    if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
        // get value from value store first
        externalIndexInfo tempReadExternalStorageInfo;
        memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
        string tempReadValueStr;
        IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr);
        rawValueStr.assign(tempReadValueStr);
    } else {
        char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
        memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
        string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
        rawValueStr.assign(internalRawValueStr);
    }
    if (tempInternalValueHeader.mergeFlag_ == true) {
        // get deltas from delta store
        vector<pair<bool, string>> deltaInfoVec;
        processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec);
        vector<string> finalDeltaOperatorsVec;
        auto index = 0;
        for (auto i = 0; i < deltaInfoVec.size(); i++) {
            if (deltaInfoVec[i].first == true) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Request external deltaStore when no KD separation enabled (Internal value error)" << RESET << endl;

                return false;
            } else {
                finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
            }
        }
        if (deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value) != true) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): DeltaKV merge operation fault" << RESET << endl;

            return false;
        } else {
            return true;
        }
    } else {
        value->assign(rawValueStr);
        return true;
    }
}

bool DeltaKV::PutWithOnlyDeltaStore(const string& key, const string& value)
{
    // for merge flag check, add internal value header to raw value
    char writeInternalValueBuffer[sizeof(internalValueType) + value.size()];
    internalValueType currentInternalValueType;
    currentInternalValueType.mergeFlag_ = false;
    currentInternalValueType.valueSeparatedFlag_ = false;
    currentInternalValueType.rawValueSize_ = value.size();
    memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
    memcpy(writeInternalValueBuffer + sizeof(internalValueType), value.c_str(), value.size());
    string newWriteValueStr(writeInternalValueBuffer, sizeof(internalValueType) + value.size());
    rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, newWriteValueStr);
    if (!s.ok()) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with added value header fault" << RESET << endl;

        return false;
    } else {
        // need to append anchor to delta store
        bool addAnchorStatus = HashStoreInterfaceObjPtr_->put(key, value, true);
        if (addAnchorStatus == true) {
            return true;
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Append anchor to delta store fault" << RESET << endl;

            return false;
        }
    }
}

bool DeltaKV::MergeWithOnlyDeltaStore(const string& key, const string& value)
{
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status = HashStoreInterfaceObjPtr_->put(key, value, false);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
            rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValue);
            if (!s.ok()) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

                return false;
            } else {
                return true;
            }
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write value to external storage fault" << RESET << endl;

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
        rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValue);
        if (!s.ok()) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::GetWithOnlyDeltaStore(const string& key, string* value)
{
    string internalValueStr;
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr);
    if (!s.ok()) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read underlying rocksdb fault" << RESET << endl;
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
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + tempInternalValueHeader.rawValueSize_, deltaInfoVec);
            debug_trace("read deltaInfoVec from LSM-tree size = %lu\n", deltaInfoVec.size());
            vector<string>* deltaValueFromExternalStoreVec = new vector<string>;
            if (HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec) != true) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read external deltaStore fault" << RESET << endl;
                delete deltaValueFromExternalStoreVec;
                return false;
            } else {
                vector<string> finalDeltaOperatorsVec;
                auto index = 0;
                debug_trace("Read from deltaStore object number = %lu\n", deltaValueFromExternalStoreVec->size());
                for (auto i = 0; i < deltaInfoVec.size(); i++) {
                    if (deltaInfoVec[i].first == true) {
                        finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                        index++;
                    } else {
                        finalDeltaOperatorsVec.push_back(deltaInfoVec[i].second);
                    }
                }
                if (index != deltaValueFromExternalStoreVec->size()) {
                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read external deltaStore number mismatch with requested number (Inconsistent), deltaValueFromExternalStoreVec.size = " << deltaValueFromExternalStoreVec->size() << ", current index = " << index << RESET << endl;
                    delete deltaValueFromExternalStoreVec;
                    return false;
                } else {
                    debug_trace("Start DeltaKV merge operation, internalRawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", internalRawValueStr.c_str(), finalDeltaOperatorsVec.size());
                    deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value);
                    delete deltaValueFromExternalStoreVec;
                    return true;
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
    if (value.size() >= IndexStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        externalIndexInfo currentExternalIndexInfo;
        bool status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType) + sizeof(externalIndexInfo)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.rawValueSize_ = value.size();
            currentInternalValueType.valueSeparatedFlag_ = true;
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            memcpy(writeInternalValueBuffer + sizeof(internalValueType), &currentExternalIndexInfo, sizeof(externalIndexInfo));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType) + sizeof(externalIndexInfo));
            rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, newWriteValue);
            if (!s.ok()) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

                return false;
            } else {
                bool updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, true);
                if (updateDeltaStoreWithAnchorFlagstatus == true) {
                    return true;
                } else {

                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Update anchor to current key fault" << RESET << endl;

                    return false;
                }
            }
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write value to external storage fault" << RESET << endl;

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
        rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, newWriteValue);
        if (!s.ok()) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with raw value fault" << RESET << endl;

            return false;
        } else {
            bool updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, true);
            if (updateDeltaStoreWithAnchorFlagstatus == true) {
                return true;
            } else {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Update anchor to current key fault" << RESET << endl;

                return false;
            }
        }
    }
}

bool DeltaKV::MergeWithValueAndDeltaStore(const string& key, const string& value)
{
    if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
        bool status = HashStoreInterfaceObjPtr_->put(key, value, false);
        if (status == true) {
            char writeInternalValueBuffer[sizeof(internalValueType)];
            internalValueType currentInternalValueType;
            currentInternalValueType.mergeFlag_ = false;
            currentInternalValueType.valueSeparatedFlag_ = true;
            currentInternalValueType.rawValueSize_ = value.size();
            memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
            string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
            rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValue);
            if (!s.ok()) {

                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

                return false;
            } else {
                return true;
            }
        } else {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write value to external storage fault" << RESET << endl;

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
        rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValue);
        if (!s.ok()) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;

            return false;
        } else {
            return true;
        }
    }
}

bool DeltaKV::GetWithValueAndDeltaStore(const string& key, string* value)
{
    string internalValueStr;
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr);
    if (!s.ok()) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read underlying rocksdb fault" << RESET << endl;

        return false;
    } else {
        internalValueType tempInternalValueHeader;
        memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
        string rawValueStr;
        if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
            // read value from value store
            externalIndexInfo tempReadExternalStorageInfo;
            memcpy(&tempReadExternalStorageInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo));
            string tempReadValueStr;
            IndexStoreInterfaceObjPtr_->get(key, tempReadExternalStorageInfo, &tempReadValueStr);
            rawValueStr.assign(tempReadValueStr);
        } else {
            char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
            memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
            string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);
            rawValueStr.assign(internalRawValueStr);
        }
        if (tempInternalValueHeader.mergeFlag_ == true) {
            // get deltas from delta store
            vector<pair<bool, string>> deltaInfoVec;
            processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec);
            bool isAnyDeltasAreExtratedFlag = false;
            for (auto it : deltaInfoVec) {
                if (it.first == true) {
                    isAnyDeltasAreExtratedFlag = true;
                }
            }
            if (isAnyDeltasAreExtratedFlag == true) {
                // should read external delta store
                vector<string>* deltaValueFromExternalStoreVec = new vector<string>;
                if (HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec) != true) {

                    cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read external deltaStore fault" << RESET << endl;

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

                        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read external deltaStore number mismatch with requested number (Inconsistent), deltaValueFromExternalStoreVec.size = " << deltaValueFromExternalStoreVec->size() << ", current index = " << index << RESET << endl;

                        delete deltaValueFromExternalStoreVec;
                        return false;
                    } else {
                        debug_trace("Start DeltaKV merge operation, rawValueStr = %s, finalDeltaOperatorsVec.size = %lu\n", rawValueStr.c_str(), finalDeltaOperatorsVec.size());
                        deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value);
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
                return true;
            }

        } else {
            value->assign(rawValueStr);
            return true;
        }
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == true) {
        if (PutWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag == false && isValueStoreInUseFlag == true) {
        if (PutWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == false) {
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
    if (isBatchedOperationsWithBuffer_ == true) {
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
    if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == true) {
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
    } else if (isDeltaStoreInUseFlag == false && isValueStoreInUseFlag == true) {
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
    } else if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == false) {
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
    if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == true) {
        if (MergeWithValueAndDeltaStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag == false && isValueStoreInUseFlag == true) {
        if (MergeWithOnlyValueStore(key, value) == false) {
            return false;
        } else {
            return true;
        }
    } else if (isDeltaStoreInUseFlag == true && isValueStoreInUseFlag == false) {
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
        rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), currentKey, &tempValue);
        values->push_back(tempValue);
        if (!s.ok()) {
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
    rocksdb::Status s = pointerToRawRocksDB_->SingleDelete(rocksdb::WriteOptions(), key);
    if (!s.ok()) {
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::PutWithWriteBatch(const string& key, const string& value)
{
    if (isBatchedOperationsWithBuffer_ == true) {
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
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Batched operation with buffer not enabled " << RESET << endl;
        return false;
    }
}

bool DeltaKV::MergeWithWriteBatch(const string& key, const string& value)
{
    if (isBatchedOperationsWithBuffer_ == true) {
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

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Batched operation with buffer not enabled " << RESET << endl;

        return false;
    }
}

void DeltaKV::processBatchedOperationsWorker()
{
    if (notifyWriteBatchMQ_ == nullptr) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): message queue not initial" << RESET << endl;

        return;
    }
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
            // commit to delta store
            bool putToDeltaStoreStatus = HashStoreInterfaceObjPtr_->multiPut(keyToDeltaStoreVec, valueToDeltaStoreVec, isAnchorFlagToDeltaStoreVec);
            if (putToDeltaStoreStatus == false) {
                cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write batched objects to underlying DeltaStore fault" << RESET << endl;
            } else {
                for (auto index = 0; index < keyToDeltaStoreVec.size(); index++) {
                    if (isAnchorFlagToDeltaStoreVec[index] == true) {
                        // write value;
                    } else {
                        char writeInternalValueBuffer[sizeof(internalValueType)];
                        internalValueType currentInternalValueType;
                        currentInternalValueType.mergeFlag_ = false;
                        currentInternalValueType.rawValueSize_ = valueToDeltaStoreVec[index].size();
                        currentInternalValueType.valueSeparatedFlag_ = true;
                        memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                        string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
                        rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), keyToDeltaStoreVec[index], newWriteValue);
                        if (!s.ok()) {
                            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Write underlying rocksdb with external storage index fault" << RESET << endl;
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

// TODO: upper functions are not complete

bool DeltaKV::launchThreadPool(uint64_t totalThreadNumber)
{
    threadpool_ = new boost::asio::thread_pool(totalThreadNumber);
    return true;
}

bool DeltaKV::deleteThreadPool()
{
    threadpool_->join();
    delete threadpool_;
    return true;
}

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>& mergeOperatorsVec)
{
    uint64_t internalValueSize = internalValue.size();
    debug_trace("internalValueSize = %lu, skipSize = %lu\n", internalValueSize, skipSize);
    uint64_t currentProcessLocationIndex = skipSize;
    while (currentProcessLocationIndex != internalValueSize) {
        internalValueType currentInternalValueTypeHeader;
        memcpy(&currentInternalValueTypeHeader, internalValue.c_str() + currentProcessLocationIndex, sizeof(internalValueType));
        currentProcessLocationIndex += sizeof(internalValueType);
        if (currentInternalValueTypeHeader.valueSeparatedFlag_ != true) {
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
