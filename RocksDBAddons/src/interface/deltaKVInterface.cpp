#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // request merge operation when the value is found
    char contentBuffer[existing_value->size()];
    memcpy(contentBuffer, existing_value->data(), existing_value->size());
    internalValueType tempInternalValueTypeStructForCheck;
    memcpy(&tempInternalValueTypeStructForCheck, existing_value->data(), sizeof(internalValueType));
    tempInternalValueTypeStructForCheck.mergeFlag_ = true;
    memcpy(contentBuffer, &tempInternalValueTypeStructForCheck, sizeof(internalValueType));
    string newValueStr(contentBuffer, existing_value->size());
    new_value->assign(newValueStr);
    for (auto operandListIt : operand_list) {
        new_value->append(operandListIt);
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

DeltaKV::DeltaKV(DeltaKVOptions& options, const string& name)
{
    // Create objects
    if (options.enable_deltaStore == true) {
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, hashStoreGCManagerPtr_);
    }
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] add HashStoreInterfaceObjPtr_ success" << RESET << endl;
    // start threadPool, memPool, etc.
    launchThreadPool(options.deltaKV_thread_number_limit);
    ioService_.post(boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] start thread for HashStoreFileOperator::operationWorker success" << RESET << endl;
}

DeltaKV::~DeltaKV()
{
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        delete HashStoreInterfaceObjPtr_;
        // delete related object pointers
        delete hashStoreFileManagerPtr_;
        delete hashStoreFileOperatorPtr_;
        delete hashStoreGCManagerPtr_;
    }
    deleteThreadPool();
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Deconstruction] delete thread pool and underlying rocksdb success" << RESET << endl;
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, name = " << name << RESET << endl;
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, pointerToRawRocksDB_ = " << &pointerToRawRocksDB_ << RESET << endl;
    rocksdb::Status s = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!s.ok()) {
        cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Construction] Can't open underlying rocksdb" << RESET << endl;
        return false;
    } else {
        cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb success" << RESET << endl;
    }
    // start threadPool, memPool, etc.
    launchThreadPool(options.deltaKV_thread_number_limit);
    // Create objects
    if (options.enable_deltaStore == true && HashStoreInterfaceObjPtr_ == nullptr) {
        options.rocksdbRawOptions_.merge_operator.reset(new RocksDBInternalMergeOperator); // reset internal merge operator
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, hashStoreGCManagerPtr_);
        cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] add deltaStore success" << RESET << endl;
        // create deltaStore related threads
        ioService_.post(boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
        cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] start thread for HashStoreFileOperator::operationWorker success" << RESET << endl;
    }
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
        cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] add deltaStore success" << RESET << endl;
    }

    return true;
}

bool DeltaKV::Close()
{
    delete pointerToRawRocksDB_;
    if (pointerToRawRocksDB_) {
        return false;
    } else {
        if (HashStoreInterfaceObjPtr_ != nullptr) {
            delete HashStoreInterfaceObjPtr_;
            // delete related object pointers
            delete hashStoreFileManagerPtr_;
            delete hashStoreFileOperatorPtr_;
            delete hashStoreGCManagerPtr_;
        }
        return true;
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        // try extract value
        if (value.size() >= IndexStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
            externalIndexInfo currentExternalIndexInfo;
            bool status = IndexStoreInterfaceObjPtr_->put(key, value, &currentExternalIndexInfo);
            debug_info("currentExternalIndexInfo %u %u %u\n", currentExternalIndexInfo.externalFileID_, 
                currentExternalIndexInfo.externalFileOffset_, currentExternalIndexInfo.externalContentSize_);
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
                    cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Write underlying rocksdb with external storage index fault" << RESET << endl;
                    return false;
                } else {
                    if (HashStoreInterfaceObjPtr_ != nullptr) {
                        bool updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, false);
                        if (updateDeltaStoreWithAnchorFlagstatus == true) {
                            return true;
                        } else {
                            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Update anchor to current key fault" << RESET << endl;
                            return false;
                        }
                    } else {
                        return true;
                    }
                }
            } else {
                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Write value to external storage fault" << RESET << endl;
                return false;
            }
        } else {
            rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, value);
            if (!s.ok()) {
                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Write underlying rocksdb with raw value fault" << RESET << endl;
                return false;
            } else {
                if (HashStoreInterfaceObjPtr_ != nullptr) {
                    bool updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, false);
                    if (updateDeltaStoreWithAnchorFlagstatus == true) {
                        return true;
                    } else {
                        cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Update anchor to current key fault" << RESET << endl;
                        return false;
                    }
                } else {
                    return true;
                }
            }
        }
    } else {
        rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, value);
        if (!s.ok()) {
            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Write underlying rocksdb with raw value fault" << RESET << endl;
            return false;
        } else {
            if (HashStoreInterfaceObjPtr_ != nullptr) {
                bool updateDeltaStoreWithAnchorFlagstatus = HashStoreInterfaceObjPtr_->put(key, value, false);
                if (updateDeltaStoreWithAnchorFlagstatus == true) {
                    return true;
                } else {
                    cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Put] Update anchor to current key fault" << RESET << endl;
                    return false;
                }
            } else {
                return true;
            }
        }
    }
}

bool DeltaKV::Get(const string& key, string* value)
{
    string internalValueStr;
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr);
    if (!s.ok()) {
        cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read underlying rocksdb fault" << RESET << endl;
        return false;
    } else {
        if (IndexStoreInterfaceObjPtr_ != nullptr) {
            // check value status
            internalValueType tempInternalValueHeader;
            memcpy(&tempInternalValueHeader, internalValueStr.c_str(), sizeof(internalValueType));
            if (tempInternalValueHeader.valueSeparatedFlag_ == true) {
                // get value from value store first
                string externalRawValue;

                externalIndexInfo currentExternalIndexInfo;
                memcpy(&currentExternalIndexInfo, internalValueStr.c_str() + sizeof(internalValueType), sizeof(externalIndexInfo)); 
                debug_info("currentExternalIndexInfo %u %u %u\n", currentExternalIndexInfo.externalFileID_, 
                    currentExternalIndexInfo.externalFileOffset_, currentExternalIndexInfo.externalContentSize_);
                IndexStoreInterfaceObjPtr_->get(key, currentExternalIndexInfo, &externalRawValue); 

                if (tempInternalValueHeader.mergeFlag_ == true) {
                    // get deltas from delta store
                    vector<pair<bool, string>>* deltaInfoVec;
                    processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec);
                    if (HashStoreInterfaceObjPtr_ != nullptr) {
                        vector<string>* deltaValueFromExternalStoreVec;
                        if (HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec) != true) {
                            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore fault" << RESET << endl;
                            return false;
                        } else {
                            vector<string> finalDeltaOperatorsVec;
                            auto index = 0;
                            for (auto i = 0; i < deltaInfoVec->size(); i++) {
                                if (deltaInfoVec->at(i).first == true) {
                                    finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                                    index++;
                                } else {
                                    finalDeltaOperatorsVec.push_back(deltaInfoVec->at(i).second);
                                }
                            }
                            if (index != (deltaValueFromExternalStoreVec->size() - 1)) {
                                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore number mismatch with requested number (Inconsistent)" << RESET << endl;
                                return false;
                            } else {
                                deltaKVMergeOperatorPtr_->Merge(externalRawValue, finalDeltaOperatorsVec, value);
                                return true;
                            }
                        }
                    } else {
                        vector<string> finalDeltaOperatorsVec;
                        auto index = 0;
                        for (auto i = 0; i < deltaInfoVec->size(); i++) {
                            if (deltaInfoVec->at(i).first == true) {
                                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore when no KD separation enabled (Internal value error)" << RESET << endl;
                                return false;
                            } else {
                                finalDeltaOperatorsVec.push_back(deltaInfoVec->at(i).second);
                            }
                        }
                        if (deltaKVMergeOperatorPtr_->Merge(externalRawValue, finalDeltaOperatorsVec, value) != true) {
                            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] DeltaKV merge operation fault" << RESET << endl;
                            return false;
                        } else {
                            return true;
                        }
                    }
                } else {
                    value->assign(externalRawValue);
                    return true;
                }
            } else {
                // value stored inside LSM-tree
                char rawValueContentBuffer[tempInternalValueHeader.rawValueSize_];
                memcpy(rawValueContentBuffer, internalValueStr.c_str() + sizeof(internalValueType), tempInternalValueHeader.rawValueSize_);
                string internalRawValueStr(rawValueContentBuffer, tempInternalValueHeader.rawValueSize_);

                if (tempInternalValueHeader.mergeFlag_ == true) {
                    // get deltas from delta store
                    vector<pair<bool, string>>* deltaInfoVec;
                    processValueWithMergeRequestToValueAndMergeOperations(internalValueStr, sizeof(internalValueType) + sizeof(externalIndexInfo), deltaInfoVec);
                    if (HashStoreInterfaceObjPtr_ != nullptr) {
                        vector<string>* deltaValueFromExternalStoreVec;
                        if (HashStoreInterfaceObjPtr_->get(key, deltaValueFromExternalStoreVec) != true) {
                            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore fault" << RESET << endl;
                            return false;
                        } else {
                            vector<string> finalDeltaOperatorsVec;
                            auto index = 0;
                            for (auto i = 0; i < deltaInfoVec->size(); i++) {
                                if (deltaInfoVec->at(i).first == true) {
                                    finalDeltaOperatorsVec.push_back(deltaValueFromExternalStoreVec->at(index));
                                    index++;
                                } else {
                                    finalDeltaOperatorsVec.push_back(deltaInfoVec->at(i).second);
                                }
                            }
                            if (index != (deltaValueFromExternalStoreVec->size() - 1)) {
                                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore number mismatch with requested number (Inconsistent)" << RESET << endl;
                                return false;
                            } else {
                                deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value);
                                return true;
                            }
                        }
                    } else {
                        vector<string> finalDeltaOperatorsVec;
                        auto index = 0;
                        for (auto i = 0; i < deltaInfoVec->size(); i++) {
                            if (deltaInfoVec->at(i).first == true) {
                                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] Read external deltaStore when no KD separation enabled (Internal value error)" << RESET << endl;
                                return false;
                            } else {
                                finalDeltaOperatorsVec.push_back(deltaInfoVec->at(i).second);
                            }
                        }
                        if (deltaKVMergeOperatorPtr_->Merge(internalRawValueStr, finalDeltaOperatorsVec, value) != true) {
                            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Get] DeltaKV merge operation fault" << RESET << endl;
                            return false;
                        } else {
                            return true;
                        }
                    }
                } else {
                    value->assign(internalRawValueStr);
                    return true;
                }
            }
        } else {
            value->assign(internalValueStr);
            return true;
        }
    }
}

bool DeltaKV::Merge(const string& key, const string& value)
{
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        // try extract value
        if (value.size() >= HashStoreInterfaceObjPtr_->getExtractSizeThreshold()) {
            bool status = HashStoreInterfaceObjPtr_->put(key, value, false);
            if (status == true) {
                char writeInternalValueBuffer[sizeof(internalValueType)];
                internalValueType currentInternalValueType;
                currentInternalValueType.mergeFlag_ = false;
                currentInternalValueType.rawValueSize_ = value.size();
                currentInternalValueType.valueSeparatedFlag_ = true;
                memcpy(writeInternalValueBuffer, &currentInternalValueType, sizeof(internalValueType));
                string newWriteValue(writeInternalValueBuffer, sizeof(internalValueType));
                rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, newWriteValue);
                if (!s.ok()) {
                    cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Merge] Write underlying rocksdb with external storage index fault" << RESET << endl;
                    return false;
                } else {
                    return true;
                }
            } else {
                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Merge] Write value to external storage fault" << RESET << endl;
                return false;
            }
        } else {
            rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, value);
            if (!s.ok()) {
                cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Merge] Write underlying rocksdb with raw value fault" << RESET << endl;
                return false;
            } else {
                return true;
            }
        }
    } else {
        rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, value);
        if (!s.ok()) {
            cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Merge] Write underlying rocksdb with raw value fault" << RESET << endl;
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

// TODO: upper functions are not complete

bool DeltaKV::launchThreadPool(uint64_t totalThreadNumber)
{
    /*
     * This will start the ioService_ processing loop. All tasks
     * assigned with ioService_.post() will start executing.
     */
    boost::asio::io_service::work work(ioService_);

    /*
     * This will add totalThreadNumber threads to the thread pool.
     */
    for (uint64_t i = 0; i < totalThreadNumber; i++) {
        threadpool_.create_thread(
            boost::bind(&boost::asio::io_service::run, &ioService_));
    }
    return true;
}

bool DeltaKV::deleteThreadPool()
{
    /*
     * This will stop the ioService_ processing loop. Any tasks
     * you add behind this point will not execute.
     */
    ioService_.stop();

    /*
     * Will wait till all the threads in the thread pool are finished with
     * their assigned tasks and 'join' them. Just assume the threads inside
     * the threadpool_ will be destroyed by this method.
     */
    threadpool_.join_all();
    return true;
}

bool DeltaKV::processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>* mergeOperatorsVec)
{
    uint64_t internalValueSize = internalValue.size();
    uint64_t currentProcessLocationIndex = skipSize;
    while (currentProcessLocationIndex != internalValueSize) {
        internalValueType currentInternalValueTypeHeader;
        memcpy(&currentInternalValueTypeHeader, internalValue.c_str() + currentProcessLocationIndex, sizeof(internalValueType));
        currentProcessLocationIndex += sizeof(internalValueType);
        if (currentInternalValueTypeHeader.valueSeparatedFlag_ != true) {
            string currentValue(internalValue.c_str() + currentProcessLocationIndex, currentInternalValueTypeHeader.rawValueSize_);
            currentProcessLocationIndex += currentInternalValueTypeHeader.rawValueSize_;
            mergeOperatorsVec->push_back(make_pair(false, currentValue));
        } else {
            mergeOperatorsVec->push_back(make_pair(true, ""));
        }
    }
    return true;
}
} // namespace DELTAKV_NAMESPACE
