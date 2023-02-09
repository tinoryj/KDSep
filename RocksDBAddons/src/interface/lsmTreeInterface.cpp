#include "interface/lsmTreeInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool LsmTreeInterface::Open(DeltaKVOptions& options, const string& name) {
    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocksdbRawOptions_.merge_operator.reset(new RocksDBInternalMergeOperator); // reset
    }

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

    // Create objects
    if (options.enable_valueStore == true && IndexStoreInterfaceObjPtr_ == nullptr) {
        isValueStoreInUseFlag_ = true;
        IndexStoreInterfaceObjPtr_ = new IndexStoreInterface(&options, name, pointerToRawRocksDB_);
        valueExtractSize_ = IndexStoreInterfaceObjPtr_->getExtractSizeThreshold();
    }

    if (options.enable_valueStore) {
        lsmTreeRunningMode_ = kValueLog;
        cerr << "lsmTreeRunningMode_ = kValueLog" << endl;
    } else {
        lsmTreeRunningMode_ = kNoValueLog;
        cerr << "lsmTreeRunningMode_ = kNoValueLog" << endl;
    }
}

bool LsmTreeInterface::~LsmTreeInterface() {
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        cerr << "[DeltaKV Interface] Try delete IndexStore" << endl;
        delete IndexStoreInterfaceObjPtr_;
    }
}

bool LsmTreeInterface::Put(mempoolHandler_t objectPairMemPoolHandler)
{
    if (lsmTreeRunningMode_ == kNoValueLog) { // no value log
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
    } else {  // use value log. Let value log determine whether to separate key and values!
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(objectPairMemPoolHandler, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == true) {
            return true;
        } else {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.valuePtr_);
            return false;
        }
    }
}

bool LsmTreeInterface::MultiWriteWithBatch(std::vector<mempoolHandler_t>& objectPairMemPoolHandlerForPut, std::vector<mempoolHandler_t>& objectPairMemPoolHandlerForMerge) {
    if (lsmTreeRunningMode_ == kNoValueLog) {
        rocksdb::WriteOptions batchedWriteOperation;
        batchedWriteOperation.sync = false;
        rocksdb::WriteBatch batch;

        for (auto& it : objectPairMemPoolHandlerForPut) {
            rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
            rocksdb::Slice newValue(it.valuePtr_, it.valueSize_);
            rocksDBStatus = batch.Put(newKey, newValue);
        }
        for (auto& it : objectPairMemPoolHandlerForMerge) {
            if (it.isAnchorFlag_ == false) {
                rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
                rocksdb::Slice newValue(it.valuePtr_, it.valueSize_);
                rocksDBStatus = batch.Merge(newKey, newValue);
            }
        }
        STAT_PROCESS(pointerToRawRocksDB_->Write(batchedWriteOperation, &batch), StatsType::DELTAKV_PUT_MERGE_ROCKSDB);
        STAT_PROCESS(pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
    } else {
    }
}

bool LsmTreeInterface::Merge(const char* key, uint32_t keySize, const char* value, uint32_t valueSize)
{
    rocksdb::Status rocksDBStatus;
    rocksdb::Slice newKey(key, keySize);
    rocksdb::Slice newValue(value, valueSize);
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with merge value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

bool LsmTreeInterface::Merge(mempoolHandler_t objectPairMemPoolHandler)
{
    return Merge(objectPairMemPoolHandler.keyPtr_, objectPairMemPoolHandler.keySize_, objectPairMemPoolHandler.valuePtr_, objectPairMemPoolHandler.valueSize_);
}

bool LsmTreeInterface::Get(const string& key, string* value)
{
    if (lsmTreeRunningMode_ == kNoValueLog) {
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, value), StatsType::DELTAKV_GET_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        } else {
            return true;
        }
    } else {
        return IndexStoreInterfaceObjPtr_->get(key, value); 
    }
}

//bool DeltaKV::GetWithValueStore(const string& key, string* value, uint32_t& /*maxSequenceNumber*/, bool /*getByWriteBackFlag*/)
/*{
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
        bool mergeStatus;
        STAT_PROCESS(mergeStatus = deltaKVMergeOperatorPtr_->Merge(rawValueStr, finalDeltaOperatorsVec, value), StatsType::FULL_MERGE);
        if (mergeStatus != true) {
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
*/

}
