#include "interface/lsmTreeInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool LsmTreeInterface::Open(DeltaKVOptions& options, const string& name) {
    mergeOperator_ = new RocksDBInternalMergeOperator;
//    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocksdbRawOptions_.merge_operator.reset(mergeOperator_); // reset
//    }

    rocksdb::Status rocksDBStatus = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Can't open underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
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

    return true;
}

LsmTreeInterface::LsmTreeInterface() {
}

LsmTreeInterface::~LsmTreeInterface() {
}

bool LsmTreeInterface::Close() {
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        delete IndexStoreInterfaceObjPtr_;
    }
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
    return true;
}

bool LsmTreeInterface::Put(const mempoolHandler_t& memPoolHandler)
{
    if (lsmTreeRunningMode_ == kNoValueLog || memPoolHandler.valueSize_ < valueExtractSize_) {
         // no value log
        char valueBuffer[memPoolHandler.valueSize_ + sizeof(internalValueType)];
        internalValueType header(false, false, memPoolHandler.sequenceNumber_, memPoolHandler.valueSize_);
        memcpy(valueBuffer, &header, sizeof(header));
        memcpy(valueBuffer + sizeof(header), memPoolHandler.valuePtr_, memPoolHandler.valueSize_);

        rocksdb::Status rocksDBStatus;
        rocksdb::Slice newKey(memPoolHandler.keyPtr_, memPoolHandler.keySize_);
        rocksdb::Slice newValue(valueBuffer, memPoolHandler.valueSize_ + sizeof(internalValueType));
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Put(internalWriteOption_, newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        }
        return rocksDBStatus.ok();
    } else {  // use value log. Let value log determine whether to separate key and values!
        bool status;
        STAT_PROCESS(status = IndexStoreInterfaceObjPtr_->put(memPoolHandler, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == false) {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", memPoolHandler.keyPtr_, memPoolHandler.valuePtr_);
        }
        return status;
    }
}


// Do not create headers
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

// Merge for no separation. 
bool LsmTreeInterface::Merge(const mempoolHandler_t& memPoolHandler)
{
    internalValueType header(false, false, memPoolHandler.sequenceNumber_, memPoolHandler.valueSize_);
    char valueBuffer[memPoolHandler.valueSize_ + sizeof(header)];
    memcpy(valueBuffer, &header, sizeof(header));
    memcpy(valueBuffer + sizeof(header), memPoolHandler.valuePtr_, memPoolHandler.valueSize_);
//    return Merge(memPoolHandler.keyPtr_, memPoolHandler.keySize_, valueBuffer, memPoolHandler.valueSize_ + sizeof(header));

    rocksdb::Status rocksDBStatus;
    rocksdb::Slice newKey(memPoolHandler.keyPtr_, memPoolHandler.keySize_);
    rocksdb::Slice newValue(valueBuffer, memPoolHandler.valueSize_ + sizeof(header));
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
    
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Merge underlying rocksdb with raw value fault, key = %s, status = %s\n", newKey.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    }
    return true;
}

// return value: has header
// values in LSM-tree: has header
// values in vLog or returned by vLog: no header
bool LsmTreeInterface::Get(const string& key, string* value)
{
    if (lsmTreeRunningMode_ == kNoValueLog) {
        rocksdb::Status rocksDBStatus;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, value), StatsType::DELTAKV_GET_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        }

        return true;
    } else {
        rocksdb::Status rocksDBStatus;
        string internalValueStr;
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, &internalValueStr), StatsType::DELTAKV_GET_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        }

        // check value status
        internalValueType header;
        memcpy(&header, internalValueStr.c_str(), sizeof(internalValueType));

        if (header.valueSeparatedFlag_ == true) {
            string vLogValue; 
            externalIndexInfo vLogIndex;
            memcpy(&vLogIndex, internalValueStr.c_str() + sizeof(header), sizeof(vLogIndex));
            STAT_PROCESS(IndexStoreInterfaceObjPtr_->get(key, vLogIndex, &vLogValue), StatsType::DELTAKV_GET_INDEXSTORE);

            string remainingDeltas = internalValueStr.substr(sizeof(header) + sizeof(vLogIndex));  // remaining deltas


            int valueBufferSize = sizeof(header) + vLogValue.size();
            char valueBuffer[valueBufferSize];

            header.rawValueSize_ -= 4;
            header.valueSeparatedFlag_ = false;
            memcpy(valueBuffer, &header, sizeof(header));
            memcpy(valueBuffer + sizeof(header), vLogValue.c_str(), vLogValue.size());

            // replace the external value index with the raw value
            if (remainingDeltas.empty() == false) {
                Slice key("lsmInterface", 12);
                Slice existingValue(valueBuffer, valueBufferSize);
                deque<string> operandList;
                operandList.push_back(remainingDeltas);
                
                mergeOperator_->FullMerge(key, &existingValue, operandList, value, nullptr);
            } else {
                value->assign(valueBuffer, valueBufferSize);
            }

            return true;
        } else {
            value->assign(internalValueStr);
            return true;
        }
    } 
}

// Start from initial batch. It will let the caller deal with the merge batches first in initialBatch. 
bool LsmTreeInterface::MultiWriteWithBatch(const vector<mempoolHandler_t>& memPoolHandlersPut, rocksdb::WriteBatch* mergeBatch) {
    rocksdb::WriteOptions batchedWriteOperation;
    batchedWriteOperation.sync = false;

    struct timeval tv;
    gettimeofday(&tv, 0);
    if (lsmTreeRunningMode_ == kNoValueLog) {
        for (auto& it : memPoolHandlersPut) {
            internalValueType header(false, false, it.sequenceNumber_, it.valueSize_);
            char valueBuffer[it.valueSize_ + sizeof(header)];
            memcpy(valueBuffer, &header, sizeof(header));
            memcpy(valueBuffer + sizeof(header), it.valuePtr_, it.valueSize_);

            rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
            rocksdb::Slice newValue(valueBuffer, it.valueSize_ + sizeof(header));
            mergeBatch->Put(newKey, newValue);
        }
    } else {
        vector<mempoolHandler_t> memPoolHandlerForPutVlog;
        for (auto& it : memPoolHandlersPut) {
            if (it.valueSize_ < valueExtractSize_) {
                internalValueType header(false, false, it.sequenceNumber_, it.valueSize_);
                char valueBuffer[it.valueSize_ + sizeof(header)];
                memcpy(valueBuffer, &header, sizeof(header));
                memcpy(valueBuffer + sizeof(header), it.valuePtr_, it.valueSize_);

                rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
                rocksdb::Slice newValue(valueBuffer, it.valueSize_ + sizeof(header));
                mergeBatch->Put(newKey, newValue);
            } else {
                memPoolHandlerForPutVlog.push_back(it);
            }
        }

        if (!memPoolHandlerForPutVlog.empty()) {
            STAT_PROCESS(IndexStoreInterfaceObjPtr_->multiPut(memPoolHandlersPut), StatsType::LSM_FLUSH_VLOG);
        }
    }

    if (mergeBatch->Count() == 0) {
        return true;
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_PRE_PUT, tv);

    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Write(batchedWriteOperation, mergeBatch), StatsType::LSM_FLUSH_ROCKSDB);

    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write with batch on underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }

    if (lsmTreeRunningMode_ == kNoValueLog) {
        STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->FlushWAL(true), StatsType::LSM_FLUSH_WAL);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Flush WAL, status = %s\n", rocksDBStatus.ToString().c_str());
            return false;
        }
    }
    return true;
}

bool LsmTreeInterface::Scan(const string& targetStartKey, 
        int len,
        vector<string>& keys, vector<string>& values) {
    rocksdb::Iterator* it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    it->Seek(targetStartKey);
    int cnt = 0;
    bool ret = true;
    keys.clear();
    values.clear();
    vector<string> values_lsm;
    for (; it->Valid() && cnt < len; it->Next()) {
        keys.push_back(it->key().ToString());
        values_lsm.push_back(it->value().ToString());
        cnt++;
    }
    delete it;

    if (lsmTreeRunningMode_ == kNoValueLog) {
        for (auto& it : values_lsm) {
            values.push_back(it);
        }
        return true;
    }

    vector<bool> isSeparated;
    vector<string> separated_keys;
    vector<externalIndexInfo> vLogIndices;
    vector<string> remainingDeltasVec;

    len = keys.size();

    isSeparated.resize(len);
    separated_keys.resize(len);
    vLogIndices.resize(len);
    remainingDeltasVec.resize(len);

    vector<string> vLogValues;
    int separated_cnt = 0;

    for (uint64_t i = 0; i < len; i++) {
        string& internalValueStr = values_lsm.at(i);
        internalValueType header;
        memcpy(&header, internalValueStr.c_str(), sizeof(internalValueType));

        if (header.valueSeparatedFlag_ == true) {
            isSeparated[i] = true;

            string vLogValue; 
            externalIndexInfo vLogIndex;
            memcpy(&vLogIndex, internalValueStr.c_str() + sizeof(header), sizeof(vLogIndex));

            separated_keys[separated_cnt] = keys.at(i);
            vLogIndices[separated_cnt] = vLogIndex;
            remainingDeltasVec[separated_cnt] = internalValueStr.substr(sizeof(header) + sizeof(vLogIndex));  
            separated_cnt++;
        } else {
            isSeparated.at(i) = false;
        }
    }

    IndexStoreInterfaceObjPtr_->multiGet(separated_keys, len, vLogIndices, vLogValues);

    separated_cnt = 0;
    values.clear();
    for (uint64_t i = 0; i < len; i++) {
        string& internalValueStr = values_lsm.at(i);
        internalValueType header;
        memcpy(&header, internalValueStr.c_str(), sizeof(internalValueType));

        if (isSeparated[i] == true) {
            string& remainingDeltas = remainingDeltasVec.at(separated_cnt);  // remaining deltas
            string& vLogValue = vLogValues.at(separated_cnt);

            int valueBufferSize = sizeof(header) + vLogValue.size();
            char valueBuffer[valueBufferSize];

            header.rawValueSize_ -= 4;
            header.valueSeparatedFlag_ = false;
            memcpy(valueBuffer, &header, sizeof(header));
            memcpy(valueBuffer + sizeof(header), vLogValue.c_str(), vLogValue.size());

            string* value = nullptr;

            // replace the external value index with the raw value
            if (remainingDeltas.empty() == false) {
                Slice key("lsmInterface", 12);
                Slice existingValue(valueBuffer, valueBufferSize);
                deque<string> operandList;
                operandList.push_back(remainingDeltas);
                
                mergeOperator_->FullMerge(key, &existingValue, operandList, value, nullptr);
                values.push_back(*value);
                delete value;
            } else {
                values.push_back(string(valueBuffer, valueBufferSize));
            }

            separated_cnt++;
        } else {
            values.push_back(internalValueStr);
        }
    }

    return ret;
}


void LsmTreeInterface::GetRocksDBProperty(const string& property, string* str) {
    pointerToRawRocksDB_->GetProperty(property.c_str(), str);
}

}
