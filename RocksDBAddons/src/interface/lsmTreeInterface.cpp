#include "interface/lsmTreeInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool LsmTreeInterface::Open(DeltaKVOptions& options, const string& name) {
    mergeOperator_ = new RocksDBInternalMergeOperator;
//    if (options.enable_deltaStore == true || options.enable_valueStore == true) {
        options.rocks_opt.merge_operator.reset(mergeOperator_); // reset
//    }

    rocksdb::Status rocksDBStatus =
        rocksdb::DB::Open(options.rocks_opt, name,
                &rocksdb_);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Can't open underlying rocksdb, status = %s\n",
                rocksDBStatus.ToString().c_str());
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
    if (options.enable_valueStore == true && vlog_ == nullptr) {
        isValueStoreInUseFlag_ = true;
        vlog_ = new IndexStoreInterface(&options, name,
                rocksdb_);
        valueExtractSize_ = vlog_->getExtractSizeThreshold();
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
    if (vlog_ != nullptr) {
        delete vlog_;
    }
    if (rocksdb_ != nullptr) {
        delete rocksdb_;
    }
    return true;
}

bool LsmTreeInterface::Put(const mempoolHandler_t& obj)
{
    if (lsmTreeRunningMode_ == kNoValueLog || obj.valueSize_ < valueExtractSize_) {
         // no value log
        char valueBuffer[obj.valueSize_ + sizeof(KvHeader)];
        KvHeader header(false, false, obj.sequenceNumber_, obj.valueSize_);
        size_t header_sz = sizeof(KvHeader);

        // Put header
        if (use_varint_kv_header == false) {
            memcpy(valueBuffer, &header, header_sz);
        } else {
            header_sz = PutKVHeaderVarint(valueBuffer, header); 
        }
        // Put raw value
        memcpy(valueBuffer + header_sz, obj.valuePtr_, obj.valueSize_);

        rocksdb::Status rocksDBStatus;
        rocksdb::Slice newKey(obj.keyPtr_, obj.keySize_);
        rocksdb::Slice newValue(valueBuffer, obj.valueSize_ + header_sz);
        STAT_PROCESS(
                rocksDBStatus = rocksdb_->Put(internalWriteOption_,
                    newKey, newValue), StatsType::DELTAKV_PUT_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Write underlying rocksdb with raw value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        }
        return rocksDBStatus.ok();
    } else {  // use value log. Let value log determine whether to separate key and values!
        bool status;
        STAT_PROCESS(status = vlog_->put(obj, true), StatsType::DELTAKV_PUT_INDEXSTORE);
        if (status == false) {
            debug_error("[ERROR] Write value to external storage fault, key = %s, value = %s\n", obj.keyPtr_, obj.valuePtr_);
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
    STAT_PROCESS(rocksDBStatus = rocksdb_->Merge(internalMergeOption_, newKey, newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write underlying rocksdb with merge value fault, key = %s, value = %s, status = %s\n", newKey.ToString().c_str(), newValue.ToString().c_str(), rocksDBStatus.ToString().c_str());
        return false;
    } else {
        return true;
    }
}

// Merge for no separation. 
bool LsmTreeInterface::Merge(const mempoolHandler_t& obj)
{
    KvHeader header(false, false, obj.sequenceNumber_, obj.valueSize_);
    size_t value_sz = obj.valueSize_ + sizeof(KvHeader);
    char valueBuffer[value_sz];
    size_t header_sz = sizeof(KvHeader);
    if (use_varint_kv_header == false) {
        memcpy(valueBuffer, &header, header_sz);
    } else {
        header_sz = PutKVHeaderVarint(valueBuffer, header);
    }
    memcpy(valueBuffer + header_sz, obj.valuePtr_, obj.valueSize_);
    value_sz = header_sz + obj.valueSize_;

//    return Merge(obj.keyPtr_, obj.keySize_, valueBuffer, obj.valueSize_ + sizeof(header));

    rocksdb::Status rocksDBStatus;
    rocksdb::Slice newKey(obj.keyPtr_, obj.keySize_);
    rocksdb::Slice newValue(valueBuffer, value_sz);
    STAT_PROCESS(rocksDBStatus = rocksdb_->Merge(internalMergeOption_, newKey,
                newValue), StatsType::DELTAKV_MERGE_ROCKSDB);
    
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Merge underlying rocksdb with raw value fault," 
                " key = %s, status = %s\n", newKey.ToString().c_str(),
                rocksDBStatus.ToString().c_str());
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
        STAT_PROCESS(
                rocksDBStatus =
                rocksdb_->Get(rocksdb::ReadOptions(), key, value),
                StatsType::DELTAKV_GET_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        }

        return true;
    } else {
        rocksdb::Status rocksDBStatus;
        string lsm_value;
        STAT_PROCESS(
                rocksDBStatus = rocksdb_->Get(rocksdb::ReadOptions(), key, &lsm_value), StatsType::DELTAKV_GET_ROCKSDB);
        if (!rocksDBStatus.ok()) {
            debug_error("[ERROR] Read underlying rocksdb with raw value fault, key = %s, status = %s\n", key.c_str(), rocksDBStatus.ToString().c_str());
            return false;
        }

        // check value status
        KvHeader header;
        size_t header_sz = sizeof(KvHeader);

        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz); 
        }

        if (header.valueSeparatedFlag_ == true) {
            string vLogValue; 
            externalIndexInfo vLogIndex;
            size_t index_sz = sizeof(externalIndexInfo);
            if (use_varint_index == false) {
                memcpy(&vLogIndex, lsm_value.c_str() + header_sz,
                        sizeof(vLogIndex));
            } else {
                vLogIndex = GetVlogIndexVarint(lsm_value.data() +
                    header_sz, index_sz);
            }
            STAT_PROCESS(vlog_->get(key, vLogIndex, &vLogValue), StatsType::DELTAKV_GET_INDEXSTORE);

            // remaining deltas
            string remainingDeltas = lsm_value.substr(header_sz + index_sz);  

            // The header will be larger. Use sizeof() function here
            int valueBufferSize = sizeof(header) + vLogValue.size();
            char valueBuffer[valueBufferSize];

            // Prepare for merges
            // TODO extracted the sequence number
            header.rawValueSize_ = vLogValue.size();
            header.valueSeparatedFlag_ = false;

            // Put header to buffer
            if (use_varint_kv_header == false) {
                memcpy(valueBuffer, &header, header_sz);
            } else {
                header_sz = PutKVHeaderVarint(valueBuffer, header);
            }
            // Put raw value to buffer
            memcpy(valueBuffer + header_sz, vLogValue.c_str(), vLogValue.size());
            valueBufferSize = header_sz + vLogValue.size();

            // 1. replace the external value index with the raw value
            // 2. merge with the existing deltas, if any
            if (remainingDeltas.size() > 0) {
                Slice key("lsmInterface", 12);
                Slice existingValue(valueBuffer, valueBufferSize);
                deque<string> operandList;
                operandList.push_back(remainingDeltas);
                
                mergeOperator_->FullMerge(key, &existingValue, operandList,
                        value, nullptr);
            } else {
                value->assign(valueBuffer, valueBufferSize);
            }

            return true;
        } else {
            value->assign(lsm_value);
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
    size_t header_sz = sizeof(KvHeader);
    size_t value_sz;
    if (lsmTreeRunningMode_ == kNoValueLog) {
        for (auto& it : memPoolHandlersPut) {
            KvHeader header(false, false, it.sequenceNumber_, it.valueSize_);
            // Reserve enough space. Use sizeof() here
            char valueBuffer[it.valueSize_ + sizeof(header)];

            if (use_varint_kv_header == false) {
                memcpy(valueBuffer, &header, header_sz);
            } else {
                header_sz = PutKVHeaderVarint(valueBuffer, header);
            }
            memcpy(valueBuffer + header_sz, it.valuePtr_, it.valueSize_);
            value_sz = header_sz + it.valueSize_;

            rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
            rocksdb::Slice newValue(valueBuffer, value_sz);
            mergeBatch->Put(newKey, newValue);
        }
    } else {
        vector<mempoolHandler_t> objects_for_vlog_put;
        for (auto& it : memPoolHandlersPut) {
            if (it.valueSize_ < valueExtractSize_) {
                KvHeader header(false, false, it.sequenceNumber_, it.valueSize_);
                // Reserve enough space. Use sizeof() here
                char valueBuffer[it.valueSize_ + sizeof(header)];

                if (use_varint_kv_header == false) {
                    memcpy(valueBuffer, &header, header_sz);
                } else {
                    header_sz = PutKVHeaderVarint(valueBuffer, header);
                }
                memcpy(valueBuffer + header_sz, it.valuePtr_, it.valueSize_);
                value_sz = header_sz + it.valueSize_;

                rocksdb::Slice newKey(it.keyPtr_, it.keySize_);
                rocksdb::Slice newValue(valueBuffer, value_sz);
                mergeBatch->Put(newKey, newValue);
            } else {
                objects_for_vlog_put.push_back(it);
            }
        }

        if (!objects_for_vlog_put.empty()) {
            STAT_PROCESS(vlog_->multiPut(memPoolHandlersPut),
                    StatsType::LSM_FLUSH_VLOG);
        }
    }

    if (mergeBatch->Count() == 0) {
        return true;
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_PRE_PUT, tv);

    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = rocksdb_->Write(batchedWriteOperation,
                mergeBatch), StatsType::LSM_FLUSH_ROCKSDB);

    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write with batch on underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }

    if (lsmTreeRunningMode_ == kNoValueLog) {
        STAT_PROCESS(rocksDBStatus = rocksdb_->FlushWAL(true), StatsType::LSM_FLUSH_WAL);
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
    rocksdb::Iterator* it = rocksdb_->NewIterator(rocksdb::ReadOptions());
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
    size_t header_sz = sizeof(KvHeader);

    for (uint64_t i = 0; i < len; i++) {
        string& lsm_value = values_lsm.at(i);
        KvHeader header;

        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            // change header_sz
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz);
        }

        if (header.valueSeparatedFlag_ == true) {
            isSeparated[i] = true;

            string vLogValue; 
            externalIndexInfo vLogIndex;
            size_t index_sz = 0;
            if (use_varint_index == false) {
                memcpy(&vLogIndex, lsm_value.c_str() + header_sz,
                        sizeof(vLogIndex));
                index_sz = sizeof(vLogIndex);
            } else {
                vLogIndex = GetVlogIndexVarint(lsm_value.data() +
                        header_sz, index_sz);
            }

            separated_keys[separated_cnt] = keys.at(i);
            vLogIndices[separated_cnt] = vLogIndex;
            remainingDeltasVec[separated_cnt] =
                lsm_value.substr(header_sz + index_sz);  
            separated_cnt++;
        } else {
            isSeparated.at(i) = false;
        }
    }

    vlog_->multiGet(separated_keys, len, vLogIndices, vLogValues);

    separated_cnt = 0;
    values.clear();
    for (uint64_t i = 0; i < len; i++) {
        string& lsm_value = values_lsm.at(i);
        KvHeader header;
        if (use_varint_kv_header == false) {
            memcpy(&header, lsm_value.c_str(), header_sz);
        } else {
            // change header_sz
            header = GetKVHeaderVarint(lsm_value.c_str(), header_sz);
        }

        if (isSeparated[i] == true) {
            // remaining deltas
            string& remainingDeltas = remainingDeltasVec.at(separated_cnt);  
            string& vLogValue = vLogValues.at(separated_cnt);

            // Reserve enough space, use sizeof()
            int valueBufferSize = sizeof(header) + vLogValue.size();
            char valueBuffer[valueBufferSize];

            // Prepare for merges
            // extracted the sequence number
            header.rawValueSize_ = vLogValue.size();
            header.valueSeparatedFlag_ = false;

            // Put header to buffer
            if (use_varint_kv_header == false) {
                memcpy(valueBuffer, &header, header_sz);
            } else {
                header_sz = PutKVHeaderVarint(valueBuffer, header);
            }
            memcpy(valueBuffer + header_sz, vLogValue.c_str(), vLogValue.size());
            valueBufferSize = header_sz + vLogValue.size();

            string* value = nullptr;

            // replace the external value index with the raw value
            if (remainingDeltas.size() > 0) {
                Slice key("lsmInterface", 12);
                Slice existingValue(valueBuffer, valueBufferSize);
                deque<string> operandList;
                operandList.push_back(remainingDeltas);
                
                mergeOperator_->FullMerge(key, &existingValue, operandList,
                        value, nullptr);
                values.push_back(*value);
                delete value;
            } else {
                values.push_back(string(valueBuffer, valueBufferSize));
            }

            separated_cnt++;
        } else {
            values.push_back(lsm_value);
        }
    }

    return ret;
}


void LsmTreeInterface::GetRocksDBProperty(const string& property, string* str) {
    rocksdb_->GetProperty(property.c_str(), str);
}

}
