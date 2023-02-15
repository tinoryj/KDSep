#include "interface/lsmTreeInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool LsmTreeInterface::Open(DeltaKVOptions& options, const string& name) {
    rocksdb::Status rocksDBStatus = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Can't open underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }

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

    return true;
}

LsmTreeInterface::LsmTreeInterface() {
}

LsmTreeInterface::~LsmTreeInterface() {
}

bool LsmTreeInterface::Close() {
    if (IndexStoreInterfaceObjPtr_ != nullptr) {
        cerr << "[DeltaKV Interface] Try delete IndexStore" << endl;
        delete IndexStoreInterfaceObjPtr_;
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

            char valueBuffer[vLogValue.size() + internalValueStr.size() - sizeof(vLogIndex)];
            header.valueSeparatedFlag_ = false;
            internalValueStr = internalValueStr.substr(sizeof(header) + sizeof(vLogIndex));  // remaining deltas

            // replace the external value index with the raw value
            memcpy(valueBuffer, &header, sizeof(header));
            memcpy(valueBuffer + sizeof(header), vLogValue.c_str(), vLogValue.size());
            if (internalValueStr.empty() == false) {
                memcpy(valueBuffer + sizeof(header) + vLogValue.size(), internalValueStr.c_str(), internalValueStr.size());
            }

            value->assign(string(valueBuffer, vLogValue.size() + internalValueStr.size() - sizeof(vLogIndex)));
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
            IndexStoreInterfaceObjPtr_->multiPut(memPoolHandlersPut);
        }
    }

    rocksdb::Status rocksDBStatus;
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->Write(batchedWriteOperation, mergeBatch), StatsType::DELTAKV_PUT_MERGE_ROCKSDB);

    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Write with batch on underlying rocksdb, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }
    STAT_PROCESS(rocksDBStatus = pointerToRawRocksDB_->FlushWAL(true), StatsType::BATCH_FLUSH_WAL);
    if (!rocksDBStatus.ok()) {
        debug_error("[ERROR] Flush WAL, status = %s\n", rocksDBStatus.ToString().c_str());
        return false;
    }
    return true;
}

}
