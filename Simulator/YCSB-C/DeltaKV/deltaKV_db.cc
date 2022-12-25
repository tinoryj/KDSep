#include "deltaKV_db.h"

#include <bits/stdc++.h>
#include <sys/time.h>

#include <iostream>

#include "db/extern_db_config.h"

using namespace std;

namespace ycsbc {

vector<string> split(string str, string token) {
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0) result.push_back(str);
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

class FieldUpdateMergeOperator : public MergeOperator {
   public:
    // Gives the client a way to express the read -> modify -> write semantics
    // key:         (IN) The key that's associated with this merge operation.
    // existing:    (IN) null indicates that the key does not exist before this op
    // operand_list:(IN) the sequence of merge operations to apply, front() first.
    // new_value:  (OUT) Client is responsible for filling the merge result here
    // logger:      (IN) Client could use this to log errors during merge.
    //
    // Return true on success, false on failure/corruption/etc.
    bool FullMerge(const Slice &key, const Slice *existing_value,
                   const std::deque<std::string> &operand_list,
                   std::string *new_value, Logger *logger) const override {
        // cout << "Do full merge operation in as field update" << endl;
        // cout << existing_value->data() << "\n Size=" << existing_value->size() << endl;
        // new_value->assign(existing_value->ToString());
        // if (existing_value == nullptr) {
        //     cout << "Merge operation existing value = nullptr" << endl;
        //     return false;
        // }
        // cout << "Merge operation existing value size = " << existing_value->size() << endl;
        vector<std::string> words = split(existing_value->ToString(), ",");
        // for (long unsigned int i = 0; i < words.size(); i++) {
        //     cout << "Index = " << i << ", Words = " << words[i] << endl;
        // }
        for (auto q : operand_list) {
            // cout << "Operand list content = " << q << endl;
            vector<string> operandVector = split(q, ",");
            for (long unsigned int i = 0; i < operandVector.size(); i += 2) {
                words[stoi(operandVector[i])] = operandVector[i + 1];
            }
        }
        string temp;
        for (long unsigned int i = 0; i < words.size() - 1; i++) {
            temp += words[i] + ",";
        }
        temp += words[words.size() - 1];
        new_value->assign(temp);
        // cout << new_value->data() << "\n Size=" << new_value->length() <<endl;
        return true;
    };

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const Slice &key, const Slice &left_operand,
                      const Slice &right_operand, std::string *new_value,
                      Logger *logger) const override {
        // cout << "Do partial merge operation in as field update" << endl;
        string allOperandListStr = left_operand.ToString();
        allOperandListStr.append(",");
        allOperandListStr.append(right_operand.ToString());
        // cerr << "Find raw partial merge = " << allOperandListStr << endl
        //      << endl;
        unordered_map<int, string> findIndexMap;
        stack<pair<string, string>> finalResultStack;
        vector<string> operandVector = split(allOperandListStr, ",");
        // cerr << "result vec size = " << operandVector.size() << endl;
        for (long unsigned int i = 0; i < operandVector.size(); i += 2) {
            // cerr << "result vec index = " << i << ", content = " << operandVector[i] << endl;
            int index = stoi(operandVector[i]);
            if (findIndexMap.find(index) == findIndexMap.end()) {
                findIndexMap.insert(make_pair(index, operandVector[i + 1]));
            } else {
                findIndexMap.at(index).assign(operandVector[i + 1]);
            }
        }
        // cerr << "result map size = " << findIndexMap.size() << endl;
        string finalResultStr = "";
        for (auto it : findIndexMap) {
            finalResultStr.append(to_string(it.first));
            finalResultStr.append(",");
            finalResultStr.append(it.second);
            finalResultStr.append(",");
        }
        finalResultStr = finalResultStr.substr(0, finalResultStr.size() - 1);
        // cerr << "Find partial merge = " << finalResultStr << endl
        //      << endl;
        new_value->assign(finalResultStr);
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char *Name() const override { return "FieldUpdateMergeOperator"; }
};

DeltaKVDB::DeltaKVDB(const char *dbfilename, const std::string &config_file_path) {
    // outputStream_.open("operations.log", ios::binary | ios::out);
    // if (!outputStream_.is_open()) {
    //     cerr << "Load logging files error" << endl;
    // }
    // get rocksdb config
    struct timeval tv1;
    DELTAKV_NAMESPACE::StatsRecorder::getInstance()->openStatistics(tv1);
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    bool directIO = config.getDirectIO();
    bool fakeDirectIO = config.getFakeDirectIO();  // for testing
    bool keyValueSeparation = config.getKeyValueSeparation();
    bool keyDeltaSeparation = config.getKeyDeltaSeparation();
    bool blobDbKeyValueSeparation = config.getBlobDbKeyValueSeparation();
    size_t memtableSize = config.getMemtable();
    uint64_t debugLevel = config.getDebugLevel();
    DebugManager::getInstance().setDebugLevel(debugLevel);
    cerr << "debug level set to " << debugLevel << endl;

    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;

    if (directIO == true) {
        options_.rocksdbRawOptions_.use_direct_reads = true;
        options_.rocksdbRawOptions_.use_direct_io_for_flush_and_compaction = true;
        options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kDirectIO;
    } else {
        options_.rocksdbRawOptions_.allow_mmap_reads = true;
        options_.rocksdbRawOptions_.allow_mmap_writes = true;
        options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kAlignLinuxIO;
    }
    if (blobDbKeyValueSeparation == true) {
        cerr << "Enabled Blob based KV separation" << endl;
        bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize / 8);
        options_.rocksdbRawOptions_.enable_blob_files = true;
        options_.rocksdbRawOptions_.min_blob_size = 0;                                                 // Default 0
        options_.rocksdbRawOptions_.blob_file_size = config.getBlobFileSize() * 1024;                  // Default 256*1024*1024
        options_.rocksdbRawOptions_.blob_compression_type = kNoCompression;                            // Default kNoCompression
        options_.rocksdbRawOptions_.enable_blob_garbage_collection = true;                             // Default false
        options_.rocksdbRawOptions_.blob_garbage_collection_age_cutoff = 0.25;                         // Default 0.25
        options_.rocksdbRawOptions_.blob_garbage_collection_force_threshold = 1.0;                     // Default 1.0
        options_.rocksdbRawOptions_.blob_compaction_readahead_size = 0;                                // Default 0
        options_.rocksdbRawOptions_.blob_file_starting_level = 0;                                      // Default 0
        options_.rocksdbRawOptions_.blob_cache = rocksdb::NewLRUCache(blockCacheSize / 8 * 7);         // Default nullptr, bbto.block_cache
        options_.rocksdbRawOptions_.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;  // Default kDisable
        assert(!keyValueSeparation);
    } else {
        bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    }
    if (keyValueSeparation == true) {
        cerr << "Enabled vLog based KV separation" << endl;
        options_.enable_valueStore = true;
    }
    if (keyDeltaSeparation == true) {
        cerr << "Enabled DeltaLog based KD separation" << endl;
        // deltaKV settings
        options_.enable_deltaStore = true;
        uint64_t deltaLogCacheObjectNumber = config.getDeltaLogCacheObjectNumber();
        if (deltaLogCacheObjectNumber != 0) {
            options_.enable_deltaStore_KDLevel_cache = true;
            options_.deltaStore_KDLevel_cache_item_number = deltaLogCacheObjectNumber;
        }
        options_.deltaStore_prefix_tree_initial_bit_number_ = config.getPrefixTreeBitNumber();
        options_.deltaStore_operationNumberForMetadataCommitThreshold_ = config.getDelteLogMetadataCommitLatency();
        options_.deltaStore_single_file_maximum_size = config.getDeltaLogFileSize();
        options_.deltaStore_file_flush_buffer_size_limit_ = config.getDeltaLogFileFlushSize();
        options_.deltaStore_op_worker_thread_number_limit_ = config.getDeltaLogOpWorkerThreadNumber();
        options_.deltaStore_gc_worker_thread_number_limit_ = config.getDeltaLogGCWorkerThreadNumber();
        options_.hashStore_max_file_number_ = config.getDeltaLogMaxFileNumber();
        options_.deltaStore_operationNumberForForcedSingleFileGCThreshold_ = config.getDelteLogMetadataCommitLatency();
        bool enable_gc_flag = config.getDeltaStoreGCEnableStatus();
        options_.deltaStore_write_back_during_reads_threshold = config.getDeltaStoreWriteBackDuringReadsThreshold();
        options_.deltaStore_write_back_during_gc_threshold = config.getDeltaStoreWriteBackDuringGCThreshold();
        if (options_.deltaStore_write_back_during_reads_threshold == 0 && options_.deltaStore_write_back_during_gc_threshold == 0) {
            options_.enable_write_back_optimization_ = false;
        } else {
            options_.enable_write_back_optimization_ = true;
        }
        if (enable_gc_flag == true) {
            options_.enable_deltaStore_garbage_collection = true;
            options_.deltaStore_operationNumberForForcedSingleFileGCThreshold_ = config.getDelteLogForcedGCLatency();
            options_.deltaStore_split_garbage_collection_start_single_file_minimum_occupancy_ = config.getDeltaLogSplitGCThreshold();
            options_.deltaStore_garbage_collection_start_single_file_minimum_occupancy = config.getDeltaLogGCThreshold();
        } else {
            options_.enable_deltaStore_garbage_collection = false;
        }
    }
    options_.enable_batched_operations_ = config.getDeltaStoreBatchEnableStatus();
    options_.batched_operations_number_ = config.getDeltaKVWriteBatchSize();

    if (fakeDirectIO) {
        cerr << "Enabled fake I/O, do not sync" << endl;
        options_.rocksdbRawOptions_.use_direct_reads = false;
        options_.rocksdbRawOptions_.use_direct_io_for_flush_and_compaction = false;
        options_.rocksdbRawOptions_.allow_mmap_reads = true;
        options_.rocksdbRawOptions_.allow_mmap_writes = true;
        options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kAlignLinuxIO;
        options_.rocksdb_sync_put = false;
        options_.rocksdb_sync_merge = false;
    } else {
        options_.rocksdb_sync_put = !keyValueSeparation;
        options_.rocksdb_sync_merge = !keyDeltaSeparation;
    }
    options_.enable_key_value_cache_ = config.getDeltaKVCacheEnableStatus();
    options_.key_value_cache_object_number_ = config.getDeltaKVCacheSize();

    options_.deltaKV_merge_operation_ptr.reset(new DELTAKV_NAMESPACE::DeltaKVFieldUpdateMergeOperator);
    options_.rocksdbRawOptions_.merge_operator.reset(new FieldUpdateMergeOperator);

    options_.rocksdbRawOptions_.create_if_missing = true;
    options_.rocksdbRawOptions_.write_buffer_size = memtableSize;
    // options_.rocksdbRawOptions_.compaction_pri = rocksdb::kMinOverlappingRatio;
    if (config.getTiered()) {
        options_.rocksdbRawOptions_.compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    options_.rocksdbRawOptions_.max_background_jobs = config.getNumThreads();
    options_.rocksdbRawOptions_.disable_auto_compactions = config.getNoCompaction();
    options_.rocksdbRawOptions_.level_compaction_dynamic_level_bytes = true;
    options_.rocksdbRawOptions_.target_file_size_base = config.getTargetFileSizeBase() * 1024;
    cerr << "Sync status = " << options_.rocksdb_sync_put << " " << options_.rocksdb_sync_merge << endl;
    cerr << "write buffer size " << options_.rocksdbRawOptions_.write_buffer_size << endl;
    cerr << "write buffer number " << options_.rocksdbRawOptions_.max_write_buffer_number << endl;
    cerr << "num compaction trigger "
         << options_.rocksdbRawOptions_.level0_file_num_compaction_trigger << endl;
    cerr << "targe file size base " << options_.rocksdbRawOptions_.target_file_size_base << endl;
    cerr << "level size base " << options_.rocksdbRawOptions_.max_bytes_for_level_base << endl;

    if (!compression) {
        options_.rocksdbRawOptions_.compression = rocksdb::kNoCompression;
    }
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    options_.rocksdbRawOptions_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.rocksdbRawOptions_.statistics = rocksdb::CreateDBStatistics();

    cerr << "Start create DeltaKVDB instance" << endl;

    DELTAKV_NAMESPACE::ConfigManager::getInstance().setConfigPath(config_file_path.c_str());

    bool dbOpenStatus = db_.Open(options_, dbfilename);
    if (!dbOpenStatus) {
        cerr << "Can't open DeltaKV " << dbfilename << endl;
        exit(0);
    } else {
        rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
    }
}

int DeltaKVDB::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {
    string value;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    int ret = db_.Get(key, &value);
    DELTAKV_NAMESPACE::StatsRecorder::getInstance()->timeProcess(DELTAKV_NAMESPACE::StatsType::DELTAKV_GET, tv);
    // outputStream_ << "YCSB Read " << key << " " << value << endl;
    return ret;
}

int DeltaKVDB::Scan(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {
    vector<string> keys, values;
    // db_.GetByPrefix(key, &keys, &values);
    return 1;
}

int DeltaKVDB::Insert(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
    rocksdb::Status s;
    string fullValue;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    bool status = db_.Put(key, fullValue);
    DELTAKV_NAMESPACE::StatsRecorder::getInstance()->timeProcess(DELTAKV_NAMESPACE::StatsType::DELTAKV_PUT, tv);
    if (!status) {
        cerr << "insert error"
             << endl;
        exit(0);
    }
    return 1;
}

int DeltaKVDB::Update(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    for (KVPair &p : values) {
        bool status = db_.Merge(key, p.second);
        if (!status) {
            cout << "Merge value failed" << endl;
            exit(-1);
        }
        // outputStream_ << "[YCSB] Update op, key = " << key << ", op value = " << p.second << endl;
    }
    DELTAKV_NAMESPACE::StatsRecorder::getInstance()->timeProcess(DELTAKV_NAMESPACE::StatsType::DELTAKV_MERGE, tv);
    // s = db_.Flush(rocksdb::FlushOptions());
    return 1;
}

int DeltaKVDB::OverWrite(const std::string &table, const std::string &key,
                         std::vector<KVPair> &values) {
    string fullValue;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    bool status = db_.Put(key, fullValue);
    if (!status) {
        cerr << "OverWrite error" << endl;
        exit(0);
    }
    DELTAKV_NAMESPACE::StatsRecorder::getInstance()->timeProcess(DELTAKV_NAMESPACE::StatsType::DELTAKV_PUT, tv);
    // outputStream_ << "[YCSB] Overwrite op, key = " << key << ", value = " << fullValue << endl;
    return 1;
}

int DeltaKVDB::Delete(const std::string &table, const std::string &key) {
    return db_.SingleDelete(key);  // Undefined result
}

void DeltaKVDB::printStats() {
    db_.pointerToRawRocksDB_->Flush(rocksdb::FlushOptions());
    string stats;
    db_.pointerToRawRocksDB_->GetProperty("rocksdb.stats", &stats);
    cout << stats << endl;
    // cout << options_.statistics->ToString() << endl;
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    cout << "Get RocksDB Build-in Perf Context: " << endl;
    cout << rocksdb::get_perf_context()->ToString() << endl;
    cout << "Get RocksDB Build-in I/O Stats Context: " << endl;
    cout << rocksdb::get_iostats_context()->ToString() << endl;
    cout << "Get RocksDB Build-in Total Stats Context: " << endl;
    cout << options_.rocksdbRawOptions_.statistics->ToString() << endl;
}

DeltaKVDB::~DeltaKVDB() {
    cerr << "1. Close stats recorder" << endl;
    DELTAKV_NAMESPACE::StatsRecorder::DestroyInstance();
    cerr << "2. Close output stream" << endl;
    outputStream_.close();
    cerr << "3. Close db" << endl;
    db_.Close();
    cerr << "4. Close DeltaKVDB complete" << endl;
}
}  // namespace ycsbc
