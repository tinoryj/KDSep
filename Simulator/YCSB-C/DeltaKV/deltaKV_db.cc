#include "deltaKV_db.h"

#include <bits/stdc++.h>
#include <sys/time.h>

#include <iostream>

#include "db/extern_db_config.h"

using namespace std;

namespace ycsbc {

struct timeval timestartFull;
struct timeval timeendFull;
struct timeval timestartPart;
struct timeval timeendPart;
uint64_t counter_full = 0;
double totalTimeFull = 0;
uint64_t counter_part = 0;
double totalTimePart = 0;

DeltaKVDB::DeltaKVDB(const char *dbfilename, const std::string &config_file_path) {
    // outputStream_.open("operations.log", ios::binary | ios::out);
    // if (!outputStream_.is_open()) {
    //     cerr << "Load logging files error" << endl;
    // }
    // get rocksdb config
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    bool directIO = config.getDirectIO();
    bool keyValueSeparation = config.getKeyValueSeparation();
    bool keyDeltaSeparation = config.getKeyDeltaSeparation();
    size_t memtableSize = config.getMemtable();
    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    if (directIO == true) {
        options_.rocksdbRawOptions_.use_direct_reads = true;
        options_.rocksdbRawOptions_.use_direct_io_for_flush_and_compaction = true;
    } else {
        options_.rocksdbRawOptions_.allow_mmap_reads = true;
        options_.rocksdbRawOptions_.allow_mmap_writes = true;
    }
    options_.enable_batched_operations_ = false;
    options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kFstream;
    if (keyValueSeparation == true) {
        cerr << "Enabled Blob based KV separation" << endl;
        options_.enable_valueStore = true;
    }
    if (keyDeltaSeparation == true) {
        cerr << "Enabled DeltaLog based KD separation" << endl;
        // deltaKV settings
        options_.enable_deltaStore = true;
        options_.enable_deltaStore_KDLevel_cache = true;
        options_.deltaStore_operationNumberForMetadataCommitThreshold_ = 10000;
        options_.deltaStore_single_file_maximum_size = 1 * 1024;
        options_.deltaKV_merge_operation_ptr.reset(new DELTAKV_NAMESPACE::DeltaKVFieldUpdateMergeOperator);
    }
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
    bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    options_.rocksdbRawOptions_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.rocksdbRawOptions_.statistics = rocksdb::CreateDBStatistics();

    cerr << "Start create DeltaKVDB instance" << endl;

    bool dbOpenStatus = db_.Open(options_, dbfilename);
    if (!dbOpenStatus) {
        cerr << "Can't open DeltaKV " << dbfilename << endl;
        exit(0);
    }
}

int DeltaKVDB::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {
    string value;
    // cerr << "[YCSB] Read op, key = " << key << endl;
    return db_.Get(key, &value);
}

int DeltaKVDB::Scan(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {
    vector<string> keys, values;
    db_.GetByPrefix(key, &keys, &values);
    return 1;
}

int DeltaKVDB::Insert(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
    rocksdb::Status s;
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    bool status = db_.Put(key, fullValue);
    if (!status) {
        cerr << "insert error"
             << endl;
        exit(0);
    }
    return 1;
}

int DeltaKVDB::Update(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
    for (KVPair &p : values) {
        bool status = db_.Merge(key, p.second);
        if (!status) {
            cout << "Merge value failed" << endl;
            exit(-1);
        }
        // outputStream_ << "[YCSB] Update op, key = " << key << ", op value = " << p.second << endl;
    }
    // s = db_.Flush(rocksdb::FlushOptions());
    return 1;
}

int DeltaKVDB::OverWrite(const std::string &table, const std::string &key,
                         std::vector<KVPair> &values) {
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    bool status = db_.Put(key, fullValue);
    if (!status) {
        cerr << "OverWrite error" << endl;
        exit(0);
    }
    // outputStream_ << "[YCSB] Overwrite op, key = " << key << ", value = " << fullValue << endl;
    return 1;
}

int DeltaKVDB::Delete(const std::string &table, const std::string &key) {
    return db_.SingleDelete(key);  // Undefined result
}

void DeltaKVDB::printStats() {
}

DeltaKVDB::~DeltaKVDB() {
    outputStream_.close();
    db_.Close();
}
}  // namespace ycsbc
