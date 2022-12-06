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
        counter_full++;
        gettimeofday(&timestartFull, NULL);
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
        gettimeofday(&timeendFull, NULL);
        totalTimeFull += 1000000 * (timeendFull.tv_sec - timestartFull.tv_sec) +
                         timeendFull.tv_usec - timestartFull.tv_usec;
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
        counter_part++;
        gettimeofday(&timestartPart, NULL);
        new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
        gettimeofday(&timeendPart, NULL);
        totalTimePart += 1000000 * (timeendPart.tv_sec - timestartPart.tv_sec) +
                         timeendPart.tv_usec - timestartPart.tv_usec;
        // cout << left_operand.data() << "\n Size=" << left_operand.size() << endl;
        // cout << right_operand.data() << "\n Size=" << right_operand.size() <<
        // endl; cout << new_value << "\n Size=" << new_value->length() << endl;
        // new_value->assign(left_operand.data(), left_operand.size());
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
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    bool directIO = config.getDirectIO();
    bool keyValueSeparation = config.getKeyValueSeparation();
    bool keyDeltaSeparation = config.getKeyDeltaSeparation();
    bool blobDbKeyValueSeparation = config.getBlobDbKeyValueSeparation();
    size_t memtableSize = config.getMemtable();
    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    if (directIO == true) {
        options_.rocksdbRawOptions_.use_direct_reads = true;
        options_.rocksdbRawOptions_.use_direct_io_for_flush_and_compaction = true;
        options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kDirectIO;
    } else {
        options_.rocksdbRawOptions_.allow_mmap_reads = true;
        options_.rocksdbRawOptions_.allow_mmap_writes = true;
        options_.fileOperationMethod_ = DELTAKV_NAMESPACE::kFstream;
    }
    options_.enable_batched_operations_ = false;
    if (blobDbKeyValueSeparation == true) {
        cerr << "Enabled Blob based KV separation" << endl;
        options_.rocksdbRawOptions_.enable_blob_files = true;
        options_.rocksdbRawOptions_.min_blob_size = 0;                                                 // Default 0
        options_.rocksdbRawOptions_.blob_file_size = config.getBlobFileSize() * 1024;                  // Default 256*1024*1024
        options_.rocksdbRawOptions_.blob_compression_type = kNoCompression;                            // Default kNoCompression
        options_.rocksdbRawOptions_.enable_blob_garbage_collection = false;                            // Default false
        options_.rocksdbRawOptions_.blob_garbage_collection_age_cutoff = 0.25;                         // Default 0.25
        options_.rocksdbRawOptions_.blob_garbage_collection_force_threshold = 1.0;                     // Default 1.0
        options_.rocksdbRawOptions_.blob_compaction_readahead_size = 0;                                // Default 0
        options_.rocksdbRawOptions_.blob_file_starting_level = 0;                                      // Default 0
        options_.rocksdbRawOptions_.blob_cache = nullptr;                                              // Default nullptr
        options_.rocksdbRawOptions_.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;  // Default kDisable
        assert(!keyValueSeparation);
    }
    if (keyValueSeparation == true) {
        cerr << "Enabled vLog based KV separation" << endl;
        options_.enable_valueStore = true;
    }
    if (keyDeltaSeparation == true) {
        cerr << "Enabled DeltaLog based KD separation" << endl;
        // deltaKV settings
        options_.enable_deltaStore = true;
        options_.enable_deltaStore_KDLevel_cache = true;
        options_.deltaStore_operationNumberForMetadataCommitThreshold_ = 1000;
        options_.deltaStore_single_file_maximum_size = 1 * 1024;
        options_.deltaStore_file_flush_buffer_size_limit_ = 256;
        options_.deltaStore_thread_number_limit = 3;
    }
    if (keyValueSeparation == true || keyDeltaSeparation == true) {
        options_.deltaKV_merge_operation_ptr.reset(new DELTAKV_NAMESPACE::DeltaKVFieldUpdateMergeOperator);
    } else {
        options_.rocksdbRawOptions_.merge_operator.reset(new FieldUpdateMergeOperator);
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
    int ret = db_.Get(key, &value);
    // outputStream_ << "YCSB Read " << key << " " << value << endl;
    return ret;
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
