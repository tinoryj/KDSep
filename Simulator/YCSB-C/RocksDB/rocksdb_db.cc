#include "rocksdb_db.h"
#include "db/extern_db_config.h"
#include <iostream>
#include <sys/time.h>

using namespace std;
using namespace rocksdb;

namespace ycsbc {

struct timeval timestartFull;
struct timeval timeendFull;
struct timeval timestartPart;
struct timeval timeendPart;
uint64_t counter_full = 0;
double totalTimeFull = 0;
uint64_t counter_part = 0;
double totalTimePart = 0;

vector<string> split(string str, string token)
{
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0)
                result.push_back(str);
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
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override
    {

        counter_full++;
        gettimeofday(&timestartFull, NULL);
        // cout << existing_value->data() << "\n Size=" << existing_value->size() << endl;
        // new_value->assign(existing_value->ToString());
        vector<std::string> words = split(existing_value->ToString(), ",");
        // for (long unsigned int i = 0; i < words.size(); i++) {
        //     cout << "Index = " << i << ", Words = " << words[i] << endl;
        // }
        for (auto q : operand_list) {
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
        totalTimeFull += 1000000 * (timeendFull.tv_sec - timestartFull.tv_sec) + timeendFull.tv_usec - timestartFull.tv_usec;
        return true;
    };

    // This function performs merge(left_op, right_op)
    // when both the operands are themselves merge operation types.
    // Save the result in *new_value and return true. If it is impossible
    // or infeasible to combine the two operations, return false instead.
    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override
    {
        counter_part++;
        gettimeofday(&timestartPart, NULL);
        new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
        gettimeofday(&timeendPart, NULL);
        totalTimePart += 1000000 * (timeendPart.tv_sec - timestartPart.tv_sec) + timeendPart.tv_usec - timestartPart.tv_usec;
        // cout << left_operand.data() << "\n Size=" << left_operand.size() << endl;
        // cout << right_operand.data() << "\n Size=" << right_operand.size() << endl;
        // cout << new_value << "\n Size=" << new_value->length() << endl;
        // new_value->assign(left_operand.data(), left_operand.size());
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char* Name() const override
    {
        return "FieldUpdateMergeOperator";
    }
};

RocksDB::RocksDB(const char* dbfilename, const std::string& config_file_path)
{
    rocksdb::Options options;
    //get rocksdb config
    ExternDBConfig config = ExternDBConfig(config_file_path);
    int bloomBits = config.getBloomBits();
    size_t blockCacheSize = config.getBlockCache();
    // bool seekCompaction = config.getSeekCompaction();
    bool compression = config.getCompression();
    // bool directIO = config.getDirectIO();
    size_t memtable = config.getMemtable();
    //set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    options.create_if_missing = true;
    options.allow_mmap_reads = true;
    options.allow_mmap_writes = true;
    options.statistics = rocksdb::CreateDBStatistics();
    options.write_buffer_size = memtable;
    options.target_file_size_base = 64 << 20;
    //options.compaction_pri = rocksdb::kMinOverlappingRatio;
    if (config.getTiered())
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.max_background_jobs = config.getNumThreads();
    options.disable_auto_compactions = config.getNoCompaction();
    options.level_compaction_dynamic_level_bytes = true;
    options.target_file_size_base = 8 << 20;
    cerr << "write buffer size " << options.write_buffer_size << endl;
    cerr << "write buffer number " << options.max_write_buffer_number << endl;
    cerr << "num compaction trigger " << options.level0_file_num_compaction_trigger << endl;
    cerr << "targe file size base " << options.target_file_size_base << endl;
    cerr << "level size base " << options.max_bytes_for_level_base << endl;
    if (!compression)
        options.compression = rocksdb::kNoCompression;
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options.merge_operator.reset(new FieldUpdateMergeOperator); // merge operators
    cerr << "Start create RocksDB instance" << endl;
    rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
    if (!s.ok()) {
        cerr << "Can't open rocksdb " << dbfilename << endl;
        exit(0);
    }
    // cout << "\nbloom bits:" << bloomBits << "bits\ndirectIO:" << (bool)directIO << "\nseekCompaction:" << (bool)seekCompaction << endl;
}

int RocksDB::Read(const std::string& table, const std::string& key, const std::vector<std::string>* fields,
    std::vector<KVPair>& result)
{
    string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
    // s = db_->Put(rocksdb::WriteOptions(), key, value); // write back
    return s.ok();
}

int RocksDB::Scan(const std::string& table, const std::string& key, int len, const std::vector<std::string>* fields,
    std::vector<std::vector<KVPair>>& result)
{
    auto it = db_->NewIterator(rocksdb::ReadOptions());
    it->Seek(key);
    std::string val;
    std::string k;
    int i;
    int cnt = 0;
    for (i = 0; i < len && it->Valid(); i++) {
        k = it->key().ToString();
        val = it->value().ToString();
        it->Next();
        if (val.empty())
            cnt++;
    }
    delete it;
    return DB::kOK;
}

int RocksDB::Insert(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
    rocksdb::Status s;
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    s = db_->Put(rocksdb::WriteOptions(), key, fullValue);
    if (!s.ok()) {
        cerr << "insert error" << s.ToString() << "\n"
             << endl;
        exit(0);
    }
    return DB::kOK;
}

int RocksDB::Update(const std::string& table, const std::string& key, std::vector<KVPair>& values)
{
    rocksdb::Status s;
    for (KVPair& p : values) {
        s = db_->Merge(rocksdb::WriteOptions(), key, p.second);
        if (!s.ok()) {
            // cout << "Merge value failed: " << s.ToString() << endl;
            exit(-1);
        }
    }
    // s = db_->Flush(rocksdb::FlushOptions());
    return s.ok();
}

int RocksDB::OverWrite(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
    rocksdb::Status s;
    string fullValue;
    for (long unsigned int i = 0; i < values.size() - 1; i++) {
        fullValue += (values[i].second + ",");
    }
    fullValue += values[values.size() - 1].second;
    s = db_->Put(rocksdb::WriteOptions(), key, fullValue);
    if (!s.ok()) {
        cerr << "insert error" << s.ToString() << endl;
        exit(0);
    }
    return DB::kOK;
}

int RocksDB::Delete(const std::string& table, const std::string& key)
{
    std::string value;
    rocksdb::Status s = db_->SingleDelete(rocksdb::WriteOptions(), key, value); // Undefined result
    return s.ok();
}

void RocksDB::printStats()
{
    string stats;
    db_->GetProperty("rocksdb.stats", &stats);
    // cout << stats << endl;
    // cout << options.statistics->ToString() << endl;
}

RocksDB::~RocksDB()
{
    cout << "Full merge operation number = " << counter_full << endl;
    cout << "Full merge operation running time = " << totalTimeFull / 1000000.0 << " s" << endl;
    cout << "Partial merge operation number = " << counter_part << endl;
    cout << "Partial merge operation running time = " << totalTimePart / 1000000.0 << " s" << endl;
    delete db_;
}
}
