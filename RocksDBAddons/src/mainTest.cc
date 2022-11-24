#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/murmurHash.hpp"

using namespace DELTAKV_NAMESPACE;

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

class FieldUpdateMergeOperatorInternal : public MergeOperator {
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
        vector<std::string> words = split(existing_value->ToString(), ",");
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
        new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
        return true;
    };

    // The name of the MergeOperator. Used to check for MergeOperator
    // mismatches (i.e., a DB created with one MergeOperator is
    // accessed using a different MergeOperator)
    const char* Name() const override { return "FieldUpdateMergeOperatorInternal"; }
};

int main()
{
    // partial function test
    // u_char murmurHashResultBuffer[16];
    // string rawStr = "test";
    // MurmurHash3_x64_128((void*)rawStr.c_str(), rawStr.size(), 0, murmurHashResultBuffer);
    // uint64_t firstFourByte;
    // string prefixStr;
    // memcpy(&firstFourByte, murmurHashResultBuffer, sizeof(uint64_t));
    // cout << hex << firstFourByte << endl;
    // while (firstFourByte != 0) {
    //     prefixStr += ((firstFourByte & 1) + '0');
    //     firstFourByte >>= 1;
    // }
    // cout << prefixStr << endl;

    // DeltaKV test
    DeltaKV db_;
    DeltaKVOptions options_;
    int bloomBits = 10;
    size_t blockCacheSize = 128;
    bool directIO = true;
    size_t memtableSize = 64;
    // set optionssc
    rocksdb::BlockBasedTableOptions bbto;
    if (directIO == true) {
        options_.rocksdbRawOptions_.use_direct_reads = true;
        options_.rocksdbRawOptions_.use_direct_io_for_flush_and_compaction = true;
    } else {
        options_.rocksdbRawOptions_.allow_mmap_reads = true;
        options_.rocksdbRawOptions_.allow_mmap_writes = true;
    }
    options_.rocksdbRawOptions_.create_if_missing = true;
    options_.rocksdbRawOptions_.write_buffer_size = memtableSize;
    options_.rocksdbRawOptions_.max_background_jobs = 8;
    options_.rocksdbRawOptions_.disable_auto_compactions = false;
    options_.rocksdbRawOptions_.level_compaction_dynamic_level_bytes = true;
    options_.rocksdbRawOptions_.target_file_size_base = 65536 * 1024;
    options_.rocksdbRawOptions_.compression = rocksdb::kNoCompression;
    if (bloomBits > 0) {
        bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
    }
    bbto.block_cache = rocksdb::NewLRUCache(blockCacheSize);
    options_.rocksdbRawOptions_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

    options_.rocksdbRawOptions_.statistics = rocksdb::CreateDBStatistics();

    // deltaKV settings
    options_.enable_deltaStore = true;
    if (options_.enable_deltaStore == false) {
        options_.rocksdbRawOptions_.merge_operator.reset(new FieldUpdateMergeOperatorInternal);
    }
    options_.deltaKV_merge_operation_ptr.reset(new FieldUpdateMergeOperator);

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Can't open DeltaKV "
             << dbNameStr << RESET << endl;
        exit(0);
    } else {
        cout << GREEN << "[INFO]:[Addons]-[MainTest] Create DeltaKV success" << RESET << endl;
    }
    // dump operations
    options_.dumpOptions("TempDB/options.dump");
    cout << GREEN << "[INFO]:[Addons]-[MainTest] Dump DeltaKV options success" << RESET << endl;
    // operations
    string key = "Key1";
    string value = "Value1,value2";
    string merge = "1,value3";
    if (!db_.Put(key, value)) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not put KV pairs to DB" << RESET << endl;
    } else {
        cerr << GREEN << "[INFO]:[Addons]-[MainTest] Put function test correct" << RESET << endl;
        string valueTempForRaw;
        if (!db_.Get(key, &valueTempForRaw)) {
            cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not get KV pairs from DB" << RESET << endl;
        } else {
            cerr << GREEN << "[INFO]:[Addons]-[MainTest] Get function test correct, value size = " << valueTempForRaw.size() << " content = " << valueTempForRaw << RESET << endl;
            if (!db_.Merge(key, merge)) {
                cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not merge KV pairs to DB" << RESET << endl;
            } else {
                cerr << GREEN << "[INFO]:[Addons]-[MainTest] Merge function test correct" << RESET << endl;
                string valueTempForMerged;
                if (!db_.Get(key, &valueTempForMerged)) {
                    cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not get merged KV pairs from DB" << RESET << endl;
                } else {
                    cerr << GREEN << "[INFO]:[Addons]-[MainTest] Read merged value function test correct, value = " << valueTempForMerged << RESET << endl;
                    return 0;
                }
            }
        }
    }
    cerr << RED << "[ERROR]:[Addons]-[MainTest] Function test not correct" << RESET << endl;
    return 0;
}