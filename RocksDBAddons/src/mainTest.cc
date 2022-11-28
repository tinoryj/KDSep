#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/appendAbleLRUCache.hpp"
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

bool testPut(DeltaKV& db_, string key, string& value)
{
    if (!db_.Put(key, value)) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Could not put KV pairs to DB" << RESET << endl;
        return false;
    } else {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Put key = " << key
             << ", value = " << value << " success" << RESET << endl;
        return true;
    }
}

bool testGet(DeltaKV& db_, string key, string& value)
{
    if (!db_.Get(key, &value)) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "):Could not read KV pairs from DB" << RESET << endl;
        return false;
    } else {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Read key = " << key
             << ", value = " << value << " success" << RESET << endl;
        return true;
    }
}

bool testMerge(DeltaKV& db_, string key, string& value)
{
    if (!db_.Merge(key, value)) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Could not merge KV pairs to DB" << RESET << endl;
        return false;
    } else {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Merge key = " << key
             << ", value = " << value << " success" << RESET << endl;
        return true;
    }
}

bool LRUCacheTest()
{
    AppendAbleLRUCache<string, vector<string>*>* keyToValueListCache_ = nullptr;
    keyToValueListCache_ = new AppendAbleLRUCache<string, vector<string>*>(1000);

    string keyStr = "key1";
    vector<string>* testValueVec = new vector<string>;
    testValueVec->push_back("test1");
    keyToValueListCache_->insertToCache(keyStr, testValueVec);
    if (keyToValueListCache_->existsInCache(keyStr) == true) {
        vector<string>* testValueVecRead = keyToValueListCache_->getFromCache(keyStr);
        for (auto index = 0; index < testValueVecRead->size(); index++) {
            cout << testValueVecRead->at(index) << endl;
        }
        testValueVecRead->push_back("test2");
        cout << "After insert" << endl;
        keyToValueListCache_->getFromCache(keyStr);
        for (auto index = 0; index < testValueVecRead->size(); index++) {
            cout << testValueVecRead->at(index) << endl;
        }
    }
    return true;
}

bool murmurHashTest(string rawStr)
{
    u_char murmurHashResultBuffer[16];
    MurmurHash3_x64_128((void*)rawStr.c_str(), rawStr.size(), 0, murmurHashResultBuffer);
    uint64_t firstFourByte;
    string prefixStr;
    memcpy(&firstFourByte, murmurHashResultBuffer, sizeof(uint64_t));
    while (firstFourByte != 0) {
        prefixStr += ((firstFourByte & 1) + '0');
        firstFourByte >>= 1;
    }
    cout << "\tPrefix first 64 bit in BIN: " << prefixStr << endl;
    return true;
}

int main()
{
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
    options_.enable_deltaStore_KDLevel_cache = true;
    if (options_.enable_deltaStore == false) {
        options_.rocksdbRawOptions_.merge_operator.reset(new FieldUpdateMergeOperatorInternal);
    }
    options_.deltaKV_merge_operation_ptr.reset(new FieldUpdateMergeOperator);

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Can't open DeltaKV "
             << dbNameStr << RESET << endl;
        exit(0);
    } else {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Create DeltaKV success" << RESET << endl;
    }
    // dump operations
    options_.dumpOptions("TempDB/options.dump");
    options_.dumpDataStructureInfo("TempDB/structure.dump");
    cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Dump DeltaKV options success" << RESET << endl;
    // operations
    string key1 = "Key001";
    string key2 = "Key002";
    string value1 = "Value1,value2";
    string value2 = "Value3,value4";
    string merge1 = "1,value5";
    string merge2 = "2,value6";

    vector<bool> testResultBoolVec;
    // put
    bool statusPut1 = testPut(db_, key1, value1);
    testResultBoolVec.push_back(statusPut1);
    string tempReadStr1;
    // get raw value
    bool statusGet1 = testGet(db_, key1, tempReadStr1);
    testResultBoolVec.push_back(statusGet1);
    bool statusMerge1 = testMerge(db_, key1, merge1);
    testResultBoolVec.push_back(statusMerge1);
    bool statusMerge2 = testMerge(db_, key1, merge2);
    testResultBoolVec.push_back(statusMerge2);
    // get merged value
    string tempReadStr2;
    bool statusGet2 = testGet(db_, key1, tempReadStr2);
    testResultBoolVec.push_back(statusGet2);
    bool statusPut2 = testPut(db_, key1, value2);
    testResultBoolVec.push_back(statusPut2);
    // get overwrited value
    string tempReadStr3;
    bool statusGet3 = testGet(db_, key1, tempReadStr3);
    testResultBoolVec.push_back(statusGet3);
    // new key test
    bool statusPut3 = testPut(db_, key2, value1);
    testResultBoolVec.push_back(statusPut3);
    string tempReadStr4;
    bool statusGet4 = testGet(db_, key2, tempReadStr4);
    testResultBoolVec.push_back(statusGet4);
    bool finalStatus = true;
    for (int i = 0; i < testResultBoolVec.size(); i++) {
        finalStatus = finalStatus && testResultBoolVec[i];
    }
    if (finalStatus) {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): MurmurHash of Keys used in this test:" << RESET << endl;
        murmurHashTest(key1);
        murmurHashTest(key2);
        cout << GREEN << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Function test correct" << RESET << endl;
    } else {
        cout << YELLOW << "[INFO]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): MurmurHash of Keys used in this test:" << RESET << endl;
        murmurHashTest(key1);
        murmurHashTest(key2);
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Function test not correct" << RESET << endl;
    }
    return 0;
}