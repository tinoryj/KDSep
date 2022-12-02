#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/appendAbleLRUCache.hpp"
#include "utils/fileOperation.hpp"
#include "utils/murmurHash.hpp"
#include "utils/timer.hpp"

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
    debug_info("Test put operation with key = %s\n", key.c_str());
    if (!db_.Put(key, value)) {
        debug_error("Could not put KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testGet(DeltaKV& db_, string key, string& value)
{
    debug_info("Test get operation with key = %s\n", key.c_str());
    if (!db_.Get(key, &value)) {
        debug_error("Could not get KV pairs from DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testMerge(DeltaKV& db_, string key, string& value)
{
    debug_info("Test merge operation with key = %s\n", key.c_str());
    if (!db_.Merge(key, value)) {
        debug_error("Could not merge KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testBatchedPut(DeltaKV& db_, string key, string& value)
{
    debug_info("Test batched put operation with key = %s\n", key.c_str());
    if (!db_.PutWithWriteBatch(key, value)) {
        debug_error("Could not put KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
        return true;
    }
}

bool testBatchedMerge(DeltaKV& db_, string key, string& value)
{
    debug_info("Test batched merge operation with key = %s\n", key.c_str());
    if (!db_.MergeWithWriteBatch(key, value)) {
        debug_error("Could not merge KV pairs to DB for key = %s\n", key.c_str());
        return false;
    } else {
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

bool fstreamTestFlush(uint64_t testNumber)
{
    Timer newTimer;
    fstream testStream;
    testStream.open("test.flush", ios::out);
    testStream.close();
    testStream.open("test.flush", ios::in | ios::out | ios::binary);
    char writeBuffer[4096];
    memset(writeBuffer, 1, 4096);
    newTimer.startTimer();
    for (int i = 0; i < testNumber; i++) {
        testStream.write(writeBuffer, 4096);
        testStream.flush();
    }
    newTimer.stopTimer("Flush");
    testStream.close();
    return true;
}

bool fstreamTestFlushSync(uint64_t testNumber)
{
    Timer newTimer;
    fstream testStream;
    testStream.open("test.sync", ios::out);
    testStream.close();
    testStream.open("test.sync", ios::in | ios::out | ios::binary);
    char writeBuffer[4096];
    memset(writeBuffer, 1, 4096);
    newTimer.startTimer();
    for (int i = 0; i < testNumber; i++) {
        testStream.write(writeBuffer, 4096);
        testStream.flush();
        testStream.sync();
    }
    newTimer.stopTimer("Flush+Sync");
    testStream.close();
    return true;
}

bool fstreamTestDIRECT(uint64_t testNumber)
{
    Timer newTimer;
    char* writeBuffer;
    auto ret = posix_memalign((void**)&writeBuffer, sysconf(_SC_PAGESIZE), 4096);
    if (ret) {
        printf("posix_memalign failed: %d %s\n", errno, strerror(errno));
        return false;
    }
    memset(writeBuffer, 1, 4096);
    const int fileDesc = open("test.direct", O_CREAT | O_WRONLY | O_DIRECT);
    if (-1 != fileDesc) {
        newTimer.startTimer();
        for (int i = 0; i < testNumber; i++) {
            write(fileDesc, writeBuffer, 4096);
        }
        newTimer.stopTimer("O_DIRECT");
    }
    free(writeBuffer);
    close(fileDesc);
    return true;
}

bool directIOTest(string path)
{
    cout << "Page size = " << sysconf(_SC_PAGE_SIZE) << endl;
    FileOperation currentFilePtr(kDirectIO);
    currentFilePtr.createFile(path);
    currentFilePtr.openFile(path);
    string writeStr = "test";
    char writeBuffer[4];
    memcpy(writeBuffer, writeStr.c_str(), 4);
    currentFilePtr.writeFile(writeBuffer, 4);
    cout << "Write content = " << writeBuffer << endl;
    char readBuffer[4];
    currentFilePtr.resetPointer(kBegin);
    currentFilePtr.readFile(readBuffer, 4);
    string readStr(readBuffer, 4);
    cout << "Read content = " << readStr << endl;
    currentFilePtr.closeFile();
    return true;
}

int main(int argc, char* argv[])
{
    // string path = "test.log";
    // directIOTest(path);
    // fstreamTestFlush(100000);
    // fstreamTestFlushSync(100000);
    // fstreamTestDIRECT(100000);
    // return 0;

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
    options_.deltaKV_merge_operation_ptr.reset(new DeltaKVFieldUpdateMergeOperator);
    options_.enable_batched_operations_ = false;
    options_.fileOperationMethod_ = kDirectIO;

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Can't open DeltaKV "
             << dbNameStr << RESET << endl;
        exit(0);
    }
    // dump operations
    options_.dumpOptions("TempDB/options.dump");
    options_.dumpDataStructureInfo("TempDB/structure.dump");
    // operations
    Timer newTimer;
    newTimer.startTimer();
    int testNumber = 10;
    if (argc != 1) {
        testNumber = stoi(argv[1]);
    }
    for (int i = 0; i < testNumber; i++) {
        string key1 = "Key001" + to_string(i);
        string key2 = "Key002" + to_string(i);
        string value1 = "value1,value2";
        string value1Merged = "value5,value6";
        string value2 = "value3,value4";
        string merge1 = "1,value5";
        string merge2 = "2,value6";
        vector<bool> testResultBoolVec;
        if (options_.enable_batched_operations_ == true) {
            // put
            bool statusPut1 = testBatchedPut(db_, key1, value1);
            testResultBoolVec.push_back(statusPut1);
            string tempReadStr1;
            // get raw value
            bool statusGet1 = testGet(db_, key1, tempReadStr1);
            testResultBoolVec.push_back(statusGet1);
            if (tempReadStr1.compare(value1) != 0) {
                cerr << BOLDRED << "[ERROR]: Error read raw value, raw value = " << value1 << ", read value = " << tempReadStr1 << endl;
                break;
            }
            for (int i = 0; i < 4; i++) {
                string mergeStr = "1,value" + to_string(i);
                testBatchedMerge(db_, key1, mergeStr);
            }
            // get merged value
            string tempReadStrTemp;
            bool statusGetTemp = testGet(db_, key1, tempReadStrTemp);
            testResultBoolVec.push_back(statusGetTemp);
            string resultStr = "value3,value2";
            if (tempReadStrTemp.compare(resultStr) != 0) {
                cerr << BOLDRED << "[ERROR]: Error read merged value, merged value = " << resultStr << ", read value = " << tempReadStrTemp << endl;
                break;
            }
        } else {
            // put
            bool statusPut1 = testPut(db_, key1, value1);
            testResultBoolVec.push_back(statusPut1);
            string tempReadStr1;
            // get raw value
            bool statusGet1 = testGet(db_, key1, tempReadStr1);
            testResultBoolVec.push_back(statusGet1);
            if (tempReadStr1.compare(value1) != 0) {
                cerr << BOLDRED << "[ERROR]: Error read raw value, raw value = " << value1 << ", read value = " << tempReadStr1 << endl;
                break;
            }
            bool statusMerge1 = testMerge(db_, key1, merge1);
            testResultBoolVec.push_back(statusMerge1);
            bool statusMerge2 = testMerge(db_, key1, merge2);
            testResultBoolVec.push_back(statusMerge2);

            // get merged value
            string tempReadStr2;
            bool statusGet2 = testGet(db_, key1, tempReadStr2);
            testResultBoolVec.push_back(statusGet2);
            if (tempReadStr2.compare(value1Merged) != 0) {
                cerr << BOLDRED << "[ERROR]: Error read merged value, merged value = " << value1Merged << ", read value = " << tempReadStr2 << endl;
                break;
            }

            bool statusPut2 = testPut(db_, key1, value2);
            testResultBoolVec.push_back(statusPut2);
            // get overwrited value
            string tempReadStr3;
            bool statusGet3 = testGet(db_, key1, tempReadStr3);
            testResultBoolVec.push_back(statusGet3);
            if (tempReadStr3.compare(value2) != 0) {
                cerr << BOLDRED << "[ERROR]: Error read overwrited value, overwrited value = "
                     << value2 << ", read value = " << tempReadStr3 << endl;
                break;
            }
        }
    }
    newTimer.stopTimer("Running DB operations time");
    db_.Close();
    return 0;
}
