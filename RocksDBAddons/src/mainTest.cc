#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "utils/murmurHash.hpp"

using namespace DELTAKV_NAMESPACE;

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

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Can't open DeltaKV "
             << dbNameStr << RESET << endl;
        exit(0);
    } else {
        cerr << GREEN << "[INFO]:[Addons]-[MainTest] Create DeltaKV success" << RESET << endl;
    }

    // operations
    string key = "Key1";
    string value = "Value1,value2";
    string merge = "1,value3";
    string valueTemp;
    if (!db_.Put(key, value)) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not put KV pairs to DB" << RESET << endl;
    } else if (!db_.Get(key, &valueTemp)) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not get KV pairs from DB" << RESET << endl;
    } else if (!db_.Merge(key, merge)) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not merge KV pairs to DB" << RESET << endl;
    } else if (!db_.Get(key, &valueTemp)) {
        cerr << RED << "[ERROR]:[Addons]-[MainTest] Could not get merged KV pairs from DB" << RESET << endl;
    } else {
        cerr << GREEN << "[INFO]:[Addons]-[MainTest] Function test fully correct" << RESET << endl;
        return 0;
    }
    cerr << RED << "[ERROR]:[Addons]-[MainTest] Function test not correct" << RESET << endl;
    return 0;
}