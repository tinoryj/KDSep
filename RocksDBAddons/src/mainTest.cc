#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"

using namespace DELTAKV_NAMESPACE;

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
    cerr << "Start create DeltaKVDB instance" << endl;

    string dbNameStr = "TempDB";
    bool dbOpenStatus = db_.Open(options_, dbNameStr);
    if (!dbOpenStatus) {
        cerr << "Can't open DeltaKV "
             << dbNameStr << endl;
        exit(0);
    } else {
        cerr << "Create DeltaKV success" << endl;
    }
    return 0;
}