#ifndef YCSB_C_EXTRA_CONFIG_H
#define YCSB_C_EXTRA_CONFIG_H

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <string>

#include "core/db.h"
// #include "leveldb/db.h"

using std::string;

namespace ycsbc {
class ExternDBConfig {
   private:
    boost::property_tree::ptree pt_;
    int bloomBits_;
    bool seekCompaction_;
    bool compression_;
    bool directIO_;
    bool useMmap_;
    bool fakeDirectIO_;
    bool noCompaction_;
    int numThreads_;
    size_t blockCache_;
    size_t blobCacheSize;
    size_t gcSize_;
    size_t memtable_;
    bool tiered_;
    double GCRatio_;
    bool key_value_separation_;
    bool key_delta_separation_;
    bool blob_db_key_value_separation_;
    uint64_t target_file_size_base_;
    uint64_t max_bytes_for_level_base_;
    uint64_t blob_file_size_;
    uint64_t deltaLog_file_size_;
    uint64_t deltaLog_file_flush_size_;
    double deltaLog_gc_threshold_;
    double deltaLog_split_gc_threshold_;
    uint64_t deltaLog_cache_object_number_;
    uint64_t deltaLog_file_number_;
    uint64_t deltaStore_operationNumberForMetadataCommitThreshold_;
    uint64_t deltaStore_operationNumberForForcedGCThreshold_;
    uint64_t deltaStore_worker_thread_number_limit;
    uint64_t deltaStore_gc_thread_number_limit;
    bool deltaStore_enable_gc;
    bool deltaKV_enable_batch;
    uint64_t deltaStore_write_back_during_reads_threshold_ = 5;
    uint64_t deltaStore_write_back_during_reads_size_threshold_ = 5;
    uint64_t deltaStore_gc_write_back_delta_num_ = 5;
    uint64_t deltaStore_gc_write_back_delta_size_ = 2000;
    uint64_t deltaKVWriteBatchSize_ = 5;
    uint64_t deltaKVCacheObjectNumber_;
    uint64_t prefixTreeBitNumber_;
    bool enableDeltaKVCache_ = false;
    bool enableRawRocksDBBatch_;
    bool enableLsmTreeDeltaMeta_;
    uint64_t blockSize;
    uint64_t minBlobSize;
    bool cacheIndexAndFilterBlocks_;
    uint64_t maxKeyValueSize;
    uint64_t maxOpenFiles;
    bool deltaStore_KDLevel_cache_use_str_t_;

    struct {
        uint64_t level;
    } debug_;

   public:
    ExternDBConfig(std::string file_path) {
        boost::property_tree::ini_parser::read_ini(file_path, pt_);
        bloomBits_ = pt_.get<int>("config.bloomBits");
        seekCompaction_ = pt_.get<bool>("config.seekCompaction");
        compression_ = pt_.get<bool>("config.compression");
        directIO_ = pt_.get<bool>("config.directIO", true);
        useMmap_ = pt_.get<bool>("config.useMmap", true);
        fakeDirectIO_ = pt_.get<bool>("config.fakeDirectIO");
        blockCache_ = pt_.get<size_t>("config.blockCache");
        blobCacheSize = pt_.get<size_t>("config.blobCacheSize", 0);
        gcSize_ = pt_.get<size_t>("config.gcSize");
        memtable_ = pt_.get<size_t>("config.memtable");
        noCompaction_ = pt_.get<bool>("config.noCompaction");
        numThreads_ = pt_.get<int>("config.numThreads");
        tiered_ = pt_.get<bool>("config.tiered");
        GCRatio_ = pt_.get<double>("config.gcRatio");
        key_value_separation_ = pt_.get<bool>("config.keyValueSeparation");
        key_delta_separation_ = pt_.get<bool>("config.keyDeltaSeparation");
        blob_db_key_value_separation_ = pt_.get<bool>("config.blobDbKeyValueSeparation");
        target_file_size_base_ = pt_.get<uint64_t>("config.targetFileSizeBase", 65536);
        max_bytes_for_level_base_ = pt_.get<uint64_t>("config.maxBytesForLevelBase", 262144);
        blob_file_size_ = pt_.get<uint64_t>("config.blobFileSize");
        deltaLog_file_size_ = pt_.get<uint64_t>("deltaStore.deltaLogFileSize");
        deltaLog_file_flush_size_ = pt_.get<uint64_t>("deltaStore.deltaLogFileFlushSize");
        deltaLog_gc_threshold_ = pt_.get<double>("deltaStore.deltaLogGCThreshold");
        deltaLog_split_gc_threshold_ = pt_.get<double>("deltaStore.deltaLogSplitGCThreshold");
        deltaLog_cache_object_number_ = pt_.get<uint64_t>("deltaStore.deltaLogCacheObjectNumber");
        deltaLog_file_number_ = pt_.get<uint64_t>("deltaStore.deltaLogMaxFileNumber");
        deltaStore_operationNumberForMetadataCommitThreshold_ = pt_.get<uint64_t>("deltaStore.deltaStore_operationNumberForMetadataCommitThreshold_");
        deltaStore_operationNumberForForcedGCThreshold_ = pt_.get<uint64_t>("deltaStore.deltaStore_operationNumberForForcedGCThreshold_");
        deltaStore_worker_thread_number_limit = pt_.get<uint64_t>("deltaStore.deltaStore_worker_thread_number_limit_");
        deltaStore_gc_thread_number_limit = pt_.get<uint64_t>("deltaStore.deltaStore_gc_thread_number_limit_");
        debug_.level = pt_.get<uint64_t>("debug.level");
        deltaStore_enable_gc = pt_.get<bool>("deltaStore.deltaStoreEnableGC");
        deltaKV_enable_batch = pt_.get<bool>("config.enableBatchedOperations");
        deltaStore_write_back_during_reads_threshold_ = pt_.get<uint64_t>("deltaStore.deltaStoreWriteBackDuringReadsThreshold");
        deltaStore_write_back_during_reads_size_threshold_ = pt_.get<uint64_t>("deltaStore.deltaStoreWriteBackDuringReadsSizeThreshold", 1000);
        deltaStore_gc_write_back_delta_num_ = pt_.get<uint64_t>("deltaStore.deltaStoreGcWriteBackDeltaNumThreshold", 0);
        deltaStore_gc_write_back_delta_size_ = pt_.get<uint64_t>("deltaStore.deltaStoreGcWriteBackDeltaSizeThreshold", 1000);
        deltaKVWriteBatchSize_ = pt_.get<uint64_t>("config.deltaKVWriteBatchSize");
        enableDeltaKVCache_ = pt_.get<bool>("config.enableDeltaKVCache");
        deltaKVCacheObjectNumber_ = pt_.get<uint64_t>("config.deltaKVCacheObjectNumber");
        prefixTreeBitNumber_ = pt_.get<uint64_t>("deltaStore.initBitNumber");
        enableRawRocksDBBatch_ = pt_.get<bool>("config.enableRawRocksDBBatch");
        maxKeyValueSize = pt_.get<uint64_t>("config.maxKeyValueSize", 4096);
        maxOpenFiles = pt_.get<uint64_t>("config.maxOpenFiles", 1048576);
        minBlobSize = pt_.get<uint64_t>("rocksdb.minBlobSize", 800);
        blockSize = pt_.get<uint64_t>("rocksdb.blockSize", 4096);
        cacheIndexAndFilterBlocks_ = pt_.get<bool>("rocksdb.cacheIndexAndFilterBlocks", false);
        enableLsmTreeDeltaMeta_ = pt_.get<bool>("config.enableLsmTreeDeltaMeta", true);
        deltaStore_KDLevel_cache_use_str_t_ = pt_.get<bool>("config.deltaStore_KDLevel_cache_use_str_t", true);
    }

    int getBloomBits() {
        return bloomBits_;
    }
    bool getSeekCompaction() {
        return seekCompaction_;
    }
    bool getCompression() {
        return compression_;
    }
    bool getDirectIO() {
        return directIO_;
    }
    bool getUseMmap() {
        return useMmap_;
    }
    bool getFakeDirectIO() {
        return fakeDirectIO_;
    }
    int getNumThreads() {
        return numThreads_;
    }
    size_t getBlockCache() {
        return blockCache_;
    }
    size_t getBlobCacheSize() {
        return blobCacheSize;
    }
    size_t getGcSize() {
        return gcSize_;
    }
    size_t getMemtable() {
        return memtable_;
    }
    bool getNoCompaction() {
        return noCompaction_;
    }
    double getGCRatio() {
        return GCRatio_;
    }
    bool getTiered() {
        return tiered_;
    }
    bool getKeyValueSeparation() {
        return key_value_separation_;
    }
    bool getKeyDeltaSeparation() {
        return key_delta_separation_;
    }
    bool getBlobDbKeyValueSeparation() {
        return blob_db_key_value_separation_;
    }
    uint64_t getTargetFileSizeBase() {
        return target_file_size_base_;
    }
    uint64_t getMaxBytesForLevelBase() {
        return max_bytes_for_level_base_;
    }
    uint64_t getBlobFileSize() {
        return blob_file_size_;
    }
    uint64_t getDeltaLogFileSize() {
        return deltaLog_file_size_;
    }

    uint64_t getDeltaLogFileFlushSize() {
        return deltaLog_file_flush_size_;
    }

    double getDeltaLogGCThreshold() {
        return deltaLog_gc_threshold_;
    }

    double getDeltaLogSplitGCThreshold() {
        return deltaLog_split_gc_threshold_;
    }

    uint64_t getDeltaLogCacheObjectNumber() {
        return deltaLog_cache_object_number_;
    }

    uint64_t getDeltaLogMaxFileNumber() {
        return deltaLog_file_number_;
    }

    uint64_t getDelteLogMetadataCommitLatency() {
        return deltaStore_operationNumberForMetadataCommitThreshold_;
    }

    uint64_t getDelteLogForcedGCLatency() {
        return deltaStore_operationNumberForForcedGCThreshold_;
    }

    uint64_t getDeltaLogOpWorkerThreadNumber() {
        return deltaStore_worker_thread_number_limit;
    }

    uint64_t getDeltaLogGCWorkerThreadNumber() {
        return deltaStore_gc_thread_number_limit;
    }

    uint64_t getDebugLevel() {
        return debug_.level;
    }

    bool getDeltaStoreGCEnableStatus() {
        return deltaStore_enable_gc;
    }

    bool getDeltaStoreBatchEnableStatus() {
        return deltaKV_enable_batch;
    }

    uint64_t getDeltaStoreWriteBackDuringReadsThreshold() {
        return deltaStore_write_back_during_reads_threshold_;
    }

    uint64_t getDeltaStoreWriteBackDuringReadsSizeThreshold() {
        return deltaStore_write_back_during_reads_size_threshold_;
    }

    uint64_t getDeltaStoreGcWriteBackDeltaNumThreshold() {
        return deltaStore_gc_write_back_delta_num_;
    }

    uint64_t getDeltaStoreGcWriteBackDeltaSizeThreshold() {
        return deltaStore_gc_write_back_delta_size_;
    }

    uint64_t getDeltaKVWriteBatchSize() {
        return deltaKVWriteBatchSize_;
    }

    bool getDeltaKVCacheEnableStatus() {
        return enableDeltaKVCache_;
    }

    uint64_t getDeltaKVCacheSize() {
        return deltaKVCacheObjectNumber_;
    }

    uint64_t getPrefixTreeBitNumber() {
        return prefixTreeBitNumber_;
    }

    bool getEnableRoaRocksDBBatch() {
        return enableRawRocksDBBatch_;
    }
    uint64_t getBlockSize() {
        return blockSize;
    }
    uint64_t getMaxKeyValueSize() {
        return maxKeyValueSize;
    }
    uint64_t getMaxOpenFiles() {
        return maxOpenFiles;
    }
    uint64_t getMinBlobSize() {
	return minBlobSize;
    }
    bool cacheIndexAndFilterBlocks() {
        return cacheIndexAndFilterBlocks_;
    }
    bool getEnableLsmTreeDeltaMeta() {
        return enableLsmTreeDeltaMeta_;
    }
    bool getDeltaStoreKDLevelCacheUseStrT() {
        return deltaStore_KDLevel_cache_use_str_t_;
    }
};
}  // namespace ycsbc

#endif  // YCSB_C_EXTRA_CONFIG_H
