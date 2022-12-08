#ifndef YCSB_C_EXTRA_CONFIG_H
#define YCSB_C_EXTRA_CONFIG_H

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <string>

#include "core/db.h"
#include "leveldb/db.h"

using std::string;

namespace ycsbc {
class ExternDBConfig {
   private:
    boost::property_tree::ptree pt_;
    int bloomBits_;
    bool seekCompaction_;
    bool compression_;
    bool directIO_;
    bool fakeDirectIO_;
    bool noCompaction_;
    int numThreads_;
    int gcThreads_;
    int maxSortedRuns_;
    int sizeRatio_;
    size_t blockCache_;
    size_t gcSize_;
    size_t memtable_;
    size_t smallThresh_;
    size_t midThresh_;
    bool preheat_;
    int gcLevel_;
    int mergeLevel_;
    bool runGC_;
    bool gcWB_;
    bool tiered_;
    bool levelMerge_;
    bool rangeMerge_;
    bool lazyMerge_;
    bool sep_before_flush_;
    double GCRatio_;
    uint64_t blockWriteSize_;
    bool intra_compation_;
    bool key_value_separation_;
    bool key_delta_separation_;
    bool blob_db_key_value_separation_;
    uint64_t target_file_size_base_;
    uint64_t blob_file_size_;
    uint64_t deltaLog_file_size_;
    uint64_t deltaLog_file_flush_size_;
    double deltaLog_gc_threshold_;
    double deltaLog_split_gc_threshold_;
    uint64_t deltaLog_cache_object_number_;
    uint64_t deltaLog_prefix_min_bit_number_;
    uint64_t deltaLog_prefix_max_bit_number_;
    uint64_t deltaStore_operationNumberForMetadataCommitThreshold_;
    uint64_t deltaStore_operationNumberForForcedGCThreshold_;
    uint64_t deltaStore_thread_number_limit;
    bool deltaStore_enable_gc;

    struct {
        uint64_t level;
    } debug_;

   public:
    ExternDBConfig(std::string file_path) {
        boost::property_tree::ini_parser::read_ini(file_path, pt_);
        bloomBits_ = pt_.get<int>("config.bloomBits");
        seekCompaction_ = pt_.get<bool>("config.seekCompaction");
        compression_ = pt_.get<bool>("config.compression");
        directIO_ = pt_.get<bool>("config.directIO");
        fakeDirectIO_ = pt_.get<bool>("config.fakeDirectIO");
        blockCache_ = pt_.get<size_t>("config.blockCache");
        sizeRatio_ = pt_.get<int>("config.sizeRatio");
        gcSize_ = pt_.get<size_t>("config.gcSize");
        memtable_ = pt_.get<size_t>("config.memtable");
        noCompaction_ = pt_.get<bool>("config.noCompaction");
        numThreads_ = pt_.get<int>("config.numThreads");
        smallThresh_ = pt_.get<size_t>("config.smallThresh");
        midThresh_ = pt_.get<size_t>("config.midThresh");
        preheat_ = pt_.get<bool>("config.preheat");
        gcLevel_ = pt_.get<int>("config.gcLevel");
        mergeLevel_ = pt_.get<int>("config.mergeLevel");
        runGC_ = pt_.get<bool>("config.runGC");
        gcWB_ = pt_.get<bool>("config.gcWB");
        tiered_ = pt_.get<bool>("config.tiered");
        levelMerge_ = pt_.get<bool>("config.levelMerge");
        rangeMerge_ = pt_.get<bool>("config.rangeMerge");
        lazyMerge_ = pt_.get<bool>("config.lazyMerge");
        sep_before_flush_ = pt_.get<bool>("config.sepBeforeFlush");
        gcThreads_ = pt_.get<int>("config.gcThreads");
        maxSortedRuns_ = pt_.get<int>("config.maxSortedRuns");
        GCRatio_ = pt_.get<double>("config.gcRatio");
        blockWriteSize_ = pt_.get<uint64_t>("config.blockWriteSize");
        intra_compation_ = pt_.get<bool>("config.intraCompaction");
        key_value_separation_ = pt_.get<bool>("config.keyValueSeparation");
        key_delta_separation_ = pt_.get<bool>("config.keyDeltaSeparation");
        blob_db_key_value_separation_ = pt_.get<bool>("config.blobDbKeyValueSeparation");
        target_file_size_base_ = pt_.get<uint64_t>("config.targetFileSizeBase");
        blob_file_size_ = pt_.get<uint64_t>("config.blobFileSize");
        deltaLog_file_size_ = pt_.get<uint64_t>("config.deltaLogFileSize");
        deltaLog_file_flush_size_ = pt_.get<uint64_t>("config.deltaLogFileFlushSize");
        deltaLog_gc_threshold_ = pt_.get<double>("config.deltaLogGCThreshold");
        deltaLog_split_gc_threshold_ = pt_.get<double>("config.deltaLogSplitGCThreshold");
        deltaLog_cache_object_number_ = pt_.get<uint64_t>("config.deltaLogCacheObjectNumber");
        deltaLog_prefix_min_bit_number_ = pt_.get<uint64_t>("config.deltaLogPrefixMinBitNumber");
        deltaLog_prefix_max_bit_number_ = pt_.get<uint64_t>("config.deltaLogPrefixMaxBitNumber");
        deltaStore_operationNumberForMetadataCommitThreshold_ = pt_.get<uint64_t>("config.deltaStore_operationNumberForMetadataCommitThreshold_");
        deltaStore_operationNumberForForcedGCThreshold_ = pt_.get<uint64_t>("config.deltaStore_operationNumberForForcedGCThreshold_");
        deltaStore_thread_number_limit = pt_.get<uint64_t>("config.deltaStore_thread_number_limit_");
        debug_.level = pt_.get<uint64_t>("debug.level");
        deltaStore_enable_gc = pt_.get<bool>("config.deltaStoreEnableGC");
    }

    int getBloomBits() {
        return bloomBits_;
    }
    uint64_t getBlockWriteSize() {
        return blockWriteSize_;
    }
    int getMaxSortedRuns() {
        return maxSortedRuns_;
    }
    int getGCThreads() {
        return gcThreads_;
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
    bool getFakeDirectIO() {
        return fakeDirectIO_;
    }
    int getNumThreads() {
        return numThreads_;
    }
    size_t getBlockCache() {
        return blockCache_;
    }
    int getSizeRatio() {
        return sizeRatio_;
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
    size_t getSmallThresh() {
        return smallThresh_;
    }
    size_t getMidThresh() {
        return midThresh_;
    }
    double getGCRatio() {
        return GCRatio_;
    }
    bool getPreheat() {
        return preheat_;
    }
    int getGCLevel() {
        return gcLevel_;
    }
    int getMergeLevel() {
        return mergeLevel_;
    }
    bool getRunGC() {
        return runGC_;
    }
    bool getGCWB() {
        return gcWB_;
    }
    bool getTiered() {
        return tiered_;
    }
    bool getLevelMerge() {
        return levelMerge_;
    }
    bool getRangeMerge() {
        return rangeMerge_;
    }
    bool getLazyMerge() {
        return lazyMerge_;
    }
    bool getSepBeforeFlush() {
        return sep_before_flush_;
    }
    bool getIntraCompaction() {
        return intra_compation_;
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

    uint64_t getDeltaLogPrefixMinBitNumber() {
        return deltaLog_prefix_min_bit_number_;
    }

    uint64_t getDeltaLogPrefixMaxBitNumber() {
        return deltaLog_prefix_max_bit_number_;
    }

    uint64_t getDelteLogMetadataCommitLatency() {
        return deltaStore_operationNumberForMetadataCommitThreshold_;
    }

    uint64_t getDelteLogForcedGCLatency() {
        return deltaStore_operationNumberForForcedGCThreshold_;
    }

    uint64_t getDeltaLogThreadNumber() {
        return deltaStore_thread_number_limit;
    }

    uint64_t getDebugLevel() {
        return debug_.level;
    }

    bool getDeltaStoreGCEnableStatus() {
        return deltaStore_enable_gc;
    }
};
}  // namespace ycsbc

#endif  // YCSB_C_EXTRA_CONFIG_H
