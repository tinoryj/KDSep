#pragma once
#include <climits>
#include <map>
#include <shared_mutex>
#include <stdio.h>
#include <sys/time.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// #include <hdr_histogram.h>
#include "common/indexStorePreDefines.hpp"

#define S2US (1000 * 1000)
#define MAX_DISK (2)

namespace DELTAKV_NAMESPACE {

enum StatsType {
    KEY_IDENTIFY,
    WORKLOAD_OTHERS,
    YCSB_OPERATION,
    YCSB_OPERATION_EXTRA,
    YCSB_READ_GEN,
    YCSB_UPDATE_GEN,
    YCSB_INSERT_GEN,
    DELTAKV_INSERT_MEMPOOL,
    DSTORE_PREFIX,
    DSTORE_MULTIPUT_PREFIX,
    DS_GEN_PREFIX_SHIFT,
    DSTORE_EXIST_FLAG,
    DELTAKV_HASHSTORE_CREATE_NEW_BUCKET,
    DSTORE_GET_HANDLER,
    DSTORE_MULTIPUT_GET_HANDLER,
    DSTORE_GET_GET_HANDLER,
    DSTORE_GET_HANDLER_LOOP,
    DELTAKV_GC_READ,
    DELTAKV_GC_PROCESS,
    DELTAKV_GC_PARTIAL_MERGE,
    DELTAKV_GC_WRITE,

    /* DeltaKV write back */
    DELTAKV_WRITE_BACK,
    DELTAKV_GC_WRITE_BACK,
    DELTAKV_GET_PUT_WRITE_BACK,
    DELTAKV_WRITE_BACK_PUT,
    DELTAKV_WRITE_BACK_GET,
    DELTAKV_WRITE_BACK_FULL_MERGE,
    DELTAKV_WRITE_BACK_WAIT_BUFFER,
    DELTAKV_WRITE_BACK_NO_WAIT_BUFFER,
    DELTAKV_WRITE_BACK_CHECK_BUFFER,

    /* Cache map find */
    DELTAKV_HASHSTORE_CACHE_FIND,
    DELTAKV_HASHSTORE_CACHE_PROMOTE,

    /* DeltaKV dStore MultPut breakdown */
    DS_MULTIPUT_UPDATE_FILTER,
    DS_MULTIPUT_PREPARE_FILE_HEADER,
    DS_MULTIPUT_PREPARE_FILE_CONTENT,
    DS_WRITE_FUNCTION,
    DS_FILE_FUNC_REAL_WRITE,
    DS_FILE_FUNC_REAL_FLUSH,
    DS_MULTIPUT_INSERT_CACHE,
    DS_MULTIPUT_INSERT_CACHE_CHECK,
    DS_MULTIPUT_INSERT_CACHE_UPDATE,

    DELTAKV_HASHSTORE_WAIT_BUFFER,
    DELTAKV_PUT_HASHSTORE_GET_HANDLER,
    DELTAKV_PUT_HASHSTORE_WAIT,
    DELTAKV_PUT_HASHSTORE_WAIT_ANCHOR,
    DELTAKV_PUT_HASHSTORE_WAIT_DELTA,
    DELTAKV_PUT_HASHSTORE_QUEUE,
    DELTAKV_PUT_HASHSTORE_QUEUE_ANCHOR,
    DELTAKV_PUT_HASHSTORE_QUEUE_DELTA,
    DELTAKV_HASHSTORE_PUT,
    DELTAKV_HASHSTORE_PUT_IO_TRAFFIC,
    DELTAKV_HASHSTORE_PUT_COMMIT_LOG,
    DELTAKV_HASHSTORE_GET_FILE_HANDLER,
    DELTAKV_HASHSTORE_GET_CACHE,
    DELTAKV_HASHSTORE_GET_INSERT_CACHE,
    DELTAKV_HASHSTORE_GET_PROCESS_TEST,
    DELTAKV_HASHSTORE_GET_PROCESS,
    DELTAKV_HASHSTORE_GET_IO,
    DELTAKV_HASHSTORE_GET_IO_ALL,
    DELTAKV_HASHSTORE_GET_IO_SORTED,
    DELTAKV_HASHSTORE_GET_IO_UNSORTED,
    DELTAKV_HASHSTORE_GET_IO_BOTH,
    DELTAKV_HASHSTORE_SYNC,
    DELTAKV_HASHSTORE_WAIT_SYNC,
    DELTAKV_HASHSTORE_WORKER_GC,
    DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE,
    DELTAKV_HASHSTORE_WORKER_GC_BEFORE_SPLIT,
    DELTAKV_SCAN,
    DELTAKV_GET,
    LSM_INTERFACE_GET,
    DELTAKV_GET_ROCKSDB,
    DELTAKV_GET_INDEXSTORE,
    DELTAKV_GET_HASHSTORE,
    DELTAKV_GET_HASHSTORE_GET_HANDLER,
    DELTAKV_GET_PROCESS_BUFFER,
    DELTAKV_GET_FULL_MERGE,
    DELTAKV_MERGE,
    DELTAKV_MERGE_ROCKSDB,
    DELTAKV_MERGE_INDEXSTORE,
    DELTAKV_MERGE_HASHSTORE,
    /* DeltaKV batch read interface */
    DKV_GET_WAIT_BUFFER,
    DKV_GET_READ_BUFFER,
    DELTAKV_BATCH_READ_GET_KEY,
    DELTAKV_BATCH_READ_MERGE,
    DELTAKV_BATCH_READ_MERGE_ALL,
    DELTAKV_BATCH_READ_STORE,

    FM_UPDATE_META,

    /* merge requests */
    DKV_MERGE_LOCK_1,
    DKV_MERGE_LOCK_2,
    DKV_MERGE_APPEND_BUFFER,
    DKV_MERGE_FULL_MERGE,

    DKV_LSM_INTERFACE_OP,

    /* put requests */
    DKV_PUT_LOCK_1,
    DKV_PUT_LOCK_2,
    DKV_PUT_APPEND_BUFFER,

    /* get requests */
    DKV_GET_FULL_MERGE,
    DKV_LSM_INTERFACE_GET,

    /* Flush buffer */
    DKV_FLUSH,
    DKV_FLUSH_DEDUP,
    DKV_DEDUP_FULL_MERGE,
    DKV_DEDUP_PARTIAL_MERGE,
    DKV_FLUSH_WITH_DSTORE,
    DKV_FLUSH_WITH_NO_DSTORE,
    DKV_FLUSH_MUTIPUT_DSTORE,
    DKV_FLUSH_LSM_INTERFACE,
    LSM_FLUSH_VLOG,
    LSM_FLUSH_ROCKSDB,
    LSM_FLUSH_ROCKSDB_FULLMERGE,
    LSM_FLUSH_ROCKSDB_PARTIALMERGE,
    LSM_FLUSH_PRE_PUT,
    LSM_FLUSH_WAL,

    /* DeltaKV interface */
    DELTAKV_PUT,
    DELTAKV_PUT_ROCKSDB,
    DELTAKV_PUT_INDEXSTORE,
    DKV_PUT_DSTORE,
    DS_PUT_GET_HANDLER,
    DS_MULTIPUT_GET_HANDLER,
    DS_MULTIPUT_GET_SINGLE_HANDLER,
    DS_MULTIPUT_PUT_TO_JOB_QUEUE,
    DS_MULTIPUT_PROCESS_HANDLERS,
    DS_MULTIPUT_PUT_TO_JOB_QUEUE_OPERATOR,
    DS_MULTIPUT_WAIT_HANDLERS,
    DS_MULTIPUT_DIRECT_OP,

    /* delta merge */
    FULL_MERGE,
    PARTIAL_MERGE,

    /* batch */
    BATCH_PLAIN_ROCKSDB,
    BATCH_KV_KD,
    BATCH_KV,
    BATCH_KD,

    /* batch flush */

    /* vlog r/w */
    DEVICE_WRITE,
    DEVICE_READ,

    /* op */
    OP_GET,
    OP_MULTIPUT,
    OP_PUT,
    OP_FLUSH,
    OP_FIND,
    GC_SELECT_MERGE,
    GC_SELECT_MERGE_GET_NODES,
    GC_SELECT_MERGE_SELECT_MERGE,
    GC_SELECT_MERGE_AFTER_SELECT,
    MERGE,
    MERGE_WAIT_LOCK,
    MERGE_CREATE_HANDLER,
    MERGE_WAIT_LOCK3,
    MERGE_FILE1,
    MERGE_FILE2,
    MERGE_FILE3,
    MERGE_METADATA,
    SPLIT,
    SPLIT_HANDLER,
    SPLIT_IN_MEMORY,
    SPLIT_WRITE_FILES,
    SPLIT_METADATA,
    REWRITE,
    REWRITE_GET_FILE_ID,
    REWRITE_ADD_HEADER,
    REWRITE_CLOSE_FILE,
    REWRITE_CREATE_FILE,
    REWRITE_OPEN_FILE,
    REWRITE_BEFORE_WRITE,
    REWRITE_WRITE,
    REWRITE_AFTER_WRITE,
    /* Set */
    SET,
    SET_KEY_LOOKUP,
    SET_KEY_WRITE,
    SET_KEY_WRITE_SHADOW,
    SET_VALUE,
    /* Update */
    UPDATE,
    UPDATE_KEY_LOOKUP,
    UPDATE_KEY_LOOKUP_LSM,
    UPDATE_KEY_LOOKUP_CACHE,
    UPDATE_KEY_WRITE,
    UPDATE_KEY_WRITE_LSM,
    UPDATE_KEY_WRITE_LSM_GC,
    UPDATE_KEY_WRITE_CACHE,
    UPDATE_KEY_WRITE_SHADOW,
    UPDATE_VALUE,
    /* Merge */
    MERGE_INDEX_UPDATE,
    /* Read */
    GET,
    GET_KEY_LOOKUP,
    GET_VALUE,
    /* Scan */
    SCAN,
    /* GC */
    GC_WRITE_BYTES,
    GC_SCAN_BYTES,
    GC_READ,
    GC_READ_AHEAD,
    GC_PHASE_TEST,
    GC_PHASE_TEST_2,
    GC_PHASE_TEST_21,
    GC_PHASE_TEST_22,
    GC_PHASE_TEST_3,
    GC_PHASE_TEST_31,
    GC_PHASE_TEST_32,
    GC_PHASE_TEST_311,
    GC_PRE_FLUSH,
    GC_FLUSH,
    GC_IN_FLUSH,
    GC_IN_FLUSH_WITH_SYNC,
    GC_KEY_LOOKUP,
    GC_OTHERS,
    GC_TOTAL,
    GC_UPDATE_COUNT,
    /* LSM / Key processing */
    KEY_GET_ALL,
    KEY_GET_CACHE,
    KEY_GET_LSM,
    KEY_GET_SHADOW,
    KEY_SET_ALL,
    KEY_SET_CACHE,
    KEY_SET_LSM,
    KEY_SET_LSM_BATCH,
    KEY_SET_SHADOW,
    KEY_UPDATE_CACHE,
    /* FLUSH */
    GROUP_IN_POOL_FLUSH,
    GROUP_OTHER_FLUSH,
    POOL_FLUSH,
    POOL_FLUSH_NO_GC,
    POOL_FLUSH_WAIT,
    FLUSH_BYTES,
    /* Metadata consistency */
    LOG_TIME,
    /* Device */
    DATA_WRITE_BYTES,
    FLUSH_SYNC,
    /* Others */
    UPDATE_TO_MAIN,
    UPDATE_TO_LOG,
    GC_RATIO_UPDATE,
    GC_INVALID_BYTES_UPDATE,
    /* DeltaKV Interface */
    DELTAKV_CACHE_INSERT_NEW,
    DELTAKV_CACHE_INSERT_MERGE,
    DELTAKV_CACHE_GET,
    
    /* Bucket key filter */
    FILTER_READ_EXIST_FALSE,
    FILTER_READ_EXIST_TRUE,
    FILTER_READ_TIMES,

    /* End */
    NUMLENGTH
};

class StatsRecorder {

public:
    static unsigned long long timeAddto(struct timeval& start_time, unsigned long long& resTime);

    static StatsRecorder* getInstance();
    static void DestroyInstance();

#define STAT_PROCESS(_FUNC_, _TYPE_)                                  \
    do {                                                              \
        struct timeval startTime;                                     \
        gettimeofday(&startTime, 0);                                  \
        _FUNC_;                                                       \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime); \
    } while (0);

#define STAT_TIME_PROCESS_VS(_FUNC_, _TYPE_, _VS_)                                \
    do {                                                                          \
        struct timeval startTime;                                                 \
        gettimeofday(&startTime, 0);                                              \
        _FUNC_;                                                                   \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime, 0, 1, _VS_); \
    } while (0);

    bool inline IsGCStart()
    {
        return startGC;
    }

    void totalProcess(StatsType stat, size_t diff, size_t count = 1);
    unsigned long long timeProcess(StatsType stat, struct timeval& start_time, size_t diff = 0, size_t count = 1, unsigned long long valueSize = 0);

    void inline IOBytesWrite(unsigned int bytes, unsigned int diskId)
    {
        if (!statisticsOpen)
            return;
        IOBytes[diskId].first += bytes;
    }

    void inline IOBytesRead(unsigned int bytes, unsigned int diskId)
    {
        if (!statisticsOpen)
            return;
        IOBytes[diskId].second += bytes;
    }

    void inline DeltaGcBytesWrite(unsigned int bytes, unsigned int logicalBytes, bool syncStat)
    {
        if (!statisticsOpen)
            return;
        if (syncStat) {
            std::unique_lock<shared_mutex> w_lock(deltaGCWriteMtx_);
            DeltaGcPhysicalBytes.first += bytes;
            DeltaGcPhysicalTimes.first++;
            DeltaGcLogicalBytes.first += logicalBytes;
            DeltaGcLogicalTimes.first++;
        } else {
            DeltaGcPhysicalBytes.first += bytes;
            DeltaGcPhysicalTimes.first++;
            DeltaGcLogicalBytes.first += logicalBytes;
            DeltaGcLogicalTimes.first++;
        }
    }

    void inline DeltaGcBytesRead(unsigned int bytes, unsigned int logicalBytes, bool syncStat)
    {
        if (!statisticsOpen)
            return;
        if (syncStat) {
            std::unique_lock<shared_mutex> w_lock(deltaGCReadMtx_);
            DeltaGcPhysicalBytes.second += bytes;
            DeltaGcPhysicalTimes.second++;
            DeltaGcLogicalBytes.second += logicalBytes;
            DeltaGcLogicalTimes.second++;
        } else {
            DeltaGcPhysicalBytes.second += bytes;
            DeltaGcPhysicalTimes.second++;
            DeltaGcLogicalBytes.second += logicalBytes;
            DeltaGcLogicalTimes.second++;
        }
    }

    void inline DeltaOPBytesWrite(unsigned int bytes, unsigned int logicalBytes, bool syncStat)
    {
        if (!statisticsOpen)
            return;
        if (syncStat) {
            std::unique_lock<shared_mutex> w_lock(deltaOPWriteMtx_);
            DeltaOPPhysicalBytes.first += bytes;
            DeltaOPPhysicalTimes.first++;
            DeltaOPLogicalBytes.first += logicalBytes;
            DeltaOPLogicalTimes.first++;
        } else {
            DeltaOPPhysicalBytes.first += bytes;
            DeltaOPPhysicalTimes.first++;
            DeltaOPLogicalBytes.first += logicalBytes;
            DeltaOPLogicalTimes.first++;
        }
    }

    void inline DeltaOPBytesRead(unsigned int bytes, unsigned int logicalBytes, bool syncStat)
    {
        if (!statisticsOpen)
            return;
        if (syncStat) {
            std::unique_lock<shared_mutex> w_lock(deltaOPWriteMtx_);
            DeltaOPPhysicalBytes.second += bytes;
            DeltaOPPhysicalTimes.second++;
            DeltaOPLogicalBytes.second += logicalBytes;
            DeltaOPLogicalTimes.second++;
        } else {
            DeltaOPPhysicalBytes.second += bytes;
            DeltaOPPhysicalTimes.second++;
            DeltaOPLogicalBytes.second += logicalBytes;
            DeltaOPLogicalTimes.second++;
        }
    }

    void minMaxGCUpdate(unsigned int mn, unsigned int mx)
    {
        if (!statisticsOpen)
            return;
        if (mn < min[GC_UPDATE_COUNT])
            min[GC_UPDATE_COUNT] = mn;
        if (mx > max[GC_UPDATE_COUNT])
            max[GC_UPDATE_COUNT] = mx;
    }

    void openStatistics(struct timeval& start_time);
    void printProcess(const char* arg1, unsigned int i);

    void putGCGroupStats(unsigned long long validMain, unsigned long long validLog, unsigned long long invalidMain, unsigned long long invalidLog, unsigned long long validLastLog);
    void putFlushGroupStats(unsigned long long validDataGroups, std::unordered_map<group_id_t, unsigned long long>& counts);

private:
    StatsRecorder();
    ~StatsRecorder();

    static StatsRecorder* mInstance;

    bool statisticsOpen;
    bool startGC;
    unsigned long long time[NUMLENGTH];
    unsigned long long total[NUMLENGTH];
    unsigned long long max[NUMLENGTH];
    unsigned long long min[NUMLENGTH];
    unsigned long long counts[NUMLENGTH];
    unsigned long long gcNums;
    unsigned long long gcInMemNums;
    unsigned long long us;
    unsigned long long lsmLookupTime;
    unsigned long long approximateMemoryUsage;
    std::vector<std::pair<unsigned long long, unsigned long long>> IOBytes; /* write,read */
    std::pair<unsigned long long, unsigned long long> DeltaGcPhysicalBytes = { 0, 0 }; /* write,read */
    std::pair<unsigned long long, unsigned long long> DeltaGcPhysicalTimes = { 0, 0 };
    std::pair<unsigned long long, unsigned long long> DeltaOPPhysicalBytes = { 0, 0 }; /* write,read */
    std::pair<unsigned long long, unsigned long long> DeltaOPPhysicalTimes = { 0, 0 };
    std::pair<unsigned long long, unsigned long long> DeltaGcLogicalBytes = { 0, 0 }; /* write,read */
    std::pair<unsigned long long, unsigned long long> DeltaGcLogicalTimes = { 0, 0 };
    std::pair<unsigned long long, unsigned long long> DeltaOPLogicalBytes = { 0, 0 }; /* write,read */
    std::pair<unsigned long long, unsigned long long> DeltaOPLogicalTimes = { 0, 0 };
    //    struct hdr_histogram *_updateTimeHistogram;
    //    struct hdr_histogram *_getTimeHistogram;
    //    std::map<unsigned long long, struct hdr_histogram*> _getByValueSizeHistogram;
    //    std::map<unsigned long long, struct hdr_histogram*> _updateByValueSizeHistogram;

    struct Stats {
        unsigned long long* buckets[2];
        unsigned long long sum[2];
        unsigned long long count[2];
    };

    struct {
        struct Stats valid;
        struct Stats invalid;
        struct Stats validLastLog;
        unsigned int bucketLen;
        unsigned long long bucketSize;
    } gcGroupBytesCount;

    struct Stats flushGroupCount;
    int flushGroupCountBucketLen;

    std::shared_mutex deltaGCWriteMtx_, deltaGCReadMtx_, deltaOPWriteMtx_, deltaOPReadMtx_;
    enum {
        MAIN,
        LOG
    };
};

}
