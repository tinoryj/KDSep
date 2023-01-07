#include "utils/statsRecorder.hh"
#include <boost/concept_check.hpp>

namespace DELTAKV_NAMESPACE {

StatsRecorder* StatsRecorder::mInstance = NULL;

unsigned long long inline timevalToMicros(struct timeval& res)
{
    return res.tv_sec * S2US + res.tv_usec;
}

long long unsigned int StatsRecorder::timeAddto(timeval& start_time, long long unsigned int& resTime)
{
    struct timeval end_time, res;
    unsigned long long diff;
    gettimeofday(&end_time, NULL);
    timersub(&end_time, &start_time, &res);
    diff = timevalToMicros(res);
    resTime += diff;
    return diff;
}

StatsRecorder* StatsRecorder::getInstance()
{
    if (mInstance == NULL) {
        mInstance = new StatsRecorder();
    }
    return mInstance;
}

void StatsRecorder::DestroyInstance()
{
    if (mInstance != NULL) {
        delete mInstance;
        mInstance = NULL;
    }
}

StatsRecorder::StatsRecorder()
{
    // init counters, e.g. bytes and time
    for (unsigned int i = 0; i < NUMLENGTH; i++) {
        time[i] = 0;
        total[i] = 0;
        max[i] = 0;
        min[i] = 1 << 31;
        counts[i] = 0;
    }
    statisticsOpen = false;
    startGC = false;

    // init disk write bytes counters
    int N = MAX_DISK;
    IOBytes.resize(N);
    for (int i = 0; i < N; i++) {
        IOBytes[i] = std::pair<unsigned long long, unsigned long long>(0, 0);
    }

    // init gc bytes stats
    unsigned long long segmentSize = 1048576; // ConfigManager::getInstance().getMainSegmentSize();
    int K = MAX_DISK;
    // max log segment in a group
    unsigned long long factor = K * 8;
    // accuracy
    unsigned long long bytesPerSlot = 4096;
    unsigned long long numBuckets = segmentSize / bytesPerSlot * factor + 1;
    for (int i = 0; i < 2; i++) {
        gcGroupBytesCount.valid.buckets[i] = new unsigned long long[numBuckets];
        gcGroupBytesCount.invalid.buckets[i] = new unsigned long long[numBuckets];
        gcGroupBytesCount.validLastLog.buckets[i] = new unsigned long long[numBuckets];
        for (unsigned long long j = 0; j < numBuckets; j++) {
            gcGroupBytesCount.valid.buckets[i][j] = 0;
            gcGroupBytesCount.invalid.buckets[i][j] = 0;
            gcGroupBytesCount.validLastLog.buckets[i][j] = 0;
        }
        gcGroupBytesCount.valid.sum[i] = 0;
        gcGroupBytesCount.invalid.sum[i] = 0;
        gcGroupBytesCount.validLastLog.sum[i] = 0;
        gcGroupBytesCount.valid.count[i] = 0;
        gcGroupBytesCount.invalid.count[i] = 0;
        gcGroupBytesCount.validLastLog.count[i] = 0;
    }
    gcGroupBytesCount.bucketLen = numBuckets;
    gcGroupBytesCount.bucketSize = bytesPerSlot;
    int maxGroup = 2; // ConfigManager::getInstance().getNumMainSegment() + 1;
    flushGroupCountBucketLen = segmentSize / 512 + 1;
    flushGroupCount.buckets[0] = new unsigned long long[maxGroup]; // data stripes in each flush
    flushGroupCount.buckets[1] = new unsigned long long[flushGroupCountBucketLen]; // updates in each data stripe
    for (int i = 0; i < maxGroup; i++) {
        flushGroupCount.buckets[0][i] = 0;
    }
    for (int i = 0; i < flushGroupCountBucketLen; i++) {
        flushGroupCount.buckets[1][i] = 0;
    }
    for (int i = 0; i < 2; i++) {
        flushGroupCount.sum[i] = 0;
        flushGroupCount.count[i] = 0;
    }

    //    _updateTimeHistogram = 0;
    //    hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_updateTimeHistogram);
    //    _getTimeHistogram = 0;
    //    hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_getTimeHistogram);
}

StatsRecorder::~StatsRecorder()
{
    // print all stats before destory
    fprintf(stdout, "==============================================================\n");

#define PRINT_SUM(_NAME_, _TYPE_)                                    \
    do {                                                             \
        fprintf(stdout, "%-24s sum:%16llu\n", _NAME_, time[_TYPE_]); \
    } while (0);

#define PRINT_FULL(_NAME_, _TYPE_, _SUM_)                                                                                                                                                       \
    do {                                                                                                                                                                                        \
        fprintf(stdout, "%-30s sum:%16llu count:%12llu avg.:%10.2lf per.:%6.2lf%%\n", _NAME_, time[_TYPE_], counts[_TYPE_], time[_TYPE_] * 1.0 / counts[_TYPE_], time[_TYPE_] * 100.0 / _SUM_); \
    } while (0);

    fprintf(stdout, "------------------------- Total -------------------------------------\n");
    PRINT_FULL("workload-others", WORKLOAD_OTHERS, (time[WORKLOAD_OTHERS] + time[DELTAKV_PUT] + time[DELTAKV_GET] + time[DELTAKV_MERGE]));
    PRINT_FULL("DeltaKV-put", DELTAKV_PUT, (time[WORKLOAD_OTHERS] + time[DELTAKV_PUT] + time[DELTAKV_GET] + time[DELTAKV_MERGE]));
    PRINT_FULL("DeltaKV-get", DELTAKV_GET, (time[WORKLOAD_OTHERS] + time[DELTAKV_PUT] + time[DELTAKV_GET] + time[DELTAKV_MERGE]));
    PRINT_FULL("DeltaKV-merge", DELTAKV_MERGE, (time[WORKLOAD_OTHERS] + time[DELTAKV_PUT] + time[DELTAKV_GET] + time[DELTAKV_MERGE]));

    fprintf(stdout, "------------------------- DELTAKV Temp  -------------------------------------\n");
    PRINT_FULL("DeltaKV-tmp1", DELTAKV_TMP1, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-tmp2", DELTAKV_TMP2, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-tmp3", DELTAKV_TMP3, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-create-new-bucket", DELTAKV_HASHSTORE_CREATE_NEW_BUCKET, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-create-gc-bucket", DELTAKV_HASHSTORE_CREATE_GC_BUCKET, time[DELTAKV_PUT]);
    PRINT_FULL("DKV-gc-write-back", DELTAKV_GC_WRITE_BACK, time[DELTAKV_GC_WRITE_BACK]);
    PRINT_FULL("DKV-get-write-back", DELTAKV_GET_WRITE_BACK, time[DELTAKV_GET_WRITE_BACK]);

    fprintf(stdout, "------------------------- DELTAKV Request -----------------------------------\n");
    PRINT_FULL("DeltaKV-put", DELTAKV_PUT, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-put-rocksdb", DELTAKV_PUT_ROCKSDB, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-put-vLog", DELTAKV_PUT_INDEXSTORE, time[DELTAKV_PUT]);
    PRINT_FULL("DeltaKV-put-dStore", DELTAKV_PUT_HASHSTORE, time[DELTAKV_PUT]);
    fprintf(stdout, "\n");
    PRINT_FULL("DeltaKV-get", DELTAKV_GET, time[DELTAKV_GET]);
    PRINT_FULL("DeltaKV-get-rocksdb", DELTAKV_GET_ROCKSDB, time[DELTAKV_GET]);
    PRINT_FULL("DeltaKV-get-vLog", DELTAKV_GET_INDEXSTORE, time[DELTAKV_GET]);
    PRINT_FULL("DeltaKV-get-dStore", DELTAKV_GET_HASHSTORE, time[DELTAKV_GET]);
    fprintf(stdout, "\n");
    PRINT_FULL("DeltaKV-merge", DELTAKV_MERGE, time[DELTAKV_MERGE]);
    PRINT_FULL("DeltaKV-merge-rocksdb", DELTAKV_MERGE_ROCKSDB, time[DELTAKV_MERGE]);
    PRINT_FULL("DeltaKV-merge-vLog", DELTAKV_MERGE_INDEXSTORE, time[DELTAKV_MERGE]);
    PRINT_FULL("DeltaKV-merge-dStore", DELTAKV_MERGE_HASHSTORE, time[DELTAKV_MERGE]);
    fprintf(stdout, "\n");
    PRINT_FULL("DeltaKV-get-cache", DELTAKV_CACHE_GET, time[DELTAKV_CACHE_GET]);
    PRINT_FULL("DeltaKV-insert-cache-new", DELTAKV_CACHE_INSERT_NEW, time[DELTAKV_CACHE_INSERT_NEW]);
    PRINT_FULL("DeltaKV-insert-cache-merge", DELTAKV_CACHE_INSERT_MERGE, time[DELTAKV_CACHE_INSERT_MERGE]);

    fprintf(stdout, "-------------- DeltaKV Merge request Breakdown ------------------------------\n");
    PRINT_FULL("All", DELTAKV_MERGE, time[DELTAKV_MERGE]); 
    PRINT_FULL("  lock-1", MERGE_LOCK_1, time[DELTAKV_MERGE]);
    PRINT_FULL("  lock-2", MERGE_LOCK_2, time[DELTAKV_MERGE]);
    PRINT_FULL("  postlock-full", MERGE_AFTER_LOCK_FULL, time[DELTAKV_MERGE]);
    PRINT_FULL("  postlock-not-full", MERGE_AFTER_LOCK_NOT_FULL, time[DELTAKV_MERGE]);

    fprintf(stdout, "-------------- DeltaKV Batch Get Breakdown ------------------------------\n");
    PRINT_FULL("All", DELTAKV_BATCH_READ, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("Buffer-wait", DELTAKV_BATCH_READ_WAIT_BUFFER, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("Buffer-read", DELTAKV_BATCH_READ_NO_WAIT_BUFFER, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("  Buffer-get-return", DELTAKV_BATCH_READ_GET_KEY, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("  Buffer-get-merge-return", DELTAKV_BATCH_READ_MERGE, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("  Buffer-miss-no-wait", DELTAKV_BATCH_READ_MERGE_ALL, time[DELTAKV_BATCH_READ]);
    PRINT_FULL("Read-store", DELTAKV_BATCH_READ_STORE, time[DELTAKV_BATCH_READ]);

    fprintf(stdout, "-------------- DeltaKV Single batch ------------------------------\n");
    PRINT_FULL("Plain-rocksdb", BATCH_PLAIN_ROCKSDB, time[BATCH_PLAIN_ROCKSDB]);
    PRINT_FULL("  put", DELTAKV_PUT_ROCKSDB, time[BATCH_PLAIN_ROCKSDB]);
    PRINT_FULL("  merge", DELTAKV_MERGE_ROCKSDB, time[BATCH_PLAIN_ROCKSDB]);
    PRINT_FULL("  put-merge", DELTAKV_PUT_MERGE_ROCKSDB, time[BATCH_PLAIN_ROCKSDB]);
    PRINT_FULL("  flush-wal", BATCH_FLUSH_WAL, time[BATCH_PLAIN_ROCKSDB]);
    PRINT_FULL("KV-KD", BATCH_KV_KD, time[BATCH_KV_KD]);
    PRINT_FULL("  hashStore", DELTAKV_PUT_HASHSTORE, time[BATCH_KV_KD]);
    PRINT_FULL("    get-handler", DS_MULTIPUT_GET_HANDLER, time[BATCH_KV_KD]);
    PRINT_FULL("    put-jobqueue", DS_MULTIPUT_PUT_TO_JOB_QUEUE, time[BATCH_KV_KD]);
    PRINT_FULL("    direct-op", DS_MULTIPUT_DIRECT_OP, time[BATCH_KV_KD]);
    PRINT_FULL("  vLog", DELTAKV_PUT_INDEXSTORE, time[BATCH_KV_KD]);
    PRINT_FULL("  rocksdb", DELTAKV_MERGE_ROCKSDB, time[BATCH_KV_KD]);


    fprintf(stdout, "-------------- DeltaKV Batch OP Breakdown ------------------------------\n");
    PRINT_FULL("op-read", OP_GET, time[OP_GET]);
    PRINT_FULL("op-multiput", OP_MULTIPUT, time[OP_MULTIPUT]);
    PRINT_FULL("op-put", OP_PUT, time[OP_PUT]);


    fprintf(stdout, "-------------- DeltaKV HashStore Put Breakdown ------------------------------\n");
    PRINT_FULL("worker-put-file-handler", DELTAKV_HASHSTORE_PUT, (time[DELTAKV_MERGE] + time[DELTAKV_PUT]));
    PRINT_FULL("worker-put-file-write", DELTAKV_HASHSTORE_PUT_IO_TRAFFIC, time[DELTAKV_HASHSTORE_PUT]);

    fprintf(stdout, "-------------- DeltaKV HashStore Get Breakdown ------------------------------\n");
    PRINT_FULL("worker-get-file-handler", DELTAKV_HASHSTORE_GET, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-get-cache", DELTAKV_HASHSTORE_GET_CACHE, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-insert-cache", DELTAKV_HASHSTORE_GET_INSERT_CACHE, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-get-file-process-test", DELTAKV_HASHSTORE_GET_PROCESS_TEST, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-get-file-process", DELTAKV_HASHSTORE_GET_PROCESS, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-get-file-io", DELTAKV_HASHSTORE_GET_IO, (time[DELTAKV_HASHSTORE_GET]));
    PRINT_FULL("worker-wait-buffer-lock", DELTAKV_HASHSTORE_WAIT_BUFFER, (time[DELTAKV_HASHSTORE_GET]));

    fprintf(stdout, "-------------- DeltaKV HashStore GC Breakdown ------------------------------\n");
    PRINT_FULL("worker-gc", DELTAKV_HASHSTORE_WORKER_GC, (time[DELTAKV_HASHSTORE_WORKER_GC]));
    PRINT_FULL("worker-gc-before-rewrite", DELTAKV_HASHSTORE_WORKER_GC_BEFORE_REWRITE, (time[DELTAKV_HASHSTORE_WORKER_GC]));
    PRINT_FULL("worker-gc-before-split", DELTAKV_HASHSTORE_WORKER_GC_BEFORE_SPLIT, (time[DELTAKV_HASHSTORE_WORKER_GC]));
    PRINT_FULL("- gc read", DELTAKV_GC_READ, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("- gc write", DELTAKV_GC_WRITE, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("select-merge", GC_SELECT_MERGE, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  slmerge-get-nodes", GC_SELECT_MERGE_GET_NODES, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  slmerge-select", GC_SELECT_MERGE_SELECT_MERGE, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  slmerge-after-select", GC_SELECT_MERGE_AFTER_SELECT, time[DELTAKV_HASHSTORE_WORKER_GC]);

    PRINT_FULL("merge", MERGE, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  wait-lock", MERGE_WAIT_LOCK, time[MERGE]);
    PRINT_FULL("  handler", MERGE_CREATE_HANDLER, time[MERGE]);
    PRINT_FULL("  wait-lock3", MERGE_WAIT_LOCK3, time[MERGE]);
    PRINT_FULL("  file1", MERGE_FILE1, time[MERGE]);
    PRINT_FULL("  file2", MERGE_FILE2, time[MERGE]);
    PRINT_FULL("  file3", MERGE_FILE3, time[MERGE]);
    PRINT_FULL("  metadata", MERGE_METADATA, time[MERGE]);

    PRINT_FULL("split", SPLIT, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  handler", SPLIT_HANDLER, time[SPLIT]);
    PRINT_FULL("  in-memory", SPLIT_IN_MEMORY, time[SPLIT]);
    PRINT_FULL("  write", SPLIT_WRITE_FILES, time[SPLIT]);
    PRINT_FULL("  metadata", SPLIT_METADATA, time[SPLIT]);

    PRINT_FULL("rewrite", REWRITE, time[DELTAKV_HASHSTORE_WORKER_GC]);
    PRINT_FULL("  file-id", REWRITE_GET_FILE_ID, time[REWRITE]);
    PRINT_FULL("  add-header", REWRITE_ADD_HEADER, time[REWRITE]);
    PRINT_FULL("  close-file", REWRITE_CLOSE_FILE, time[REWRITE]);
    PRINT_FULL("  create-file", REWRITE_CREATE_FILE, time[REWRITE]);
    PRINT_FULL("  open-file", REWRITE_OPEN_FILE, time[REWRITE]);
    PRINT_FULL("  write", REWRITE_WRITE, time[REWRITE]);
    PRINT_FULL("  after-write", REWRITE_AFTER_WRITE, time[REWRITE]);

    fprintf(stdout, "-------------------------- SET Request --------------------------------------\n");
    PRINT_FULL("SetOverall", SET, time[SET]);
    PRINT_FULL("SetKeyLookupTime", SET_KEY_LOOKUP, time[SET]);
    PRINT_FULL("SetKeyWriteTime", SET_KEY_WRITE, time[SET]);
    PRINT_FULL("SetKeyWriteSWTime", SET_KEY_WRITE_SHADOW, time[SET]);
    PRINT_FULL("SetValueTime", SET_VALUE, time[SET]);

    fprintf(stdout, "------------------------- UPDATE Request ------------------------------------\n");
    PRINT_FULL("UpdateOverall", UPDATE, time[UPDATE]);
    //    fprintf(stdout, "%-24s %14.3lf\n", "- mean:", hdr_mean(_updateTimeHistogram));
    //    fprintf(stdout, "%-24s %14.3lf\n", "- stddev:", hdr_stddev(_updateTimeHistogram));
    //    fprintf(stdout, "%-24s %14ld\n", "- min:", hdr_min(_updateTimeHistogram));
    //    fprintf(stdout, "%-24s %14ld\n", "- max:", hdr_max(_updateTimeHistogram));
    // fprintf(stdout, "%-24s %14ld\n", "- 25-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 25.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 50-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 50.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 75-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 75.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 90-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 90.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 95-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 95.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 97-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 97.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 99-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 99.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 99.9-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 99.9));
    // fprintf(stdout, "%-24s %14ld\n", "- 99.99-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 99.99));
    // fprintf(stdout, "%-24s %14ld\n", "- 100-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 100.0));
    //    fprintf(stderr, "%-24s %14ld\n", "Update latency 95-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 95.0));
    // fprintf(stderr, "%-24s %14ld\n", "Update latency 99-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 95.0));
    // fprintf(stderr, "%-24s %14ld\n", "Update latency 100-th-%:", hdr_value_at_percentile(_updateTimeHistogram, 100.0));
    // for (auto h : _updateByValueSizeHistogram) {
    //    fprintf(stdout, "%-24s %llu:\n", "- Value of size", h.first);
    //    fprintf(stdout, "%-24s %14.3lf\n", "  - mean:", hdr_mean(h.second));
    //    fprintf(stdout, "%-24s %14.3lf\n",   "  - stddev:", hdr_stddev(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - min:", hdr_min(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - max:", hdr_max(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 25-th-%:", hdr_value_at_percentile(h.second, 25.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 50-th-%:", hdr_value_at_percentile(h.second, 50.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 75-th-%:", hdr_value_at_percentile(h.second, 75.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 90-th-%:", hdr_value_at_percentile(h.second, 90.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 95-th-%:", hdr_value_at_percentile(h.second, 95.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 97-th-%:", hdr_value_at_percentile(h.second, 97.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99-th-%:", hdr_value_at_percentile(h.second, 99.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99.9-th-%:", hdr_value_at_percentile(h.second, 99.9));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99.99-th-%:", hdr_value_at_percentile(h.second, 99.99));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 100-th-%:", hdr_value_at_percentile(h.second, 100.0));
    //    fprintf(stderr, "%-24s [%llu] %14ld\n", "Update latency 95-th-%:", h.first, hdr_value_at_percentile(h.second, 95.0));
    //    fprintf(stderr, "%-24s [%llu] %14ld\n", "Update latency 100-th-%:", h.first, hdr_value_at_percentile(h.second, 100.0));
    //}
    PRINT_FULL("UpdateKeyLookupTime", UPDATE_KEY_LOOKUP, time[UPDATE]);
    PRINT_FULL("UpdateKeyWriteTime", UPDATE_KEY_WRITE, time[UPDATE]);
    PRINT_FULL("- UpdateKeyToLSM", UPDATE_KEY_WRITE_LSM, time[UPDATE]);
    PRINT_FULL("  - KeyToCache", KEY_SET_CACHE, time[UPDATE]);
    PRINT_FULL("- UpdateKeyToLSM (GC)", UPDATE_KEY_WRITE_LSM_GC, time[UPDATE]);
    PRINT_FULL("- KeyUpdateCache", KEY_UPDATE_CACHE, time[UPDATE]);
    PRINT_FULL("- UpdateKeyToSW", UPDATE_KEY_WRITE_SHADOW, time[UPDATE]);
    PRINT_FULL("UpdateValueTime", UPDATE_VALUE, time[UPDATE]);
    PRINT_FULL("WBRatioUpdateTime", GC_RATIO_UPDATE, time[UPDATE]);
    PRINT_FULL("InvalidUpdateTime", GC_INVALID_BYTES_UPDATE, time[UPDATE]);

    PRINT_FULL("FlushLog/Group", POOL_FLUSH, time[UPDATE]);
    PRINT_FULL("- Flush w/o GC", POOL_FLUSH_NO_GC, time[UPDATE]);
    PRINT_FULL("- Flush wait", POOL_FLUSH_WAIT, time[UPDATE]);
    PRINT_FULL("- GCTotal", GC_TOTAL, time[UPDATE]);

    PRINT_FULL("LogMeta", LOG_TIME, time[UPDATE]);

    fprintf(stdout, "-------------------------- GET Request --------------------------------------\n");
    PRINT_FULL("GetOverall", GET, time[GET]);
    //    fprintf(stdout, "%-24s %14.3lf\n", "- mean:", hdr_mean(_getTimeHistogram));
    //    fprintf(stdout, "%-24s %14.3lf\n", "- stddev:", hdr_stddev(_getTimeHistogram));
    //    fprintf(stdout, "%-24s %14ld\n", "- min:", hdr_min(_getTimeHistogram));
    //    fprintf(stdout, "%-24s %14ld\n", "- max:", hdr_max(_getTimeHistogram));
    // fprintf(stdout, "%-24s %14ld\n", "- 25-th-%:", hdr_value_at_percentile(_getTimeHistogram, 25.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 50-th-%:", hdr_value_at_percentile(_getTimeHistogram, 50.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 75-th-%:", hdr_value_at_percentile(_getTimeHistogram, 75.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 90-th-%:", hdr_value_at_percentile(_getTimeHistogram, 90.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 95-th-%:", hdr_value_at_percentile(_getTimeHistogram, 95.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 97-th-%:", hdr_value_at_percentile(_getTimeHistogram, 97.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 99-th-%:", hdr_value_at_percentile(_getTimeHistogram, 99.0));
    // fprintf(stdout, "%-24s %14ld\n", "- 99.9-th-%:", hdr_value_at_percentile(_getTimeHistogram, 99.9));
    // fprintf(stdout, "%-24s %14ld\n", "- 99.99-th-%:", hdr_value_at_percentile(_getTimeHistogram, 99.99));
    // fprintf(stdout, "%-24s %14ld\n", "- 100-th-%:", hdr_value_at_percentile(_getTimeHistogram, 100.0));
    //    fprintf(stderr, "%-24s %14ld\n", "Get latency 95-th-%:", hdr_value_at_percentile(_getTimeHistogram, 95.0));
    // fprintf(stderr, "%-24s %14ld\n", "Get latency 99-th-%:", hdr_value_at_percentile(_getTimeHistogram, 95.0));
    // fprintf(stderr, "%-24s %14ld\n", "Get latency 100-th-%:", hdr_value_at_percentile(_getTimeHistogram, 100.0));
    // for (auto h : _getByValueSizeHistogram) {

    //    fprintf(stdout, "%-24s %llu:\n", "- Value of size", h.first);
    //    fprintf(stdout, "%-24s %14.3lf\n", "  - mean:", hdr_mean(h.second));
    //    fprintf(stdout, "%-24s %14.3lf\n",   "  - stddev:", hdr_stddev(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - min:", hdr_min(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - max:", hdr_max(h.second));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 25-th-%:", hdr_value_at_percentile(h.second, 25.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 50-th-%:", hdr_value_at_percentile(h.second, 50.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 75-th-%:", hdr_value_at_percentile(h.second, 75.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 90-th-%:", hdr_value_at_percentile(h.second, 90.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 95-th-%:", hdr_value_at_percentile(h.second, 95.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 97-th-%:", hdr_value_at_percentile(h.second, 97.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99-th-%:", hdr_value_at_percentile(h.second, 99.0));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99.9-th-%:", hdr_value_at_percentile(h.second, 99.9));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 99.99-th-%:", hdr_value_at_percentile(h.second, 99.99));
    //    fprintf(stdout, "%-24s %14ld\n",   "  - 100-th-%:", hdr_value_at_percentile(h.second, 100.0));
    //    fprintf(stderr, "%-24s [%llu] %14ld\n", "Get latency 95-th-%:", h.first, hdr_value_at_percentile(h.second, 95.0));
    //    fprintf(stderr, "%-24s [%llu] %14ld\n", "Get latency 100-th-%:", h.first, hdr_value_at_percentile(h.second, 100.0));
    //}
    PRINT_FULL("GetKeyLookupTime", GET_KEY_LOOKUP, time[GET]);
    PRINT_FULL("GetValueTime    ", GET_VALUE, time[GET]);

    fprintf(stdout, "------------------------- SCAN Request --------------------------------------\n");
    PRINT_FULL("Scan Time", SCAN, time[SCAN]);

    fprintf(stdout, "----------------------------- FLUSH -----------------------------------------\n");
    PRINT_FULL("GroupFlushInPool", GROUP_IN_POOL_FLUSH, time[POOL_FLUSH]);
    PRINT_FULL("GroupFlushOthers", GROUP_OTHER_FLUSH, time[POOL_FLUSH]);
    PRINT_FULL("GCinFlush", GC_IN_FLUSH, time[POOL_FLUSH]);
    PRINT_FULL("GCinFlush(+Sync)", GC_IN_FLUSH_WITH_SYNC, time[POOL_FLUSH]);
    PRINT_FULL("FlushSync", FLUSH_SYNC, time[POOL_FLUSH]);

    fprintf(stdout, "---------------------------- GC Stats ---------------------------------------\n");
    PRINT_FULL("GCTotalInternal", GC_TOTAL, time[GC_TOTAL]);
    PRINT_FULL("GCinFlush", GC_IN_FLUSH, time[GC_TOTAL]);
    PRINT_FULL("GCinOthers", GC_OTHERS, time[GC_TOTAL]);
    PRINT_FULL("KeyLookup", GC_KEY_LOOKUP, time[GC_TOTAL]);
    PRINT_FULL("GCReadData", GC_READ, time[GC_TOTAL]);
    PRINT_FULL("GCReadAheadData", GC_READ_AHEAD, time[GC_TOTAL]);
    PRINT_FULL("GCFlushPreWrite", GC_PRE_FLUSH, time[GC_TOTAL]);
    PRINT_FULL("GCFlushWrite", GC_FLUSH, time[GC_TOTAL]);
    PRINT_FULL("UpdateKeyToLSM (GC)", UPDATE_KEY_WRITE_LSM_GC, time[GC_TOTAL]);
    PRINT_FULL(" - KeyUpdateCache", KEY_UPDATE_CACHE, time[GC_TOTAL]);
    PRINT_FULL("(Test) Phase 1", GC_PHASE_TEST, time[GC_TOTAL]);

    fprintf(stdout, "------------------------- LSM and Key Cache ---------------------------------\n");
    PRINT_FULL("GetKeyShadow", KEY_GET_SHADOW, time[KEY_GET_ALL]);
    PRINT_FULL("GetKeyCache", KEY_GET_CACHE, time[KEY_GET_ALL]);
    PRINT_FULL("GetKeyLSM", KEY_GET_LSM, time[KEY_GET_ALL]);
    PRINT_FULL("SetKeyLSM", KEY_SET_LSM, time[KEY_SET_ALL]);
    PRINT_FULL("SetKeyLSM (Batch)", KEY_SET_LSM_BATCH, time[KEY_SET_ALL]);
    PRINT_FULL("SetKeyCache", KEY_SET_CACHE, time[KEY_SET_ALL]);

    fprintf(stdout, "-------------------- DeltaStore GC Bytes Counters ------------------------------\n");

    fprintf(stdout, "dStore GC Physical write bytes     : %16llu (average %16llu, %8llu times)\n",
        DeltaGcPhysicalBytes.first, DeltaGcPhysicalBytes.first / (DeltaGcPhysicalTimes.first + 1), DeltaGcPhysicalTimes.first);
    fprintf(stdout, "dStore GC Logical write bytes      : %16llu (average %16llu, %8llu times)\n",
        DeltaGcLogicalBytes.first, DeltaGcLogicalBytes.first / (DeltaGcLogicalTimes.first + 1), DeltaGcLogicalTimes.first);
    fprintf(stdout, "dStore GC Physical read bytes      : %16llu (average %16llu, %8llu times)\n",
        DeltaGcPhysicalBytes.second, DeltaGcPhysicalBytes.second / (DeltaGcPhysicalTimes.second + 1), DeltaGcPhysicalTimes.second);
    fprintf(stdout, "dStore GC Logical read bytes       : %16llu (average %16llu, %8llu times)\n",
        DeltaGcLogicalBytes.second, DeltaGcLogicalBytes.second / (DeltaGcLogicalTimes.second + 1), DeltaGcLogicalTimes.second);
    fprintf(stdout, "dStore GC read amplification       : %16f\n", (double)DeltaGcPhysicalBytes.second / (DeltaGcLogicalBytes.second));
    fprintf(stdout, "dStore GC write amplification      : %16f\n", (double)DeltaGcPhysicalBytes.first / (DeltaGcLogicalBytes.first));

    fprintf(stdout, "-------------------- DeltaStore OP Bytes Counters ------------------------------\n");

    fprintf(stdout, "dStore OP Physical write bytes     : %16llu (average %16llu, %8llu times)\n",
        DeltaOPPhysicalBytes.first, DeltaOPPhysicalBytes.first / (DeltaOPPhysicalTimes.first + 1), DeltaOPPhysicalTimes.first);
    fprintf(stdout, "dStore OP Logical write bytes      : %16llu (average %16llu, %8llu times)\n",
        DeltaOPLogicalBytes.first, DeltaOPLogicalBytes.first / (DeltaOPLogicalTimes.first + 1), DeltaOPLogicalTimes.first);
    fprintf(stdout, "dStore OP Physical read bytes      : %16llu (average %16llu, %8llu times)\n",
        DeltaOPPhysicalBytes.second, DeltaOPPhysicalBytes.second / (DeltaOPPhysicalTimes.second + 1), DeltaOPPhysicalTimes.second);
    fprintf(stdout, "dStore OP Logical read bytes       : %16llu (average %16llu, %8llu times)\n",
        DeltaOPLogicalBytes.second, DeltaOPLogicalBytes.second / (DeltaOPLogicalTimes.second + 1), DeltaOPLogicalTimes.second);
    fprintf(stdout, "dStore OP read amplification       : %16f\n", (double)DeltaOPPhysicalBytes.second / (DeltaOPLogicalBytes.second));
    fprintf(stdout, "dStore OP write amplification      : %16f\n", (double)DeltaOPPhysicalBytes.first / (DeltaOPLogicalBytes.first));

    fprintf(stdout, "------------------------- vLog Bytes Counters ------------------------------------\n");
    unsigned long long writeIOSum = 0, readIOSum = 0;
    for (int i = 0; i < MAX_DISK; i++) {
        fprintf(stdout, "Disk %5d                : (Write) %16llu (Read) %16llu\n", i, IOBytes[i].first, IOBytes[i].second);
        writeIOSum += IOBytes[i].first;
        readIOSum += IOBytes[i].second;
    }
    fprintf(stdout, "Total disk write          : %16llu\n", writeIOSum);
    fprintf(stdout, "Total disk read           : %16llu\n", readIOSum);
    fprintf(stdout, "Flushed bytes             : %16llu\n", total[FLUSH_BYTES]);
    fprintf(stdout,
        "GC Ops count              : %16llu\n"
        "GC write bytes            : %16llu\n"
        "GC scan bytes             : %16llu\n"
        "GC update count           : (min) %16llu\n (max) %16llu\n",
        counts[GC_TOTAL], total[GC_WRITE_BYTES], total[GC_SCAN_BYTES], min[GC_UPDATE_COUNT], max[GC_UPDATE_COUNT]);
    fprintf(stdout,
        "Update counter count      : (main) %16llu (log) %16llu\n"
        "Update counter bytes      : (main) %16llu (log) %16llu\n",
        counts[UPDATE_TO_MAIN], counts[UPDATE_TO_LOG], total[UPDATE_TO_MAIN], total[UPDATE_TO_LOG]);

    /*
    fprintf(stdout,
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            ,"Valid bytes (Main)", gcGroupBytesCount.valid.sum[MAIN], gcGroupBytesCount.valid.count[MAIN], gcGroupBytesCount.valid.sum[MAIN] * 1.0 / gcGroupBytesCount.valid.count[MAIN]
            ,"Valid bytes (Log)", gcGroupBytesCount.valid.sum[LOG], gcGroupBytesCount.valid.count[LOG], gcGroupBytesCount.valid.sum[LOG] * 1.0 / gcGroupBytesCount.valid.count[LOG]
            ,"Invalid bytes (Main)", gcGroupBytesCount.invalid.sum[MAIN], gcGroupBytesCount.invalid.count[MAIN], gcGroupBytesCount.invalid.sum[MAIN] * 1.0 / gcGroupBytesCount.invalid.count[MAIN]
            ,"Invalid bytes (Log)", gcGroupBytesCount.invalid.sum[LOG], gcGroupBytesCount.invalid.count[LOG], gcGroupBytesCount.invalid.sum[LOG] * 1.0 / gcGroupBytesCount.invalid.count[LOG]
            ,"Valid bytes (Last Log)", gcGroupBytesCount.validLastLog.sum[LOG], gcGroupBytesCount.validLastLog.count[LOG], gcGroupBytesCount.validLastLog.sum[LOG] * 1.0 / gcGroupBytesCount.validLastLog.count[LOG]
    );
    fprintf(stdout,"GC valid/invalid bytes bucket size: %lld\n", gcGroupBytesCount.bucketSize);

#define PRINT_GC_BUCKETS(_BYTES_TYPE_, _DATA_TYPE_, _NAME_) do { \
        fprintf(stdout, "%s\n",_NAME_); \
        for (unsigned int i = 0; i < gcGroupBytesCount.bucketLen; i++) { \
            if (gcGroupBytesCount._BYTES_TYPE_.buckets[_DATA_TYPE_][i] == 0) \
                continue; \
            fprintf(stdout, "[<= %16llu] = %12llu\n", gcGroupBytesCount.bucketSize * i, gcGroupBytesCount._BYTES_TYPE_.buckets[_DATA_TYPE_][i]); \
        } \
    } while(0);

    PRINT_GC_BUCKETS(valid, MAIN, "Valid bytes (Main)");
    PRINT_GC_BUCKETS(valid, LOG, "Valid bytes (Log)");
    PRINT_GC_BUCKETS(invalid, MAIN, "Invalid bytes (Main)");
    PRINT_GC_BUCKETS(invalid, LOG, "Invalid bytes (Log)");
    PRINT_GC_BUCKETS(validLastLog, LOG, "Valid bytes (Last Log)");

#undef PRINT_GC_BUCKETS


    for (int r = 0; r < 2; r++) {
        fprintf(stdout,
            "%20s sum:%16llu count:%12llu avg.:%6.2lf\n"
            , (r==0?"Flush data stripe":"Updates per stripe"), flushGroupCount.sum[r], flushGroupCount.count[r], flushGroupCount.sum[r] * 1.0 / flushGroupCount.count[r]);
        for (uint32_t i = 0; i < (r == 0? ConfigManager::getInstance().getNumMainSegment(): flushGroupCountBucketLen); i++) {
            if (flushGroupCount.buckets[r][i] == 0)
                continue;
            if (i+1 < (r == 0? ConfigManager::getInstance().getNumMainSegment(): flushGroupCountBucketLen)) {
                fprintf(stdout, "[= %16d] = %12llu\n",i , flushGroupCount.buckets[r][i]);
            } else {
                fprintf(stdout, "[<= %16d] = %12llu\n",i , flushGroupCount.buckets[r][i]);
            }
        }
    }

    */

    for (int i = 0; i < 2; i++) {
        delete[] gcGroupBytesCount.valid.buckets[i];
        delete[] gcGroupBytesCount.invalid.buckets[i];
        delete[] gcGroupBytesCount.validLastLog.buckets[i];
        delete[] flushGroupCount.buckets[i];
    }

    //    free(_updateTimeHistogram);
    //    _updateTimeHistogram = 0;
    //    free(_getTimeHistogram);
    //    _getTimeHistogram = 0;
    //    for (auto h : _updateByValueSizeHistogram) {
    //        free(h.second);
    //    }
    //    for (auto h : _getByValueSizeHistogram) {
    //        free(h.second);
    //    }
    //    _updateByValueSizeHistogram.clear();
    //    _getByValueSizeHistogram.clear();

#undef PRINT_SUM
#undef PRINT_FULL
    fprintf(stdout, "==============================================================\n");
}

void StatsRecorder::totalProcess(StatsType stat, size_t diff, size_t count)
{
    if (!statisticsOpen)
        return;

    total[stat] += diff;
    // update min and max as well
    if (counts[stat] == 0) {
        max[stat] = total[stat];
        min[stat] = total[stat];
    } else if (max[stat] < total[stat]) {
        max[stat] = total[stat];
    } else if (min[stat] > total[stat]) {
        min[stat] = total[stat];
    }
    counts[stat] += count;
}

unsigned long long StatsRecorder::timeProcess(StatsType stat, struct timeval& start_time, size_t diff, size_t count, unsigned long long valueSize)
{
    unsigned long long ret = 0;
    if (!statisticsOpen)
        return 0;

    // update time spent
    ret = timeAddto(start_time, time[stat]);

    if (stat == StatsType::UPDATE) {
        //        hdr_record_value(_updateTimeHistogram, ret);
        //        if (valueSize > 0) {
        //            if (_updateByValueSizeHistogram.count(valueSize) == 0) {
        //                _updateByValueSizeHistogram[valueSize] = 0;
        //                hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_updateByValueSizeHistogram[valueSize]);
        //            }
        //            hdr_record_value(_updateByValueSizeHistogram.at(valueSize), ret);
        //        }
    } else if (stat == StatsType::GET) {
        //        hdr_record_value(_getTimeHistogram, ret);
        //        if (valueSize > 0) {
        //            if (_getByValueSizeHistogram.count(valueSize) == 0) {
        //                _getByValueSizeHistogram[valueSize] = 0;
        //                hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_getByValueSizeHistogram[valueSize]);
        //            }
        //            hdr_record_value(_getByValueSizeHistogram.at(valueSize), ret);
        //        }
    }

    if (stat == StatsType::UPDATE_KEY_WRITE_LSM || stat == StatsType::UPDATE_KEY_WRITE_SHADOW) {
        time[StatsType::UPDATE_KEY_WRITE] += ret;
        // update total
        if (diff != 0) {
            totalProcess(stat, diff, count);
        } else {
            counts[StatsType::UPDATE_KEY_WRITE] += count;
        }
    }

    // update total
    if (diff != 0) {
        totalProcess(stat, diff);
    } else {
        counts[stat] += count;
    }
    return ret;
}

void StatsRecorder::openStatistics(timeval& start_time)
{
    unsigned long long diff = 0;
    statisticsOpen = true;
    timeAddto(start_time, diff);
    fprintf(stdout, "Last Phase Duration :%llu us\n", diff);
}

void StatsRecorder::putGCGroupStats(unsigned long long validMain, unsigned long long validLog, unsigned long long invalidMain, unsigned long long invalidLog, unsigned long long validLastLog)
{
    unsigned int bucketIndex = 0;

#define PROCESS(_BYTES_, _BYTES_TYPE_, _DATA_TYPE_)                            \
    do {                                                                       \
        bucketIndex = _BYTES_ / gcGroupBytesCount.bucketSize;                  \
        if (bucketIndex >= gcGroupBytesCount.bucketLen) {                      \
            bucketIndex = gcGroupBytesCount.bucketLen - 1;                     \
        }                                                                      \
        gcGroupBytesCount._BYTES_TYPE_.buckets[_DATA_TYPE_][bucketIndex] += 1; \
        gcGroupBytesCount._BYTES_TYPE_.sum[_DATA_TYPE_] += _BYTES_;            \
        gcGroupBytesCount._BYTES_TYPE_.count[_DATA_TYPE_] += 1;                \
    } while (0)

    PROCESS(validMain, valid, MAIN);
    PROCESS(validLog, valid, LOG);
    PROCESS(invalidMain, invalid, MAIN);
    PROCESS(invalidLog, invalid, LOG);
    PROCESS(validLastLog, validLastLog, LOG);

#undef PROCESS
}

void StatsRecorder::putFlushGroupStats(unsigned long long dataGroup, std::unordered_map<group_id_t, unsigned long long>& count)
{
    // stripes per flush
    flushGroupCount.buckets[0][dataGroup] += 1;
    flushGroupCount.sum[0] += dataGroup;
    flushGroupCount.count[0] += 1;
    // updates per stripe flushed
    for (auto c : count) {
        flushGroupCount.sum[1] += c.second;
        flushGroupCount.count[1] += 1;
        if (c.second >= (unsigned long long)flushGroupCountBucketLen) {
            c.second = flushGroupCountBucketLen * (unsigned long long)1 - 1;
        }
        flushGroupCount.buckets[1][c.second] += 1;
    }
}

}
