//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <sstream>
#include <string>

#include "cache/compressed_secondary_cache.h"
#include "db/db_test_util.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBDeltaLogBasicTest : public DBTestBase {
 protected:
  DBDeltaLogBasicTest()
      : DBTestBase("db_deltaLog_basic_test", /* env_do_fsync */ false) {}
};

TEST_F(DBDeltaLogBasicTest, GetDeltaLog) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char deltaLog_value[] = "deltaLog_value";

  ASSERT_OK(Put(key, deltaLog_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get(key), deltaLog_value);

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches; however, the deltaLog itself can
  // only be read from the deltaLog file, so the read should return Incomplete.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                  .IsIncomplete());
}

TEST_F(DBDeltaLogBasicTest, GetDeltaLogFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_deltaLog_files = true;
  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char deltaLog_value[] = "deltaLog_value";

  ASSERT_OK(Put(key, deltaLog_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  read_options.fill_cache = false;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, deltaLog_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. Since we didn't re-fill the cache, the
    // deltaLog itself can only be read from the deltaLog file, so the read
    // should return Incomplete.
    ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                    .IsIncomplete());
    ASSERT_TRUE(result.empty());
  }

  read_options.fill_cache = true;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, deltaLog_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. The table and the necessary
    // blocks/deltaLogs should already be in their respective caches.
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, deltaLog_value);
  }
}

TEST_F(DBDeltaLogBasicTest, IterateDeltaLogsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_deltaLog_files = true;
  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.statistics = CreateDBStatistics();

  Reopen(options);

  int num_deltaLogs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> deltaLogs;

  for (int i = 0; i < num_deltaLogs; ++i) {
    keys.push_back("key" + std::to_string(i));
    deltaLogs.push_back("deltaLog" + std::to_string(i));
    ASSERT_OK(Put(keys[i], deltaLogs[i]));
  }
  ASSERT_OK(Flush());

  ReadOptions read_options;

  {
    read_options.fill_cache = false;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), deltaLogs[i]);
      ++i;
    }
    ASSERT_EQ(i, num_deltaLogs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
              0);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. Since we didn't re-fill the cache,
    // the deltaLog itself can only be read from the deltaLog file, so
    // iter->Valid() should be false.
    iter->SeekToFirst();
    ASSERT_NOK(iter->status());
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
              0);
  }

  {
    read_options.fill_cache = true;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Read deltaLogs from the file and refill the cache.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), deltaLogs[i]);
      ++i;
    }
    ASSERT_EQ(i, num_deltaLogs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
              num_deltaLogs);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. The table and the necessary
    // blocks/deltaLogs should already be in their respective caches.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), deltaLogs[i]);
      ++i;
    }
    ASSERT_EQ(i, num_deltaLogs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
              0);
  }
}

TEST_F(DBDeltaLogBasicTest, IterateDeltaLogsFromCachePinning) {
  constexpr size_t min_deltaLog_size = 6;

  Options options = GetDefaultOptions();

  LRUCacheOptions cache_options;
  cache_options.capacity = 2048;
  cache_options.num_shard_bits = 0;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;

  options.deltaLog_cache = NewLRUCache(cache_options);
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = min_deltaLog_size;

  Reopen(options);

  // Put then iterate over three key-values. The second value is below the size
  // limit and is thus stored inline; the other two are stored separately as
  // deltaLogs. We expect to have something pinned in the cache iff we are
  // positioned on a deltaLog.

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "long_value";
  static_assert(sizeof(first_value) - 1 >= min_deltaLog_size,
                "first_value too short to be stored as deltaLog");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "short";
  static_assert(sizeof(second_value) - 1 < min_deltaLog_size,
                "second_value too long to be inlined");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_deltaLog_size,
                "third_value too short to be stored as deltaLog");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  {
    ReadOptions read_options;
    read_options.fill_cache = true;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.deltaLog_cache->GetPinnedUsage(), 0);
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.deltaLog_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.deltaLog_cache->GetPinnedUsage(), 0);
  }
}

TEST_F(DBDeltaLogBasicTest, MultiGetDeltaLogs) {
  constexpr size_t min_deltaLog_size = 6;

  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = min_deltaLog_size;

  Reopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as
  // deltaLogs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_deltaLog_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_deltaLog_size,
                "second_value too short to be stored as deltaLog");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_deltaLog_size,
                "third_value too short to be stored as deltaLog");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches. The first (inlined) value should be
  // successfully read; however, the two deltaLog values could only be read from
  // the deltaLog file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }
}

TEST_F(DBDeltaLogBasicTest, MultiGetDeltaLogsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  constexpr size_t min_deltaLog_size = 6;
  options.min_deltaLog_size = min_deltaLog_size;
  options.create_if_missing = true;
  options.enable_deltaLog_files = true;
  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  DestroyAndReopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as
  // deltaLogs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_deltaLog_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_deltaLog_size,
                "second_value too short to be stored as deltaLog");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_deltaLog_size,
                "third_value too short to be stored as deltaLog");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.fill_cache = false;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. The first (inlined) value should be
  // successfully read; however, the two deltaLog values could only be read from
  // the deltaLog file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }

  // Fill the cache when reading deltaLogs from the deltaLog file.
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. All deltaLogs should be successfully read
  // from the cache.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }
}

#ifndef ROCKSDB_LITE
TEST_F(DBDeltaLogBasicTest, MultiGetWithDirectIO) {
  Options options = GetDefaultOptions();

  // First, create an external SST file ["b"].
  const std::string file_path = dbname_ + "/test.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), GetDefaultOptions());
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    ASSERT_OK(sst_file_writer.Put("b", "b_value"));
    ASSERT_OK(sst_file_writer.Finish());
  }

  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 1000;
  options.use_direct_reads = true;
  options.allow_ingest_behind = true;

  // Open DB with fixed-prefix sst-partitioner so that compaction will cut
  // new table file when encountering a new key whose 1-byte prefix changes.
  constexpr size_t key_len = 1;
  options.sst_partitioner_factory =
      NewSstPartitionerFixedPrefixFactory(key_len);

  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    ROCKSDB_GTEST_SKIP("This test requires direct IO support");
    return;
  }
  ASSERT_OK(s);

  constexpr size_t num_keys = 3;
  constexpr size_t deltaLog_size = 3000;

  constexpr char first_key[] = "a";
  const std::string first_deltaLog(deltaLog_size, 'a');
  ASSERT_OK(Put(first_key, first_deltaLog));

  constexpr char second_key[] = "b";
  const std::string second_deltaLog(2 * deltaLog_size, 'b');
  ASSERT_OK(Put(second_key, second_deltaLog));

  constexpr char third_key[] = "d";
  const std::string third_deltaLog(deltaLog_size, 'd');
  ASSERT_OK(Put(third_key, third_deltaLog));

  // first_deltaLog, second_deltaLog and third_deltaLog in the same deltaLog
  // file.
  //      SST                    DeltaLog file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|
  //       |       |       |         ^       ^        ^
  //       |       |       |         |       |        |
  //       |       |       +---------|-------|--------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  constexpr char fourth_key[] = "c";
  const std::string fourth_deltaLog(deltaLog_size, 'c');
  ASSERT_OK(Put(fourth_key, fourth_deltaLog));
  // fourth_deltaLog in another deltaLog file.
  //      SST                    DeltaLog file                 SST     DeltaLog
  //      file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|  ["c"]   |'cccc'|
  //       |       |       |         ^       ^        ^      |       ^
  //       |       |       |         |       |        |      |       |
  //       |       |       +---------|-------|--------+      +-------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));

  // Due to the above sst partitioner, we get 4 L1 files. The deltaLog files are
  // unchanged.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_EQ(4, NumTableFilesAtLevel(/*level=*/1));

  {
    // Ingest the external SST file into bottommost level.
    std::vector<std::string> ext_files{file_path};
    IngestExternalFileOptions opts;
    opts.ingest_behind = true;
    ASSERT_OK(
        db_->IngestExternalFile(db_->DefaultColumnFamily(), ext_files, opts));
  }

  // Now the database becomes as follows.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  //
  // L6          ["b"]

  {
    // Compact ["b"] to bottommost level.
    Slice begin = Slice(second_key);
    Slice end = Slice(second_key);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, &begin, &end));
  }

  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]           ["c"]       |       |   ["d"]       |
  //       |               |         |       |               |
  //       |               +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------|-----------------+
  //               |
  // L6          ["b"]
  ASSERT_EQ(3, NumTableFilesAtLevel(/*level=*/1));
  ASSERT_EQ(1, NumTableFilesAtLevel(/*level=*/6));

  bool called = false;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:AlignedReqs", [&](void* arg) {
        auto* aligned_reqs = static_cast<std::vector<FSReadRequest>*>(arg);
        assert(aligned_reqs);
        ASSERT_EQ(1, aligned_reqs->size());
        called = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<Slice, num_keys> keys{{first_key, third_key, second_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    // The MultiGet(), when constructing the KeyContexts, will process the keys
    // in such order: a, d, b. The reason is that ["a"] and ["d"] are in L1,
    // while ["b"] resides in L6.
    // Consequently, the original FSReadRequest list prepared by
    // Version::MultiGetdeltaLog() will be for "a", "d" and "b". It is unsorted
    // as follows:
    //
    // ["a", offset=30, len=3033],
    // ["d", offset=9096, len=3033],
    // ["b", offset=3063, len=6033]
    //
    // If we do not sort them before calling MultiRead() in DirectIO, then the
    // underlying IO merging logic will yield two requests.
    //
    // [offset=0, len=4096] (for "a")
    // [offset=0, len=12288] (result of merging the request for "d" and "b")
    //
    // We need to sort them in Version::MultiGetDeltaLog() so that the
    // underlying IO merging logic in DirectIO mode works as expected. The
    // correct behavior will be one aligned request:
    //
    // [offset=0, len=12288]

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys, &keys[0],
                  &values[0], &statuses[0]);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_TRUE(called);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_deltaLog);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], third_deltaLog);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], second_deltaLog);
  }
}
#endif  // !ROCKSDB_LITE

TEST_F(DBDeltaLogBasicTest, MultiGetDeltaLogsFromMultipleFiles) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.min_deltaLog_size = 0;
  options.create_if_missing = true;
  options.enable_deltaLog_files = true;
  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr size_t kNumDeltaLogFiles = 3;
  constexpr size_t kNumDeltaLogsPerFile = 3;
  constexpr size_t kNumKeys = kNumDeltaLogsPerFile * kNumDeltaLogFiles;

  std::vector<std::string> key_strs;
  std::vector<std::string> value_strs;
  for (size_t i = 0; i < kNumDeltaLogFiles; ++i) {
    for (size_t j = 0; j < kNumDeltaLogsPerFile; ++j) {
      std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
      std::string value =
          "value_as_deltaLog" + std::to_string(i) + "_" + std::to_string(j);
      ASSERT_OK(Put(key, value));
      key_strs.push_back(key);
      value_strs.push_back(value);
    }
    ASSERT_OK(Flush());
  }
  assert(key_strs.size() == kNumKeys);
  std::array<Slice, kNumKeys> keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    keys[i] = key_strs[i];
  }

  ReadOptions read_options;
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = false;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys, &keys[0],
                  &values[0], &statuses[0]);

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys, &keys[0],
                  &values[0], &statuses[0]);

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_TRUE(statuses[i].IsIncomplete());
      ASSERT_TRUE(values[i].empty());
    }
  }

  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys, &keys[0],
                  &values[0], &statuses[0]);

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys, &keys[0],
                  &values[0], &statuses[0]);

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }
}

TEST_F(DBDeltaLogBasicTest, GetDeltaLog_CorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  ASSERT_OK(Put(key, deltaLog));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::Get::TamperWithDeltaLogIndex", [](void* arg) {
        Slice* const deltaLog_index = static_cast<Slice*>(arg);
        assert(deltaLog_index);
        assert(!deltaLog_index->empty());
        deltaLog_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBDeltaLogBasicTest, MultiGetDeltaLog_CorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;
  options.create_if_missing = true;

  DestroyAndReopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_strs;
  std::array<std::string, kNumOfKeys> value_strs;
  std::array<Slice, kNumOfKeys + 1> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_strs[i] = "foo" + std::to_string(i);
    value_strs[i] = "deltaLog_value" + std::to_string(i);
    ASSERT_OK(Put(key_strs[i], value_strs[i]));
    keys[i] = key_strs[i];
  }

  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";
  ASSERT_OK(Put(key, deltaLog));
  keys[kNumOfKeys] = key;

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::MultiGet::TamperWithDeltaLogIndex", [&key](void* arg) {
        KeyContext* const key_context = static_cast<KeyContext*>(arg);
        assert(key_context);
        assert(key_context->key);

        if (*(key_context->key) == key) {
          Slice* const deltaLog_index = key_context->value;
          assert(deltaLog_index);
          assert(!deltaLog_index->empty());
          deltaLog_index->remove_prefix(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<PinnableSlice, kNumOfKeys + 1> values;
  std::array<Status, kNumOfKeys + 1> statuses;
  db_->MultiGet(ReadOptions(), dbfull()->DefaultColumnFamily(), kNumOfKeys + 1,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/false);
  for (size_t i = 0; i < kNumOfKeys + 1; ++i) {
    if (i != kNumOfKeys) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ("deltaLog_value" + std::to_string(i), values[i]);
    } else {
      ASSERT_TRUE(statuses[i].IsCorruption());
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBDeltaLogBasicTest, MultiGetDeltaLog_ExceedSoftLimit) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_bufs;
  std::array<std::string, kNumOfKeys> value_bufs;
  std::array<Slice, kNumOfKeys> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_bufs[i] = "foo" + std::to_string(i);
    value_bufs[i] = "deltaLog_value" + std::to_string(i);
    ASSERT_OK(Put(key_bufs[i], value_bufs[i]));
    keys[i] = key_bufs[i];
  }
  ASSERT_OK(Flush());

  std::array<PinnableSlice, kNumOfKeys> values;
  std::array<Status, kNumOfKeys> statuses;
  ReadOptions read_opts;
  read_opts.value_size_soft_limit = 1;
  db_->MultiGet(read_opts, dbfull()->DefaultColumnFamily(), kNumOfKeys,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/true);
  for (const auto& s : statuses) {
    ASSERT_TRUE(s.IsAborted());
  }
}

TEST_F(DBDeltaLogBasicTest, GetDeltaLog_InlinedTTLIndex) {
  constexpr uint64_t min_deltaLog_size = 10;

  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = min_deltaLog_size;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char deltaLog[] = "short";
  static_assert(sizeof(short) - 1 < min_deltaLog_size,
                "DeltaLog too long to be inlined");

  // Fake an inlined TTL deltaLog index.
  std::string deltaLog_index;

  constexpr uint64_t expiration = 1234567890;

  DeltaLogIndex::EncodeInlinedTTL(&deltaLog_index, expiration, deltaLog);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutDeltaLogIndex(&batch, 0, key, deltaLog_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBDeltaLogBasicTest, GetDeltaLog_IndexWithInvalidFileNumber) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key[] = "key";

  // Fake a deltaLog index referencing a non-existent deltaLog file.
  std::string deltaLog_index;

  constexpr uint64_t deltaLog_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  DeltaLogIndex::EncodeDeltaLog(&deltaLog_index, deltaLog_file_number, offset,
                                size, kNoCompression);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutDeltaLogIndex(&batch, 0, key, deltaLog_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

#ifndef ROCKSDB_LITE
TEST_F(DBDeltaLogBasicTest, GenerateIOTracing) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;
  std::string trace_file = dbname_ + "/io_trace_file";

  Reopen(options);
  {
    // Create IO trace file
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file, &trace_writer));
    ASSERT_OK(db_->StartIOTrace(TraceOptions(), std::move(trace_writer)));

    constexpr char key[] = "key";
    constexpr char deltaLog_value[] = "deltaLog_value";

    ASSERT_OK(Put(key, deltaLog_value));
    ASSERT_OK(Flush());
    ASSERT_EQ(Get(key), deltaLog_value);

    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file));
  }
  {
    // Parse trace file to check file operations related to deltaLog files are
    // recorded.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(
        NewFileTraceReader(env_, EnvOptions(), trace_file, &trace_reader));
    IOTraceReader reader(std::move(trace_reader));

    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));

    // Read records.
    int deltaLog_files_op_count = 0;
    Status status;
    while (true) {
      IOTraceRecord record;
      status = reader.ReadIOOp(&record);
      if (!status.ok()) {
        break;
      }
      if (record.file_name.find("deltaLog") != std::string::npos) {
        deltaLog_files_op_count++;
      }
    }
    // Assuming deltaLog files will have Append, Close and then Read operations.
    ASSERT_GT(deltaLog_files_op_count, 2);
  }
}
#endif  // !ROCKSDB_LITE

TEST_F(DBDeltaLogBasicTest, BestEffortsRecovery_MissingNewestDeltaLogFile) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;
  options.create_if_missing = true;
  Reopen(options);

  ASSERT_OK(dbfull()->DisableFileDeletions());
  constexpr int kNumTableFiles = 2;
  for (int i = 0; i < kNumTableFiles; ++i) {
    for (char ch = 'a'; ch != 'c'; ++ch) {
      std::string key(1, ch);
      ASSERT_OK(Put(key, "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  Close();

  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  std::string deltaLog_file_path;
  uint64_t max_deltaLog_file_num = kInvalidDeltaLogFileNumber;
  for (const auto& fname : files) {
    uint64_t file_num = 0;
    FileType type;
    if (ParseFileName(fname, &file_num, /*info_log_name_prefix=*/"", &type) &&
        type == kDeltaLogFile) {
      if (file_num > max_deltaLog_file_num) {
        max_deltaLog_file_num = file_num;
        deltaLog_file_path = dbname_ + "/" + fname;
      }
    }
  }
  ASSERT_OK(env_->DeleteFile(deltaLog_file_path));

  options.best_efforts_recovery = true;
  Reopen(options);
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("value" + std::to_string(kNumTableFiles - 2), value);
}

TEST_F(DBDeltaLogBasicTest, GetMergeDeltaLogWithPut) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v3"));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Key1", &value));
  ASSERT_EQ(Get("Key1"), "v1,v2,v3");
}

TEST_F(DBDeltaLogBasicTest, MultiGetMergeDeltaLogWithPut) {
  constexpr size_t num_keys = 3;

  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key0", "v0_0"));
  ASSERT_OK(Put("Key1", "v1_0"));
  ASSERT_OK(Put("Key2", "v2_0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_1"));
  ASSERT_OK(Merge("Key1", "v1_1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_2"));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{"Key0", "Key1", "Key2"}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys, &keys[0],
                &values[0], &statuses[0]);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "v0_0,v0_1,v0_2");

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "v1_0,v1_1");

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v2_0");
}

#ifndef ROCKSDB_LITE
TEST_F(DBDeltaLogBasicTest, Properties) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr size_t key1_size = sizeof(key1) - 1;

  constexpr char key2[] = "key2";
  constexpr size_t key2_size = sizeof(key2) - 1;

  constexpr char key3[] = "key3";
  constexpr size_t key3_size = sizeof(key3) - 1;

  constexpr char deltaLog[] = "00000000000000";
  constexpr size_t deltaLog_size = sizeof(deltaLog) - 1;

  constexpr char longer_deltaLog[] = "00000000000000000000";
  constexpr size_t longer_deltaLog_size = sizeof(longer_deltaLog) - 1;

  ASSERT_OK(Put(key1, deltaLog));
  ASSERT_OK(Put(key2, longer_deltaLog));
  ASSERT_OK(Flush());

  constexpr size_t first_deltaLog_file_expected_size =
      DeltaLogLogHeader::kSize +
      DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key1_size) +
      deltaLog_size +
      DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_deltaLog_size + DeltaLogLogFooter::kSize;

  ASSERT_OK(Put(key3, deltaLog));
  ASSERT_OK(Flush());

  constexpr size_t second_deltaLog_file_expected_size =
      DeltaLogLogHeader::kSize +
      DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key3_size) +
      deltaLog_size + DeltaLogLogFooter::kSize;

  constexpr size_t total_expected_size =
      first_deltaLog_file_expected_size + second_deltaLog_file_expected_size;

  // Number of deltaLog files
  uint64_t num_deltaLog_files = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumDeltaLogFiles,
                                  &num_deltaLog_files));
  ASSERT_EQ(num_deltaLog_files, 2);

  // Total size of live deltaLog files
  uint64_t live_deltaLog_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveDeltaLogFileSize,
                                  &live_deltaLog_file_size));
  ASSERT_EQ(live_deltaLog_file_size, total_expected_size);

  // Total amount of garbage in live deltaLog files
  {
    uint64_t live_deltaLog_file_garbage_size = 0;
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kLiveDeltaLogFileGarbageSize,
                            &live_deltaLog_file_garbage_size));
    ASSERT_EQ(live_deltaLog_file_garbage_size, 0);
  }

  // Total size of all deltaLog files across all versions
  // Note: this should be the same as above since we only have one
  // version at this point.
  uint64_t total_deltaLog_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalDeltaLogFileSize,
                                  &total_deltaLog_file_size));
  ASSERT_EQ(total_deltaLog_file_size, total_expected_size);

  // Delete key2 to create some garbage
  ASSERT_OK(Delete(key2));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  constexpr size_t expected_garbage_size =
      DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_deltaLog_size;

  constexpr double expected_space_amp =
      static_cast<double>(total_expected_size) /
      (total_expected_size - expected_garbage_size);

  // DeltaLog file stats
  std::string deltaLog_stats;
  ASSERT_TRUE(
      db_->GetProperty(DB::Properties::kDeltaLogStats, &deltaLog_stats));

  std::ostringstream oss;
  oss << "Number of deltaLog files: 2\nTotal size of deltaLog files: "
      << total_expected_size
      << "\nTotal size of garbage in deltaLog files: " << expected_garbage_size
      << "\nDeltaLog file space amplification: " << expected_space_amp << '\n';

  ASSERT_EQ(deltaLog_stats, oss.str());

  // Total amount of garbage in live deltaLog files
  {
    uint64_t live_deltaLog_file_garbage_size = 0;
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kLiveDeltaLogFileGarbageSize,
                            &live_deltaLog_file_garbage_size));
    ASSERT_EQ(live_deltaLog_file_garbage_size, expected_garbage_size);
  }
}

TEST_F(DBDeltaLogBasicTest, PropertiesMultiVersion) {
  Options options = GetDefaultOptions();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";
  constexpr char key3[] = "key3";

  constexpr size_t key_size = sizeof(key1) - 1;
  static_assert(sizeof(key2) - 1 == key_size, "unexpected size: key2");
  static_assert(sizeof(key3) - 1 == key_size, "unexpected size: key3");

  constexpr char deltaLog[] = "0000000000";
  constexpr size_t deltaLog_size = sizeof(deltaLog) - 1;

  ASSERT_OK(Put(key1, deltaLog));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(key2, deltaLog));
  ASSERT_OK(Flush());

  // Create an iterator to keep the current version alive
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_OK(iter->status());

  // Note: the Delete and subsequent compaction results in the first deltaLog
  // file not making it to the final version. (It is still part of the previous
  // version kept alive by the iterator though.) On the other hand, the Put
  // results in a third deltaLog file.
  ASSERT_OK(Delete(key1));
  ASSERT_OK(Put(key3, deltaLog));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  // Total size of all deltaLog files across all versions: between the two
  // versions, we should have three deltaLog files of the same size with one
  // deltaLog each. The version kept alive by the iterator contains the first
  // and the second deltaLog file, while the final version contains the second
  // and the third deltaLog file. (The second deltaLog file is thus shared by
  // the two versions but should be counted only once.)
  uint64_t total_deltaLog_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalDeltaLogFileSize,
                                  &total_deltaLog_file_size));
  ASSERT_EQ(
      total_deltaLog_file_size,
      3 * (DeltaLogLogHeader::kSize +
           DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
           deltaLog_size + DeltaLogLogFooter::kSize));
}
#endif  // !ROCKSDB_LITE

class DBDeltaLogBasicIOErrorTest
    : public DBDeltaLogBasicTest,
      public testing::WithParamInterface<std::string> {
 protected:
  DBDeltaLogBasicIOErrorTest() : sync_point_(GetParam()) {
    fault_injection_env_.reset(new FaultInjectionTestEnv(env_));
  }
  ~DBDeltaLogBasicIOErrorTest() { Close(); }

  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

class DBDeltaLogBasicIOErrorMultiGetTest : public DBDeltaLogBasicIOErrorTest {
 public:
  DBDeltaLogBasicIOErrorMultiGetTest() : DBDeltaLogBasicIOErrorTest() {}
};

INSTANTIATE_TEST_CASE_P(DBDeltaLogBasicTest, DBDeltaLogBasicIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "DeltaLogFileReader::OpenFile:NewRandomAccessFile",
                            "DeltaLogFileReader::GetDeltaLog:ReadFromFile"}));

INSTANTIATE_TEST_CASE_P(
    DBDeltaLogBasicTest, DBDeltaLogBasicIOErrorMultiGetTest,
    ::testing::ValuesIn(std::vector<std::string>{
        "DeltaLogFileReader::OpenFile:NewRandomAccessFile",
        "DeltaLogFileReader::MultiGetDeltaLog:ReadFromFile"}));

TEST_P(DBDeltaLogBasicIOErrorTest, GetDeltaLog_IOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char deltaLog_value[] = "deltaLog_value";

  ASSERT_OK(Put(key, deltaLog_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBDeltaLogBasicIOErrorMultiGetTest, MultiGetDeltaLogs_IOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{first_key, second_key}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys, &keys[0],
                &values[0], &statuses[0]);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(statuses[0].IsIOError());
  ASSERT_TRUE(statuses[1].IsIOError());
}

TEST_P(DBDeltaLogBasicIOErrorMultiGetTest, MultipleDeltaLogFiles) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char key1[] = "key1";
  constexpr char value1[] = "deltaLog1";

  ASSERT_OK(Put(key1, value1));
  ASSERT_OK(Flush());

  constexpr char key2[] = "key2";
  constexpr char value2[] = "deltaLog2";

  ASSERT_OK(Put(key2, value2));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{key1, key2}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  bool first_deltaLog_file = true;
  SyncPoint::GetInstance()->SetCallBack(
      sync_point_, [&first_deltaLog_file, this](void* /* arg */) {
        if (first_deltaLog_file) {
          first_deltaLog_file = false;
          return;
        }
        fault_injection_env_->SetFilesystemActive(false,
                                                  Status::IOError(sync_point_));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(value1, values[0]);
  ASSERT_TRUE(statuses[1].IsIOError());
}

namespace {

class ReadDeltaLogCompactionFilter : public CompactionFilter {
 public:
  ReadDeltaLogCompactionFilter() = default;
  const char* Name() const override {
    return "rocksdb.compaction.filter.read.deltaLog";
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice& existing_value, std::string* new_value,
      std::string* /*skip_until*/) const override {
    if (value_type != CompactionFilter::ValueType::kValue) {
      return CompactionFilter::Decision::kKeep;
    }
    assert(new_value);
    new_value->assign(existing_value.data(), existing_value.size());
    return CompactionFilter::Decision::kChangeValue;
  }
};

}  // anonymous namespace

TEST_P(DBDeltaLogBasicIOErrorTest, CompactionFilterReadDeltaLog_IOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 0;
  options.create_if_missing = true;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ReadDeltaLogCompactionFilter);
  options.compaction_filter = compaction_filter_guard.get();

  DestroyAndReopen(options);
  constexpr char key[] = "foo";
  constexpr char deltaLog_value[] = "foo_deltaLog_value";
  ASSERT_OK(Put(key, deltaLog_value));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBDeltaLogBasicTest, WarmCacheWithDeltaLogsDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_deltaLog_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_deltaLog_garbage_collection = true;
  options.deltaLog_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_deltaLog_cache = PrepopulateDeltaLogCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumDeltaLogs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= kNumDeltaLogs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(
        Put(std::to_string(i + kNumDeltaLogs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(DELTALOG_DB_CACHE_ADD));
    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumDeltaLogs)));
    ASSERT_EQ(0, options.statistics->getTickerCount(DELTALOG_DB_CACHE_MISS));
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(DELTALOG_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(kNumDeltaLogs * 2,
            options.statistics->getTickerCount(DELTALOG_DB_CACHE_ADD));
}

#ifndef ROCKSDB_LITE
TEST_F(DBDeltaLogBasicTest, DynamicallyWarmCacheDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.deltaLog_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_deltaLog_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_deltaLog_garbage_collection = true;
  options.deltaLog_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_deltaLog_cache = PrepopulateDeltaLogCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumDeltaLogs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= 5; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(
        Put(std::to_string(i + kNumDeltaLogs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(
        2, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumDeltaLogs)));
    ASSERT_EQ(
        0, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD));
    ASSERT_EQ(
        0, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS));
    ASSERT_EQ(
        2, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT));
  }

  ASSERT_OK(dbfull()->SetOptions({{"prepopulate_deltaLog_cache", "kDisable"}}));

  for (size_t i = 6; i <= kNumDeltaLogs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(
        Put(std::to_string(i + kNumDeltaLogs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(
        0, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumDeltaLogs)));
    ASSERT_EQ(
        2, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD));
    ASSERT_EQ(
        2, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS));
    ASSERT_EQ(
        0, options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(0, options.statistics->getTickerCount(DELTALOG_DB_CACHE_ADD));
}
#endif  // !ROCKSDB_LITE

TEST_F(DBDeltaLogBasicTest, WarmCacheWithDeltaLogsSecondary) {
  CompressedSecondaryCacheOptions secondary_cache_opts;
  secondary_cache_opts.capacity = 1 << 20;
  secondary_cache_opts.num_shard_bits = 0;
  secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  secondary_cache_opts.compression_type = kNoCompression;

  LRUCacheOptions primary_cache_opts;
  primary_cache_opts.capacity = 1024;
  primary_cache_opts.num_shard_bits = 0;
  primary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  primary_cache_opts.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts);

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  options.enable_deltaLog_files = true;
  options.deltaLog_cache = NewLRUCache(primary_cache_opts);
  options.prepopulate_deltaLog_cache = PrepopulateDeltaLogCache::kFlushOnly;

  DestroyAndReopen(options);

  // Note: only one of the two deltaLogs fit in the primary cache at any given
  // time.
  constexpr char first_key[] = "foo";
  constexpr size_t first_deltaLog_size = 512;
  const std::string first_deltaLog(first_deltaLog_size, 'a');

  constexpr char second_key[] = "bar";
  constexpr size_t second_deltaLog_size = 768;
  const std::string second_deltaLog(second_deltaLog_size, 'b');

  // First deltaLog is inserted into primary cache during flush.
  ASSERT_OK(Put(first_key, first_deltaLog));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
            1);

  // Second deltaLog is inserted into primary cache during flush,
  // First deltaLog is evicted but only a dummy handle is inserted into
  // secondary cache.
  ASSERT_OK(Put(second_key, second_deltaLog));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_ADD),
            1);

  // First deltaLog is inserted into primary cache.
  // Second deltaLog is evicted but only a dummy handle is inserted into
  // secondary cache.
  ASSERT_EQ(Get(first_key), first_deltaLog);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS),
            1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT),
            0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);
  // Second deltaLog is inserted into primary cache,
  // First deltaLog is evicted and is inserted into secondary cache.
  ASSERT_EQ(Get(second_key), second_deltaLog);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS),
            1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT),
            0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);

  // First deltaLog's dummy item is inserted into primary cache b/c of lookup.
  // Second deltaLog is still in primary cache.
  ASSERT_EQ(Get(first_key), first_deltaLog);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS),
            0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT),
            1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);

  // First deltaLog's item is inserted into primary cache b/c of lookup.
  // Second deltaLog is evicted and inserted into secondary cache.
  ASSERT_EQ(Get(first_key), first_deltaLog);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_MISS),
            0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(DELTALOG_DB_CACHE_HIT),
            1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
