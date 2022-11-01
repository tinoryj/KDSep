//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include "cache/charged_cache.h"
#include "cache/compressed_secondary_cache.h"
#include "db/db_test_util.h"
#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_file_cache.h"
#include "db/deltaLog/deltaLog_file_reader.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/deltaLog_log_writer.h"
#include "db/deltaLog/deltaLog_source.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test deltaLog file with `num` deltaLogs in it.
void WriteDeltaLogFile(const ImmutableOptions& immutable_options,
                       uint32_t column_family_id, bool has_ttl,
                       const ExpirationRange& expiration_range_header,
                       const ExpirationRange& expiration_range_footer,
                       uint64_t deltaLog_file_number,
                       const std::vector<Slice>& keys,
                       const std::vector<Slice>& deltaLogs,
                       CompressionType compression,
                       std::vector<uint64_t>& deltaLog_offsets,
                       std::vector<uint64_t>& deltaLog_sizes) {
  assert(!immutable_options.cf_paths.empty());
  size_t num = keys.size();
  assert(num == deltaLogs.size());
  assert(num == deltaLog_offsets.size());
  assert(num == deltaLog_sizes.size());

  const std::string deltaLog_file_path = DeltaLogFileName(
      immutable_options.cf_paths.front().path, deltaLog_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), deltaLog_file_path,
                            &file, FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), deltaLog_file_path, FileOptions(),
                             immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;

  DeltaLogLogWriter deltaLog_log_writer(
      std::move(file_writer), immutable_options.clock, statistics,
      deltaLog_file_number, use_fsync, do_flush);

  DeltaLogLogHeader header(column_family_id, compression, has_ttl,
                           expiration_range_header);

  ASSERT_OK(deltaLog_log_writer.WriteHeader(header));

  std::vector<std::string> compressed_deltaLogs(num);
  std::vector<Slice> deltaLogs_to_write(num);
  if (kNoCompression == compression) {
    for (size_t i = 0; i < num; ++i) {
      deltaLogs_to_write[i] = deltaLogs[i];
      deltaLog_sizes[i] = deltaLogs[i].size();
    }
  } else {
    CompressionOptions opts;
    CompressionContext context(compression);
    constexpr uint64_t sample_for_compression = 0;
    CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                         compression, sample_for_compression);

    constexpr uint32_t compression_format_version = 2;

    for (size_t i = 0; i < num; ++i) {
      ASSERT_TRUE(CompressData(deltaLogs[i], info, compression_format_version,
                               &compressed_deltaLogs[i]));
      deltaLogs_to_write[i] = compressed_deltaLogs[i];
      deltaLog_sizes[i] = compressed_deltaLogs[i].size();
    }
  }

  for (size_t i = 0; i < num; ++i) {
    uint64_t key_offset = 0;
    ASSERT_OK(deltaLog_log_writer.AddRecord(keys[i], deltaLogs_to_write[i],
                                            &key_offset, &deltaLog_offsets[i]));
  }

  DeltaLogLogFooter footer;
  footer.deltaLog_count = num;
  footer.expiration_range = expiration_range_footer;

  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(deltaLog_log_writer.AppendFooter(footer, &checksum_method,
                                             &checksum_value));
}

}  // anonymous namespace

class DeltaLogSourceTest : public DBTestBase {
 protected:
 public:
  explicit DeltaLogSourceTest()
      : DBTestBase("deltaLog_source_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_deltaLog_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = 8 << 20;
    co.num_shard_bits = 2;
    co.metadata_charge_policy = kDontChargeCacheMetadata;
    co.high_pri_pool_ratio = 0.2;
    co.low_pri_pool_ratio = 0.2;
    options_.deltaLog_cache = NewLRUCache(co);
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

TEST_F(DeltaLogSourceTest, GetDeltaLogsFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "DeltaLogSourceTest_GetDeltaLogsFromCache"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr size_t num_deltaLogs = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> deltaLog_strs;

  for (size_t i = 0; i < num_deltaLogs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    deltaLog_strs.push_back("deltaLog" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltaLogs;

  uint64_t file_size = DeltaLogLogHeader::kSize;
  for (size_t i = 0; i < num_deltaLogs; ++i) {
    keys.push_back({key_strs[i]});
    deltaLogs.push_back({deltaLog_strs[i]});
    file_size +=
        DeltaLogLogRecord::kHeaderSize + keys[i].size() + deltaLogs[i].size();
  }
  file_size += DeltaLogLogFooter::kSize;

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    keys, deltaLogs, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // DeltaLog file cache

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, deltaLog_file_read_hist, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // GetDeltaLog
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t deltaLog_bytes = 0;
    uint64_t total_bytes = 0;

    read_options.fill_cache = false;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], deltaLog_file_number, deltaLog_offsets[i],
          file_size, deltaLog_sizes[i], kNoCompression, prefetch_buffer,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
      total_bytes += bytes_read;
    }

    // Retrieved the deltaLog cache num_deltaLogs * 3 times via
    // TEST_DeltaLogInCache, GetDeltaLog, and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, num_deltaLogs);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, total_bytes);
    ASSERT_GE((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);

    read_options.fill_cache = true;
    deltaLog_bytes = 0;
    total_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], deltaLog_file_number, deltaLog_offsets[i],
          file_size, deltaLog_sizes[i], kNoCompression, prefetch_buffer,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      deltaLog_bytes += deltaLog_sizes[i];
      total_bytes += bytes_read;
      ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, i);
      ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, total_bytes);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, total_bytes);
    }

    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, num_deltaLogs);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, num_deltaLogs);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, total_bytes);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), num_deltaLogs);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), num_deltaLogs);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_bytes);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE),
              deltaLog_bytes);

    read_options.fill_cache = true;
    total_bytes = 0;
    deltaLog_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], deltaLog_file_number, deltaLog_offsets[i],
          file_size, deltaLog_sizes[i], kNoCompression, prefetch_buffer,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
      total_bytes += bytes_read;            // on-disk deltaLog record size
      deltaLog_bytes += deltaLog_sizes[i];  // cached deltaLog value size
    }

    // Retrieved the deltaLog cache num_deltaLogs * 3 times via
    // TEST_DeltaLogInCache, GetDeltaLog, and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_deltaLogs * 3);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_deltaLogs * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);

    // Cache-only GetDeltaLog
    read_options.read_tier = ReadTier::kBlockCacheTier;
    total_bytes = 0;
    deltaLog_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], deltaLog_file_number, deltaLog_offsets[i],
          file_size, deltaLog_sizes[i], kNoCompression, prefetch_buffer,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
      total_bytes += bytes_read;
      deltaLog_bytes += deltaLog_sizes[i];
    }

    // Retrieved the deltaLog cache num_deltaLogs * 3 times via
    // TEST_DeltaLogInCache, GetDeltaLog, and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_deltaLogs * 3);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_deltaLogs * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.deltaLog_cache->EraseUnRefEntries();

  {
    // Cache-only GetDeltaLog
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;

    read_options.read_tier = ReadTier::kBlockCacheTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_TRUE(deltaLog_source
                      .GetDeltaLog(read_options, keys[i], deltaLog_file_number,
                                   deltaLog_offsets[i], file_size,
                                   deltaLog_sizes[i], kNoCompression,
                                   prefetch_buffer, &values[i], &bytes_read)
                      .IsIncomplete());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
    }

    // Retrieved the deltaLog cache num_deltaLogs * 3 times via
    // TEST_DeltaLogInCache, GetDeltaLog, and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // GetDeltaLog from non-existing file
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t file_number = 100;  // non-existing file

    read_options.read_tier = ReadTier::kReadAllTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                        deltaLog_offsets[i]));

      ASSERT_TRUE(deltaLog_source
                      .GetDeltaLog(read_options, keys[i], file_number,
                                   deltaLog_offsets[i], file_size,
                                   deltaLog_sizes[i], kNoCompression,
                                   prefetch_buffer, &values[i], &bytes_read)
                      .IsIOError());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                        deltaLog_offsets[i]));
    }

    // Retrieved the deltaLog cache num_deltaLogs * 3 times via
    // TEST_DeltaLogInCache, GetDeltaLog, and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(DeltaLogSourceTest, GetCompressedDeltaLogs) {
  if (!Snappy_Supported()) {
    return;
  }

  const CompressionType compression = kSnappyCompression;

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "DeltaLogSourceTest_GetCompressedDeltaLogs"),
      0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr size_t num_deltaLogs = 256;

  std::vector<std::string> key_strs;
  std::vector<std::string> deltaLog_strs;

  for (size_t i = 0; i < num_deltaLogs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    deltaLog_strs.push_back("deltaLog" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltaLogs;

  for (size_t i = 0; i < num_deltaLogs; ++i) {
    keys.push_back({key_strs[i]});
    deltaLogs.push_back({deltaLog_strs[i]});
  }

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  constexpr size_t capacity = 1024;
  auto backing_cache = NewLRUCache(capacity);  // DeltaLog file cache

  FileOptions file_options;
  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;
  std::vector<PinnableSlice> values(keys.size());

  {
    // Snappy Compression
    const uint64_t file_number = 1;

    read_options.read_tier = ReadTier::kReadAllTier;

    WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                      expiration_range, expiration_range, file_number, keys,
                      deltaLogs, compression, deltaLog_offsets, deltaLog_sizes);

    CacheHandleGuard<DeltaLogFileReader> deltaLog_file_reader;
    ASSERT_OK(deltaLog_source.GetDeltaLogFileReader(file_number,
                                                    &deltaLog_file_reader));
    ASSERT_NE(deltaLog_file_reader.GetValue(), nullptr);

    const uint64_t file_size = deltaLog_file_reader.GetValue()->GetFileSize();
    ASSERT_EQ(deltaLog_file_reader.GetValue()->GetCompressionType(),
              compression);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_NE(deltaLogs[i].size() /*uncompressed size*/,
                deltaLog_sizes[i] /*compressed size*/);
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                        deltaLog_offsets[i]));
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], file_number, deltaLog_offsets[i], file_size,
          deltaLog_sizes[i], compression, nullptr /*prefetch_buffer*/,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i] /*uncompressed deltaLog*/);
      ASSERT_NE(values[i].size(), deltaLog_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                       deltaLog_offsets[i]));
    }

    ASSERT_GE((int)get_perf_context()->deltaLog_decompress_time, 0);

    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                       deltaLog_offsets[i]));

      // Compressed deltaLog size is passed in GetDeltaLog
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], file_number, deltaLog_offsets[i], file_size,
          deltaLog_sizes[i], compression, nullptr /*prefetch_buffer*/,
          &values[i], &bytes_read));
      ASSERT_EQ(values[i], deltaLogs[i] /*uncompressed deltaLog*/);
      ASSERT_NE(values[i].size(), deltaLog_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                       deltaLog_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);
  }
}

TEST_F(DeltaLogSourceTest, MultiGetDeltaLogsFromMultiFiles) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "DeltaLogSourceTest_MultiGetDeltaLogsFromMultiFiles"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_files = 2;
  constexpr size_t num_deltaLogs = 32;

  std::vector<std::string> key_strs;
  std::vector<std::string> deltaLog_strs;

  for (size_t i = 0; i < num_deltaLogs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    deltaLog_strs.push_back("deltaLog" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltaLogs;

  uint64_t file_size = DeltaLogLogHeader::kSize;
  uint64_t deltaLog_value_bytes = 0;
  for (size_t i = 0; i < num_deltaLogs; ++i) {
    keys.push_back({key_strs[i]});
    deltaLogs.push_back({deltaLog_strs[i]});
    deltaLog_value_bytes += deltaLogs[i].size();
    file_size +=
        DeltaLogLogRecord::kHeaderSize + keys[i].size() + deltaLogs[i].size();
  }
  file_size += DeltaLogLogFooter::kSize;
  const uint64_t deltaLog_records_bytes =
      file_size - DeltaLogLogHeader::kSize - DeltaLogLogFooter::kSize;

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  {
    // Write key/deltaLog pairs to multiple deltaLog files.
    for (size_t i = 0; i < deltaLog_files; ++i) {
      const uint64_t file_number = i + 1;
      WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                        expiration_range, expiration_range, file_number, keys,
                        deltaLogs, kNoCompression, deltaLog_offsets,
                        deltaLog_sizes);
    }
  }

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // DeltaLog file cache

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, deltaLog_file_read_hist, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;

  {
    // MultiGetDeltaLog
    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;

    autovector<DeltaLogFileReadRequests> deltaLog_reqs;
    std::array<autovector<DeltaLogReadRequest>, deltaLog_files>
        deltaLog_reqs_in_file;
    std::array<PinnableSlice, num_deltaLogs * deltaLog_files> value_buf;
    std::array<Status, num_deltaLogs * deltaLog_files> statuses_buf;

    for (size_t i = 0; i < deltaLog_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltaLogs; ++j) {
        deltaLog_reqs_in_file[i].emplace_back(
            keys[j], deltaLog_offsets[j], deltaLog_sizes[j], kNoCompression,
            &value_buf[i * num_deltaLogs + j],
            &statuses_buf[i * num_deltaLogs + j]);
      }
      deltaLog_reqs.emplace_back(file_number, file_size,
                                 deltaLog_reqs_in_file[i]);
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    deltaLog_source.MultiGetDeltaLog(read_options, deltaLog_reqs, &bytes_read);

    for (size_t i = 0; i < deltaLog_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltaLogs; ++j) {
        ASSERT_OK(statuses_buf[i * num_deltaLogs + j]);
        ASSERT_EQ(value_buf[i * num_deltaLogs + j], deltaLogs[j]);
        ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                         deltaLog_offsets[j]));
      }
    }

    // Retrieved all deltaLogs from 2 deltaLog files twice via MultiGetDeltaLog
    // and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_deltaLogs * deltaLog_files);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count,
              num_deltaLogs * deltaLog_files);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte,
              deltaLog_records_bytes * deltaLog_files);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * deltaLog_files);  // MultiGetDeltaLog
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_deltaLogs * deltaLog_files);  // TEST_DeltaLogInCache
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD),
              num_deltaLogs * deltaLog_files);  // MultiGetDeltaLog
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_value_bytes * deltaLog_files);  // TEST_DeltaLogInCache
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE),
              deltaLog_value_bytes * deltaLog_files);  // MultiGetDeltaLog

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    autovector<DeltaLogReadRequest> fake_deltaLog_reqs_in_file;
    std::array<PinnableSlice, num_deltaLogs> fake_value_buf;
    std::array<Status, num_deltaLogs> fake_statuses_buf;

    const uint64_t fake_file_number = 100;
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      fake_deltaLog_reqs_in_file.emplace_back(
          keys[i], deltaLog_offsets[i], deltaLog_sizes[i], kNoCompression,
          &fake_value_buf[i], &fake_statuses_buf[i]);
    }

    // Add a fake multi-get deltaLog request.
    deltaLog_reqs.emplace_back(fake_file_number, file_size,
                               fake_deltaLog_reqs_in_file);

    deltaLog_source.MultiGetDeltaLog(read_options, deltaLog_reqs, &bytes_read);

    // Check the real deltaLog read requests.
    for (size_t i = 0; i < deltaLog_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltaLogs; ++j) {
        ASSERT_OK(statuses_buf[i * num_deltaLogs + j]);
        ASSERT_EQ(value_buf[i * num_deltaLogs + j], deltaLogs[j]);
        ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                         deltaLog_offsets[j]));
      }
    }

    // Check the fake deltaLog request.
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(fake_statuses_buf[i].IsIOError());
      ASSERT_TRUE(fake_value_buf[i].empty());
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          fake_file_number, file_size, deltaLog_offsets[i]));
    }

    // Retrieved all deltaLogs from 3 deltaLog files (including the fake one)
    // twice via MultiGetDeltaLog and TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_deltaLogs * deltaLog_files * 2);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count,
              0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte,
              0);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    // Fake deltaLog requests: MultiGetDeltaLog and TEST_DeltaLogInCache
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 2);
    // Real deltaLog requests: MultiGetDeltaLog and TEST_DeltaLogInCache
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_deltaLogs * deltaLog_files * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    // Real deltaLog requests: MultiGetDeltaLog and TEST_DeltaLogInCache
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_value_bytes * deltaLog_files * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(DeltaLogSourceTest, MultiGetDeltaLogsFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_,
                            "DeltaLogSourceTest_MultiGetDeltaLogsFromCache"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr size_t num_deltaLogs = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> deltaLog_strs;

  for (size_t i = 0; i < num_deltaLogs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    deltaLog_strs.push_back("deltaLog" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltaLogs;

  uint64_t file_size = DeltaLogLogHeader::kSize;
  for (size_t i = 0; i < num_deltaLogs; ++i) {
    keys.push_back({key_strs[i]});
    deltaLogs.push_back({deltaLog_strs[i]});
    file_size +=
        DeltaLogLogRecord::kHeaderSize + keys[i].size() + deltaLogs[i].size();
  }
  file_size += DeltaLogLogFooter::kSize;

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    keys, deltaLogs, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // DeltaLog file cache

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, deltaLog_file_read_hist, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // MultiGetDeltaLogFromOneFile
    uint64_t bytes_read = 0;
    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<PinnableSlice, num_deltaLogs> value_buf;
    autovector<DeltaLogReadRequest> deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; i += 2) {  // even index
      deltaLog_reqs.emplace_back(keys[i], deltaLog_offsets[i],
                                 deltaLog_sizes[i], kNoCompression,
                                 &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    // Get half of deltaLogs
    deltaLog_source.MultiGetDeltaLogFromOneFile(read_options,
                                                deltaLog_file_number, file_size,
                                                deltaLog_reqs, &bytes_read);

    uint64_t fs_read_bytes = 0;
    uint64_t ca_read_bytes = 0;
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (i % 2 == 0) {
        ASSERT_OK(statuses_buf[i]);
        ASSERT_EQ(value_buf[i], deltaLogs[i]);
        ASSERT_TRUE(value_buf[i].IsPinned());
        fs_read_bytes +=
            deltaLog_sizes[i] + keys[i].size() + DeltaLogLogRecord::kHeaderSize;
        ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
            deltaLog_file_number, file_size, deltaLog_offsets[i]));
        ca_read_bytes += deltaLog_sizes[i];
      } else {
        statuses_buf[i].PermitUncheckedError();
        ASSERT_TRUE(value_buf[i].empty());
        ASSERT_FALSE(value_buf[i].IsPinned());
        ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
            deltaLog_file_number, file_size, deltaLog_offsets[i]));
      }
    }

    constexpr int num_even_deltaLogs = num_deltaLogs / 2;
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_even_deltaLogs);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count,
              num_even_deltaLogs);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte,
              fs_read_bytes);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_even_deltaLogs);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD),
              num_even_deltaLogs);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              ca_read_bytes);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE),
              ca_read_bytes);

    // Get the rest of deltaLogs
    for (size_t i = 1; i < num_deltaLogs; i += 2) {  // odd index
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));

      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[i], deltaLog_file_number, deltaLog_offsets[i],
          file_size, deltaLog_sizes[i], kNoCompression, prefetch_buffer,
          &value_buf[i], &bytes_read));
      ASSERT_EQ(value_buf[i], deltaLogs[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_EQ(bytes_read, DeltaLogLogRecord::kHeaderSize + keys[i].size() +
                                deltaLog_sizes[i]);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
    }

    // Cache-only MultiGetDeltaLogFromOneFile
    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    deltaLog_reqs.clear();
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      deltaLog_reqs.emplace_back(keys[i], deltaLog_offsets[i],
                                 deltaLog_sizes[i], kNoCompression,
                                 &value_buf[i], &statuses_buf[i]);
    }

    deltaLog_source.MultiGetDeltaLogFromOneFile(read_options,
                                                deltaLog_file_number, file_size,
                                                deltaLog_reqs, &bytes_read);

    uint64_t deltaLog_bytes = 0;
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_OK(statuses_buf[i]);
      ASSERT_EQ(value_buf[i], deltaLogs[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
      deltaLog_bytes += deltaLog_sizes[i];
    }

    // Retrieved the deltaLog cache num_deltaLogs * 2 times via GetDeltaLog and
    // TEST_DeltaLogInCache.
    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count,
              num_deltaLogs * 2);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);   // blocking i/o
    ASSERT_GE((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT),
              num_deltaLogs * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ),
              deltaLog_bytes * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.deltaLog_cache->EraseUnRefEntries();

  {
    // Cache-only MultiGetDeltaLogFromOneFile
    uint64_t bytes_read = 0;
    read_options.read_tier = ReadTier::kBlockCacheTier;

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<PinnableSlice, num_deltaLogs> value_buf;
    autovector<DeltaLogReadRequest> deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; i++) {
      deltaLog_reqs.emplace_back(keys[i], deltaLog_offsets[i],
                                 deltaLog_sizes[i], kNoCompression,
                                 &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    deltaLog_source.MultiGetDeltaLogFromOneFile(read_options,
                                                deltaLog_file_number, file_size,
                                                deltaLog_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIncomplete());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          deltaLog_file_number, file_size, deltaLog_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // MultiGetDeltaLogFromOneFile from non-existing file
    uint64_t bytes_read = 0;
    uint64_t non_existing_file_number = 100;
    read_options.read_tier = ReadTier::kReadAllTier;

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<PinnableSlice, num_deltaLogs> value_buf;
    autovector<DeltaLogReadRequest> deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; i++) {
      deltaLog_reqs.emplace_back(keys[i], deltaLog_offsets[i],
                                 deltaLog_sizes[i], kNoCompression,
                                 &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          non_existing_file_number, file_size, deltaLog_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    deltaLog_source.MultiGetDeltaLogFromOneFile(
        read_options, non_existing_file_number, file_size, deltaLog_reqs,
        &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIOError());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(
          non_existing_file_number, file_size, deltaLog_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->deltaLog_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->deltaLog_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->deltaLog_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_MISS),
              num_deltaLogs * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTALOG_DB_CACHE_BYTES_WRITE), 0);
  }
}

class DeltaLogSecondaryCacheTest : public DBTestBase {
 protected:
 public:
  explicit DeltaLogSecondaryCacheTest()
      : DBTestBase("deltaLog_secondary_cache_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_deltaLog_files = true;
    options_.create_if_missing = true;

    // Set a small cache capacity to evict entries from the cache, and to test
    // that secondary cache is used properly.
    lru_cache_opts_.capacity = 1024;
    lru_cache_opts_.num_shard_bits = 0;
    lru_cache_opts_.strict_capacity_limit = true;
    lru_cache_opts_.metadata_charge_policy = kDontChargeCacheMetadata;
    lru_cache_opts_.high_pri_pool_ratio = 0.2;
    lru_cache_opts_.low_pri_pool_ratio = 0.2;

    secondary_cache_opts_.capacity = 8 << 20;  // 8 MB
    secondary_cache_opts_.num_shard_bits = 0;
    secondary_cache_opts_.metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy;

    // Read deltaLogs from the secondary cache if they are not in the primary
    // cache
    options_.lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  Options options_;

  LRUCacheOptions lru_cache_opts_;
  CompressedSecondaryCacheOptions secondary_cache_opts_;

  std::string db_id_;
  std::string db_session_id_;
};

TEST_F(DeltaLogSecondaryCacheTest, GetDeltaLogsFromSecondaryCache) {
  if (!Snappy_Supported()) {
    return;
  }

  secondary_cache_opts_.compression_type = kSnappyCompression;
  lru_cache_opts_.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts_);
  options_.deltaLog_cache = NewLRUCache(lru_cache_opts_);

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "DeltaLogSecondaryCacheTest_GetDeltaLogsFromSecondaryCache"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t file_number = 1;

  Random rnd(301);

  std::vector<std::string> key_strs{"key0", "key1"};
  std::vector<std::string> deltaLog_strs{rnd.RandomString(512),
                                         rnd.RandomString(768)};

  std::vector<Slice> keys{key_strs[0], key_strs[1]};
  std::vector<Slice> deltaLogs{deltaLog_strs[0], deltaLog_strs[1]};

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, file_number, keys,
                    deltaLogs, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache(new DeltaLogFileCache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      deltaLog_file_read_hist, nullptr /*IOTracer*/));

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  CacheHandleGuard<DeltaLogFileReader> file_reader;
  ASSERT_OK(deltaLog_source.GetDeltaLogFileReader(file_number, &file_reader));
  ASSERT_NE(file_reader.GetValue(), nullptr);
  const uint64_t file_size = file_reader.GetValue()->GetFileSize();
  ASSERT_EQ(file_reader.GetValue()->GetCompressionType(), kNoCompression);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  auto deltaLog_cache = options_.deltaLog_cache;
  auto secondary_cache = lru_cache_opts_.secondary_cache;

  Cache::CreateCallback create_cb = [](const void* buf, size_t size,
                                       void** out_obj,
                                       size_t* charge) -> Status {
    CacheAllocationPtr allocation(new char[size]);

    return DeltaLogContents::CreateCallback(std::move(allocation), buf, size,
                                            out_obj, charge);
  };

  {
    // GetDeltaLog
    std::vector<PinnableSlice> values(keys.size());

    read_options.fill_cache = true;
    get_perf_context()->Reset();

    // key0 should be filled to the primary cache from the deltaLog file.
    ASSERT_OK(deltaLog_source.GetDeltaLog(
        read_options, keys[0], file_number, deltaLog_offsets[0], file_size,
        deltaLog_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
        &values[0], nullptr /* bytes_read */));
    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and key0's dummy item is inserted into secondary
    // cache. key1 should be filled to the primary cache from the deltaLog file.
    ASSERT_OK(deltaLog_source.GetDeltaLog(
        read_options, keys[1], file_number, deltaLog_offsets[1], file_size,
        deltaLog_sizes[1], kNoCompression, nullptr /* prefetch_buffer */,
        &values[1], nullptr /* bytes_read */));

    // Release cache handle
    values[1].Reset();

    // key0 should be filled to the primary cache from the deltaLog file. key1
    // should be evicted and key1's dummy item is inserted into secondary cache.
    ASSERT_OK(deltaLog_source.GetDeltaLog(
        read_options, keys[0], file_number, deltaLog_offsets[0], file_size,
        deltaLog_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
        &values[0], nullptr /* bytes_read */));
    ASSERT_EQ(values[0], deltaLogs[0]);
    ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                     deltaLog_offsets[0]));

    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and is inserted into secondary cache.
    // key1 should be filled to the primary cache from the deltaLog file.
    ASSERT_OK(deltaLog_source.GetDeltaLog(
        read_options, keys[1], file_number, deltaLog_offsets[1], file_size,
        deltaLog_sizes[1], kNoCompression, nullptr /* prefetch_buffer */,
        &values[1], nullptr /* bytes_read */));
    ASSERT_EQ(values[1], deltaLogs[1]);
    ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                     deltaLog_offsets[1]));

    // Release cache handle
    values[1].Reset();

    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

    // deltaLog_cache here only looks at the primary cache since we didn't
    // provide the cache item helper for the secondary cache. However, since
    // key0 is demoted to the secondary cache, we shouldn't be able to find it
    // in the primary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(deltaLog_offsets[0]);
      const Slice key0 = cache_key.AsSlice();
      auto handle0 = deltaLog_cache->Lookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key0's item should be in the secondary cache.
      bool is_in_sec_cache = false;
      auto sec_handle0 =
          secondary_cache->Lookup(key0, create_cb, true,
                                  /*advise_erase=*/true, is_in_sec_cache);
      ASSERT_FALSE(is_in_sec_cache);
      ASSERT_NE(sec_handle0, nullptr);
      ASSERT_TRUE(sec_handle0->IsReady());
      auto value = static_cast<DeltaLogContents*>(sec_handle0->Value());
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), deltaLogs[0]);
      delete value;

      // key0 doesn't exist in the deltaLog cache although key0's dummy
      // item exist in the secondary cache.
      ASSERT_FALSE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                        deltaLog_offsets[0]));
    }

    // key1 should exists in the primary cache. key1's dummy item exists
    // in the secondary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(deltaLog_offsets[1]);
      const Slice key1 = cache_key.AsSlice();
      auto handle1 = deltaLog_cache->Lookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      deltaLog_cache->Release(handle1);

      bool is_in_sec_cache = false;
      auto sec_handle1 =
          secondary_cache->Lookup(key1, create_cb, true,
                                  /*advise_erase=*/true, is_in_sec_cache);
      ASSERT_FALSE(is_in_sec_cache);
      ASSERT_EQ(sec_handle1, nullptr);

      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                       deltaLog_offsets[1]));
    }

    {
      // fetch key0 from the deltaLog file to the primary cache.
      // key1 is evicted and inserted into the secondary cache.
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys[0], file_number, deltaLog_offsets[0], file_size,
          deltaLog_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
          &values[0], nullptr /* bytes_read */));
      ASSERT_EQ(values[0], deltaLogs[0]);

      // Release cache handle
      values[0].Reset();

      // key0 should be in the primary cache.
      CacheKey cache_key0 = base_cache_key.WithOffset(deltaLog_offsets[0]);
      const Slice key0 = cache_key0.AsSlice();
      auto handle0 = deltaLog_cache->Lookup(key0, statistics);
      ASSERT_NE(handle0, nullptr);
      auto value =
          static_cast<DeltaLogContents*>(deltaLog_cache->Value(handle0));
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), deltaLogs[0]);
      deltaLog_cache->Release(handle0);

      // key1 is not in the primary cache and is in the secondary cache.
      CacheKey cache_key1 = base_cache_key.WithOffset(deltaLog_offsets[1]);
      const Slice key1 = cache_key1.AsSlice();
      auto handle1 = deltaLog_cache->Lookup(key1, statistics);
      ASSERT_EQ(handle1, nullptr);

      // erase key0 from the primary cache.
      deltaLog_cache->Erase(key0);
      handle0 = deltaLog_cache->Lookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key1 promotion should succeed due to the primary cache being empty. we
      // did't call secondary cache's Lookup() here, because it will remove the
      // key but it won't be able to promote the key to the primary cache.
      // Instead we use the end-to-end deltaLog source API to read key1.
      // In function TEST_DeltaLogInCache, key1's dummy item is inserted into
      // the primary cache and a standalone handle is checked by GetValue().
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(file_number, file_size,
                                                       deltaLog_offsets[1]));

      // key1's dummy handle is in the primary cache and key1's item is still
      // in the secondary cache. So, the primary cache's Lookup() can only
      // get a dummy handle.
      handle1 = deltaLog_cache->Lookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      // handl1 is a dummy handle.
      ASSERT_EQ(deltaLog_cache->Value(handle1), nullptr);
      deltaLog_cache->Release(handle1);
    }
  }
}

class DeltaLogSourceCacheReservationTest : public DBTestBase {
 public:
  explicit DeltaLogSourceCacheReservationTest()
      : DBTestBase("deltaLog_source_cache_reservation_test",
                   /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_deltaLog_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = kCacheCapacity;
    co.num_shard_bits = kNumShardBits;
    co.metadata_charge_policy = kDontChargeCacheMetadata;

    co.high_pri_pool_ratio = 0.0;
    co.low_pri_pool_ratio = 0.0;
    std::shared_ptr<Cache> deltaLog_cache = NewLRUCache(co);

    co.high_pri_pool_ratio = 0.5;
    co.low_pri_pool_ratio = 0.5;
    std::shared_ptr<Cache> block_cache = NewLRUCache(co);

    options_.deltaLog_cache = deltaLog_cache;
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    BlockBasedTableOptions block_based_options;
    block_based_options.no_block_cache = false;
    block_based_options.block_cache = block_cache;
    block_based_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kDeltaLogCache,
         {/* charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
    options_.table_factory.reset(
        NewBlockBasedTableFactory(block_based_options));

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  void GenerateKeysAndDeltaLogs() {
    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      key_strs_.push_back("key" + std::to_string(i));
      deltaLog_strs_.push_back("deltaLog" + std::to_string(i));
    }

    deltaLog_file_size_ = DeltaLogLogHeader::kSize;
    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      keys_.push_back({key_strs_[i]});
      deltaLogs_.push_back({deltaLog_strs_[i]});
      deltaLog_file_size_ += DeltaLogLogRecord::kHeaderSize + keys_[i].size() +
                             deltaLogs_[i].size();
    }
    deltaLog_file_size_ += DeltaLogLogFooter::kSize;
  }

  static constexpr std::size_t kSizeDummyEntry = CacheReservationManagerImpl<
      CacheEntryRole::kDeltaLogCache>::GetDummyEntrySize();
  static constexpr std::size_t kCacheCapacity = 1 * kSizeDummyEntry;
  static constexpr int kNumShardBits = 0;  // 2^0 shard

  static constexpr uint32_t kColumnFamilyId = 1;
  static constexpr bool kHasTTL = false;
  static constexpr uint64_t kDeltaLogFileNumber = 1;
  static constexpr size_t kNumDeltaLogs = 16;

  std::vector<Slice> keys_;
  std::vector<Slice> deltaLogs_;
  std::vector<std::string> key_strs_;
  std::vector<std::string> deltaLog_strs_;
  uint64_t deltaLog_file_size_;

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

#ifndef ROCKSDB_LITE
TEST_F(DeltaLogSourceCacheReservationTest, SimpleCacheReservation) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "DeltaLogSourceCacheReservationTest_SimpleCacheReservation"),
      0);

  GenerateKeysAndDeltaLogs();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr ExpirationRange expiration_range;

  std::vector<uint64_t> deltaLog_offsets(keys_.size());
  std::vector<uint64_t> deltaLog_sizes(keys_.size());

  WriteDeltaLogFile(immutable_options, kColumnFamilyId, kHasTTL,
                    expiration_range, expiration_range, kDeltaLogFileNumber,
                    keys_, deltaLogs_, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, deltaLog_file_read_hist, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(deltaLog_source.GetDeltaLogCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys_[i], kDeltaLogFileNumber, deltaLog_offsets[i],
          deltaLog_file_size_, deltaLog_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    // num_deltaLogs is 16, so the total deltaLog cache usage is less than a
    // single dummy entry. Therefore, cache reservation manager only reserves
    // one dummy entry here.
    uint64_t deltaLog_bytes = 0;
    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys_[i], kDeltaLogFileNumber, deltaLog_offsets[i],
          deltaLog_file_size_, deltaLog_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      size_t charge = 0;
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          kDeltaLogFileNumber, deltaLog_file_size_, deltaLog_offsets[i],
          &charge));

      deltaLog_bytes += charge;
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), deltaLog_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.deltaLog_cache->GetUsage());
    }
  }

  {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                      kDeltaLogFileNumber);
    size_t deltaLog_bytes = options_.deltaLog_cache->GetUsage();

    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      size_t charge = 0;
      ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
          kDeltaLogFileNumber, deltaLog_file_size_, deltaLog_offsets[i],
          &charge));

      CacheKey cache_key = base_cache_key.WithOffset(deltaLog_offsets[i]);
      // We didn't call options_.deltaLog_cache->Erase() here, this is because
      // the cache wrapper's Erase() method must be called to update the
      // cache usage after erasing the cache entry.
      deltaLog_source.GetDeltaLogCache()->Erase(cache_key.AsSlice());
      if (i == kNumDeltaLogs - 1) {
        // All the deltaLogs got removed from the cache. cache_res_mgr should
        // not reserve any space for them.
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      } else {
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      }
      deltaLog_bytes -= charge;
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), deltaLog_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.deltaLog_cache->GetUsage());
    }
  }
}

TEST_F(DeltaLogSourceCacheReservationTest,
       IncreaseCacheReservationOnFullCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_,
                            "DeltaLogSourceCacheReservationTest_"
                            "IncreaseCacheReservationOnFullCache"),
      0);

  GenerateKeysAndDeltaLogs();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  constexpr size_t deltaLog_size = kSizeDummyEntry / (kNumDeltaLogs / 2);
  for (size_t i = 0; i < kNumDeltaLogs; ++i) {
    deltaLog_file_size_ -= deltaLogs_[i].size();  // old deltaLog size
    deltaLog_strs_[i].resize(deltaLog_size, '@');
    deltaLogs_[i] = Slice(deltaLog_strs_[i]);
    deltaLog_file_size_ += deltaLogs_[i].size();  // new deltaLog size
  }

  std::vector<uint64_t> deltaLog_offsets(keys_.size());
  std::vector<uint64_t> deltaLog_sizes(keys_.size());

  constexpr ExpirationRange expiration_range;
  WriteDeltaLogFile(immutable_options, kColumnFamilyId, kHasTTL,
                    expiration_range, expiration_range, kDeltaLogFileNumber,
                    keys_, deltaLogs_, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileCache> deltaLog_file_cache =
      std::make_unique<DeltaLogFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, deltaLog_file_read_hist, nullptr /*IOTracer*/);

  DeltaLogSource deltaLog_source(&immutable_options, db_id_, db_session_id_,
                                 deltaLog_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(deltaLog_source.GetDeltaLogCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys_[i], kDeltaLogFileNumber, deltaLog_offsets[i],
          deltaLog_file_size_, deltaLog_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    // Since we resized each deltaLog to be kSizeDummyEntry / (num_deltaLogs /
    // 2), we can't fit all the deltaLogs in the cache at the same time, which
    // means we should observe cache evictions once we reach the cache's
    // capacity. Due to the overhead of the cache and the DeltaLogContents
    // objects, as well as jemalloc bin sizes, this happens after inserting
    // seven deltaLogs.
    uint64_t deltaLog_bytes = 0;
    for (size_t i = 0; i < kNumDeltaLogs; ++i) {
      ASSERT_OK(deltaLog_source.GetDeltaLog(
          read_options, keys_[i], kDeltaLogFileNumber, deltaLog_offsets[i],
          deltaLog_file_size_, deltaLog_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      // Release cache handle
      values[i].Reset();

      if (i < kNumDeltaLogs / 2 - 1) {
        size_t charge = 0;
        ASSERT_TRUE(deltaLog_source.TEST_DeltaLogInCache(
            kDeltaLogFileNumber, deltaLog_file_size_, deltaLog_offsets[i],
            &charge));

        deltaLog_bytes += charge;
      }

      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), deltaLog_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.deltaLog_cache->GetUsage());
    }
  }
}
#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
