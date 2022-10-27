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
#include "db/delta/delta_contents.h"
#include "db/delta/delta_file_cache.h"
#include "db/delta/delta_file_reader.h"
#include "db/delta/delta_log_format.h"
#include "db/delta/delta_log_writer.h"
#include "db/delta/delta_source.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test delta file with `num` deltas in it.
void WriteDeltaFile(const ImmutableOptions& immutable_options,
                    uint32_t column_family_id, bool has_ttl,
                    const ExpirationRange& expiration_range_header,
                    const ExpirationRange& expiration_range_footer,
                    uint64_t delta_file_number, const std::vector<Slice>& keys,
                    const std::vector<Slice>& deltas,
                    CompressionType compression,
                    std::vector<uint64_t>& delta_offsets,
                    std::vector<uint64_t>& delta_sizes) {
  assert(!immutable_options.cf_paths.empty());
  size_t num = keys.size();
  assert(num == deltas.size());
  assert(num == delta_offsets.size());
  assert(num == delta_sizes.size());

  const std::string delta_file_path =
      DeltaFileName(immutable_options.cf_paths.front().path, delta_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), delta_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), delta_file_path, FileOptions(),
                             immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;

  DeltaLogWriter delta_log_writer(std::move(file_writer),
                                  immutable_options.clock, statistics,
                                  delta_file_number, use_fsync, do_flush);

  DeltaLogHeader header(column_family_id, compression, has_ttl,
                        expiration_range_header);

  ASSERT_OK(delta_log_writer.WriteHeader(header));

  std::vector<std::string> compressed_deltas(num);
  std::vector<Slice> deltas_to_write(num);
  if (kNoCompression == compression) {
    for (size_t i = 0; i < num; ++i) {
      deltas_to_write[i] = deltas[i];
      delta_sizes[i] = deltas[i].size();
    }
  } else {
    CompressionOptions opts;
    CompressionContext context(compression);
    constexpr uint64_t sample_for_compression = 0;
    CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                         compression, sample_for_compression);

    constexpr uint32_t compression_format_version = 2;

    for (size_t i = 0; i < num; ++i) {
      ASSERT_TRUE(CompressData(deltas[i], info, compression_format_version,
                               &compressed_deltas[i]));
      deltas_to_write[i] = compressed_deltas[i];
      delta_sizes[i] = compressed_deltas[i].size();
    }
  }

  for (size_t i = 0; i < num; ++i) {
    uint64_t key_offset = 0;
    ASSERT_OK(delta_log_writer.AddRecord(keys[i], deltas_to_write[i],
                                         &key_offset, &delta_offsets[i]));
  }

  DeltaLogFooter footer;
  footer.delta_count = num;
  footer.expiration_range = expiration_range_footer;

  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(
      delta_log_writer.AppendFooter(footer, &checksum_method, &checksum_value));
}

}  // anonymous namespace

class DeltaSourceTest : public DBTestBase {
 protected:
 public:
  explicit DeltaSourceTest()
      : DBTestBase("delta_source_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_delta_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = 8 << 20;
    co.num_shard_bits = 2;
    co.metadata_charge_policy = kDontChargeCacheMetadata;
    co.high_pri_pool_ratio = 0.2;
    co.low_pri_pool_ratio = 0.2;
    options_.delta_cache = NewLRUCache(co);
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

TEST_F(DeltaSourceTest, GetDeltasFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "DeltaSourceTest_GetDeltasFromCache"), 0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr size_t num_deltas = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> delta_strs;

  for (size_t i = 0; i < num_deltas; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    delta_strs.push_back("delta" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltas;

  uint64_t file_size = DeltaLogHeader::kSize;
  for (size_t i = 0; i < num_deltas; ++i) {
    keys.push_back({key_strs[i]});
    deltas.push_back({delta_strs[i]});
    file_size +=
        DeltaLogRecord::kHeaderSize + keys[i].size() + deltas[i].size();
  }
  file_size += DeltaLogFooter::kSize;

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, keys, deltas,
                 kNoCompression, delta_offsets, delta_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Delta file cache

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, delta_file_read_hist, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // GetDelta
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t delta_bytes = 0;
    uint64_t total_bytes = 0;

    read_options.fill_cache = false;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));

      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], delta_file_number, delta_offsets[i], file_size,
          delta_sizes[i], kNoCompression, prefetch_buffer, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));
      total_bytes += bytes_read;
    }

    // Retrieved the delta cache num_deltas * 3 times via TEST_DeltaInCache,
    // GetDelta, and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, num_deltas);
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, total_bytes);
    ASSERT_GE((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);

    read_options.fill_cache = true;
    delta_bytes = 0;
    total_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));

      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], delta_file_number, delta_offsets[i], file_size,
          delta_sizes[i], kNoCompression, prefetch_buffer, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      delta_bytes += delta_sizes[i];
      total_bytes += bytes_read;
      ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, i);
      ASSERT_EQ((int)get_perf_context()->delta_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->delta_read_byte, total_bytes);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));

      ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->delta_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->delta_read_byte, total_bytes);
    }

    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, num_deltas);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, num_deltas);
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, total_bytes);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), num_deltas);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), num_deltas);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_bytes);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE),
              delta_bytes);

    read_options.fill_cache = true;
    total_bytes = 0;
    delta_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));

      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], delta_file_number, delta_offsets[i], file_size,
          delta_sizes[i], kNoCompression, prefetch_buffer, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));
      total_bytes += bytes_read;      // on-disk delta record size
      delta_bytes += delta_sizes[i];  // cached delta value size
    }

    // Retrieved the delta cache num_deltas * 3 times via TEST_DeltaInCache,
    // GetDelta, and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, num_deltas * 3);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), num_deltas * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);

    // Cache-only GetDelta
    read_options.read_tier = ReadTier::kBlockCacheTier;
    total_bytes = 0;
    delta_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));

      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], delta_file_number, delta_offsets[i], file_size,
          delta_sizes[i], kNoCompression, prefetch_buffer, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));
      total_bytes += bytes_read;
      delta_bytes += delta_sizes[i];
    }

    // Retrieved the delta cache num_deltas * 3 times via TEST_DeltaInCache,
    // GetDelta, and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, num_deltas * 3);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), num_deltas * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.delta_cache->EraseUnRefEntries();

  {
    // Cache-only GetDelta
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;

    read_options.read_tier = ReadTier::kBlockCacheTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));

      ASSERT_TRUE(delta_source
                      .GetDelta(read_options, keys[i], delta_file_number,
                                delta_offsets[i], file_size, delta_sizes[i],
                                kNoCompression, prefetch_buffer, &values[i],
                                &bytes_read)
                      .IsIncomplete());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));
    }

    // Retrieved the delta cache num_deltas * 3 times via TEST_DeltaInCache,
    // GetDelta, and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // GetDelta from non-existing file
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t file_number = 100;  // non-existing file

    read_options.read_tier = ReadTier::kReadAllTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                  delta_offsets[i]));

      ASSERT_TRUE(delta_source
                      .GetDelta(read_options, keys[i], file_number,
                                delta_offsets[i], file_size, delta_sizes[i],
                                kNoCompression, prefetch_buffer, &values[i],
                                &bytes_read)
                      .IsIOError());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                  delta_offsets[i]));
    }

    // Retrieved the delta cache num_deltas * 3 times via TEST_DeltaInCache,
    // GetDelta, and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 3);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(DeltaSourceTest, GetCompressedDeltas) {
  if (!Snappy_Supported()) {
    return;
  }

  const CompressionType compression = kSnappyCompression;

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "DeltaSourceTest_GetCompressedDeltas"), 0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr size_t num_deltas = 256;

  std::vector<std::string> key_strs;
  std::vector<std::string> delta_strs;

  for (size_t i = 0; i < num_deltas; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    delta_strs.push_back("delta" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltas;

  for (size_t i = 0; i < num_deltas; ++i) {
    keys.push_back({key_strs[i]});
    deltas.push_back({delta_strs[i]});
  }

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  constexpr size_t capacity = 1024;
  auto backing_cache = NewLRUCache(capacity);  // Delta file cache

  FileOptions file_options;
  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;
  std::vector<PinnableSlice> values(keys.size());

  {
    // Snappy Compression
    const uint64_t file_number = 1;

    read_options.read_tier = ReadTier::kReadAllTier;

    WriteDeltaFile(immutable_options, column_family_id, has_ttl,
                   expiration_range, expiration_range, file_number, keys,
                   deltas, compression, delta_offsets, delta_sizes);

    CacheHandleGuard<DeltaFileReader> delta_file_reader;
    ASSERT_OK(delta_source.GetDeltaFileReader(file_number, &delta_file_reader));
    ASSERT_NE(delta_file_reader.GetValue(), nullptr);

    const uint64_t file_size = delta_file_reader.GetValue()->GetFileSize();
    ASSERT_EQ(delta_file_reader.GetValue()->GetCompressionType(), compression);

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_NE(deltas[i].size() /*uncompressed size*/,
                delta_sizes[i] /*compressed size*/);
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                  delta_offsets[i]));
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], file_number, delta_offsets[i], file_size,
          delta_sizes[i], compression, nullptr /*prefetch_buffer*/, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i] /*uncompressed delta*/);
      ASSERT_NE(values[i].size(), delta_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                 delta_offsets[i]));
    }

    ASSERT_GE((int)get_perf_context()->delta_decompress_time, 0);

    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                 delta_offsets[i]));

      // Compressed delta size is passed in GetDelta
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], file_number, delta_offsets[i], file_size,
          delta_sizes[i], compression, nullptr /*prefetch_buffer*/, &values[i],
          &bytes_read));
      ASSERT_EQ(values[i], deltas[i] /*uncompressed delta*/);
      ASSERT_NE(values[i].size(), delta_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                 delta_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);
  }
}

TEST_F(DeltaSourceTest, MultiGetDeltasFromMultiFiles) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_,
                            "DeltaSourceTest_MultiGetDeltasFromMultiFiles"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_files = 2;
  constexpr size_t num_deltas = 32;

  std::vector<std::string> key_strs;
  std::vector<std::string> delta_strs;

  for (size_t i = 0; i < num_deltas; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    delta_strs.push_back("delta" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltas;

  uint64_t file_size = DeltaLogHeader::kSize;
  uint64_t delta_value_bytes = 0;
  for (size_t i = 0; i < num_deltas; ++i) {
    keys.push_back({key_strs[i]});
    deltas.push_back({delta_strs[i]});
    delta_value_bytes += deltas[i].size();
    file_size +=
        DeltaLogRecord::kHeaderSize + keys[i].size() + deltas[i].size();
  }
  file_size += DeltaLogFooter::kSize;
  const uint64_t delta_records_bytes =
      file_size - DeltaLogHeader::kSize - DeltaLogFooter::kSize;

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  {
    // Write key/delta pairs to multiple delta files.
    for (size_t i = 0; i < delta_files; ++i) {
      const uint64_t file_number = i + 1;
      WriteDeltaFile(immutable_options, column_family_id, has_ttl,
                     expiration_range, expiration_range, file_number, keys,
                     deltas, kNoCompression, delta_offsets, delta_sizes);
    }
  }

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Delta file cache

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, delta_file_read_hist, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;

  {
    // MultiGetDelta
    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;

    autovector<DeltaFileReadRequests> delta_reqs;
    std::array<autovector<DeltaReadRequest>, delta_files> delta_reqs_in_file;
    std::array<PinnableSlice, num_deltas * delta_files> value_buf;
    std::array<Status, num_deltas * delta_files> statuses_buf;

    for (size_t i = 0; i < delta_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltas; ++j) {
        delta_reqs_in_file[i].emplace_back(
            keys[j], delta_offsets[j], delta_sizes[j], kNoCompression,
            &value_buf[i * num_deltas + j], &statuses_buf[i * num_deltas + j]);
      }
      delta_reqs.emplace_back(file_number, file_size, delta_reqs_in_file[i]);
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    delta_source.MultiGetDelta(read_options, delta_reqs, &bytes_read);

    for (size_t i = 0; i < delta_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltas; ++j) {
        ASSERT_OK(statuses_buf[i * num_deltas + j]);
        ASSERT_EQ(value_buf[i * num_deltas + j], deltas[j]);
        ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                   delta_offsets[j]));
      }
    }

    // Retrieved all deltas from 2 delta files twice via MultiGetDelta and
    // TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count,
              num_deltas * delta_files);
    ASSERT_EQ((int)get_perf_context()->delta_read_count,
              num_deltas * delta_files);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte,
              delta_records_bytes * delta_files);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS),
              num_deltas * delta_files);  // MultiGetDelta
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT),
              num_deltas * delta_files);  // TEST_DeltaInCache
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD),
              num_deltas * delta_files);  // MultiGetDelta
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_value_bytes * delta_files);  // TEST_DeltaInCache
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE),
              delta_value_bytes * delta_files);  // MultiGetDelta

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    autovector<DeltaReadRequest> fake_delta_reqs_in_file;
    std::array<PinnableSlice, num_deltas> fake_value_buf;
    std::array<Status, num_deltas> fake_statuses_buf;

    const uint64_t fake_file_number = 100;
    for (size_t i = 0; i < num_deltas; ++i) {
      fake_delta_reqs_in_file.emplace_back(
          keys[i], delta_offsets[i], delta_sizes[i], kNoCompression,
          &fake_value_buf[i], &fake_statuses_buf[i]);
    }

    // Add a fake multi-get delta request.
    delta_reqs.emplace_back(fake_file_number, file_size,
                            fake_delta_reqs_in_file);

    delta_source.MultiGetDelta(read_options, delta_reqs, &bytes_read);

    // Check the real delta read requests.
    for (size_t i = 0; i < delta_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_deltas; ++j) {
        ASSERT_OK(statuses_buf[i * num_deltas + j]);
        ASSERT_EQ(value_buf[i * num_deltas + j], deltas[j]);
        ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                   delta_offsets[j]));
      }
    }

    // Check the fake delta request.
    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(fake_statuses_buf[i].IsIOError());
      ASSERT_TRUE(fake_value_buf[i].empty());
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(fake_file_number, file_size,
                                                  delta_offsets[i]));
    }

    // Retrieved all deltas from 3 delta files (including the fake one) twice
    // via MultiGetDelta and TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count,
              num_deltas * delta_files * 2);
    ASSERT_EQ((int)get_perf_context()->delta_read_count,
              0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte,
              0);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    // Fake delta requests: MultiGetDelta and TEST_DeltaInCache
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 2);
    // Real delta requests: MultiGetDelta and TEST_DeltaInCache
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT),
              num_deltas * delta_files * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    // Real delta requests: MultiGetDelta and TEST_DeltaInCache
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_value_bytes * delta_files * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(DeltaSourceTest, MultiGetDeltasFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "DeltaSourceTest_MultiGetDeltasFromCache"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr size_t num_deltas = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> delta_strs;

  for (size_t i = 0; i < num_deltas; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    delta_strs.push_back("delta" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> deltas;

  uint64_t file_size = DeltaLogHeader::kSize;
  for (size_t i = 0; i < num_deltas; ++i) {
    keys.push_back({key_strs[i]});
    deltas.push_back({delta_strs[i]});
    file_size +=
        DeltaLogRecord::kHeaderSize + keys[i].size() + deltas[i].size();
  }
  file_size += DeltaLogFooter::kSize;

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, keys, deltas,
                 kNoCompression, delta_offsets, delta_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Delta file cache

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, delta_file_read_hist, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // MultiGetDeltaFromOneFile
    uint64_t bytes_read = 0;
    std::array<Status, num_deltas> statuses_buf;
    std::array<PinnableSlice, num_deltas> value_buf;
    autovector<DeltaReadRequest> delta_reqs;

    for (size_t i = 0; i < num_deltas; i += 2) {  // even index
      delta_reqs.emplace_back(keys[i], delta_offsets[i], delta_sizes[i],
                              kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    // Get half of deltas
    delta_source.MultiGetDeltaFromOneFile(read_options, delta_file_number,
                                          file_size, delta_reqs, &bytes_read);

    uint64_t fs_read_bytes = 0;
    uint64_t ca_read_bytes = 0;
    for (size_t i = 0; i < num_deltas; ++i) {
      if (i % 2 == 0) {
        ASSERT_OK(statuses_buf[i]);
        ASSERT_EQ(value_buf[i], deltas[i]);
        ASSERT_TRUE(value_buf[i].IsPinned());
        fs_read_bytes +=
            delta_sizes[i] + keys[i].size() + DeltaLogRecord::kHeaderSize;
        ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                   delta_offsets[i]));
        ca_read_bytes += delta_sizes[i];
      } else {
        statuses_buf[i].PermitUncheckedError();
        ASSERT_TRUE(value_buf[i].empty());
        ASSERT_FALSE(value_buf[i].IsPinned());
        ASSERT_FALSE(delta_source.TEST_DeltaInCache(
            delta_file_number, file_size, delta_offsets[i]));
      }
    }

    constexpr int num_even_deltas = num_deltas / 2;
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, num_even_deltas);
    ASSERT_EQ((int)get_perf_context()->delta_read_count,
              num_even_deltas);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte,
              fs_read_bytes);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), num_even_deltas);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), num_even_deltas);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              ca_read_bytes);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE),
              ca_read_bytes);

    // Get the rest of deltas
    for (size_t i = 1; i < num_deltas; i += 2) {  // odd index
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));

      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[i], delta_file_number, delta_offsets[i], file_size,
          delta_sizes[i], kNoCompression, prefetch_buffer, &value_buf[i],
          &bytes_read));
      ASSERT_EQ(value_buf[i], deltas[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_EQ(bytes_read,
                DeltaLogRecord::kHeaderSize + keys[i].size() + delta_sizes[i]);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));
    }

    // Cache-only MultiGetDeltaFromOneFile
    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    delta_reqs.clear();
    for (size_t i = 0; i < num_deltas; ++i) {
      delta_reqs.emplace_back(keys[i], delta_offsets[i], delta_sizes[i],
                              kNoCompression, &value_buf[i], &statuses_buf[i]);
    }

    delta_source.MultiGetDeltaFromOneFile(read_options, delta_file_number,
                                          file_size, delta_reqs, &bytes_read);

    uint64_t delta_bytes = 0;
    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_OK(statuses_buf[i]);
      ASSERT_EQ(value_buf[i], deltas[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                 delta_offsets[i]));
      delta_bytes += delta_sizes[i];
    }

    // Retrieved the delta cache num_deltas * 2 times via GetDelta and
    // TEST_DeltaInCache.
    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, num_deltas * 2);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);   // blocking i/o
    ASSERT_GE((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), num_deltas * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ),
              delta_bytes * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.delta_cache->EraseUnRefEntries();

  {
    // Cache-only MultiGetDeltaFromOneFile
    uint64_t bytes_read = 0;
    read_options.read_tier = ReadTier::kBlockCacheTier;

    std::array<Status, num_deltas> statuses_buf;
    std::array<PinnableSlice, num_deltas> value_buf;
    autovector<DeltaReadRequest> delta_reqs;

    for (size_t i = 0; i < num_deltas; i++) {
      delta_reqs.emplace_back(keys[i], delta_offsets[i], delta_sizes[i],
                              kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    delta_source.MultiGetDeltaFromOneFile(read_options, delta_file_number,
                                          file_size, delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIncomplete());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(delta_file_number, file_size,
                                                  delta_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // MultiGetDeltaFromOneFile from non-existing file
    uint64_t bytes_read = 0;
    uint64_t non_existing_file_number = 100;
    read_options.read_tier = ReadTier::kReadAllTier;

    std::array<Status, num_deltas> statuses_buf;
    std::array<PinnableSlice, num_deltas> value_buf;
    autovector<DeltaReadRequest> delta_reqs;

    for (size_t i = 0; i < num_deltas; i++) {
      delta_reqs.emplace_back(keys[i], delta_offsets[i], delta_sizes[i],
                              kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(non_existing_file_number,
                                                  file_size, delta_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    delta_source.MultiGetDeltaFromOneFile(read_options,
                                          non_existing_file_number, file_size,
                                          delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIOError());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(non_existing_file_number,
                                                  file_size, delta_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->delta_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->delta_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->delta_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->delta_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_MISS), num_deltas * 2);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(DELTA_DB_CACHE_BYTES_WRITE), 0);
  }
}

class DeltaSecondaryCacheTest : public DBTestBase {
 protected:
 public:
  explicit DeltaSecondaryCacheTest()
      : DBTestBase("delta_secondary_cache_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_delta_files = true;
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

    // Read deltas from the secondary cache if they are not in the primary cache
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

TEST_F(DeltaSecondaryCacheTest, GetDeltasFromSecondaryCache) {
  if (!Snappy_Supported()) {
    return;
  }

  secondary_cache_opts_.compression_type = kSnappyCompression;
  lru_cache_opts_.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts_);
  options_.delta_cache = NewLRUCache(lru_cache_opts_);

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "DeltaSecondaryCacheTest_GetDeltasFromSecondaryCache"),
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
  std::vector<std::string> delta_strs{rnd.RandomString(512),
                                      rnd.RandomString(768)};

  std::vector<Slice> keys{key_strs[0], key_strs[1]};
  std::vector<Slice> deltas{delta_strs[0], delta_strs[1]};

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, file_number, keys, deltas, kNoCompression,
                 delta_offsets, delta_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache(new DeltaFileCache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      delta_file_read_hist, nullptr /*IOTracer*/));

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  CacheHandleGuard<DeltaFileReader> file_reader;
  ASSERT_OK(delta_source.GetDeltaFileReader(file_number, &file_reader));
  ASSERT_NE(file_reader.GetValue(), nullptr);
  const uint64_t file_size = file_reader.GetValue()->GetFileSize();
  ASSERT_EQ(file_reader.GetValue()->GetCompressionType(), kNoCompression);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  auto delta_cache = options_.delta_cache;
  auto secondary_cache = lru_cache_opts_.secondary_cache;

  Cache::CreateCallback create_cb = [](const void* buf, size_t size,
                                       void** out_obj,
                                       size_t* charge) -> Status {
    CacheAllocationPtr allocation(new char[size]);

    return DeltaContents::CreateCallback(std::move(allocation), buf, size,
                                         out_obj, charge);
  };

  {
    // GetDelta
    std::vector<PinnableSlice> values(keys.size());

    read_options.fill_cache = true;
    get_perf_context()->Reset();

    // key0 should be filled to the primary cache from the delta file.
    ASSERT_OK(delta_source.GetDelta(
        read_options, keys[0], file_number, delta_offsets[0], file_size,
        delta_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
        &values[0], nullptr /* bytes_read */));
    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and key0's dummy item is inserted into secondary
    // cache. key1 should be filled to the primary cache from the delta file.
    ASSERT_OK(delta_source.GetDelta(
        read_options, keys[1], file_number, delta_offsets[1], file_size,
        delta_sizes[1], kNoCompression, nullptr /* prefetch_buffer */,
        &values[1], nullptr /* bytes_read */));

    // Release cache handle
    values[1].Reset();

    // key0 should be filled to the primary cache from the delta file. key1
    // should be evicted and key1's dummy item is inserted into secondary cache.
    ASSERT_OK(delta_source.GetDelta(
        read_options, keys[0], file_number, delta_offsets[0], file_size,
        delta_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
        &values[0], nullptr /* bytes_read */));
    ASSERT_EQ(values[0], deltas[0]);
    ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                               delta_offsets[0]));

    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and is inserted into secondary cache.
    // key1 should be filled to the primary cache from the delta file.
    ASSERT_OK(delta_source.GetDelta(
        read_options, keys[1], file_number, delta_offsets[1], file_size,
        delta_sizes[1], kNoCompression, nullptr /* prefetch_buffer */,
        &values[1], nullptr /* bytes_read */));
    ASSERT_EQ(values[1], deltas[1]);
    ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                               delta_offsets[1]));

    // Release cache handle
    values[1].Reset();

    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

    // delta_cache here only looks at the primary cache since we didn't provide
    // the cache item helper for the secondary cache. However, since key0 is
    // demoted to the secondary cache, we shouldn't be able to find it in the
    // primary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(delta_offsets[0]);
      const Slice key0 = cache_key.AsSlice();
      auto handle0 = delta_cache->Lookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key0's item should be in the secondary cache.
      bool is_in_sec_cache = false;
      auto sec_handle0 =
          secondary_cache->Lookup(key0, create_cb, true,
                                  /*advise_erase=*/true, is_in_sec_cache);
      ASSERT_FALSE(is_in_sec_cache);
      ASSERT_NE(sec_handle0, nullptr);
      ASSERT_TRUE(sec_handle0->IsReady());
      auto value = static_cast<DeltaContents*>(sec_handle0->Value());
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), deltas[0]);
      delete value;

      // key0 doesn't exist in the delta cache although key0's dummy
      // item exist in the secondary cache.
      ASSERT_FALSE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                  delta_offsets[0]));
    }

    // key1 should exists in the primary cache. key1's dummy item exists
    // in the secondary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(delta_offsets[1]);
      const Slice key1 = cache_key.AsSlice();
      auto handle1 = delta_cache->Lookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      delta_cache->Release(handle1);

      bool is_in_sec_cache = false;
      auto sec_handle1 =
          secondary_cache->Lookup(key1, create_cb, true,
                                  /*advise_erase=*/true, is_in_sec_cache);
      ASSERT_FALSE(is_in_sec_cache);
      ASSERT_EQ(sec_handle1, nullptr);

      ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                 delta_offsets[1]));
    }

    {
      // fetch key0 from the delta file to the primary cache.
      // key1 is evicted and inserted into the secondary cache.
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys[0], file_number, delta_offsets[0], file_size,
          delta_sizes[0], kNoCompression, nullptr /* prefetch_buffer */,
          &values[0], nullptr /* bytes_read */));
      ASSERT_EQ(values[0], deltas[0]);

      // Release cache handle
      values[0].Reset();

      // key0 should be in the primary cache.
      CacheKey cache_key0 = base_cache_key.WithOffset(delta_offsets[0]);
      const Slice key0 = cache_key0.AsSlice();
      auto handle0 = delta_cache->Lookup(key0, statistics);
      ASSERT_NE(handle0, nullptr);
      auto value = static_cast<DeltaContents*>(delta_cache->Value(handle0));
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), deltas[0]);
      delta_cache->Release(handle0);

      // key1 is not in the primary cache and is in the secondary cache.
      CacheKey cache_key1 = base_cache_key.WithOffset(delta_offsets[1]);
      const Slice key1 = cache_key1.AsSlice();
      auto handle1 = delta_cache->Lookup(key1, statistics);
      ASSERT_EQ(handle1, nullptr);

      // erase key0 from the primary cache.
      delta_cache->Erase(key0);
      handle0 = delta_cache->Lookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key1 promotion should succeed due to the primary cache being empty. we
      // did't call secondary cache's Lookup() here, because it will remove the
      // key but it won't be able to promote the key to the primary cache.
      // Instead we use the end-to-end delta source API to read key1.
      // In function TEST_DeltaInCache, key1's dummy item is inserted into the
      // primary cache and a standalone handle is checked by GetValue().
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(file_number, file_size,
                                                 delta_offsets[1]));

      // key1's dummy handle is in the primary cache and key1's item is still
      // in the secondary cache. So, the primary cache's Lookup() can only
      // get a dummy handle.
      handle1 = delta_cache->Lookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      // handl1 is a dummy handle.
      ASSERT_EQ(delta_cache->Value(handle1), nullptr);
      delta_cache->Release(handle1);
    }
  }
}

class DeltaSourceCacheReservationTest : public DBTestBase {
 public:
  explicit DeltaSourceCacheReservationTest()
      : DBTestBase("delta_source_cache_reservation_test",
                   /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_delta_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = kCacheCapacity;
    co.num_shard_bits = kNumShardBits;
    co.metadata_charge_policy = kDontChargeCacheMetadata;

    co.high_pri_pool_ratio = 0.0;
    co.low_pri_pool_ratio = 0.0;
    std::shared_ptr<Cache> delta_cache = NewLRUCache(co);

    co.high_pri_pool_ratio = 0.5;
    co.low_pri_pool_ratio = 0.5;
    std::shared_ptr<Cache> block_cache = NewLRUCache(co);

    options_.delta_cache = delta_cache;
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    BlockBasedTableOptions block_based_options;
    block_based_options.no_block_cache = false;
    block_based_options.block_cache = block_cache;
    block_based_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kDeltaCache,
         {/* charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
    options_.table_factory.reset(
        NewBlockBasedTableFactory(block_based_options));

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  void GenerateKeysAndDeltas() {
    for (size_t i = 0; i < kNumDeltas; ++i) {
      key_strs_.push_back("key" + std::to_string(i));
      delta_strs_.push_back("delta" + std::to_string(i));
    }

    delta_file_size_ = DeltaLogHeader::kSize;
    for (size_t i = 0; i < kNumDeltas; ++i) {
      keys_.push_back({key_strs_[i]});
      deltas_.push_back({delta_strs_[i]});
      delta_file_size_ +=
          DeltaLogRecord::kHeaderSize + keys_[i].size() + deltas_[i].size();
    }
    delta_file_size_ += DeltaLogFooter::kSize;
  }

  static constexpr std::size_t kSizeDummyEntry = CacheReservationManagerImpl<
      CacheEntryRole::kDeltaCache>::GetDummyEntrySize();
  static constexpr std::size_t kCacheCapacity = 1 * kSizeDummyEntry;
  static constexpr int kNumShardBits = 0;  // 2^0 shard

  static constexpr uint32_t kColumnFamilyId = 1;
  static constexpr bool kHasTTL = false;
  static constexpr uint64_t kDeltaFileNumber = 1;
  static constexpr size_t kNumDeltas = 16;

  std::vector<Slice> keys_;
  std::vector<Slice> deltas_;
  std::vector<std::string> key_strs_;
  std::vector<std::string> delta_strs_;
  uint64_t delta_file_size_;

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

#ifndef ROCKSDB_LITE
TEST_F(DeltaSourceCacheReservationTest, SimpleCacheReservation) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "DeltaSourceCacheReservationTest_SimpleCacheReservation"),
      0);

  GenerateKeysAndDeltas();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);

  constexpr ExpirationRange expiration_range;

  std::vector<uint64_t> delta_offsets(keys_.size());
  std::vector<uint64_t> delta_sizes(keys_.size());

  WriteDeltaFile(immutable_options, kColumnFamilyId, kHasTTL, expiration_range,
                 expiration_range, kDeltaFileNumber, keys_, deltas_,
                 kNoCompression, delta_offsets, delta_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, delta_file_read_hist, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(delta_source.GetDeltaCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumDeltas; ++i) {
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys_[i], kDeltaFileNumber, delta_offsets[i],
          delta_file_size_, delta_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    // num_deltas is 16, so the total delta cache usage is less than a single
    // dummy entry. Therefore, cache reservation manager only reserves one dummy
    // entry here.
    uint64_t delta_bytes = 0;
    for (size_t i = 0; i < kNumDeltas; ++i) {
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys_[i], kDeltaFileNumber, delta_offsets[i],
          delta_file_size_, delta_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      size_t charge = 0;
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(
          kDeltaFileNumber, delta_file_size_, delta_offsets[i], &charge));

      delta_bytes += charge;
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), delta_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.delta_cache->GetUsage());
    }
  }

  {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, kDeltaFileNumber);
    size_t delta_bytes = options_.delta_cache->GetUsage();

    for (size_t i = 0; i < kNumDeltas; ++i) {
      size_t charge = 0;
      ASSERT_TRUE(delta_source.TEST_DeltaInCache(
          kDeltaFileNumber, delta_file_size_, delta_offsets[i], &charge));

      CacheKey cache_key = base_cache_key.WithOffset(delta_offsets[i]);
      // We didn't call options_.delta_cache->Erase() here, this is because
      // the cache wrapper's Erase() method must be called to update the
      // cache usage after erasing the cache entry.
      delta_source.GetDeltaCache()->Erase(cache_key.AsSlice());
      if (i == kNumDeltas - 1) {
        // All the deltas got removed from the cache. cache_res_mgr should not
        // reserve any space for them.
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      } else {
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      }
      delta_bytes -= charge;
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), delta_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.delta_cache->GetUsage());
    }
  }
}

TEST_F(DeltaSourceCacheReservationTest, IncreaseCacheReservationOnFullCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_,
                            "DeltaSourceCacheReservationTest_"
                            "IncreaseCacheReservationOnFullCache"),
      0);

  GenerateKeysAndDeltas();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  constexpr size_t delta_size = kSizeDummyEntry / (kNumDeltas / 2);
  for (size_t i = 0; i < kNumDeltas; ++i) {
    delta_file_size_ -= deltas_[i].size();  // old delta size
    delta_strs_[i].resize(delta_size, '@');
    deltas_[i] = Slice(delta_strs_[i]);
    delta_file_size_ += deltas_[i].size();  // new delta size
  }

  std::vector<uint64_t> delta_offsets(keys_.size());
  std::vector<uint64_t> delta_sizes(keys_.size());

  constexpr ExpirationRange expiration_range;
  WriteDeltaFile(immutable_options, kColumnFamilyId, kHasTTL, expiration_range,
                 expiration_range, kDeltaFileNumber, keys_, deltas_,
                 kNoCompression, delta_offsets, delta_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileCache> delta_file_cache =
      std::make_unique<DeltaFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, delta_file_read_hist, nullptr /*IOTracer*/);

  DeltaSource delta_source(&immutable_options, db_id_, db_session_id_,
                           delta_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(delta_source.GetDeltaCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumDeltas; ++i) {
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys_[i], kDeltaFileNumber, delta_offsets[i],
          delta_file_size_, delta_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    // Since we resized each delta to be kSizeDummyEntry / (num_deltas / 2), we
    // can't fit all the deltas in the cache at the same time, which means we
    // should observe cache evictions once we reach the cache's capacity.
    // Due to the overhead of the cache and the DeltaContents objects, as well
    // as jemalloc bin sizes, this happens after inserting seven deltas.
    uint64_t delta_bytes = 0;
    for (size_t i = 0; i < kNumDeltas; ++i) {
      ASSERT_OK(delta_source.GetDelta(
          read_options, keys_[i], kDeltaFileNumber, delta_offsets[i],
          delta_file_size_, delta_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      // Release cache handle
      values[i].Reset();

      if (i < kNumDeltas / 2 - 1) {
        size_t charge = 0;
        ASSERT_TRUE(delta_source.TEST_DeltaInCache(
            kDeltaFileNumber, delta_file_size_, delta_offsets[i], &charge));

        delta_bytes += charge;
      }

      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), delta_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.delta_cache->GetUsage());
    }
  }
}
#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
