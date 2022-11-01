//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <string>

#include "db/deltaLog/deltaLog_file_cache.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/deltaLog_log_writer.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test deltaLog file with a single deltaLog in it.
void WriteDeltaLogFile(uint32_t column_family_id,
                       const ImmutableOptions& immutable_options,
                       uint64_t deltaLog_file_number) {
  assert(!immutable_options.cf_paths.empty());

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

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  DeltaLogLogHeader header(column_family_id, kNoCompression, has_ttl,
                           expiration_range);

  ASSERT_OK(deltaLog_log_writer.WriteHeader(header));

  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  std::string compressed_deltaLog;

  uint64_t key_offset = 0;
  uint64_t deltaLog_offset = 0;

  ASSERT_OK(deltaLog_log_writer.AddRecord(key, deltaLog, &key_offset,
                                          &deltaLog_offset));

  DeltaLogLogFooter footer;
  footer.deltaLog_count = 1;
  footer.expiration_range = expiration_range;

  std::string checksum_method;
  std::string checksum_value;

  ASSERT_OK(deltaLog_log_writer.AppendFooter(footer, &checksum_method,
                                             &checksum_value));
}

}  // anonymous namespace

class DeltaLogFileCacheTest : public testing::Test {
 protected:
  DeltaLogFileCacheTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }

  std::unique_ptr<Env> mock_env_;
};

TEST_F(DeltaLogFileCacheTest, GetDeltaLogFileReader) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileCacheTest_GetDeltaLogFileReader"),
      0);
  options.enable_deltaLog_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t deltaLog_file_number = 123;

  WriteDeltaLogFile(column_family_id, immutable_options, deltaLog_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  DeltaLogFileCache deltaLog_file_cache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      deltaLog_file_read_hist, nullptr /*IOTracer*/);

  // First try: reader should be opened and put in cache
  CacheHandleGuard<DeltaLogFileReader> first;

  ASSERT_OK(
      deltaLog_file_cache.GetDeltaLogFileReader(deltaLog_file_number, &first));
  ASSERT_NE(first.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  // Second try: reader should be served from cache
  CacheHandleGuard<DeltaLogFileReader> second;

  ASSERT_OK(
      deltaLog_file_cache.GetDeltaLogFileReader(deltaLog_file_number, &second));
  ASSERT_NE(second.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  ASSERT_EQ(first.GetValue(), second.GetValue());
}

TEST_F(DeltaLogFileCacheTest, GetDeltaLogFileReader_Race) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileCacheTest_GetDeltaLogFileReader_Race"),
      0);
  options.enable_deltaLog_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t deltaLog_file_number = 123;

  WriteDeltaLogFile(column_family_id, immutable_options, deltaLog_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  DeltaLogFileCache deltaLog_file_cache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      deltaLog_file_read_hist, nullptr /*IOTracer*/);

  CacheHandleGuard<DeltaLogFileReader> first;
  CacheHandleGuard<DeltaLogFileReader> second;

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogFileCache::GetDeltaLogFileReader:DoubleCheck",
      [&](void* /* arg */) {
        // Disabling sync points to prevent infinite recursion
        SyncPoint::GetInstance()->DisableProcessing();

        ASSERT_OK(deltaLog_file_cache.GetDeltaLogFileReader(
            deltaLog_file_number, &second));
        ASSERT_NE(second.GetValue(), nullptr);
        ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
        ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(
      deltaLog_file_cache.GetDeltaLogFileReader(deltaLog_file_number, &first));
  ASSERT_NE(first.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  ASSERT_EQ(first.GetValue(), second.GetValue());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeltaLogFileCacheTest, GetDeltaLogFileReader_IOError) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "DeltaLogFileCacheTest_GetDeltaLogFileReader_IOError"),
      0);
  options.enable_deltaLog_files = true;

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  ImmutableOptions immutable_options(options);
  FileOptions file_options;
  constexpr uint32_t column_family_id = 1;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  DeltaLogFileCache deltaLog_file_cache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      deltaLog_file_read_hist, nullptr /*IOTracer*/);

  // Note: there is no deltaLog file with the below number
  constexpr uint64_t deltaLog_file_number = 123;

  CacheHandleGuard<DeltaLogFileReader> reader;

  ASSERT_TRUE(
      deltaLog_file_cache.GetDeltaLogFileReader(deltaLog_file_number, &reader)
          .IsIOError());
  ASSERT_EQ(reader.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);
}

TEST_F(DeltaLogFileCacheTest, GetDeltaLogFileReader_CacheFull) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "DeltaLogFileCacheTest_GetDeltaLogFileReader_CacheFull"),
      0);
  options.enable_deltaLog_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t deltaLog_file_number = 123;

  WriteDeltaLogFile(column_family_id, immutable_options, deltaLog_file_number);

  constexpr size_t capacity = 0;
  constexpr int num_shard_bits = -1;  // determined automatically
  constexpr bool strict_capacity_limit = true;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity, num_shard_bits, strict_capacity_limit);

  FileOptions file_options;
  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  DeltaLogFileCache deltaLog_file_cache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      deltaLog_file_read_hist, nullptr /*IOTracer*/);

  // Insert into cache should fail since it has zero capacity and
  // strict_capacity_limit is set
  CacheHandleGuard<DeltaLogFileReader> reader;

  ASSERT_TRUE(
      deltaLog_file_cache.GetDeltaLogFileReader(deltaLog_file_number, &reader)
          .IsMemoryLimit());
  ASSERT_EQ(reader.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
