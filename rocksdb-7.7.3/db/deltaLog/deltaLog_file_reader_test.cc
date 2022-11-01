//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <string>

#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_file_reader.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/deltaLog_log_writer.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

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

// Creates a test deltaLog file with a single deltaLog in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various deltaLog file header/footer fields.
void WriteDeltaLogFile(const ImmutableOptions& immutable_options,
                       uint32_t column_family_id, bool has_ttl,
                       const ExpirationRange& expiration_range_header,
                       const ExpirationRange& expiration_range_footer,
                       uint64_t deltaLog_file_number, const Slice& key,
                       const Slice& deltaLog, CompressionType compression,
                       uint64_t* deltaLog_offset, uint64_t* deltaLog_size) {
  std::vector<Slice> keys{key};
  std::vector<Slice> deltaLogs{deltaLog};
  std::vector<uint64_t> deltaLog_offsets{0};
  std::vector<uint64_t> deltaLog_sizes{0};
  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range_header, expiration_range_footer,
                    deltaLog_file_number, keys, deltaLogs, compression,
                    deltaLog_offsets, deltaLog_sizes);
  if (deltaLog_offset) {
    *deltaLog_offset = deltaLog_offsets[0];
  }
  if (deltaLog_size) {
    *deltaLog_size = deltaLog_sizes[0];
  }
}

}  // anonymous namespace

class DeltaLogFileReaderTest : public testing::Test {
 protected:
  DeltaLogFileReaderTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }
  std::unique_ptr<Env> mock_env_;
};

TEST_F(DeltaLogFileReaderTest, CreateReaderAndGetDeltaLog) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(), "DeltaLogFileReaderTest_CreateReaderAndGetDeltaLog"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr size_t num_deltaLogs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> deltaLog_strs = {"deltaLog1", "deltaLog2",
                                                  "deltaLog3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> deltaLogs = {deltaLog_strs[0], deltaLog_strs[1],
                                        deltaLog_strs[2]};

  std::vector<uint64_t> deltaLog_offsets(keys.size());
  std::vector<uint64_t> deltaLog_sizes(keys.size());

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    keys, deltaLogs, kNoCompression, deltaLog_offsets,
                    deltaLog_sizes);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_OK(DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader));

  // Make sure the deltaLog can be retrieved with and without checksum
  // verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDeltaLog(
        read_options, keys[0], deltaLog_offsets[0], deltaLog_sizes[0],
        kNoCompression, prefetch_buffer, allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltaLogs[0]);
    ASSERT_EQ(bytes_read, deltaLog_sizes[0]);

    // MultiGetDeltaLog
    bytes_read = 0;
    size_t total_size = 0;

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<DeltaLogReadRequest, num_deltaLogs> requests_buf;
    autovector<
        std::pair<DeltaLogReadRequest*, std::unique_ptr<DeltaLogContents>>>
        deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      requests_buf[i] =
          DeltaLogReadRequest(keys[i], deltaLog_offsets[i], deltaLog_sizes[i],
                              kNoCompression, nullptr, &statuses_buf[i]);
      deltaLog_reqs.emplace_back(&requests_buf[i],
                                 std::unique_ptr<DeltaLogContents>());
    }

    reader->MultiGetDeltaLog(read_options, allocator, deltaLog_reqs,
                             &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      const auto& result = deltaLog_reqs[i].second;

      ASSERT_OK(statuses_buf[i]);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->data(), deltaLogs[i]);
      total_size += deltaLog_sizes[i];
    }
    ASSERT_EQ(bytes_read, total_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDeltaLog(
        read_options, keys[1], deltaLog_offsets[1], deltaLog_sizes[1],
        kNoCompression, prefetch_buffer, allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltaLogs[1]);

    const uint64_t key_size = keys[1].size();
    ASSERT_EQ(bytes_read,
              DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  deltaLog_sizes[1]);
  }

  // Invalid offset (too close to start of file)
  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(read_options, keys[0],
                                  deltaLog_offsets[0] - 1, deltaLog_sizes[0],
                                  kNoCompression, prefetch_buffer, allocator,
                                  &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(read_options, keys[2],
                                  deltaLog_offsets[2] + 1, deltaLog_sizes[2],
                                  kNoCompression, prefetch_buffer, allocator,
                                  &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(read_options, keys[0], deltaLog_offsets[0],
                                  deltaLog_sizes[0], kZSTD, prefetch_buffer,
                                  allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(
        reader
            ->GetDeltaLog(read_options, shorter_key,
                          deltaLog_offsets[0] -
                              (keys[0].size() - sizeof(shorter_key) + 1),
                          deltaLog_sizes[0], kNoCompression, prefetch_buffer,
                          allocator, &value, &bytes_read)
            .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDeltaLog
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice shorter_key_slice(shorter_key, sizeof(shorter_key) - 1);
    key_refs[1] = std::cref(shorter_key_slice);

    autovector<uint64_t> offsets{
        deltaLog_offsets[0],
        deltaLog_offsets[1] - (keys[1].size() - key_refs[1].get().size()),
        deltaLog_offsets[2]};

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<DeltaLogReadRequest, num_deltaLogs> requests_buf;
    autovector<
        std::pair<DeltaLogReadRequest*, std::unique_ptr<DeltaLogContents>>>
        deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      requests_buf[i] =
          DeltaLogReadRequest(key_refs[i], offsets[i], deltaLog_sizes[i],
                              kNoCompression, nullptr, &statuses_buf[i]);
      deltaLog_reqs.emplace_back(&requests_buf[i],
                                 std::unique_ptr<DeltaLogContents>());
    }

    reader->MultiGetDeltaLog(read_options, allocator, deltaLog_reqs,
                             &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (i == 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo1";
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(read_options, incorrect_key,
                                  deltaLog_offsets[0], deltaLog_sizes[0],
                                  kNoCompression, prefetch_buffer, allocator,
                                  &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDeltaLog
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice wrong_key_slice(incorrect_key, sizeof(incorrect_key) - 1);
    key_refs[2] = std::cref(wrong_key_slice);

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<DeltaLogReadRequest, num_deltaLogs> requests_buf;
    autovector<
        std::pair<DeltaLogReadRequest*, std::unique_ptr<DeltaLogContents>>>
        deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      requests_buf[i] = DeltaLogReadRequest(key_refs[i], deltaLog_offsets[i],
                                            deltaLog_sizes[i], kNoCompression,
                                            nullptr, &statuses_buf[i]);
      deltaLog_reqs.emplace_back(&requests_buf[i],
                                 std::unique_ptr<DeltaLogContents>());
    }

    reader->MultiGetDeltaLog(read_options, allocator, deltaLog_reqs,
                             &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (i == num_deltaLogs - 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect value size
  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(read_options, keys[1], deltaLog_offsets[1],
                                  deltaLog_sizes[1] + 1, kNoCompression,
                                  prefetch_buffer, allocator, &value,
                                  &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDeltaLog
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }

    std::array<Status, num_deltaLogs> statuses_buf;
    std::array<DeltaLogReadRequest, num_deltaLogs> requests_buf;

    requests_buf[0] =
        DeltaLogReadRequest(key_refs[0], deltaLog_offsets[0], deltaLog_sizes[0],
                            kNoCompression, nullptr, &statuses_buf[0]);
    requests_buf[1] = DeltaLogReadRequest(key_refs[1], deltaLog_offsets[1],
                                          deltaLog_sizes[1] + 1, kNoCompression,
                                          nullptr, &statuses_buf[1]);
    requests_buf[2] =
        DeltaLogReadRequest(key_refs[2], deltaLog_offsets[2], deltaLog_sizes[2],
                            kNoCompression, nullptr, &statuses_buf[2]);

    autovector<
        std::pair<DeltaLogReadRequest*, std::unique_ptr<DeltaLogContents>>>
        deltaLog_reqs;

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      deltaLog_reqs.emplace_back(&requests_buf[i],
                                 std::unique_ptr<DeltaLogContents>());
    }

    reader->MultiGetDeltaLog(read_options, allocator, deltaLog_reqs,
                             &bytes_read);

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (i != 1) {
        ASSERT_OK(statuses_buf[i]);
      } else {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      }
    }
  }
}

TEST_F(DeltaLogFileReaderTest, Malformed) {
  // Write a deltaLog file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_Malformed"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t deltaLog_file_number = 1;

  {
    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;

    const std::string deltaLog_file_path = DeltaLogFileName(
        immutable_options.cf_paths.front().path, deltaLog_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(NewWritableFile(immutable_options.fs.get(), deltaLog_file_path,
                              &file, FileOptions()));

    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), deltaLog_file_path,
                               FileOptions(), immutable_options.clock));

    constexpr Statistics* statistics = nullptr;
    constexpr bool use_fsync = false;
    constexpr bool do_flush = false;

    DeltaLogLogWriter deltaLog_log_writer(
        std::move(file_writer), immutable_options.clock, statistics,
        deltaLog_file_number, use_fsync, do_flush);

    DeltaLogLogHeader header(column_family_id, kNoCompression, has_ttl,
                             expiration_range);

    ASSERT_OK(deltaLog_log_writer.WriteHeader(header));
  }

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_TRUE(DeltaLogFileReader::Create(
                  immutable_options, FileOptions(), column_family_id,
                  deltaLog_file_read_hist, deltaLog_file_number,
                  nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaLogFileReaderTest, TTL) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "DeltaLogFileReaderTest_TTL"), 0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kNoCompression, &deltaLog_offset,
                    &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_TRUE(DeltaLogFileReader::Create(
                  immutable_options, FileOptions(), column_family_id,
                  deltaLog_file_read_hist, deltaLog_file_number,
                  nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaLogFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_ExpirationRangeInHeader"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  const ExpirationRange expiration_range_header(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr ExpirationRange expiration_range_footer;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range_header, expiration_range_footer,
                    deltaLog_file_number, key, deltaLog, kNoCompression,
                    &deltaLog_offset, &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_TRUE(DeltaLogFileReader::Create(
                  immutable_options, FileOptions(), column_family_id,
                  deltaLog_file_read_hist, deltaLog_file_number,
                  nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaLogFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_ExpirationRangeInFooter"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  const ExpirationRange expiration_range_footer(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range_header, expiration_range_footer,
                    deltaLog_file_number, key, deltaLog, kNoCompression,
                    &deltaLog_offset, &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_TRUE(DeltaLogFileReader::Create(
                  immutable_options, FileOptions(), column_family_id,
                  deltaLog_file_read_hist, deltaLog_file_number,
                  nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaLogFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_IncorrectColumnFamily"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kNoCompression, &deltaLog_offset,
                    &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;

  ASSERT_TRUE(DeltaLogFileReader::Create(
                  immutable_options, FileOptions(), incorrect_column_family_id,
                  deltaLog_file_read_hist, deltaLog_file_number,
                  nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaLogFileReaderTest, DeltaLogCRCError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_DeltaLogCRCError"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kNoCompression, &deltaLog_offset,
                    &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_OK(DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogFileReader::VerifyDeltaLog:CheckDeltaLogCRC", [](void* arg) {
        DeltaLogLogRecord* const record = static_cast<DeltaLogLogRecord*>(arg);
        assert(record);

        record->deltaLog_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<DeltaLogContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetDeltaLog(ReadOptions(), key, deltaLog_offset,
                                deltaLog_size, kNoCompression, prefetch_buffer,
                                allocator, &value, &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeltaLogFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_Compression"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kSnappyCompression, &deltaLog_offset,
                    &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_OK(DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader));

  // Make sure the deltaLog can be retrieved with and without checksum
  // verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDeltaLog(
        read_options, key, deltaLog_offset, deltaLog_size, kSnappyCompression,
        prefetch_buffer, allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltaLog);
    ASSERT_EQ(bytes_read, deltaLog_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDeltaLog(
        read_options, key, deltaLog_offset, deltaLog_size, kSnappyCompression,
        prefetch_buffer, allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltaLog);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  deltaLog_size);
  }
}

TEST_F(DeltaLogFileReaderTest, UncompressionError) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileReaderTest_UncompressionError"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kSnappyCompression, &deltaLog_offset,
                    &deltaLog_size);

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  ASSERT_OK(DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogFileReader::UncompressDeltaLogIfNeeded:TamperWithResult",
      [](void* arg) {
        CacheAllocationPtr* const output =
            static_cast<CacheAllocationPtr*>(arg);
        assert(output);

        output->reset();
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<DeltaLogContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetDeltaLog(ReadOptions(), key, deltaLog_offset,
                                deltaLog_size, kSnappyCompression,
                                prefetch_buffer, allocator, &value, &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DeltaLogFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  DeltaLogFileReaderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fault_injection_env_.reset(new FaultInjectionTestEnv(mock_env_.get()));
  }

  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DeltaLogFileReaderTest, DeltaLogFileReaderIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "DeltaLogFileReader::OpenFile:GetFileSize",
                            "DeltaLogFileReader::OpenFile:NewRandomAccessFile",
                            "DeltaLogFileReader::ReadHeader:ReadFromFile",
                            "DeltaLogFileReader::ReadFooter:ReadFromFile",
                            "DeltaLogFileReader::GetDeltaLog:ReadFromFile"}));

TEST_P(DeltaLogFileReaderIOErrorTest, IOError) {
  // Simulates an I/O error during the specified step

  Options options;
  options.env = fault_injection_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(fault_injection_env_.get(),
                            "DeltaLogFileReaderIOErrorTest_IOError"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kNoCompression, &deltaLog_offset,
                    &deltaLog_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  const Status s = DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader);

  const bool fail_during_create =
      (sync_point_ != "DeltaLogFileReader::GetDeltaLog:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(ReadOptions(), key, deltaLog_offset,
                                  deltaLog_size, kNoCompression,
                                  prefetch_buffer, allocator, &value,
                                  &bytes_read)
                    .IsIOError());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DeltaLogFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  DeltaLogFileReaderDecodingErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
  }

  std::unique_ptr<Env> mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(
    DeltaLogFileReaderTest, DeltaLogFileReaderDecodingErrorTest,
    ::testing::ValuesIn(std::vector<std::string>{
        "DeltaLogFileReader::ReadHeader:TamperWithResult",
        "DeltaLogFileReader::ReadFooter:TamperWithResult",
        "DeltaLogFileReader::GetDeltaLog:TamperWithResult"}));

TEST_P(DeltaLogFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(), "DeltaLogFileReaderDecodingErrorTest_DecodingError"),
      0);
  options.enable_deltaLog_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t deltaLog_file_number = 1;
  constexpr char key[] = "key";
  constexpr char deltaLog[] = "deltaLog";

  uint64_t deltaLog_offset = 0;
  uint64_t deltaLog_size = 0;

  WriteDeltaLogFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, deltaLog_file_number,
                    key, deltaLog, kNoCompression, &deltaLog_offset,
                    &deltaLog_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [](void* arg) {
    Slice* const slice = static_cast<Slice*>(arg);
    assert(slice);
    assert(!slice->empty());

    slice->remove_prefix(1);
  });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* deltaLog_file_read_hist = nullptr;

  std::unique_ptr<DeltaLogFileReader> reader;

  const Status s = DeltaLogFileReader::Create(
      immutable_options, FileOptions(), column_family_id,
      deltaLog_file_read_hist, deltaLog_file_number, nullptr /*IOTracer*/,
      &reader);

  const bool fail_during_create =
      sync_point_ != "DeltaLogFileReader::GetDeltaLog:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<DeltaLogContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDeltaLog(ReadOptions(), key, deltaLog_offset,
                                  deltaLog_size, kNoCompression,
                                  prefetch_buffer, allocator, &value,
                                  &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
