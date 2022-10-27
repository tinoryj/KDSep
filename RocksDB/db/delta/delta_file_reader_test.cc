//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <string>

#include "db/delta/delta_contents.h"
#include "db/delta/delta_file_reader.h"
#include "db/delta/delta_log_format.h"
#include "db/delta/delta_log_writer.h"
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

// Creates a test delta file with a single delta in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various delta file header/footer fields.
void WriteDeltaFile(const ImmutableOptions& immutable_options,
                    uint32_t column_family_id, bool has_ttl,
                    const ExpirationRange& expiration_range_header,
                    const ExpirationRange& expiration_range_footer,
                    uint64_t delta_file_number, const Slice& key,
                    const Slice& delta, CompressionType compression,
                    uint64_t* delta_offset, uint64_t* delta_size) {
  std::vector<Slice> keys{key};
  std::vector<Slice> deltas{delta};
  std::vector<uint64_t> delta_offsets{0};
  std::vector<uint64_t> delta_sizes{0};
  WriteDeltaFile(immutable_options, column_family_id, has_ttl,
                 expiration_range_header, expiration_range_footer,
                 delta_file_number, keys, deltas, compression, delta_offsets,
                 delta_sizes);
  if (delta_offset) {
    *delta_offset = delta_offsets[0];
  }
  if (delta_size) {
    *delta_size = delta_sizes[0];
  }
}

}  // anonymous namespace

class DeltaFileReaderTest : public testing::Test {
 protected:
  DeltaFileReaderTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }
  std::unique_ptr<Env> mock_env_;
};

TEST_F(DeltaFileReaderTest, CreateReaderAndGetDelta) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_CreateReaderAndGetDelta"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr size_t num_deltas = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> delta_strs = {"delta1", "delta2", "delta3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> deltas = {delta_strs[0], delta_strs[1],
                                     delta_strs[2]};

  std::vector<uint64_t> delta_offsets(keys.size());
  std::vector<uint64_t> delta_sizes(keys.size());

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, keys, deltas,
                 kNoCompression, delta_offsets, delta_sizes);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_OK(DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the delta can be retrieved with and without checksum verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDelta(read_options, keys[0], delta_offsets[0],
                               delta_sizes[0], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltas[0]);
    ASSERT_EQ(bytes_read, delta_sizes[0]);

    // MultiGetDelta
    bytes_read = 0;
    size_t total_size = 0;

    std::array<Status, num_deltas> statuses_buf;
    std::array<DeltaReadRequest, num_deltas> requests_buf;
    autovector<std::pair<DeltaReadRequest*, std::unique_ptr<DeltaContents>>>
        delta_reqs;

    for (size_t i = 0; i < num_deltas; ++i) {
      requests_buf[i] =
          DeltaReadRequest(keys[i], delta_offsets[i], delta_sizes[i],
                           kNoCompression, nullptr, &statuses_buf[i]);
      delta_reqs.emplace_back(&requests_buf[i],
                              std::unique_ptr<DeltaContents>());
    }

    reader->MultiGetDelta(read_options, allocator, delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
      const auto& result = delta_reqs[i].second;

      ASSERT_OK(statuses_buf[i]);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->data(), deltas[i]);
      total_size += delta_sizes[i];
    }
    ASSERT_EQ(bytes_read, total_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDelta(read_options, keys[1], delta_offsets[1],
                               delta_sizes[1], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), deltas[1]);

    const uint64_t key_size = keys[1].size();
    ASSERT_EQ(bytes_read,
              DeltaLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  delta_sizes[1]);
  }

  // Invalid offset (too close to start of file)
  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, keys[0], delta_offsets[0] - 1,
                               delta_sizes[0], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, keys[2], delta_offsets[2] + 1,
                               delta_sizes[2], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, keys[0], delta_offsets[0],
                               delta_sizes[0], kZSTD, prefetch_buffer,
                               allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, shorter_key,
                               delta_offsets[0] -
                                   (keys[0].size() - sizeof(shorter_key) + 1),
                               delta_sizes[0], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDelta
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice shorter_key_slice(shorter_key, sizeof(shorter_key) - 1);
    key_refs[1] = std::cref(shorter_key_slice);

    autovector<uint64_t> offsets{
        delta_offsets[0],
        delta_offsets[1] - (keys[1].size() - key_refs[1].get().size()),
        delta_offsets[2]};

    std::array<Status, num_deltas> statuses_buf;
    std::array<DeltaReadRequest, num_deltas> requests_buf;
    autovector<std::pair<DeltaReadRequest*, std::unique_ptr<DeltaContents>>>
        delta_reqs;

    for (size_t i = 0; i < num_deltas; ++i) {
      requests_buf[i] =
          DeltaReadRequest(key_refs[i], offsets[i], delta_sizes[i],
                           kNoCompression, nullptr, &statuses_buf[i]);
      delta_reqs.emplace_back(&requests_buf[i],
                              std::unique_ptr<DeltaContents>());
    }

    reader->MultiGetDelta(read_options, allocator, delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
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
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, incorrect_key, delta_offsets[0],
                               delta_sizes[0], kNoCompression, prefetch_buffer,
                               allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDelta
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice wrong_key_slice(incorrect_key, sizeof(incorrect_key) - 1);
    key_refs[2] = std::cref(wrong_key_slice);

    std::array<Status, num_deltas> statuses_buf;
    std::array<DeltaReadRequest, num_deltas> requests_buf;
    autovector<std::pair<DeltaReadRequest*, std::unique_ptr<DeltaContents>>>
        delta_reqs;

    for (size_t i = 0; i < num_deltas; ++i) {
      requests_buf[i] =
          DeltaReadRequest(key_refs[i], delta_offsets[i], delta_sizes[i],
                           kNoCompression, nullptr, &statuses_buf[i]);
      delta_reqs.emplace_back(&requests_buf[i],
                              std::unique_ptr<DeltaContents>());
    }

    reader->MultiGetDelta(read_options, allocator, delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
      if (i == num_deltas - 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect value size
  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(read_options, keys[1], delta_offsets[1],
                               delta_sizes[1] + 1, kNoCompression,
                               prefetch_buffer, allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetDelta
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }

    std::array<Status, num_deltas> statuses_buf;
    std::array<DeltaReadRequest, num_deltas> requests_buf;

    requests_buf[0] =
        DeltaReadRequest(key_refs[0], delta_offsets[0], delta_sizes[0],
                         kNoCompression, nullptr, &statuses_buf[0]);
    requests_buf[1] =
        DeltaReadRequest(key_refs[1], delta_offsets[1], delta_sizes[1] + 1,
                         kNoCompression, nullptr, &statuses_buf[1]);
    requests_buf[2] =
        DeltaReadRequest(key_refs[2], delta_offsets[2], delta_sizes[2],
                         kNoCompression, nullptr, &statuses_buf[2]);

    autovector<std::pair<DeltaReadRequest*, std::unique_ptr<DeltaContents>>>
        delta_reqs;

    for (size_t i = 0; i < num_deltas; ++i) {
      delta_reqs.emplace_back(&requests_buf[i],
                              std::unique_ptr<DeltaContents>());
    }

    reader->MultiGetDelta(read_options, allocator, delta_reqs, &bytes_read);

    for (size_t i = 0; i < num_deltas; ++i) {
      if (i != 1) {
        ASSERT_OK(statuses_buf[i]);
      } else {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      }
    }
  }
}

TEST_F(DeltaFileReaderTest, Malformed) {
  // Write a delta file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "DeltaFileReaderTest_Malformed"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t delta_file_number = 1;

  {
    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;

    const std::string delta_file_path = DeltaFileName(
        immutable_options.cf_paths.front().path, delta_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(NewWritableFile(immutable_options.fs.get(), delta_file_path,
                              &file, FileOptions()));

    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), delta_file_path, FileOptions(),
                               immutable_options.clock));

    constexpr Statistics* statistics = nullptr;
    constexpr bool use_fsync = false;
    constexpr bool do_flush = false;

    DeltaLogWriter delta_log_writer(std::move(file_writer),
                                    immutable_options.clock, statistics,
                                    delta_file_number, use_fsync, do_flush);

    DeltaLogHeader header(column_family_id, kNoCompression, has_ttl,
                          expiration_range);

    ASSERT_OK(delta_log_writer.WriteHeader(header));
  }

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_TRUE(DeltaFileReader::Create(immutable_options, FileOptions(),
                                      column_family_id, delta_file_read_hist,
                                      delta_file_number, nullptr /*IOTracer*/,
                                      &reader)
                  .IsCorruption());
}

TEST_F(DeltaFileReaderTest, TTL) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "DeltaFileReaderTest_TTL"), 0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kNoCompression, &delta_offset, &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_TRUE(DeltaFileReader::Create(immutable_options, FileOptions(),
                                      column_family_id, delta_file_read_hist,
                                      delta_file_number, nullptr /*IOTracer*/,
                                      &reader)
                  .IsCorruption());
}

TEST_F(DeltaFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_ExpirationRangeInHeader"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  const ExpirationRange expiration_range_header(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr ExpirationRange expiration_range_footer;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl,
                 expiration_range_header, expiration_range_footer,
                 delta_file_number, key, delta, kNoCompression, &delta_offset,
                 &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_TRUE(DeltaFileReader::Create(immutable_options, FileOptions(),
                                      column_family_id, delta_file_read_hist,
                                      delta_file_number, nullptr /*IOTracer*/,
                                      &reader)
                  .IsCorruption());
}

TEST_F(DeltaFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_ExpirationRangeInFooter"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  const ExpirationRange expiration_range_footer(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl,
                 expiration_range_header, expiration_range_footer,
                 delta_file_number, key, delta, kNoCompression, &delta_offset,
                 &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_TRUE(DeltaFileReader::Create(immutable_options, FileOptions(),
                                      column_family_id, delta_file_read_hist,
                                      delta_file_number, nullptr /*IOTracer*/,
                                      &reader)
                  .IsCorruption());
}

TEST_F(DeltaFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_IncorrectColumnFamily"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kNoCompression, &delta_offset, &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;

  ASSERT_TRUE(DeltaFileReader::Create(immutable_options, FileOptions(),
                                      incorrect_column_family_id,
                                      delta_file_read_hist, delta_file_number,
                                      nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(DeltaFileReaderTest, DeltaCRCError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_DeltaCRCError"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kNoCompression, &delta_offset, &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_OK(DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaFileReader::VerifyDelta:CheckDeltaCRC", [](void* arg) {
        DeltaLogRecord* const record = static_cast<DeltaLogRecord*>(arg);
        assert(record);

        record->delta_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<DeltaContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetDelta(ReadOptions(), key, delta_offset, delta_size,
                             kNoCompression, prefetch_buffer, allocator, &value,
                             &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeltaFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "DeltaFileReaderTest_Compression"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kSnappyCompression, &delta_offset, &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_OK(DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the delta can be retrieved with and without checksum verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDelta(read_options, key, delta_offset, delta_size,
                               kSnappyCompression, prefetch_buffer, allocator,
                               &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), delta);
    ASSERT_EQ(bytes_read, delta_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetDelta(read_options, key, delta_offset, delta_size,
                               kSnappyCompression, prefetch_buffer, allocator,
                               &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), delta);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              DeltaLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  delta_size);
  }
}

TEST_F(DeltaFileReaderTest, UncompressionError) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderTest_UncompressionError"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kSnappyCompression, &delta_offset, &delta_size);

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  ASSERT_OK(DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaFileReader::UncompressDeltaIfNeeded:TamperWithResult",
      [](void* arg) {
        CacheAllocationPtr* const output =
            static_cast<CacheAllocationPtr*>(arg);
        assert(output);

        output->reset();
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<DeltaContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetDelta(ReadOptions(), key, delta_offset, delta_size,
                             kSnappyCompression, prefetch_buffer, allocator,
                             &value, &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DeltaFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  DeltaFileReaderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fault_injection_env_.reset(new FaultInjectionTestEnv(mock_env_.get()));
  }

  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DeltaFileReaderTest, DeltaFileReaderIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "DeltaFileReader::OpenFile:GetFileSize",
                            "DeltaFileReader::OpenFile:NewRandomAccessFile",
                            "DeltaFileReader::ReadHeader:ReadFromFile",
                            "DeltaFileReader::ReadFooter:ReadFromFile",
                            "DeltaFileReader::GetDelta:ReadFromFile"}));

TEST_P(DeltaFileReaderIOErrorTest, IOError) {
  // Simulates an I/O error during the specified step

  Options options;
  options.env = fault_injection_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(fault_injection_env_.get(),
                            "DeltaFileReaderIOErrorTest_IOError"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kNoCompression, &delta_offset, &delta_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  const Status s = DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      (sync_point_ != "DeltaFileReader::GetDelta:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(ReadOptions(), key, delta_offset, delta_size,
                               kNoCompression, prefetch_buffer, allocator,
                               &value, &bytes_read)
                    .IsIOError());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DeltaFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  DeltaFileReaderDecodingErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
  }

  std::unique_ptr<Env> mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DeltaFileReaderTest, DeltaFileReaderDecodingErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "DeltaFileReader::ReadHeader:TamperWithResult",
                            "DeltaFileReader::ReadFooter:TamperWithResult",
                            "DeltaFileReader::GetDelta:TamperWithResult"}));

TEST_P(DeltaFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaFileReaderDecodingErrorTest_DecodingError"),
      0);
  options.enable_delta_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t delta_file_number = 1;
  constexpr char key[] = "key";
  constexpr char delta[] = "delta";

  uint64_t delta_offset = 0;
  uint64_t delta_size = 0;

  WriteDeltaFile(immutable_options, column_family_id, has_ttl, expiration_range,
                 expiration_range, delta_file_number, key, delta,
                 kNoCompression, &delta_offset, &delta_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [](void* arg) {
    Slice* const slice = static_cast<Slice*>(arg);
    assert(slice);
    assert(!slice->empty());

    slice->remove_prefix(1);
  });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* delta_file_read_hist = nullptr;

  std::unique_ptr<DeltaFileReader> reader;

  const Status s = DeltaFileReader::Create(
      immutable_options, FileOptions(), column_family_id, delta_file_read_hist,
      delta_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      sync_point_ != "DeltaFileReader::GetDelta:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<DeltaContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetDelta(ReadOptions(), key, delta_offset, delta_size,
                               kNoCompression, prefetch_buffer, allocator,
                               &value, &bytes_read)
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
