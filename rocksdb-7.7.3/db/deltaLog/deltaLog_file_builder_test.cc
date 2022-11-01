//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <cinttypes>
#include <string>
#include <utility>
#include <vector>

#include "db/deltaLog/deltaLog_file_addition.h"
#include "db/deltaLog/deltaLog_file_builder.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/deltaLog_log_sequential_reader.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class TestFileNumberGenerator {
 public:
  uint64_t operator()() { return ++next_file_number_; }

 private:
  uint64_t next_file_number_ = 1;
};

class DeltaLogFileBuilderTest : public testing::Test {
 protected:
  DeltaLogFileBuilderTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  void VerifyDeltaLogFile(
      uint64_t deltaLog_file_number, const std::string& deltaLog_file_path,
      uint32_t column_family_id, CompressionType deltaLog_compression_type,
      const std::vector<std::pair<std::string, std::string>>&
          expected_key_value_pairs,
      const std::vector<std::string>& deltaLog_indexes) {
    assert(expected_key_value_pairs.size() == deltaLog_indexes.size());

    std::unique_ptr<FSRandomAccessFile> file;
    constexpr IODebugContext* dbg = nullptr;
    ASSERT_OK(fs_->NewRandomAccessFile(deltaLog_file_path, file_options_, &file,
                                       dbg));

    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), deltaLog_file_path,
                                   clock_));

    constexpr Statistics* statistics = nullptr;
    DeltaLogLogSequentialReader deltaLog_log_reader(std::move(file_reader),
                                                    clock_, statistics);

    DeltaLogLogHeader header;
    ASSERT_OK(deltaLog_log_reader.ReadHeader(&header));
    ASSERT_EQ(header.version, kVersion1);
    ASSERT_EQ(header.column_family_id, column_family_id);
    ASSERT_EQ(header.compression, deltaLog_compression_type);
    ASSERT_FALSE(header.has_ttl);
    ASSERT_EQ(header.expiration_range, ExpirationRange());

    for (size_t i = 0; i < expected_key_value_pairs.size(); ++i) {
      DeltaLogLogRecord record;
      uint64_t deltaLog_offset = 0;

      ASSERT_OK(deltaLog_log_reader.ReadRecord(
          &record, DeltaLogLogSequentialReader::kReadHeaderKeyDeltaLog,
          &deltaLog_offset));

      // Check the contents of the deltaLog file
      const auto& expected_key_value = expected_key_value_pairs[i];
      const auto& key = expected_key_value.first;
      const auto& value = expected_key_value.second;

      ASSERT_EQ(record.key_size, key.size());
      ASSERT_EQ(record.value_size, value.size());
      ASSERT_EQ(record.expiration, 0);
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, value);

      // Make sure the deltaLog reference returned by the builder points to the
      // right place
      DeltaLogIndex deltaLog_index;
      ASSERT_OK(deltaLog_index.DecodeFrom(deltaLog_indexes[i]));
      ASSERT_FALSE(deltaLog_index.IsInlined());
      ASSERT_FALSE(deltaLog_index.HasTTL());
      ASSERT_EQ(deltaLog_index.file_number(), deltaLog_file_number);
      ASSERT_EQ(deltaLog_index.offset(), deltaLog_offset);
      ASSERT_EQ(deltaLog_index.size(), value.size());
    }

    DeltaLogLogFooter footer;
    ASSERT_OK(deltaLog_log_reader.ReadFooter(&footer));
    ASSERT_EQ(footer.deltaLog_count, expected_key_value_pairs.size());
    ASSERT_EQ(footer.expiration_range, ExpirationRange());
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  SystemClock* clock_;
  FileOptions file_options_;
};

TEST_F(DeltaLogFileBuilderTest, BuildAndCheckOneFile) {
  // Build a single deltaLog file
  constexpr size_t number_of_deltaLogs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 4;
  constexpr size_t value_offset = 1234;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderTest_BuildAndCheckOneFile"),
      0);
  options.enable_deltaLog_files = true;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_deltaLogs);
  std::vector<std::string> deltaLog_indexes(number_of_deltaLogs);

  for (size_t i = 0; i < number_of_deltaLogs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    auto& deltaLog_index = deltaLog_indexes[i];

    ASSERT_OK(builder.Add(key, value, &deltaLog_index));
    ASSERT_FALSE(deltaLog_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t deltaLog_file_number = 2;

  ASSERT_EQ(deltaLog_file_paths.size(), 1);

  const std::string& deltaLog_file_path = deltaLog_file_paths[0];

  ASSERT_EQ(deltaLog_file_path,
            DeltaLogFileName(immutable_options.cf_paths.front().path,
                             deltaLog_file_number));

  ASSERT_EQ(deltaLog_file_additions.size(), 1);

  const auto& deltaLog_file_addition = deltaLog_file_additions[0];

  ASSERT_EQ(deltaLog_file_addition.GetDeltaLogFileNumber(),
            deltaLog_file_number);
  ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogCount(),
            number_of_deltaLogs);
  ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogBytes(),
            number_of_deltaLogs *
                (DeltaLogLogRecord::kHeaderSize + key_size + value_size));

  // Verify the contents of the new deltaLog file as well as the deltaLog
  // references
  VerifyDeltaLogFile(deltaLog_file_number, deltaLog_file_path, column_family_id,
                     kNoCompression, expected_key_value_pairs,
                     deltaLog_indexes);
}

TEST_F(DeltaLogFileBuilderTest, BuildAndCheckMultipleFiles) {
  // Build multiple deltaLog files: file size limit is set to the size of a
  // single value, so each deltaLog ends up in a file of its own
  constexpr size_t number_of_deltaLogs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "DeltaLogFileBuilderTest_BuildAndCheckMultipleFiles"),
      0);
  options.enable_deltaLog_files = true;
  options.deltaLog_file_size = value_size;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_deltaLogs);
  std::vector<std::string> deltaLog_indexes(number_of_deltaLogs);

  for (size_t i = 0; i < number_of_deltaLogs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    auto& deltaLog_index = deltaLog_indexes[i];

    ASSERT_OK(builder.Add(key, value, &deltaLog_index));
    ASSERT_FALSE(deltaLog_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_EQ(deltaLog_file_paths.size(), number_of_deltaLogs);
  ASSERT_EQ(deltaLog_file_additions.size(), number_of_deltaLogs);

  for (size_t i = 0; i < number_of_deltaLogs; ++i) {
    const uint64_t deltaLog_file_number = i + 2;

    ASSERT_EQ(deltaLog_file_paths[i],
              DeltaLogFileName(immutable_options.cf_paths.front().path,
                               deltaLog_file_number));

    const auto& deltaLog_file_addition = deltaLog_file_additions[i];

    ASSERT_EQ(deltaLog_file_addition.GetDeltaLogFileNumber(),
              deltaLog_file_number);
    ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogCount(), 1);
    ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogBytes(),
              DeltaLogLogRecord::kHeaderSize + key_size + value_size);
  }

  // Verify the contents of the new deltaLog files as well as the deltaLog
  // references
  for (size_t i = 0; i < number_of_deltaLogs; ++i) {
    std::vector<std::pair<std::string, std::string>> expected_key_value_pair{
        expected_key_value_pairs[i]};
    std::vector<std::string> deltaLog_index{deltaLog_indexes[i]};

    VerifyDeltaLogFile(i + 2, deltaLog_file_paths[i], column_family_id,
                       kNoCompression, expected_key_value_pair, deltaLog_index);
  }
}

TEST_F(DeltaLogFileBuilderTest, InlinedValues) {
  // All values are below the min_deltaLog_size threshold; no deltaLog files get
  // written
  constexpr size_t number_of_deltaLogs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderTest_InlinedValues"),
      0);
  options.enable_deltaLog_files = true;
  options.min_deltaLog_size = 1024;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  for (size_t i = 0; i < number_of_deltaLogs; ++i) {
    const std::string key = std::to_string(i);
    assert(key.size() == key_size);

    const std::string value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    std::string deltaLog_index;
    ASSERT_OK(builder.Add(key, value, &deltaLog_index));
    ASSERT_TRUE(deltaLog_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_TRUE(deltaLog_file_paths.empty());
  ASSERT_TRUE(deltaLog_file_additions.empty());
}

TEST_F(DeltaLogFileBuilderTest, Compression) {
  // Build a deltaLog file with a compressed deltaLog
  if (!Snappy_Supported()) {
    return;
  }

  constexpr size_t key_size = 1;
  constexpr size_t value_size = 100;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderTest_Compression"),
      0);
  options.enable_deltaLog_files = true;
  options.deltaLog_compression_type = kSnappyCompression;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  const std::string key("1");
  const std::string uncompressed_value(value_size, 'x');

  std::string deltaLog_index;

  ASSERT_OK(builder.Add(key, uncompressed_value, &deltaLog_index));
  ASSERT_FALSE(deltaLog_index.empty());

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t deltaLog_file_number = 2;

  ASSERT_EQ(deltaLog_file_paths.size(), 1);

  const std::string& deltaLog_file_path = deltaLog_file_paths[0];

  ASSERT_EQ(deltaLog_file_path,
            DeltaLogFileName(immutable_options.cf_paths.front().path,
                             deltaLog_file_number));

  ASSERT_EQ(deltaLog_file_additions.size(), 1);

  const auto& deltaLog_file_addition = deltaLog_file_additions[0];

  ASSERT_EQ(deltaLog_file_addition.GetDeltaLogFileNumber(),
            deltaLog_file_number);
  ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogCount(), 1);

  CompressionOptions opts;
  CompressionContext context(kSnappyCompression);
  constexpr uint64_t sample_for_compression = 0;

  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                       kSnappyCompression, sample_for_compression);

  std::string compressed_value;
  ASSERT_TRUE(Snappy_Compress(info, uncompressed_value.data(),
                              uncompressed_value.size(), &compressed_value));

  ASSERT_EQ(
      deltaLog_file_addition.GetTotalDeltaLogBytes(),
      DeltaLogLogRecord::kHeaderSize + key_size + compressed_value.size());

  // Verify the contents of the new deltaLog file as well as the deltaLog
  // reference
  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs{
      {key, compressed_value}};
  std::vector<std::string> deltaLog_indexes{deltaLog_index};

  VerifyDeltaLogFile(deltaLog_file_number, deltaLog_file_path, column_family_id,
                     kSnappyCompression, expected_key_value_pairs,
                     deltaLog_indexes);
}

TEST_F(DeltaLogFileBuilderTest, CompressionError) {
  // Simulate an error during compression
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderTest_CompressionError"),
      0);
  options.enable_deltaLog_files = true;
  options.deltaLog_compression_type = kSnappyCompression;
  options.env = mock_env_.get();
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  SyncPoint::GetInstance()->SetCallBack("CompressData:TamperWithReturnValue",
                                        [](void* arg) {
                                          bool* ret = static_cast<bool*>(arg);
                                          *ret = false;
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr char key[] = "1";
  constexpr char value[] = "deadbeef";

  std::string deltaLog_index;

  ASSERT_TRUE(builder.Add(key, value, &deltaLog_index).IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  constexpr uint64_t deltaLog_file_number = 2;

  ASSERT_EQ(deltaLog_file_paths.size(), 1);
  ASSERT_EQ(deltaLog_file_paths[0],
            DeltaLogFileName(immutable_options.cf_paths.front().path,
                             deltaLog_file_number));

  ASSERT_TRUE(deltaLog_file_additions.empty());
}

TEST_F(DeltaLogFileBuilderTest, Checksum) {
  // Build a deltaLog file with checksum

  class DummyFileChecksumGenerator : public FileChecksumGenerator {
   public:
    void Update(const char* /* data */, size_t /* n */) override {}

    void Finalize() override {}

    std::string GetChecksum() const override { return std::string("dummy"); }

    const char* Name() const override { return "DummyFileChecksum"; }
  };

  class DummyFileChecksumGenFactory : public FileChecksumGenFactory {
   public:
    std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
        const FileChecksumGenContext& /* context */) override {
      return std::unique_ptr<FileChecksumGenerator>(
          new DummyFileChecksumGenerator);
    }

    const char* Name() const override { return "DummyFileChecksumGenFactory"; }
  };

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderTest_Checksum"),
      0);
  options.enable_deltaLog_files = true;
  options.file_checksum_gen_factory =
      std::make_shared<DummyFileChecksumGenFactory>();
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  const std::string key("1");
  const std::string value("deadbeef");

  std::string deltaLog_index;

  ASSERT_OK(builder.Add(key, value, &deltaLog_index));
  ASSERT_FALSE(deltaLog_index.empty());

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t deltaLog_file_number = 2;

  ASSERT_EQ(deltaLog_file_paths.size(), 1);

  const std::string& deltaLog_file_path = deltaLog_file_paths[0];

  ASSERT_EQ(deltaLog_file_path,
            DeltaLogFileName(immutable_options.cf_paths.front().path,
                             deltaLog_file_number));

  ASSERT_EQ(deltaLog_file_additions.size(), 1);

  const auto& deltaLog_file_addition = deltaLog_file_additions[0];

  ASSERT_EQ(deltaLog_file_addition.GetDeltaLogFileNumber(),
            deltaLog_file_number);
  ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogCount(), 1);
  ASSERT_EQ(deltaLog_file_addition.GetTotalDeltaLogBytes(),
            DeltaLogLogRecord::kHeaderSize + key.size() + value.size());
  ASSERT_EQ(deltaLog_file_addition.GetChecksumMethod(), "DummyFileChecksum");
  ASSERT_EQ(deltaLog_file_addition.GetChecksumValue(), "dummy");

  // Verify the contents of the new deltaLog file as well as the deltaLog
  // reference
  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs{
      {key, value}};
  std::vector<std::string> deltaLog_indexes{deltaLog_index};

  VerifyDeltaLogFile(deltaLog_file_number, deltaLog_file_path, column_family_id,
                     kNoCompression, expected_key_value_pairs,
                     deltaLog_indexes);
}

class DeltaLogFileBuilderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  DeltaLogFileBuilderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  FileOptions file_options_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(
    DeltaLogFileBuilderTest, DeltaLogFileBuilderIOErrorTest,
    ::testing::ValuesIn(std::vector<std::string>{
        "DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded:NewWritableFile",
        "DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded:WriteHeader",
        "DeltaLogFileBuilder::WriteDeltaLogToFile:AddRecord",
        "DeltaLogFileBuilder::WriteDeltaLogToFile:AppendFooter"}));

TEST_P(DeltaLogFileBuilderIOErrorTest, IOError) {
  // Simulate an I/O error during the specified step of Add()
  // Note: deltaLog_file_size will be set to value_size in order for the first
  // deltaLog to trigger close
  constexpr size_t value_size = 8;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "DeltaLogFileBuilderIOErrorTest_IOError"),
      0);
  options.enable_deltaLog_files = true;
  options.deltaLog_file_size = value_size;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> deltaLog_file_paths;
  std::vector<DeltaLogFileAddition> deltaLog_file_additions;

  DeltaLogFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, "" /*db_id*/, "" /*db_session_id*/, job_id,
      column_family_id, column_family_name, io_priority, write_hint,
      nullptr /*IOTracer*/, nullptr /*DeltaLogFileCompletionCallback*/,
      DeltaLogFileCreationReason::kFlush, &deltaLog_file_paths,
      &deltaLog_file_additions);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr char key[] = "1";
  constexpr char value[] = "deadbeef";

  std::string deltaLog_index;

  ASSERT_TRUE(builder.Add(key, value, &deltaLog_index).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  if (sync_point_ ==
      "DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded:NewWritableFile") {
    ASSERT_TRUE(deltaLog_file_paths.empty());
  } else {
    constexpr uint64_t deltaLog_file_number = 2;

    ASSERT_EQ(deltaLog_file_paths.size(), 1);
    ASSERT_EQ(deltaLog_file_paths[0],
              DeltaLogFileName(immutable_options.cf_paths.front().path,
                               deltaLog_file_number));
  }

  ASSERT_TRUE(deltaLog_file_additions.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
