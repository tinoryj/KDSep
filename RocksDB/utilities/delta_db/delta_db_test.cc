//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/delta_db/delta_db.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/delta/delta_index.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/delta_db/delta_db_impl.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

class DeltaDBTest : public testing::Test {
 public:
  const int kMaxDeltaSize = 1 << 14;

  struct DeltaIndexVersion {
    DeltaIndexVersion() = default;
    DeltaIndexVersion(std::string _user_key, uint64_t _file_number,
                      uint64_t _expiration, SequenceNumber _sequence,
                      ValueType _type)
        : user_key(std::move(_user_key)),
          file_number(_file_number),
          expiration(_expiration),
          sequence(_sequence),
          type(_type) {}

    std::string user_key;
    uint64_t file_number = kInvalidDeltaFileNumber;
    uint64_t expiration = kNoExpiration;
    SequenceNumber sequence = 0;
    ValueType type = kTypeValue;
  };

  DeltaDBTest()
      : dbname_(test::PerThreadDBPath("delta_db_test")), delta_db_(nullptr) {
    mock_clock_ = std::make_shared<MockSystemClock>(SystemClock::Default());
    mock_env_.reset(new CompositeEnvWrapper(Env::Default(), mock_clock_));
    fault_injection_env_.reset(new FaultInjectionTestEnv(Env::Default()));

    Status s = DestroyDeltaDB(dbname_, Options(), DeltaDBOptions());
    assert(s.ok());
  }

  ~DeltaDBTest() override {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Destroy();
  }

  Status TryOpen(DeltaDBOptions bdb_options = DeltaDBOptions(),
                 Options options = Options()) {
    options.create_if_missing = true;
    if (options.env == mock_env_.get()) {
      // Need to disable stats dumping and persisting which also use
      // RepeatableThread, which uses InstrumentedCondVar::TimedWaitInternal.
      // With mocked time, this can hang on some platforms (MacOS)
      // because (a) on some platforms, pthread_cond_timedwait does not appear
      // to release the lock for other threads to operate if the deadline time
      // is already passed, and (b) TimedWait calls are currently a bad
      // abstraction because the deadline parameter is usually computed from
      // Env time, but is interpreted in real clock time.
      options.stats_dump_period_sec = 0;
      options.stats_persist_period_sec = 0;
    }
    return DeltaDB::Open(options, bdb_options, dbname_, &delta_db_);
  }

  void Open(DeltaDBOptions bdb_options = DeltaDBOptions(),
            Options options = Options()) {
    ASSERT_OK(TryOpen(bdb_options, options));
  }

  void Reopen(DeltaDBOptions bdb_options = DeltaDBOptions(),
              Options options = Options()) {
    assert(delta_db_ != nullptr);
    delete delta_db_;
    delta_db_ = nullptr;
    Open(bdb_options, options);
  }

  void Close() {
    assert(delta_db_ != nullptr);
    delete delta_db_;
    delta_db_ = nullptr;
  }

  void Destroy() {
    if (delta_db_) {
      Options options = delta_db_->GetOptions();
      DeltaDBOptions bdb_options = delta_db_->GetDeltaDBOptions();
      delete delta_db_;
      delta_db_ = nullptr;
      ASSERT_OK(DestroyDeltaDB(dbname_, options, bdb_options));
    }
  }

  DeltaDBImpl *delta_db_impl() {
    return reinterpret_cast<DeltaDBImpl *>(delta_db_);
  }

  Status Put(const Slice &key, const Slice &value,
             std::map<std::string, std::string> *data = nullptr) {
    Status s = delta_db_->Put(WriteOptions(), key, value);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  void Delete(const std::string &key,
              std::map<std::string, std::string> *data = nullptr) {
    ASSERT_OK(delta_db_->Delete(WriteOptions(), key));
    if (data != nullptr) {
      data->erase(key);
    }
  }

  Status PutWithTTL(const Slice &key, const Slice &value, uint64_t ttl,
                    std::map<std::string, std::string> *data = nullptr) {
    Status s = delta_db_->PutWithTTL(WriteOptions(), key, value, ttl);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  Status PutUntil(const Slice &key, const Slice &value, uint64_t expiration) {
    return delta_db_->PutUntil(WriteOptions(), key, value, expiration);
  }

  void PutRandomWithTTL(const std::string &key, uint64_t ttl, Random *rnd,
                        std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxDeltaSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(
        delta_db_->PutWithTTL(WriteOptions(), Slice(key), Slice(value), ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomUntil(const std::string &key, uint64_t expiration, Random *rnd,
                      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxDeltaSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(delta_db_->PutUntil(WriteOptions(), Slice(key), Slice(value),
                                  expiration));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    PutRandom(delta_db_, key, rnd, data);
  }

  void PutRandom(DB *db, const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxDeltaSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(db->Put(WriteOptions(), Slice(key), Slice(value)));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomToWriteBatch(
      const std::string &key, Random *rnd, WriteBatch *batch,
      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxDeltaSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(batch->Put(key, value));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  // Verify delta db contain expected data and nothing more.
  void VerifyDB(const std::map<std::string, std::string> &data) {
    VerifyDB(delta_db_, data);
  }

  void VerifyDB(DB *db, const std::map<std::string, std::string> &data) {
    // Verify normal Get
    auto *cfh = db->DefaultColumnFamily();
    for (auto &p : data) {
      PinnableSlice value_slice;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value_slice));
      ASSERT_EQ(p.second, value_slice.ToString());
      std::string value;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value));
      ASSERT_EQ(p.second, value);
    }

    // Verify iterators
    Iterator *iter = db->NewIterator(ReadOptions());
    iter->SeekToFirst();
    for (auto &p : data) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(p.first, iter->key().ToString());
      ASSERT_EQ(p.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }

  void VerifyBaseDB(
      const std::map<std::string, KeyVersion> &expected_versions) {
    auto *bdb_impl = static_cast<DeltaDBImpl *>(delta_db_);
    DB *db = delta_db_->GetRootDB();
    const size_t kMaxKeys = 10000;
    std::vector<KeyVersion> versions;
    ASSERT_OK(GetAllKeyVersions(db, "", "", kMaxKeys, &versions));
    ASSERT_EQ(expected_versions.size(), versions.size());
    size_t i = 0;
    for (auto &key_version : expected_versions) {
      const KeyVersion &expected_version = key_version.second;
      ASSERT_EQ(expected_version.user_key, versions[i].user_key);
      ASSERT_EQ(expected_version.sequence, versions[i].sequence);
      ASSERT_EQ(expected_version.type, versions[i].type);
      if (versions[i].type == kTypeValue) {
        ASSERT_EQ(expected_version.value, versions[i].value);
      } else {
        ASSERT_EQ(kTypeDeltaIndex, versions[i].type);
        PinnableSlice value;
        ASSERT_OK(bdb_impl->TEST_GetDeltaValue(versions[i].user_key,
                                               versions[i].value, &value));
        ASSERT_EQ(expected_version.value, value.ToString());
      }
      i++;
    }
  }

  void VerifyBaseDBDeltaIndex(
      const std::map<std::string, DeltaIndexVersion> &expected_versions) {
    const size_t kMaxKeys = 10000;
    std::vector<KeyVersion> versions;
    ASSERT_OK(
        GetAllKeyVersions(delta_db_->GetRootDB(), "", "", kMaxKeys, &versions));
    ASSERT_EQ(versions.size(), expected_versions.size());

    size_t i = 0;
    for (const auto &expected_pair : expected_versions) {
      const DeltaIndexVersion &expected_version = expected_pair.second;

      ASSERT_EQ(versions[i].user_key, expected_version.user_key);
      ASSERT_EQ(versions[i].sequence, expected_version.sequence);
      ASSERT_EQ(versions[i].type, expected_version.type);
      if (versions[i].type != kTypeDeltaIndex) {
        ASSERT_EQ(kInvalidDeltaFileNumber, expected_version.file_number);
        ASSERT_EQ(kNoExpiration, expected_version.expiration);

        ++i;
        continue;
      }

      DeltaIndex delta_index;
      ASSERT_OK(delta_index.DecodeFrom(versions[i].value));

      const uint64_t file_number = !delta_index.IsInlined()
                                       ? delta_index.file_number()
                                       : kInvalidDeltaFileNumber;
      ASSERT_EQ(file_number, expected_version.file_number);

      const uint64_t expiration =
          delta_index.HasTTL() ? delta_index.expiration() : kNoExpiration;
      ASSERT_EQ(expiration, expected_version.expiration);

      ++i;
    }
  }

  void InsertDeltas() {
    WriteOptions wo;
    std::string value;

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      uint64_t ttl = rnd.Next() % 86400;
      PutRandomWithTTL("key" + std::to_string(i % 500), ttl, &rnd, nullptr);
    }

    for (size_t i = 0; i < 10; i++) {
      Delete("key" + std::to_string(i % 500));
    }
  }

  const std::string dbname_;
  std::shared_ptr<MockSystemClock> mock_clock_;
  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  DeltaDB *delta_db_;
};  // class DeltaDBTest

TEST_F(DeltaDBTest, Put) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(DeltaDBTest, PutWithTTL) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.min_delta_size = 0;
  bdb_options.delta_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_clock_->SetCurrentTime(50);
  for (size_t i = 0; i < 100; i++) {
    uint64_t ttl = rnd.Next() % 100;
    PutRandomWithTTL("key" + std::to_string(i), ttl, &rnd,
                     (ttl <= 50 ? nullptr : &data));
  }
  mock_clock_->SetCurrentTime(100);
  auto *bdb_impl = static_cast<DeltaDBImpl *>(delta_db_);
  auto delta_files = bdb_impl->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  ASSERT_TRUE(delta_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseDeltaFile(delta_files[0]));
  VerifyDB(data);
}

TEST_F(DeltaDBTest, PutUntil) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.min_delta_size = 0;
  bdb_options.delta_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_clock_->SetCurrentTime(50);
  for (size_t i = 0; i < 100; i++) {
    uint64_t expiration = rnd.Next() % 100 + 50;
    PutRandomUntil("key" + std::to_string(i), expiration, &rnd,
                   (expiration <= 100 ? nullptr : &data));
  }
  mock_clock_->SetCurrentTime(100);
  auto *bdb_impl = static_cast<DeltaDBImpl *>(delta_db_);
  auto delta_files = bdb_impl->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  ASSERT_TRUE(delta_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseDeltaFile(delta_files[0]));
  VerifyDB(data);
}

TEST_F(DeltaDBTest, StackableDBGet) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i++) {
    StackableDB *db = delta_db_;
    ColumnFamilyHandle *column_family = db->DefaultColumnFamily();
    std::string key = "key" + std::to_string(i);
    PinnableSlice pinnable_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &pinnable_value));
    std::string string_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &string_value));
    ASSERT_EQ(string_value, pinnable_value.ToString());
    ASSERT_EQ(string_value, data[key]);
  }
}

TEST_F(DeltaDBTest, GetExpiration) {
  Options options;
  options.env = mock_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  mock_clock_->SetCurrentTime(100);
  Open(bdb_options, options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(PutWithTTL("key2", "value2", 200));
  PinnableSlice value;
  uint64_t expiration;
  ASSERT_OK(delta_db_->Get(ReadOptions(), "key1", &value, &expiration));
  ASSERT_EQ("value1", value.ToString());
  ASSERT_EQ(kNoExpiration, expiration);
  ASSERT_OK(delta_db_->Get(ReadOptions(), "key2", &value, &expiration));
  ASSERT_EQ("value2", value.ToString());
  ASSERT_EQ(300 /* = 100 + 200 */, expiration);
}

TEST_F(DeltaDBTest, GetIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;  // Make sure value write to delta file
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  ColumnFamilyHandle *column_family = delta_db_->DefaultColumnFamily();
  PinnableSlice value;
  ASSERT_OK(Put("foo", "bar"));
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  Status s = delta_db_->Get(ReadOptions(), column_family, "foo", &value);
  ASSERT_TRUE(s.IsIOError());
  // Reactivate file system to allow test to close DB.
  fault_injection_env_->SetFilesystemActive(true);
}

TEST_F(DeltaDBTest, PutIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;  // Make sure value write to delta file
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  ASSERT_TRUE(Put("foo", "v1").IsIOError());
  fault_injection_env_->SetFilesystemActive(true, Status::IOError());
  ASSERT_OK(Put("bar", "v1"));
}

TEST_F(DeltaDBTest, WriteBatch) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("key" + std::to_string(j * 100 + i), &rnd, &batch,
                            &data);
    }

    ASSERT_OK(delta_db_->Write(WriteOptions(), &batch));
  }
  VerifyDB(data);
}

TEST_F(DeltaDBTest, Delete) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i += 5) {
    Delete("key" + std::to_string(i), &data);
  }
  VerifyDB(data);
}

TEST_F(DeltaDBTest, DeleteBatch) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd);
  }
  WriteBatch batch;
  for (size_t i = 0; i < 100; i++) {
    ASSERT_OK(batch.Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(delta_db_->Write(WriteOptions(), &batch));
  // DB should be empty.
  VerifyDB({});
}

TEST_F(DeltaDBTest, Override) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + std::to_string(i), &rnd, nullptr);
  }
  // override all the keys
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  VerifyDB(data);
}

#ifdef SNAPPY
TEST_F(DeltaDBTest, Compression) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("put-key" + std::to_string(i), &rnd, &data);
  }
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("write-batch-key" + std::to_string(j * 100 + i),
                            &rnd, &batch, &data);
    }
    ASSERT_OK(delta_db_->Write(WriteOptions(), &batch));
  }
  VerifyDB(data);
}

TEST_F(DeltaDBTest, DecompressAfterReopen) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("put-key" + std::to_string(i), &rnd, &data);
  }
  VerifyDB(data);
  bdb_options.compression = CompressionType::kNoCompression;
  Reopen(bdb_options);
  VerifyDB(data);
}

TEST_F(DeltaDBTest, EnableDisableCompressionGC) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.garbage_collection_cutoff = 1.0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  size_t data_idx = 0;
  for (; data_idx < 100; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  ASSERT_EQ(kSnappyCompression, delta_files[0]->GetCompressionType());

  // disable compression
  bdb_options.compression = kNoCompression;
  Reopen(bdb_options);

  // Add more data with new compression type
  for (; data_idx < 200; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(2, delta_files.size());
  ASSERT_EQ(kNoCompression, delta_files[1]->GetCompressionType());

  // Enable GC. If we do it earlier the snapshot release triggered compaction
  // may compact files and trigger GC before we can verify there are two files.
  bdb_options.enable_garbage_collection = true;
  Reopen(bdb_options);

  // Trigger compaction
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  VerifyDB(data);

  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  for (auto bfile : delta_files) {
    ASSERT_EQ(kNoCompression, bfile->GetCompressionType());
  }

  // enabling the compression again
  bdb_options.compression = kSnappyCompression;
  Reopen(bdb_options);

  // Add more data with new compression type
  for (; data_idx < 300; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  // Trigger compaction
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  VerifyDB(data);

  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  for (auto bfile : delta_files) {
    ASSERT_EQ(kSnappyCompression, bfile->GetCompressionType());
  }
}

#ifdef LZ4
// Test switch compression types and run GC, it needs both Snappy and LZ4
// support.
TEST_F(DeltaDBTest, ChangeCompressionGC) {
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.garbage_collection_cutoff = 1.0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = kLZ4Compression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  size_t data_idx = 0;
  for (; data_idx < 100; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  ASSERT_EQ(kLZ4Compression, delta_files[0]->GetCompressionType());

  // Change compression type
  bdb_options.compression = kSnappyCompression;
  Reopen(bdb_options);

  // Add more data with Snappy compression type
  for (; data_idx < 200; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  // Verify delta file compression type
  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(2, delta_files.size());
  ASSERT_EQ(kSnappyCompression, delta_files[1]->GetCompressionType());

  // Enable GC. If we do it earlier the snapshot release triggered compaction
  // may compact files and trigger GC before we can verify there are two files.
  bdb_options.enable_garbage_collection = true;
  Reopen(bdb_options);

  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyDB(data);

  delta_db_impl()->TEST_DeleteObsoleteFiles();
  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  for (auto bfile : delta_files) {
    ASSERT_EQ(kSnappyCompression, bfile->GetCompressionType());
  }

  // Disable compression
  bdb_options.compression = kNoCompression;
  Reopen(bdb_options);
  for (; data_idx < 300; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyDB(data);

  delta_db_impl()->TEST_DeleteObsoleteFiles();
  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  for (auto bfile : delta_files) {
    ASSERT_EQ(kNoCompression, bfile->GetCompressionType());
  }

  // switching different compression types to generate mixed compression types
  bdb_options.compression = kSnappyCompression;
  Reopen(bdb_options);
  for (; data_idx < 400; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  bdb_options.compression = kLZ4Compression;
  Reopen(bdb_options);
  for (; data_idx < 500; data_idx++) {
    PutRandom("put-key" + std::to_string(data_idx), &rnd, &data);
  }
  VerifyDB(data);

  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyDB(data);

  delta_db_impl()->TEST_DeleteObsoleteFiles();
  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  for (auto bfile : delta_files) {
    ASSERT_EQ(kLZ4Compression, bfile->GetCompressionType());
  }
}
#endif  // LZ4
#endif  // SNAPPY

TEST_F(DeltaDBTest, MultipleWriters) {
  Open(DeltaDBOptions());

  std::vector<port::Thread> workers;
  std::vector<std::map<std::string, std::string>> data_set(10);
  for (uint32_t i = 0; i < 10; i++)
    workers.push_back(port::Thread(
        [&](uint32_t id) {
          Random rnd(301 + id);
          for (int j = 0; j < 100; j++) {
            std::string key =
                "key" + std::to_string(id) + "_" + std::to_string(j);
            if (id < 5) {
              PutRandom(key, &rnd, &data_set[id]);
            } else {
              WriteBatch batch;
              PutRandomToWriteBatch(key, &rnd, &batch, &data_set[id]);
              ASSERT_OK(delta_db_->Write(WriteOptions(), &batch));
            }
          }
        },
        i));
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 10; i++) {
    workers[i].join();
    data.insert(data_set[i].begin(), data_set[i].end());
  }
  VerifyDB(data);
}

TEST_F(DeltaDBTest, SstFileManager) {
  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1);
  SstFileManagerImpl *sfm =
      static_cast<SstFileManagerImpl *>(sst_file_manager.get());

  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.enable_garbage_collection = true;
  bdb_options.garbage_collection_cutoff = 1.0;
  Options db_options;

  int files_scheduled_to_delete = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void *arg) {
        assert(arg);
        const std::string *const file_path =
            static_cast<const std::string *>(arg);
        if (file_path->find(".delta") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);

  // Create one obselete file and clean it.
  ASSERT_OK(delta_db_->Put(WriteOptions(), "foo", "bar"));
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  std::shared_ptr<DeltaFile> bfile = delta_files[0];
  ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(bfile));
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  delta_db_impl()->TEST_DeleteObsoleteFiles();

  // Even if SSTFileManager is not set, DB is creating a dummy one.
  ASSERT_EQ(1, files_scheduled_to_delete);
  Destroy();
  // Make sure that DestroyDeltaDB() also goes through delete scheduler.
  ASSERT_EQ(2, files_scheduled_to_delete);
  SyncPoint::GetInstance()->DisableProcessing();
  sfm->WaitForEmptyTrash();
}

TEST_F(DeltaDBTest, SstFileManagerRestart) {
  int files_scheduled_to_delete = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void *arg) {
        assert(arg);
        const std::string *const file_path =
            static_cast<const std::string *>(arg);
        if (file_path->find(".delta") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });

  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1);
  SstFileManagerImpl *sfm =
      static_cast<SstFileManagerImpl *>(sst_file_manager.get());

  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  Options db_options;

  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);
  std::string delta_dir = delta_db_impl()->TEST_delta_dir();
  ASSERT_OK(delta_db_->Put(WriteOptions(), "foo", "bar"));
  Close();

  // Create 3 dummy trash files under the delta_dir
  const auto &fs = db_options.env->GetFileSystem();
  ASSERT_OK(CreateFile(fs, delta_dir + "/000666.delta.trash", "", false));
  ASSERT_OK(CreateFile(fs, delta_dir + "/000888.delta.trash", "", true));
  ASSERT_OK(
      CreateFile(fs, delta_dir + "/something_not_match.trash", "", false));

  // Make sure that reopening the DB rescan the existing trash files
  Open(bdb_options, db_options);
  ASSERT_EQ(files_scheduled_to_delete, 2);

  sfm->WaitForEmptyTrash();

  // There should be exact one file under the delta dir now.
  std::vector<std::string> all_files;
  ASSERT_OK(db_options.env->GetChildren(delta_dir, &all_files));
  int nfiles = 0;
  for (const auto &f : all_files) {
    assert(!f.empty());
    if (f[0] == '.') {
      continue;
    }
    nfiles++;
  }
  ASSERT_EQ(nfiles, 1);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeltaDBTest, SnapshotAndGarbageCollection) {
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.enable_garbage_collection = true;
  bdb_options.garbage_collection_cutoff = 1.0;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.disable_auto_compactions = true;

  // i = when to take snapshot
  for (int i = 0; i < 4; i++) {
    Destroy();
    Open(bdb_options, options);

    const Snapshot *snapshot = nullptr;

    // First file
    ASSERT_OK(Put("key1", "value"));
    if (i == 0) {
      snapshot = delta_db_->GetSnapshot();
    }

    auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
    ASSERT_EQ(1, delta_files.size());
    ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_files[0]));

    // Second file
    ASSERT_OK(Put("key2", "value"));
    if (i == 1) {
      snapshot = delta_db_->GetSnapshot();
    }

    delta_files = delta_db_impl()->TEST_GetDeltaFiles();
    ASSERT_EQ(2, delta_files.size());
    auto bfile = delta_files[1];
    ASSERT_FALSE(bfile->Immutable());
    ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(bfile));

    // Third file
    ASSERT_OK(Put("key3", "value"));
    if (i == 2) {
      snapshot = delta_db_->GetSnapshot();
    }

    ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_TRUE(bfile->Obsolete());
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(),
              bfile->GetObsoleteSequence());

    Delete("key2");
    if (i == 3) {
      snapshot = delta_db_->GetSnapshot();
    }

    ASSERT_EQ(4, delta_db_impl()->TEST_GetDeltaFiles().size());
    delta_db_impl()->TEST_DeleteObsoleteFiles();

    if (i >= 2) {
      // The snapshot shouldn't see data in bfile
      ASSERT_EQ(2, delta_db_impl()->TEST_GetDeltaFiles().size());
      delta_db_->ReleaseSnapshot(snapshot);
    } else {
      // The snapshot will see data in bfile, so the file shouldn't be deleted
      ASSERT_EQ(4, delta_db_impl()->TEST_GetDeltaFiles().size());
      delta_db_->ReleaseSnapshot(snapshot);
      delta_db_impl()->TEST_DeleteObsoleteFiles();
      ASSERT_EQ(2, delta_db_impl()->TEST_GetDeltaFiles().size());
    }
  }
}

TEST_F(DeltaDBTest, ColumnFamilyNotSupported) {
  Options options;
  options.env = mock_env_.get();
  mock_clock_->SetCurrentTime(0);
  Open(DeltaDBOptions(), options);
  ColumnFamilyHandle *default_handle = delta_db_->DefaultColumnFamily();
  ColumnFamilyHandle *handle = nullptr;
  std::string value;
  std::vector<std::string> values;
  // The call simply pass through to base db. It should succeed.
  ASSERT_OK(
      delta_db_->CreateColumnFamily(ColumnFamilyOptions(), "foo", &handle));
  ASSERT_TRUE(
      delta_db_->Put(WriteOptions(), handle, "k", "v").IsNotSupported());
  ASSERT_TRUE(delta_db_->PutWithTTL(WriteOptions(), handle, "k", "v", 60)
                  .IsNotSupported());
  ASSERT_TRUE(delta_db_->PutUntil(WriteOptions(), handle, "k", "v", 100)
                  .IsNotSupported());
  WriteBatch batch;
  ASSERT_OK(batch.Put("k1", "v1"));
  ASSERT_OK(batch.Put(handle, "k2", "v2"));
  ASSERT_TRUE(delta_db_->Write(WriteOptions(), &batch).IsNotSupported());
  ASSERT_TRUE(delta_db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_TRUE(
      delta_db_->Get(ReadOptions(), handle, "k", &value).IsNotSupported());
  auto statuses = delta_db_->MultiGet(ReadOptions(), {default_handle, handle},
                                      {"k1", "k2"}, &values);
  ASSERT_EQ(2, statuses.size());
  ASSERT_TRUE(statuses[0].IsNotSupported());
  ASSERT_TRUE(statuses[1].IsNotSupported());
  ASSERT_EQ(nullptr, delta_db_->NewIterator(ReadOptions(), handle));
  delete handle;
}

TEST_F(DeltaDBTest, GetLiveFilesMetaData) {
  Random rnd(301);

  DeltaDBOptions bdb_options;
  bdb_options.delta_dir = "delta_dir";
  bdb_options.path_relative = true;
  bdb_options.ttl_range_secs = 10;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.env = mock_env_.get();

  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }

  constexpr uint64_t expiration = 1000ULL;
  PutRandomUntil("key100", expiration, &rnd, &data);

  std::vector<LiveFileMetaData> metadata;
  delta_db_->GetLiveFilesMetaData(&metadata);

  ASSERT_EQ(2U, metadata.size());
  // Path should be relative to db_name, but begin with slash.
  const std::string filename1("/delta_dir/000001.delta");
  ASSERT_EQ(filename1, metadata[0].name);
  ASSERT_EQ(1, metadata[0].file_number);
  ASSERT_EQ(0, metadata[0].oldest_ancester_time);
  ASSERT_EQ(kDefaultColumnFamilyName, metadata[0].column_family_name);

  const std::string filename2("/delta_dir/000002.delta");
  ASSERT_EQ(filename2, metadata[1].name);
  ASSERT_EQ(2, metadata[1].file_number);
  ASSERT_EQ(expiration, metadata[1].oldest_ancester_time);
  ASSERT_EQ(kDefaultColumnFamilyName, metadata[1].column_family_name);

  std::vector<std::string> livefile;
  uint64_t mfs;
  ASSERT_OK(delta_db_->GetLiveFiles(livefile, &mfs, false));
  ASSERT_EQ(5U, livefile.size());
  ASSERT_EQ(filename1, livefile[3]);
  ASSERT_EQ(filename2, livefile[4]);
  VerifyDB(data);
}

TEST_F(DeltaDBTest, MigrateFromPlainRocksDB) {
  constexpr size_t kNumKey = 20;
  constexpr size_t kNumIteration = 10;
  Random rnd(301);
  std::map<std::string, std::string> data;
  std::vector<bool> is_delta(kNumKey, false);

  // Write to plain rocksdb.
  Options options;
  options.create_if_missing = true;
  DB *db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + std::to_string(key_index);
    PutRandom(db, key, &rnd, &data);
  }
  VerifyDB(db, data);
  delete db;
  db = nullptr;

  // Open as delta db. Verify it can read existing data.
  Open();
  VerifyDB(delta_db_, data);
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + std::to_string(key_index);
    is_delta[key_index] = true;
    PutRandom(delta_db_, key, &rnd, &data);
  }
  VerifyDB(delta_db_, data);
  delete delta_db_;
  delta_db_ = nullptr;

  // Verify plain db return error for keys written by delta db.
  ASSERT_OK(DB::Open(options, dbname_, &db));
  std::string value;
  for (size_t i = 0; i < kNumKey; i++) {
    std::string key = "key" + std::to_string(i);
    Status s = db->Get(ReadOptions(), key, &value);
    if (data.count(key) == 0) {
      ASSERT_TRUE(s.IsNotFound());
    } else if (is_delta[i]) {
      ASSERT_TRUE(s.IsCorruption());
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(data[key], value);
    }
  }
  delete db;
}

// Test to verify that a NoSpace IOError Status is returned on reaching
// max_db_size limit.
TEST_F(DeltaDBTest, OutOfSpace) {
  // Use mock env to stop wall clock.
  Options options;
  options.env = mock_env_.get();
  DeltaDBOptions bdb_options;
  bdb_options.max_db_size = 200;
  bdb_options.is_fifo = false;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  // Each stored delta has an overhead of about 42 bytes currently.
  // So a small key + a 100 byte delta should take up ~150 bytes in the db.
  std::string value(100, 'v');
  ASSERT_OK(delta_db_->PutWithTTL(WriteOptions(), "key1", value, 60));

  // Putting another delta should fail as ading it would exceed the max_db_size
  // limit.
  Status s = delta_db_->PutWithTTL(WriteOptions(), "key2", value, 60);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_TRUE(s.IsNoSpace());
}

TEST_F(DeltaDBTest, FIFOEviction) {
  DeltaDBOptions bdb_options;
  bdb_options.max_db_size = 200;
  bdb_options.delta_file_size = 100;
  bdb_options.is_fifo = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  std::atomic<int> evict_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaDBImpl::EvictOldestDeltaFile:Evicted",
      [&](void *) { evict_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Each stored delta has an overhead of 32 bytes currently.
  // So a 100 byte delta should take up 132 bytes.
  std::string value(100, 'v');
  ASSERT_OK(delta_db_->PutWithTTL(WriteOptions(), "key1", value, 10));
  VerifyDB({{"key1", value}});

  ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());

  // Adding another 100 bytes delta would take the total size to 264 bytes
  // (2*132). max_db_size will be exceeded
  // than max_db_size and trigger FIFO eviction.
  ASSERT_OK(delta_db_->PutWithTTL(WriteOptions(), "key2", value, 60));
  ASSERT_EQ(1, evict_count);
  // key1 will exist until corresponding file be deleted.
  VerifyDB({{"key1", value}, {"key2", value}});

  // Adding another 100 bytes delta without TTL.
  ASSERT_OK(delta_db_->Put(WriteOptions(), "key3", value));
  ASSERT_EQ(2, evict_count);
  // key1 and key2 will exist until corresponding file be deleted.
  VerifyDB({{"key1", value}, {"key2", value}, {"key3", value}});

  // The fourth delta file, without TTL.
  ASSERT_OK(delta_db_->Put(WriteOptions(), "key4", value));
  ASSERT_EQ(3, evict_count);
  VerifyDB(
      {{"key1", value}, {"key2", value}, {"key3", value}, {"key4", value}});

  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(4, delta_files.size());
  ASSERT_TRUE(delta_files[0]->Obsolete());
  ASSERT_TRUE(delta_files[1]->Obsolete());
  ASSERT_TRUE(delta_files[2]->Obsolete());
  ASSERT_FALSE(delta_files[3]->Obsolete());
  auto obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
  ASSERT_EQ(3, obsolete_files.size());
  ASSERT_EQ(delta_files[0], obsolete_files[0]);
  ASSERT_EQ(delta_files[1], obsolete_files[1]);
  ASSERT_EQ(delta_files[2], obsolete_files[2]);

  delta_db_impl()->TEST_DeleteObsoleteFiles();
  obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
  ASSERT_TRUE(obsolete_files.empty());
  VerifyDB({{"key4", value}});
}

TEST_F(DeltaDBTest, FIFOEviction_NoOldestFileToEvict) {
  Options options;
  DeltaDBOptions bdb_options;
  bdb_options.max_db_size = 1000;
  bdb_options.delta_file_size = 5000;
  bdb_options.is_fifo = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  std::atomic<int> evict_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaDBImpl::EvictOldestDeltaFile:Evicted",
      [&](void *) { evict_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string value(2000, 'v');
  ASSERT_TRUE(Put("foo", std::string(2000, 'v')).IsNoSpace());
  ASSERT_EQ(0, evict_count);
}

TEST_F(DeltaDBTest, FIFOEviction_NoEnoughDeltaFilesToEvict) {
  DeltaDBOptions bdb_options;
  bdb_options.is_fifo = true;
  bdb_options.min_delta_size = 100;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  options.env = mock_env_.get();
  options.disable_auto_compactions = true;
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  Open(bdb_options, options);

  ASSERT_EQ(0, delta_db_impl()->TEST_live_sst_size());
  std::string small_value(50, 'v');
  std::map<std::string, std::string> data;
  // Insert some data into LSM tree to make sure FIFO eviction take SST
  // file size into account.
  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), small_value, &data));
  }
  ASSERT_OK(delta_db_->Flush(FlushOptions()));
  uint64_t live_sst_size = 0;
  ASSERT_TRUE(delta_db_->GetIntProperty(DB::Properties::kTotalSstFilesSize,
                                        &live_sst_size));
  ASSERT_TRUE(live_sst_size > 0);
  ASSERT_EQ(live_sst_size, delta_db_impl()->TEST_live_sst_size());

  bdb_options.max_db_size = live_sst_size + 2000;
  Reopen(bdb_options, options);
  ASSERT_EQ(live_sst_size, delta_db_impl()->TEST_live_sst_size());

  std::string value_1k(1000, 'v');
  ASSERT_OK(PutWithTTL("large_key1", value_1k, 60, &data));
  ASSERT_EQ(0, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB(data);
  // large_key2 evicts large_key1
  ASSERT_OK(PutWithTTL("large_key2", value_1k, 60, &data));
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  data.erase("large_key1");
  VerifyDB(data);
  // large_key3 get no enough space even after evicting large_key2, so it
  // instead return no space error.
  std::string value_2k(2000, 'v');
  ASSERT_TRUE(PutWithTTL("large_key3", value_2k, 60).IsNoSpace());
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  // Verify large_key2 still exists.
  VerifyDB(data);
}

// Test flush or compaction will trigger FIFO eviction since they update
// total SST file size.
TEST_F(DeltaDBTest, FIFOEviction_TriggerOnSSTSizeChange) {
  DeltaDBOptions bdb_options;
  bdb_options.max_db_size = 1000;
  bdb_options.is_fifo = true;
  bdb_options.min_delta_size = 100;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  options.env = mock_env_.get();
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  options.compression = kNoCompression;
  Open(bdb_options, options);

  std::string value(800, 'v');
  ASSERT_OK(PutWithTTL("large_key", value, 60));
  ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
  ASSERT_EQ(0, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB({{"large_key", value}});

  // Insert some small keys and flush to bring DB out of space.
  std::map<std::string, std::string> data;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), "v", &data));
  }
  ASSERT_OK(delta_db_->Flush(FlushOptions()));

  // Verify large_key is deleted by FIFO eviction.
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(0, delta_db_impl()->TEST_GetDeltaFiles().size());
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB(data);
}

TEST_F(DeltaDBTest, InlineSmallValues) {
  constexpr uint64_t kMaxExpiration = 1000;
  Random rnd(301);
  DeltaDBOptions bdb_options;
  bdb_options.ttl_range_secs = kMaxExpiration;
  bdb_options.min_delta_size = 100;
  bdb_options.delta_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  mock_clock_->SetCurrentTime(0);
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  std::map<std::string, KeyVersion> versions;
  for (size_t i = 0; i < 1000; i++) {
    bool is_small_value = rnd.Next() % 2;
    bool has_ttl = rnd.Next() % 2;
    uint64_t expiration = rnd.Next() % kMaxExpiration;
    int len = is_small_value ? 50 : 200;
    std::string key = "key" + std::to_string(i);
    std::string value = rnd.HumanReadableString(len);
    std::string delta_index;
    data[key] = value;
    SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;
    if (!has_ttl) {
      ASSERT_OK(delta_db_->Put(WriteOptions(), key, value));
    } else {
      ASSERT_OK(delta_db_->PutUntil(WriteOptions(), key, value, expiration));
    }
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);
    versions[key] =
        KeyVersion(key, value, sequence,
                   (is_small_value && !has_ttl) ? kTypeValue : kTypeDeltaIndex);
  }
  VerifyDB(data);
  VerifyBaseDB(versions);
  auto *bdb_impl = static_cast<DeltaDBImpl *>(delta_db_);
  auto delta_files = bdb_impl->TEST_GetDeltaFiles();
  ASSERT_EQ(2, delta_files.size());
  std::shared_ptr<DeltaFile> non_ttl_file;
  std::shared_ptr<DeltaFile> ttl_file;
  if (delta_files[0]->HasTTL()) {
    ttl_file = delta_files[0];
    non_ttl_file = delta_files[1];
  } else {
    non_ttl_file = delta_files[0];
    ttl_file = delta_files[1];
  }
  ASSERT_FALSE(non_ttl_file->HasTTL());
  ASSERT_TRUE(ttl_file->HasTTL());
}

TEST_F(DeltaDBTest, UserCompactionFilter) {
  class CustomerFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice & /*key*/, const Slice &value,
                std::string *new_value, bool *value_changed) const override {
      *value_changed = false;
      // changing value size to test value transitions between inlined data
      // and stored-in-delta data
      if (value.size() % 4 == 1) {
        *new_value = value.ToString();
        // double size by duplicating value
        *new_value += *new_value;
        *value_changed = true;
        return false;
      } else if (value.size() % 3 == 1) {
        *new_value = value.ToString();
        // trancate value size by half
        *new_value = new_value->substr(0, new_value->size() / 2);
        *value_changed = true;
        return false;
      } else if (value.size() % 2 == 1) {
        return true;
      }
      return false;
    }
    bool IgnoreSnapshots() const override { return true; }
    const char *Name() const override { return "CustomerFilter"; }
  };
  class CustomerFilterFactory : public CompactionFilterFactory {
    const char *Name() const override { return "CustomerFilterFactory"; }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context & /*context*/) override {
      return std::unique_ptr<CompactionFilter>(new CustomerFilter());
    }
  };

  constexpr size_t kNumPuts = 1 << 10;
  // Generate both inlined and delta value
  constexpr uint64_t kMinValueSize = 1 << 6;
  constexpr uint64_t kMaxValueSize = 1 << 8;
  constexpr uint64_t kMinDeltaSize = 1 << 7;
  static_assert(kMinValueSize < kMinDeltaSize, "");
  static_assert(kMaxValueSize > kMinDeltaSize, "");

  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = kMinDeltaSize;
  bdb_options.delta_file_size = kMaxValueSize * 10;
  bdb_options.disable_background_tasks = true;
  if (Snappy_Supported()) {
    bdb_options.compression = CompressionType::kSnappyCompression;
  }
  // case_num == 0: Test user defined compaction filter
  // case_num == 1: Test user defined compaction filter factory
  for (int case_num = 0; case_num < 2; case_num++) {
    Options options;
    if (case_num == 0) {
      options.compaction_filter = new CustomerFilter();
    } else {
      options.compaction_filter_factory.reset(new CustomerFilterFactory());
    }
    options.disable_auto_compactions = true;
    options.env = mock_env_.get();
    options.statistics = CreateDBStatistics();
    Open(bdb_options, options);

    std::map<std::string, std::string> data;
    std::map<std::string, std::string> data_after_compact;
    Random rnd(301);
    uint64_t value_size = kMinValueSize;
    int drop_record = 0;
    for (size_t i = 0; i < kNumPuts; ++i) {
      std::ostringstream oss;
      oss << "key" << std::setw(4) << std::setfill('0') << i;

      const std::string key(oss.str());
      const std::string value = rnd.HumanReadableString((int)value_size);
      const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

      ASSERT_OK(Put(key, value));
      ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);

      data[key] = value;
      if (value.length() % 4 == 1) {
        data_after_compact[key] = value + value;
      } else if (value.length() % 3 == 1) {
        data_after_compact[key] = value.substr(0, value.size() / 2);
      } else if (value.length() % 2 == 1) {
        ++drop_record;
      } else {
        data_after_compact[key] = value;
      }

      if (++value_size > kMaxValueSize) {
        value_size = kMinValueSize;
      }
    }
    // Verify full data set
    VerifyDB(data);
    // Applying compaction filter for records
    ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    // Verify data after compaction, only value with even length left.
    VerifyDB(data_after_compact);
    ASSERT_EQ(drop_record,
              options.statistics->getTickerCount(COMPACTION_KEY_DROP_USER));
    delete options.compaction_filter;
    Destroy();
  }
}

// Test user comapction filter when there is IO error on delta data.
TEST_F(DeltaDBTest, UserCompactionFilter_DeltaIOError) {
  class CustomerFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice & /*key*/, const Slice &value,
                std::string *new_value, bool *value_changed) const override {
      *new_value = value.ToString() + "_new";
      *value_changed = true;
      return false;
    }
    bool IgnoreSnapshots() const override { return true; }
    const char *Name() const override { return "CustomerFilter"; }
  };

  constexpr size_t kNumPuts = 100;
  constexpr int kValueSize = 100;

  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.delta_file_size = kValueSize * 10;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kNoCompression;

  std::vector<std::string> io_failure_cases = {
      "DeltaDBImpl::CreateDeltaFileAndWriter",
      "DeltaIndexCompactionFilterBase::WriteDeltaToNewFile",
      "DeltaDBImpl::CloseDeltaFile"};

  for (size_t case_num = 0; case_num < io_failure_cases.size(); case_num++) {
    Options options;
    options.compaction_filter = new CustomerFilter();
    options.disable_auto_compactions = true;
    options.env = fault_injection_env_.get();
    options.statistics = CreateDBStatistics();
    Open(bdb_options, options);

    std::map<std::string, std::string> data;
    Random rnd(301);
    for (size_t i = 0; i < kNumPuts; ++i) {
      std::ostringstream oss;
      oss << "key" << std::setw(4) << std::setfill('0') << i;

      const std::string key(oss.str());
      const std::string value = rnd.HumanReadableString(kValueSize);
      const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

      ASSERT_OK(Put(key, value));
      ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);
      data[key] = value;
    }

    // Verify full data set
    VerifyDB(data);

    SyncPoint::GetInstance()->SetCallBack(
        io_failure_cases[case_num], [&](void * /*arg*/) {
          fault_injection_env_->SetFilesystemActive(false, Status::IOError());
        });
    SyncPoint::GetInstance()->EnableProcessing();
    auto s = delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    ASSERT_TRUE(s.IsIOError());

    // Reactivate file system to allow test to verify and close DB.
    fault_injection_env_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    // Verify full data set after compaction failure
    VerifyDB(data);

    delete options.compaction_filter;
    Destroy();
  }
}

// Test comapction filter should remove any expired delta index.
TEST_F(DeltaDBTest, FilterExpiredDeltaIndex) {
  constexpr size_t kNumKeys = 100;
  constexpr size_t kNumPuts = 1000;
  constexpr uint64_t kMaxExpiration = 1000;
  constexpr uint64_t kCompactTime = 500;
  constexpr uint64_t kMinDeltaSize = 100;
  Random rnd(301);
  mock_clock_->SetCurrentTime(0);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = kMinDeltaSize;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, std::string> data_after_compact;
  for (size_t i = 0; i < kNumPuts; i++) {
    bool is_small_value = rnd.Next() % 2;
    bool has_ttl = rnd.Next() % 2;
    uint64_t expiration = rnd.Next() % kMaxExpiration;
    int len = is_small_value ? 10 : 200;
    std::string key = "key" + std::to_string(rnd.Next() % kNumKeys);
    std::string value = rnd.HumanReadableString(len);
    if (!has_ttl) {
      if (is_small_value) {
        std::string delta_entry;
        DeltaIndex::EncodeInlinedTTL(&delta_entry, expiration, value);
        // Fake delta index with TTL. See what it will do.
        ASSERT_GT(kMinDeltaSize, delta_entry.size());
        value = delta_entry;
      }
      ASSERT_OK(Put(key, value));
      data_after_compact[key] = value;
    } else {
      ASSERT_OK(PutUntil(key, value, expiration));
      if (expiration <= kCompactTime) {
        data_after_compact.erase(key);
      } else {
        data_after_compact[key] = value;
      }
    }
    data[key] = value;
  }
  VerifyDB(data);

  mock_clock_->SetCurrentTime(kCompactTime);
  // Take a snapshot before compaction. Make sure expired delta indexes is
  // filtered regardless of snapshot.
  const Snapshot *snapshot = delta_db_->GetSnapshot();
  // Issue manual compaction to trigger compaction filter.
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  delta_db_->ReleaseSnapshot(snapshot);
  // Verify expired delta index are filtered.
  std::vector<KeyVersion> versions;
  const size_t kMaxKeys = 10000;
  ASSERT_OK(GetAllKeyVersions(delta_db_, "", "", kMaxKeys, &versions));
  ASSERT_EQ(data_after_compact.size(), versions.size());
  for (auto &version : versions) {
    ASSERT_TRUE(data_after_compact.count(version.user_key) > 0);
  }
  VerifyDB(data_after_compact);
}

// Test compaction filter should remove any delta index where corresponding
// delta file has been removed.
TEST_F(DeltaDBTest, FilterFileNotAvailable) {
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.disable_auto_compactions = true;
  Open(bdb_options, options);

  ASSERT_OK(Put("foo", "v1"));
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  ASSERT_EQ(1, delta_files[0]->DeltaFileNumber());
  ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_files[0]));

  ASSERT_OK(Put("bar", "v2"));
  delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(2, delta_files.size());
  ASSERT_EQ(2, delta_files[1]->DeltaFileNumber());
  ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_files[1]));

  const size_t kMaxKeys = 10000;

  DB *base_db = delta_db_->GetRootDB();
  std::vector<KeyVersion> versions;
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  // Remove the first delta file and compact. foo should be remove from base db.
  delta_db_impl()->TEST_ObsoleteDeltaFile(delta_files[0]);
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(1, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  VerifyDB({{"bar", "v2"}});

  // Remove the second delta file and compact. bar should be remove from base
  // db.
  delta_db_impl()->TEST_ObsoleteDeltaFile(delta_files[1]);
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(0, versions.size());
  VerifyDB({});
}

// Test compaction filter should filter any inlined TTL keys that would have
// been dropped by last FIFO eviction if they are store out-of-line.
TEST_F(DeltaDBTest, FilterForFIFOEviction) {
  Random rnd(215);
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 100;
  bdb_options.ttl_range_secs = 60;
  bdb_options.max_db_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  mock_clock_->SetCurrentTime(0);
  options.env = mock_env_.get();
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  options.disable_auto_compactions = true;
  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, std::string> data_after_compact;
  // Insert some small values that will be inlined.
  for (int i = 0; i < 1000; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = rnd.HumanReadableString(50);
    uint64_t ttl = rnd.Next() % 120 + 1;
    ASSERT_OK(PutWithTTL(key, value, ttl, &data));
    if (ttl >= 60) {
      data_after_compact[key] = value;
    }
  }
  uint64_t num_keys_to_evict = data.size() - data_after_compact.size();
  ASSERT_OK(delta_db_->Flush(FlushOptions()));
  uint64_t live_sst_size = delta_db_impl()->TEST_live_sst_size();
  ASSERT_GT(live_sst_size, 0);
  VerifyDB(data);

  bdb_options.max_db_size = live_sst_size + 30000;
  bdb_options.is_fifo = true;
  Reopen(bdb_options, options);
  VerifyDB(data);

  // Put two large values, each on a different delta file.
  std::string large_value(10000, 'v');
  ASSERT_OK(PutWithTTL("large_key1", large_value, 90));
  ASSERT_OK(PutWithTTL("large_key2", large_value, 150));
  ASSERT_EQ(2, delta_db_impl()->TEST_GetDeltaFiles().size());
  ASSERT_EQ(0, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  data["large_key1"] = large_value;
  data["large_key2"] = large_value;
  VerifyDB(data);

  // Put a third large value which will bring the DB out of space.
  // FIFO eviction will evict the file of large_key1.
  ASSERT_OK(PutWithTTL("large_key3", large_value, 150));
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  ASSERT_EQ(2, delta_db_impl()->TEST_GetDeltaFiles().size());
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
  data.erase("large_key1");
  data["large_key3"] = large_value;
  VerifyDB(data);

  // Putting some more small values. These values shouldn't be evicted by
  // compaction filter since they are inserted after FIFO eviction.
  ASSERT_OK(PutWithTTL("foo", "v", 30, &data_after_compact));
  ASSERT_OK(PutWithTTL("bar", "v", 30, &data_after_compact));

  // FIFO eviction doesn't trigger again since there enough room for the flush.
  ASSERT_OK(delta_db_->Flush(FlushOptions()));
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));

  // Manual compact and check if compaction filter evict those keys with
  // expiration < 60.
  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // All keys with expiration < 60, plus large_key1 is filtered by
  // compaction filter.
  ASSERT_EQ(num_keys_to_evict + 1,
            statistics->getTickerCount(DELTA_DB_DELTA_INDEX_EVICTED_COUNT));
  ASSERT_EQ(1, statistics->getTickerCount(DELTA_DB_FIFO_NUM_FILES_EVICTED));
  ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
  data_after_compact["large_key2"] = large_value;
  data_after_compact["large_key3"] = large_value;
  VerifyDB(data_after_compact);
}

TEST_F(DeltaDBTest, GarbageCollection) {
  constexpr size_t kNumPuts = 1 << 10;

  constexpr uint64_t kExpiration = 1000;
  constexpr uint64_t kCompactTime = 500;

  constexpr uint64_t kKeySize = 7;  // "key" + 4 digits

  constexpr uint64_t kSmallValueSize = 1 << 6;
  constexpr uint64_t kLargeValueSize = 1 << 8;
  constexpr uint64_t kMinDeltaSize = 1 << 7;
  static_assert(kSmallValueSize < kMinDeltaSize, "");
  static_assert(kLargeValueSize > kMinDeltaSize, "");

  constexpr size_t kDeltasPerFile = 8;
  constexpr size_t kNumDeltaFiles = kNumPuts / kDeltasPerFile;
  constexpr uint64_t kDeltaFileSize =
      DeltaLogHeader::kSize +
      (DeltaLogRecord::kHeaderSize + kKeySize + kLargeValueSize) *
          kDeltasPerFile;

  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = kMinDeltaSize;
  bdb_options.delta_file_size = kDeltaFileSize;
  bdb_options.enable_garbage_collection = true;
  bdb_options.garbage_collection_cutoff = 0.25;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();

  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, KeyVersion> delta_value_versions;
  std::map<std::string, DeltaIndexVersion> delta_index_versions;

  Random rnd(301);

  // Add a bunch of large non-TTL values. These will be written to non-TTL
  // delta files and will be subject to GC.
  for (size_t i = 0; i < kNumPuts; ++i) {
    std::ostringstream oss;
    oss << "key" << std::setw(4) << std::setfill('0') << i;

    const std::string key(oss.str());
    const std::string value = rnd.HumanReadableString(kLargeValueSize);
    const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(Put(key, value));
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    delta_value_versions[key] =
        KeyVersion(key, value, sequence, kTypeDeltaIndex);
    delta_index_versions[key] =
        DeltaIndexVersion(key, /* file_number */ (i >> 3) + 1, kNoExpiration,
                          sequence, kTypeDeltaIndex);
  }

  // Add some small and/or TTL values that will be ignored during GC.
  // First, add a large TTL value will be written to its own TTL delta file.
  {
    const std::string key("key2000");
    const std::string value = rnd.HumanReadableString(kLargeValueSize);
    const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(PutUntil(key, value, kExpiration));
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    delta_value_versions[key] =
        KeyVersion(key, value, sequence, kTypeDeltaIndex);
    delta_index_versions[key] =
        DeltaIndexVersion(key, /* file_number */ kNumDeltaFiles + 1,
                          kExpiration, sequence, kTypeDeltaIndex);
  }

  // Now add a small TTL value (which will be inlined).
  {
    const std::string key("key3000");
    const std::string value = rnd.HumanReadableString(kSmallValueSize);
    const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(PutUntil(key, value, kExpiration));
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    delta_value_versions[key] =
        KeyVersion(key, value, sequence, kTypeDeltaIndex);
    delta_index_versions[key] = DeltaIndexVersion(
        key, kInvalidDeltaFileNumber, kExpiration, sequence, kTypeDeltaIndex);
  }

  // Finally, add a small non-TTL value (which will be stored as a regular
  // value).
  {
    const std::string key("key4000");
    const std::string value = rnd.HumanReadableString(kSmallValueSize);
    const SequenceNumber sequence = delta_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(Put(key, value));
    ASSERT_EQ(delta_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    delta_value_versions[key] = KeyVersion(key, value, sequence, kTypeValue);
    delta_index_versions[key] = DeltaIndexVersion(
        key, kInvalidDeltaFileNumber, kNoExpiration, sequence, kTypeValue);
  }

  VerifyDB(data);
  VerifyBaseDB(delta_value_versions);
  VerifyBaseDBDeltaIndex(delta_index_versions);

  // At this point, we should have 128 immutable non-TTL files with file numbers
  // 1..128.
  {
    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), kNumDeltaFiles);
    for (size_t i = 0; i < kNumDeltaFiles; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 1);
      ASSERT_EQ(live_imm_files[i]->GetFileSize(),
                kDeltaFileSize + DeltaLogFooter::kSize);
    }
  }

  mock_clock_->SetCurrentTime(kCompactTime);

  ASSERT_OK(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // We expect the data to remain the same and the deltas from the oldest N
  // files to be moved to new files. Sequence numbers get zeroed out during the
  // compaction.
  VerifyDB(data);

  for (auto &pair : delta_value_versions) {
    KeyVersion &version = pair.second;
    version.sequence = 0;
  }

  VerifyBaseDB(delta_value_versions);

  const uint64_t cutoff = static_cast<uint64_t>(
      bdb_options.garbage_collection_cutoff * kNumDeltaFiles);
  for (auto &pair : delta_index_versions) {
    DeltaIndexVersion &version = pair.second;

    version.sequence = 0;

    if (version.file_number == kInvalidDeltaFileNumber) {
      continue;
    }

    if (version.file_number > cutoff) {
      continue;
    }

    version.file_number += kNumDeltaFiles + 1;
  }

  VerifyBaseDBDeltaIndex(delta_index_versions);

  const Statistics *const statistics = options.statistics.get();
  assert(statistics);

  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_FILES), cutoff);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_NEW_FILES), cutoff);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_FAILURES), 0);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_KEYS_RELOCATED),
            cutoff * kDeltasPerFile);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_BYTES_RELOCATED),
            cutoff * kDeltasPerFile * kLargeValueSize);

  // At this point, we should have 128 immutable non-TTL files with file numbers
  // 33..128 and 130..161. (129 was taken by the TTL delta file.)
  {
    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), kNumDeltaFiles);
    for (size_t i = 0; i < kNumDeltaFiles; ++i) {
      uint64_t expected_file_number = i + cutoff + 1;
      if (expected_file_number > kNumDeltaFiles) {
        ++expected_file_number;
      }

      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), expected_file_number);
      ASSERT_EQ(live_imm_files[i]->GetFileSize(),
                kDeltaFileSize + DeltaLogFooter::kSize);
    }
  }
}

TEST_F(DeltaDBTest, GarbageCollectionFailure) {
  DeltaDBOptions bdb_options;
  bdb_options.min_delta_size = 0;
  bdb_options.enable_garbage_collection = true;
  bdb_options.garbage_collection_cutoff = 1.0;
  bdb_options.disable_background_tasks = true;

  Options db_options;
  db_options.statistics = CreateDBStatistics();

  Open(bdb_options, db_options);

  // Write a couple of valid deltas.
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("dead", "beef"));

  // Write a fake delta reference into the base DB that points to a non-existing
  // delta file.
  std::string delta_index;
  DeltaIndex::EncodeDelta(&delta_index, /* file_number */ 1000,
                          /* offset */ 1234,
                          /* size */ 5678, kNoCompression);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutDeltaIndex(
      &batch, delta_db_->DefaultColumnFamily()->GetID(), "key", delta_index));
  ASSERT_OK(delta_db_->GetRootDB()->Write(WriteOptions(), &batch));

  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(delta_files.size(), 1);
  auto delta_file = delta_files[0];
  ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_file));

  ASSERT_TRUE(delta_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsIOError());

  const Statistics *const statistics = db_options.statistics.get();
  assert(statistics);

  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_FILES), 0);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_NEW_FILES), 1);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_FAILURES), 1);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_NUM_KEYS_RELOCATED), 2);
  ASSERT_EQ(statistics->getTickerCount(DELTA_DB_GC_BYTES_RELOCATED), 7);
}

// File should be evicted after expiration.
TEST_F(DeltaDBTest, EvictExpiredFile) {
  DeltaDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);
  mock_clock_->SetCurrentTime(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  auto delta_file = delta_files[0];
  ASSERT_FALSE(delta_file->Immutable());
  ASSERT_FALSE(delta_file->Obsolete());
  VerifyDB(data);
  mock_clock_->SetCurrentTime(250);
  // The key should expired now.
  delta_db_impl()->TEST_EvictExpiredFiles();
  ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
  ASSERT_EQ(1, delta_db_impl()->TEST_GetObsoleteFiles().size());
  ASSERT_TRUE(delta_file->Immutable());
  ASSERT_TRUE(delta_file->Obsolete());
  delta_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(0, delta_db_impl()->TEST_GetDeltaFiles().size());
  ASSERT_EQ(0, delta_db_impl()->TEST_GetObsoleteFiles().size());
  // Make sure we don't return garbage value after delta file being evicted,
  // but the delta index still exists in the LSM tree.
  std::string val = "";
  ASSERT_TRUE(delta_db_->Get(ReadOptions(), "foo", &val).IsNotFound());
  ASSERT_EQ("", val);
}

TEST_F(DeltaDBTest, DisableFileDeletions) {
  DeltaDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (bool force : {true, false}) {
    ASSERT_OK(Put("foo", "v", &data));
    auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
    ASSERT_EQ(1, delta_files.size());
    auto delta_file = delta_files[0];
    ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_file));
    delta_db_impl()->TEST_ObsoleteDeltaFile(delta_file);
    ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
    ASSERT_EQ(1, delta_db_impl()->TEST_GetObsoleteFiles().size());
    // Call DisableFileDeletions twice.
    ASSERT_OK(delta_db_->DisableFileDeletions());
    ASSERT_OK(delta_db_->DisableFileDeletions());
    // File deletions should be disabled.
    delta_db_impl()->TEST_DeleteObsoleteFiles();
    ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
    ASSERT_EQ(1, delta_db_impl()->TEST_GetObsoleteFiles().size());
    VerifyDB(data);
    // Enable file deletions once. If force=true, file deletion is enabled.
    // Otherwise it needs to enable it for a second time.
    ASSERT_OK(delta_db_->EnableFileDeletions(force));
    delta_db_impl()->TEST_DeleteObsoleteFiles();
    if (!force) {
      ASSERT_EQ(1, delta_db_impl()->TEST_GetDeltaFiles().size());
      ASSERT_EQ(1, delta_db_impl()->TEST_GetObsoleteFiles().size());
      VerifyDB(data);
      // Call EnableFileDeletions a second time.
      ASSERT_OK(delta_db_->EnableFileDeletions(false));
      delta_db_impl()->TEST_DeleteObsoleteFiles();
    }
    // Regardless of value of `force`, file should be deleted by now.
    ASSERT_EQ(0, delta_db_impl()->TEST_GetDeltaFiles().size());
    ASSERT_EQ(0, delta_db_impl()->TEST_GetObsoleteFiles().size());
    VerifyDB({});
  }
}

TEST_F(DeltaDBTest, MaintainDeltaFileToSstMapping) {
  DeltaDBOptions bdb_options;
  bdb_options.enable_garbage_collection = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  // Register some dummy delta files.
  delta_db_impl()->TEST_AddDummyDeltaFile(1, /* immutable_sequence */ 200);
  delta_db_impl()->TEST_AddDummyDeltaFile(2, /* immutable_sequence */ 300);
  delta_db_impl()->TEST_AddDummyDeltaFile(3, /* immutable_sequence */ 400);
  delta_db_impl()->TEST_AddDummyDeltaFile(4, /* immutable_sequence */ 500);
  delta_db_impl()->TEST_AddDummyDeltaFile(5, /* immutable_sequence */ 600);

  // Initialize the delta <-> SST file mapping. First, add some SST files with
  // delta file references, then some without.
  std::vector<LiveFileMetaData> live_files;

  for (uint64_t i = 1; i <= 10; ++i) {
    LiveFileMetaData live_file;
    live_file.file_number = i;
    live_file.oldest_delta_file_number = ((i - 1) % 5) + 1;

    live_files.emplace_back(live_file);
  }

  for (uint64_t i = 11; i <= 20; ++i) {
    LiveFileMetaData live_file;
    live_file.file_number = i;

    live_files.emplace_back(live_file);
  }

  delta_db_impl()->TEST_InitializeDeltaFileToSstMapping(live_files);

  // Check that the delta <-> SST mappings have been correctly initialized.
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();

  ASSERT_EQ(delta_files.size(), 5);

  {
    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 1);
    }

    ASSERT_TRUE(delta_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  {
    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 1);
    }

    ASSERT_TRUE(delta_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a flush where the SST does not reference any delta files.
  {
    FlushJobInfo info{};
    info.file_number = 21;
    info.smallest_seqno = 1;
    info.largest_seqno = 100;

    delta_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 1);
    }

    ASSERT_TRUE(delta_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a flush where the SST references a delta file.
  {
    FlushJobInfo info{};
    info.file_number = 22;
    info.oldest_delta_file_number = 5;
    info.smallest_seqno = 101;
    info.largest_seqno = 200;

    delta_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 1);
    }

    ASSERT_TRUE(delta_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a compaction. Some inputs and outputs have delta file references,
  // some don't. There is also a trivial move (which means the SST appears on
  // both the input and the output list). Delta file 1 loses all its linked
  // SSTs, and since it got marked immutable at sequence number 200 which has
  // already been flushed, it can be marked obsolete.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 1, 1});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 2, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 6, 1});
    info.input_file_infos.emplace_back(
        CompactionFileInfo{1, 11, kInvalidDeltaFileNumber});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 23, 3});
    info.output_file_infos.emplace_back(
        CompactionFileInfo{2, 24, kInvalidDeltaFileNumber});

    delta_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {7}, {3, 8, 23}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 2);
    }

    auto obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->DeltaFileNumber(), 1);
  }

  // Simulate a failed compaction. No mappings should be updated.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 7, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 25, 3});
    info.status = Status::Corruption();

    delta_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {7}, {3, 8, 23}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 2);
    }

    auto obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->DeltaFileNumber(), 1);
  }

  // Simulate another compaction. Delta file 2 loses all its linked SSTs
  // but since it got marked immutable at sequence number 300 which hasn't
  // been flushed yet, it cannot be marked obsolete at this point.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 7, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 25, 3});

    delta_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {}, {3, 8, 23, 25}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 2);
    }

    auto obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->DeltaFileNumber(), 1);
  }

  // Simulate a flush with largest sequence number 300. This will make it
  // possible to mark delta file 2 obsolete.
  {
    FlushJobInfo info{};
    info.file_number = 26;
    info.smallest_seqno = 201;
    info.largest_seqno = 300;

    delta_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {}, {3, 8, 23, 25}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{true, true, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto &delta_file = delta_files[i];
      ASSERT_EQ(delta_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(delta_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = delta_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(live_imm_files[i]->DeltaFileNumber(), i + 3);
    }

    auto obsolete_files = delta_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 2);
    ASSERT_EQ(obsolete_files[0]->DeltaFileNumber(), 1);
    ASSERT_EQ(obsolete_files[1]->DeltaFileNumber(), 2);
  }
}

TEST_F(DeltaDBTest, ShutdownWait) {
  DeltaDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.min_delta_size = 0;
  bdb_options.disable_background_tasks = false;
  Options options;
  options.env = mock_env_.get();

  SyncPoint::GetInstance()->LoadDependency({
      {"DeltaDBImpl::EvictExpiredFiles:0", "DeltaDBTest.ShutdownWait:0"},
      {"DeltaDBTest.ShutdownWait:1", "DeltaDBImpl::EvictExpiredFiles:1"},
      {"DeltaDBImpl::EvictExpiredFiles:2", "DeltaDBTest.ShutdownWait:2"},
      {"DeltaDBTest.ShutdownWait:3", "DeltaDBImpl::EvictExpiredFiles:3"},
  });
  // Force all tasks to be scheduled immediately.
  SyncPoint::GetInstance()->SetCallBack(
      "TimeQueue::Add:item.end", [&](void *arg) {
        std::chrono::steady_clock::time_point *tp =
            static_cast<std::chrono::steady_clock::time_point *>(arg);
        *tp =
            std::chrono::steady_clock::now() - std::chrono::milliseconds(10000);
      });

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaDBImpl::EvictExpiredFiles:cb", [&](void * /*arg*/) {
        // Sleep 3 ms to increase the chance of data race.
        // We've synced up the code so that EvictExpiredFiles()
        // is called concurrently with ~DeltaDBImpl().
        // ~DeltaDBImpl() is supposed to wait for all background
        // task to shutdown before doing anything else. In order
        // to use the same test to reproduce a bug of the waiting
        // logic, we wait a little bit here, so that TSAN can
        // catch the data race.
        // We should improve the test if we find a better way.
        Env::Default()->SleepForMicroseconds(3000);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Open(bdb_options, options);
  mock_clock_->SetCurrentTime(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(1, delta_files.size());
  auto delta_file = delta_files[0];
  ASSERT_FALSE(delta_file->Immutable());
  ASSERT_FALSE(delta_file->Obsolete());
  VerifyDB(data);

  TEST_SYNC_POINT("DeltaDBTest.ShutdownWait:0");
  mock_clock_->SetCurrentTime(250);
  // The key should expired now.
  TEST_SYNC_POINT("DeltaDBTest.ShutdownWait:1");

  TEST_SYNC_POINT("DeltaDBTest.ShutdownWait:2");
  TEST_SYNC_POINT("DeltaDBTest.ShutdownWait:3");
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeltaDBTest, SyncDeltaFileBeforeClose) {
  Options options;
  options.statistics = CreateDBStatistics();

  DeltaDBOptions delta_options;
  delta_options.min_delta_size = 0;
  delta_options.bytes_per_sync = 1 << 20;
  delta_options.disable_background_tasks = true;

  Open(delta_options, options);

  ASSERT_OK(Put("foo", "bar"));

  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(delta_files.size(), 1);

  ASSERT_OK(delta_db_impl()->TEST_CloseDeltaFile(delta_files[0]));
  ASSERT_EQ(options.statistics->getTickerCount(DELTA_DB_DELTA_FILE_SYNCED), 1);
}

TEST_F(DeltaDBTest, SyncDeltaFileBeforeCloseIOError) {
  Options options;
  options.env = fault_injection_env_.get();

  DeltaDBOptions delta_options;
  delta_options.min_delta_size = 0;
  delta_options.bytes_per_sync = 1 << 20;
  delta_options.disable_background_tasks = true;

  Open(delta_options, options);

  ASSERT_OK(Put("foo", "bar"));

  auto delta_files = delta_db_impl()->TEST_GetDeltaFiles();
  ASSERT_EQ(delta_files.size(), 1);

  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogWriter::Sync", [this](void * /* arg */) {
        fault_injection_env_->SetFilesystemActive(false, Status::IOError());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = delta_db_impl()->TEST_CloseDeltaFile(delta_files[0]);

  fault_injection_env_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(s.IsIOError());
}

}  //  namespace delta_db
}  // namespace ROCKSDB_NAMESPACE

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as DeltaDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
