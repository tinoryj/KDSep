//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdint>
#include <cstring>
#include <string>

#include "db/deltaLog/deltaLog_file_garbage.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class DeltaLogFileGarbageTest : public testing::Test {
 public:
  static void TestEncodeDecode(
      const DeltaLogFileGarbage& deltaLog_file_garbage) {
    std::string encoded;
    deltaLog_file_garbage.EncodeTo(&encoded);

    DeltaLogFileGarbage decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(deltaLog_file_garbage, decoded);
  }
};

TEST_F(DeltaLogFileGarbageTest, Empty) {
  DeltaLogFileGarbage deltaLog_file_garbage;

  ASSERT_EQ(deltaLog_file_garbage.GetDeltaLogFileNumber(),
            kInvalidDeltaLogFileNumber);
  ASSERT_EQ(deltaLog_file_garbage.GetGarbageDeltaLogCount(), 0);
  ASSERT_EQ(deltaLog_file_garbage.GetGarbageDeltaLogBytes(), 0);

  TestEncodeDecode(deltaLog_file_garbage);
}

TEST_F(DeltaLogFileGarbageTest, NonEmpty) {
  constexpr uint64_t deltaLog_file_number = 123;
  constexpr uint64_t garbage_deltaLog_count = 1;
  constexpr uint64_t garbage_deltaLog_bytes = 9876;

  DeltaLogFileGarbage deltaLog_file_garbage(
      deltaLog_file_number, garbage_deltaLog_count, garbage_deltaLog_bytes);

  ASSERT_EQ(deltaLog_file_garbage.GetDeltaLogFileNumber(),
            deltaLog_file_number);
  ASSERT_EQ(deltaLog_file_garbage.GetGarbageDeltaLogCount(),
            garbage_deltaLog_count);
  ASSERT_EQ(deltaLog_file_garbage.GetGarbageDeltaLogBytes(),
            garbage_deltaLog_bytes);

  TestEncodeDecode(deltaLog_file_garbage);
}

TEST_F(DeltaLogFileGarbageTest, DecodeErrors) {
  std::string str;
  Slice slice(str);

  DeltaLogFileGarbage deltaLog_file_garbage;

  {
    const Status s = deltaLog_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "deltaLog file number"));
  }

  constexpr uint64_t deltaLog_file_number = 123;
  PutVarint64(&str, deltaLog_file_number);
  slice = str;

  {
    const Status s = deltaLog_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage deltaLog count"));
  }

  constexpr uint64_t garbage_deltaLog_count = 4567;
  PutVarint64(&str, garbage_deltaLog_count);
  slice = str;

  {
    const Status s = deltaLog_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage deltaLog bytes"));
  }

  constexpr uint64_t garbage_deltaLog_bytes = 12345678;
  PutVarint64(&str, garbage_deltaLog_bytes);
  slice = str;

  {
    const Status s = deltaLog_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field tag"));
  }

  constexpr uint32_t custom_tag = 2;
  PutVarint32(&str, custom_tag);
  slice = str;

  {
    const Status s = deltaLog_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field value"));
  }
}

TEST_F(DeltaLogFileGarbageTest, ForwardCompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_compatible_tag = 2;
        PutVarint32(output, forward_compatible_tag);

        PutLengthPrefixedSlice(output, "deadbeef");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t deltaLog_file_number = 678;
  constexpr uint64_t garbage_deltaLog_count = 9999;
  constexpr uint64_t garbage_deltaLog_bytes = 100000000;

  DeltaLogFileGarbage deltaLog_file_garbage(
      deltaLog_file_number, garbage_deltaLog_count, garbage_deltaLog_bytes);

  TestEncodeDecode(deltaLog_file_garbage);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeltaLogFileGarbageTest, ForwardIncompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaLogFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_incompatible_tag = (1 << 6) + 1;
        PutVarint32(output, forward_incompatible_tag);

        PutLengthPrefixedSlice(output, "foobar");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t deltaLog_file_number = 456;
  constexpr uint64_t garbage_deltaLog_count = 100;
  constexpr uint64_t garbage_deltaLog_bytes = 2000000;

  DeltaLogFileGarbage deltaLog_file_garbage(
      deltaLog_file_number, garbage_deltaLog_count, garbage_deltaLog_bytes);

  std::string encoded;
  deltaLog_file_garbage.EncodeTo(&encoded);

  DeltaLogFileGarbage decoded_deltaLog_file_addition;
  Slice input(encoded);
  const Status s = decoded_deltaLog_file_addition.DecodeFrom(&input);

  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Forward incompatible"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
