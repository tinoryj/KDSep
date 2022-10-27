//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdint>
#include <cstring>
#include <string>

#include "db/delta/delta_file_garbage.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class DeltaFileGarbageTest : public testing::Test {
 public:
  static void TestEncodeDecode(const DeltaFileGarbage& delta_file_garbage) {
    std::string encoded;
    delta_file_garbage.EncodeTo(&encoded);

    DeltaFileGarbage decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(delta_file_garbage, decoded);
  }
};

TEST_F(DeltaFileGarbageTest, Empty) {
  DeltaFileGarbage delta_file_garbage;

  ASSERT_EQ(delta_file_garbage.GetDeltaFileNumber(), kInvalidDeltaFileNumber);
  ASSERT_EQ(delta_file_garbage.GetGarbageDeltaCount(), 0);
  ASSERT_EQ(delta_file_garbage.GetGarbageDeltaBytes(), 0);

  TestEncodeDecode(delta_file_garbage);
}

TEST_F(DeltaFileGarbageTest, NonEmpty) {
  constexpr uint64_t delta_file_number = 123;
  constexpr uint64_t garbage_delta_count = 1;
  constexpr uint64_t garbage_delta_bytes = 9876;

  DeltaFileGarbage delta_file_garbage(delta_file_number, garbage_delta_count,
                                      garbage_delta_bytes);

  ASSERT_EQ(delta_file_garbage.GetDeltaFileNumber(), delta_file_number);
  ASSERT_EQ(delta_file_garbage.GetGarbageDeltaCount(), garbage_delta_count);
  ASSERT_EQ(delta_file_garbage.GetGarbageDeltaBytes(), garbage_delta_bytes);

  TestEncodeDecode(delta_file_garbage);
}

TEST_F(DeltaFileGarbageTest, DecodeErrors) {
  std::string str;
  Slice slice(str);

  DeltaFileGarbage delta_file_garbage;

  {
    const Status s = delta_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "delta file number"));
  }

  constexpr uint64_t delta_file_number = 123;
  PutVarint64(&str, delta_file_number);
  slice = str;

  {
    const Status s = delta_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage delta count"));
  }

  constexpr uint64_t garbage_delta_count = 4567;
  PutVarint64(&str, garbage_delta_count);
  slice = str;

  {
    const Status s = delta_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage delta bytes"));
  }

  constexpr uint64_t garbage_delta_bytes = 12345678;
  PutVarint64(&str, garbage_delta_bytes);
  slice = str;

  {
    const Status s = delta_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field tag"));
  }

  constexpr uint32_t custom_tag = 2;
  PutVarint32(&str, custom_tag);
  slice = str;

  {
    const Status s = delta_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field value"));
  }
}

TEST_F(DeltaFileGarbageTest, ForwardCompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_compatible_tag = 2;
        PutVarint32(output, forward_compatible_tag);

        PutLengthPrefixedSlice(output, "deadbeef");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t delta_file_number = 678;
  constexpr uint64_t garbage_delta_count = 9999;
  constexpr uint64_t garbage_delta_bytes = 100000000;

  DeltaFileGarbage delta_file_garbage(delta_file_number, garbage_delta_count,
                                      garbage_delta_bytes);

  TestEncodeDecode(delta_file_garbage);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeltaFileGarbageTest, ForwardIncompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "DeltaFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_incompatible_tag = (1 << 6) + 1;
        PutVarint32(output, forward_incompatible_tag);

        PutLengthPrefixedSlice(output, "foobar");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t delta_file_number = 456;
  constexpr uint64_t garbage_delta_count = 100;
  constexpr uint64_t garbage_delta_bytes = 2000000;

  DeltaFileGarbage delta_file_garbage(delta_file_number, garbage_delta_count,
                                      garbage_delta_bytes);

  std::string encoded;
  delta_file_garbage.EncodeTo(&encoded);

  DeltaFileGarbage decoded_delta_file_addition;
  Slice input(encoded);
  const Status s = decoded_delta_file_addition.DecodeFrom(&input);

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
