//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_counting_iterator.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/deltaLog/deltaLog_garbage_meter.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

void CheckInFlow(const DeltaLogGarbageMeter& deltaLog_garbage_meter,
                 uint64_t deltaLog_file_number, uint64_t count,
                 uint64_t bytes) {
  const auto& flows = deltaLog_garbage_meter.flows();

  const auto it = flows.find(deltaLog_file_number);
  if (it == flows.end()) {
    ASSERT_EQ(count, 0);
    ASSERT_EQ(bytes, 0);
    return;
  }

  const auto& in = it->second.GetInFlow();

  ASSERT_EQ(in.GetCount(), count);
  ASSERT_EQ(in.GetBytes(), bytes);
}

TEST(DeltaLogCountingIteratorTest, CountDeltaLogs) {
  // Note: the input consists of three key-values: two are deltaLog references
  // to different deltaLog files, while the third one is a plain value.
  constexpr char user_key0[] = "key0";
  constexpr char user_key1[] = "key1";
  constexpr char user_key2[] = "key2";

  const std::vector<std::string> keys{
      test::KeyStr(user_key0, 1, kTypeDeltaLogIndex),
      test::KeyStr(user_key1, 2, kTypeDeltaLogIndex),
      test::KeyStr(user_key2, 3, kTypeValue)};

  constexpr uint64_t first_deltaLog_file_number = 4;
  constexpr uint64_t first_offset = 1000;
  constexpr uint64_t first_size = 2000;

  std::string first_deltaLog_index;
  DeltaLogIndex::EncodeDeltaLog(&first_deltaLog_index,
                                first_deltaLog_file_number, first_offset,
                                first_size, kNoCompression);

  constexpr uint64_t second_deltaLog_file_number = 6;
  constexpr uint64_t second_offset = 2000;
  constexpr uint64_t second_size = 4000;

  std::string second_deltaLog_index;
  DeltaLogIndex::EncodeDeltaLog(&second_deltaLog_index,
                                second_deltaLog_file_number, second_offset,
                                second_size, kNoCompression);

  const std::vector<std::string> values{first_deltaLog_index,
                                        second_deltaLog_index, "raw_value"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  DeltaLogGarbageMeter deltaLog_garbage_meter;

  DeltaLogCountingIterator deltaLog_counter(&input, &deltaLog_garbage_meter);

  constexpr uint64_t first_expected_bytes =
      first_size + DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
                       sizeof(user_key0) - 1);
  constexpr uint64_t second_expected_bytes =
      second_size + DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
                        sizeof(user_key1) - 1);

  // Call SeekToFirst and iterate forward
  deltaLog_counter.SeekToFirst();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[0]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key0);
  ASSERT_EQ(deltaLog_counter.value(), values[0]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 1,
              first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 0, 0);

  deltaLog_counter.Next();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[1]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key1);
  ASSERT_EQ(deltaLog_counter.value(), values[1]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 1,
              first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 1,
              second_expected_bytes);

  deltaLog_counter.Next();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[2]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key2);
  ASSERT_EQ(deltaLog_counter.value(), values[2]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 1,
              first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 1,
              second_expected_bytes);

  deltaLog_counter.Next();
  ASSERT_FALSE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 1,
              first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 1,
              second_expected_bytes);

  // Do it again using NextAndGetResult
  deltaLog_counter.SeekToFirst();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[0]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key0);
  ASSERT_EQ(deltaLog_counter.value(), values[0]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 1,
              second_expected_bytes);

  {
    IterateResult result;
    ASSERT_TRUE(deltaLog_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[1]);
    ASSERT_EQ(deltaLog_counter.user_key(), user_key1);
    ASSERT_TRUE(deltaLog_counter.Valid());
    ASSERT_OK(deltaLog_counter.status());
    ASSERT_EQ(deltaLog_counter.key(), keys[1]);
    ASSERT_EQ(deltaLog_counter.value(), values[1]);
    CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_TRUE(deltaLog_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[2]);
    ASSERT_EQ(deltaLog_counter.user_key(), user_key2);
    ASSERT_TRUE(deltaLog_counter.Valid());
    ASSERT_OK(deltaLog_counter.status());
    ASSERT_EQ(deltaLog_counter.key(), keys[2]);
    ASSERT_EQ(deltaLog_counter.value(), values[2]);
    CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_FALSE(deltaLog_counter.NextAndGetResult(&result));
    ASSERT_FALSE(deltaLog_counter.Valid());
    ASSERT_OK(deltaLog_counter.status());
    CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 2,
                2 * second_expected_bytes);
  }

  // Call SeekToLast and iterate backward
  deltaLog_counter.SeekToLast();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[2]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key2);
  ASSERT_EQ(deltaLog_counter.value(), values[2]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 2,
              2 * second_expected_bytes);

  deltaLog_counter.Prev();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[1]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key1);
  ASSERT_EQ(deltaLog_counter.value(), values[1]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 3,
              3 * second_expected_bytes);

  deltaLog_counter.Prev();
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[0]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key0);
  ASSERT_EQ(deltaLog_counter.value(), values[0]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 3,
              3 * second_expected_bytes);

  deltaLog_counter.Prev();
  ASSERT_FALSE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 3,
              3 * second_expected_bytes);

  // Call Seek for all keys (plus one that's greater than all of them)
  deltaLog_counter.Seek(keys[0]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[0]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key0);
  ASSERT_EQ(deltaLog_counter.value(), values[0]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 3,
              3 * second_expected_bytes);

  deltaLog_counter.Seek(keys[1]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[1]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key1);
  ASSERT_EQ(deltaLog_counter.value(), values[1]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 4,
              4 * second_expected_bytes);

  deltaLog_counter.Seek(keys[2]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[2]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key2);
  ASSERT_EQ(deltaLog_counter.value(), values[2]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 4,
              4 * second_expected_bytes);

  deltaLog_counter.Seek("zzz");
  ASSERT_FALSE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 4,
              4 * second_expected_bytes);

  // Call SeekForPrev for all keys (plus one that's less than all of them)
  deltaLog_counter.SeekForPrev("aaa");
  ASSERT_FALSE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 4,
              4 * second_expected_bytes);

  deltaLog_counter.SeekForPrev(keys[0]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[0]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key0);
  ASSERT_EQ(deltaLog_counter.value(), values[0]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 4,
              4 * second_expected_bytes);

  deltaLog_counter.SeekForPrev(keys[1]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[1]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key1);
  ASSERT_EQ(deltaLog_counter.value(), values[1]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 5,
              5 * second_expected_bytes);

  deltaLog_counter.SeekForPrev(keys[2]);
  ASSERT_TRUE(deltaLog_counter.Valid());
  ASSERT_OK(deltaLog_counter.status());
  ASSERT_EQ(deltaLog_counter.key(), keys[2]);
  ASSERT_EQ(deltaLog_counter.user_key(), user_key2);
  ASSERT_EQ(deltaLog_counter.value(), values[2]);
  CheckInFlow(deltaLog_garbage_meter, first_deltaLog_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(deltaLog_garbage_meter, second_deltaLog_file_number, 5,
              5 * second_expected_bytes);
}

TEST(DeltaLogCountingIteratorTest, CorruptDeltaLogIndex) {
  const std::vector<std::string> keys{
      test::KeyStr("user_key", 1, kTypeDeltaLogIndex)};
  const std::vector<std::string> values{"i_am_not_a_deltaLog_index"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  DeltaLogGarbageMeter deltaLog_garbage_meter;

  DeltaLogCountingIterator deltaLog_counter(&input, &deltaLog_garbage_meter);

  deltaLog_counter.SeekToFirst();
  ASSERT_FALSE(deltaLog_counter.Valid());
  ASSERT_NOK(deltaLog_counter.status());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
