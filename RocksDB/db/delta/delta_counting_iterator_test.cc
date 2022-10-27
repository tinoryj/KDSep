//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/delta/delta_counting_iterator.h"
#include "db/delta/delta_garbage_meter.h"
#include "db/delta/delta_index.h"
#include "db/delta/delta_log_format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

void CheckInFlow(const DeltaGarbageMeter& delta_garbage_meter,
                 uint64_t delta_file_number, uint64_t count, uint64_t bytes) {
  const auto& flows = delta_garbage_meter.flows();

  const auto it = flows.find(delta_file_number);
  if (it == flows.end()) {
    ASSERT_EQ(count, 0);
    ASSERT_EQ(bytes, 0);
    return;
  }

  const auto& in = it->second.GetInFlow();

  ASSERT_EQ(in.GetCount(), count);
  ASSERT_EQ(in.GetBytes(), bytes);
}

TEST(DeltaCountingIteratorTest, CountDeltas) {
  // Note: the input consists of three key-values: two are delta references to
  // different delta files, while the third one is a plain value.
  constexpr char user_key0[] = "key0";
  constexpr char user_key1[] = "key1";
  constexpr char user_key2[] = "key2";

  const std::vector<std::string> keys{
      test::KeyStr(user_key0, 1, kTypeDeltaIndex),
      test::KeyStr(user_key1, 2, kTypeDeltaIndex),
      test::KeyStr(user_key2, 3, kTypeValue)};

  constexpr uint64_t first_delta_file_number = 4;
  constexpr uint64_t first_offset = 1000;
  constexpr uint64_t first_size = 2000;

  std::string first_delta_index;
  DeltaIndex::EncodeDelta(&first_delta_index, first_delta_file_number,
                        first_offset, first_size, kNoCompression);

  constexpr uint64_t second_delta_file_number = 6;
  constexpr uint64_t second_offset = 2000;
  constexpr uint64_t second_size = 4000;

  std::string second_delta_index;
  DeltaIndex::EncodeDelta(&second_delta_index, second_delta_file_number,
                        second_offset, second_size, kNoCompression);

  const std::vector<std::string> values{first_delta_index, second_delta_index,
                                        "raw_value"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  DeltaGarbageMeter delta_garbage_meter;

  DeltaCountingIterator delta_counter(&input, &delta_garbage_meter);

  constexpr uint64_t first_expected_bytes =
      first_size +
      DeltaLogRecord::CalculateAdjustmentForRecordHeader(sizeof(user_key0) - 1);
  constexpr uint64_t second_expected_bytes =
      second_size +
      DeltaLogRecord::CalculateAdjustmentForRecordHeader(sizeof(user_key1) - 1);

  // Call SeekToFirst and iterate forward
  delta_counter.SeekToFirst();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[0]);
  ASSERT_EQ(delta_counter.user_key(), user_key0);
  ASSERT_EQ(delta_counter.value(), values[0]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 1,
              first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 0, 0);

  delta_counter.Next();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[1]);
  ASSERT_EQ(delta_counter.user_key(), user_key1);
  ASSERT_EQ(delta_counter.value(), values[1]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 1,
              first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 1,
              second_expected_bytes);

  delta_counter.Next();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[2]);
  ASSERT_EQ(delta_counter.user_key(), user_key2);
  ASSERT_EQ(delta_counter.value(), values[2]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 1,
              first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 1,
              second_expected_bytes);

  delta_counter.Next();
  ASSERT_FALSE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 1,
              first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 1,
              second_expected_bytes);

  // Do it again using NextAndGetResult
  delta_counter.SeekToFirst();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[0]);
  ASSERT_EQ(delta_counter.user_key(), user_key0);
  ASSERT_EQ(delta_counter.value(), values[0]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 1,
              second_expected_bytes);

  {
    IterateResult result;
    ASSERT_TRUE(delta_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[1]);
    ASSERT_EQ(delta_counter.user_key(), user_key1);
    ASSERT_TRUE(delta_counter.Valid());
    ASSERT_OK(delta_counter.status());
    ASSERT_EQ(delta_counter.key(), keys[1]);
    ASSERT_EQ(delta_counter.value(), values[1]);
    CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(delta_garbage_meter, second_delta_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_TRUE(delta_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[2]);
    ASSERT_EQ(delta_counter.user_key(), user_key2);
    ASSERT_TRUE(delta_counter.Valid());
    ASSERT_OK(delta_counter.status());
    ASSERT_EQ(delta_counter.key(), keys[2]);
    ASSERT_EQ(delta_counter.value(), values[2]);
    CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(delta_garbage_meter, second_delta_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_FALSE(delta_counter.NextAndGetResult(&result));
    ASSERT_FALSE(delta_counter.Valid());
    ASSERT_OK(delta_counter.status());
    CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(delta_garbage_meter, second_delta_file_number, 2,
                2 * second_expected_bytes);
  }

  // Call SeekToLast and iterate backward
  delta_counter.SeekToLast();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[2]);
  ASSERT_EQ(delta_counter.user_key(), user_key2);
  ASSERT_EQ(delta_counter.value(), values[2]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 2,
              2 * second_expected_bytes);

  delta_counter.Prev();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[1]);
  ASSERT_EQ(delta_counter.user_key(), user_key1);
  ASSERT_EQ(delta_counter.value(), values[1]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 3,
              3 * second_expected_bytes);

  delta_counter.Prev();
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[0]);
  ASSERT_EQ(delta_counter.user_key(), user_key0);
  ASSERT_EQ(delta_counter.value(), values[0]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 3,
              3 * second_expected_bytes);

  delta_counter.Prev();
  ASSERT_FALSE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 3,
              3 * second_expected_bytes);

  // Call Seek for all keys (plus one that's greater than all of them)
  delta_counter.Seek(keys[0]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[0]);
  ASSERT_EQ(delta_counter.user_key(), user_key0);
  ASSERT_EQ(delta_counter.value(), values[0]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 3,
              3 * second_expected_bytes);

  delta_counter.Seek(keys[1]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[1]);
  ASSERT_EQ(delta_counter.user_key(), user_key1);
  ASSERT_EQ(delta_counter.value(), values[1]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 4,
              4 * second_expected_bytes);

  delta_counter.Seek(keys[2]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[2]);
  ASSERT_EQ(delta_counter.user_key(), user_key2);
  ASSERT_EQ(delta_counter.value(), values[2]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 4,
              4 * second_expected_bytes);

  delta_counter.Seek("zzz");
  ASSERT_FALSE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 4,
              4 * second_expected_bytes);

  // Call SeekForPrev for all keys (plus one that's less than all of them)
  delta_counter.SeekForPrev("aaa");
  ASSERT_FALSE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 4,
              4 * second_expected_bytes);

  delta_counter.SeekForPrev(keys[0]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[0]);
  ASSERT_EQ(delta_counter.user_key(), user_key0);
  ASSERT_EQ(delta_counter.value(), values[0]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 4,
              4 * second_expected_bytes);

  delta_counter.SeekForPrev(keys[1]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[1]);
  ASSERT_EQ(delta_counter.user_key(), user_key1);
  ASSERT_EQ(delta_counter.value(), values[1]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 5,
              5 * second_expected_bytes);

  delta_counter.SeekForPrev(keys[2]);
  ASSERT_TRUE(delta_counter.Valid());
  ASSERT_OK(delta_counter.status());
  ASSERT_EQ(delta_counter.key(), keys[2]);
  ASSERT_EQ(delta_counter.user_key(), user_key2);
  ASSERT_EQ(delta_counter.value(), values[2]);
  CheckInFlow(delta_garbage_meter, first_delta_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(delta_garbage_meter, second_delta_file_number, 5,
              5 * second_expected_bytes);
}

TEST(DeltaCountingIteratorTest, CorruptDeltaIndex) {
  const std::vector<std::string> keys{
      test::KeyStr("user_key", 1, kTypeDeltaIndex)};
  const std::vector<std::string> values{"i_am_not_a_delta_index"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  DeltaGarbageMeter delta_garbage_meter;

  DeltaCountingIterator delta_counter(&input, &delta_garbage_meter);

  delta_counter.SeekToFirst();
  ASSERT_FALSE(delta_counter.Valid());
  ASSERT_NOK(delta_counter.status());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
