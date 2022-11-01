//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/deltaLog/deltaLog_garbage_meter.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(DeltaLogGarbageMeterTest, MeasureGarbage) {
  DeltaLogGarbageMeter deltaLog_garbage_meter;

  struct DeltaLogDescriptor {
    std::string user_key;
    uint64_t deltaLog_file_number;
    uint64_t offset;
    uint64_t size;
    CompressionType compression_type;
    bool has_in_flow;
    bool has_out_flow;

    uint64_t GetExpectedBytes() const {
      return size + DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
                        user_key.size());
    }
  };

  // Note: deltaLog file 4 has the same inflow and outflow and hence no
  // additional garbage. DeltaLog file 5 has less outflow than inflow and thus
  // it does have additional garbage. DeltaLog file 6 is a newly written file
  // (i.e. no inflow, only outflow) and is thus not tracked by the meter.
  std::vector<DeltaLogDescriptor> deltaLogs{
      {"key", 4, 1234, 555, kLZ4Compression, true, true},
      {"other_key", 4, 6789, 101010, kLZ4Compression, true, true},
      {"yet_another_key", 5, 22222, 3456, kLZ4Compression, true, true},
      {"foo_key", 5, 77777, 8888, kLZ4Compression, true, true},
      {"bar_key", 5, 999999, 1212, kLZ4Compression, true, false},
      {"baz_key", 5, 1234567, 890, kLZ4Compression, true, false},
      {"new_key", 6, 7777, 9999, kNoCompression, false, true}};

  for (const auto& deltaLog : deltaLogs) {
    constexpr SequenceNumber seq = 123;
    const InternalKey key(deltaLog.user_key, seq, kTypeDeltaLogIndex);
    const Slice key_slice = key.Encode();

    std::string value;
    DeltaLogIndex::EncodeDeltaLog(&value, deltaLog.deltaLog_file_number,
                                  deltaLog.offset, deltaLog.size,
                                  deltaLog.compression_type);
    const Slice value_slice(value);

    if (deltaLog.has_in_flow) {
      ASSERT_OK(deltaLog_garbage_meter.ProcessInFlow(key_slice, value_slice));
    }
    if (deltaLog.has_out_flow) {
      ASSERT_OK(deltaLog_garbage_meter.ProcessOutFlow(key_slice, value_slice));
    }
  }

  const auto& flows = deltaLog_garbage_meter.flows();
  ASSERT_EQ(flows.size(), 2);

  {
    const auto it = flows.find(4);
    ASSERT_NE(it, flows.end());

    const auto& flow = it->second;

    constexpr uint64_t expected_count = 2;
    const uint64_t expected_bytes =
        deltaLogs[0].GetExpectedBytes() + deltaLogs[1].GetExpectedBytes();

    const auto& in = flow.GetInFlow();
    ASSERT_EQ(in.GetCount(), expected_count);
    ASSERT_EQ(in.GetBytes(), expected_bytes);

    const auto& out = flow.GetOutFlow();
    ASSERT_EQ(out.GetCount(), expected_count);
    ASSERT_EQ(out.GetBytes(), expected_bytes);

    ASSERT_TRUE(flow.IsValid());
    ASSERT_FALSE(flow.HasGarbage());
  }

  {
    const auto it = flows.find(5);
    ASSERT_NE(it, flows.end());

    const auto& flow = it->second;

    const auto& in = flow.GetInFlow();

    constexpr uint64_t expected_in_count = 4;
    const uint64_t expected_in_bytes =
        deltaLogs[2].GetExpectedBytes() + deltaLogs[3].GetExpectedBytes() +
        deltaLogs[4].GetExpectedBytes() + deltaLogs[5].GetExpectedBytes();

    ASSERT_EQ(in.GetCount(), expected_in_count);
    ASSERT_EQ(in.GetBytes(), expected_in_bytes);

    const auto& out = flow.GetOutFlow();

    constexpr uint64_t expected_out_count = 2;
    const uint64_t expected_out_bytes =
        deltaLogs[2].GetExpectedBytes() + deltaLogs[3].GetExpectedBytes();

    ASSERT_EQ(out.GetCount(), expected_out_count);
    ASSERT_EQ(out.GetBytes(), expected_out_bytes);

    ASSERT_TRUE(flow.IsValid());
    ASSERT_TRUE(flow.HasGarbage());
    ASSERT_EQ(flow.GetGarbageCount(), expected_in_count - expected_out_count);
    ASSERT_EQ(flow.GetGarbageBytes(), expected_in_bytes - expected_out_bytes);
  }
}

TEST(DeltaLogGarbageMeterTest, PlainValue) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeValue);
  const Slice key_slice = key.Encode();

  constexpr char value[] = "value";
  const Slice value_slice(value);

  DeltaLogGarbageMeter deltaLog_garbage_meter;

  ASSERT_OK(deltaLog_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_OK(deltaLog_garbage_meter.ProcessOutFlow(key_slice, value_slice));
  ASSERT_TRUE(deltaLog_garbage_meter.flows().empty());
}

TEST(DeltaLogGarbageMeterTest, CorruptInternalKey) {
  constexpr char corrupt_key[] = "i_am_corrupt";
  const Slice key_slice(corrupt_key);

  constexpr char value[] = "value";
  const Slice value_slice(value);

  DeltaLogGarbageMeter deltaLog_garbage_meter;

  ASSERT_NOK(deltaLog_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(deltaLog_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

TEST(DeltaLogGarbageMeterTest, CorruptDeltaLogIndex) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeDeltaLogIndex);
  const Slice key_slice = key.Encode();

  constexpr char value[] = "i_am_not_a_deltaLog_index";
  const Slice value_slice(value);

  DeltaLogGarbageMeter deltaLog_garbage_meter;

  ASSERT_NOK(deltaLog_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(deltaLog_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

TEST(DeltaLogGarbageMeterTest, InlinedTTLDeltaLogIndex) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeDeltaLogIndex);
  const Slice key_slice = key.Encode();

  constexpr uint64_t expiration = 1234567890;
  constexpr char inlined_value[] = "inlined";

  std::string value;
  DeltaLogIndex::EncodeInlinedTTL(&value, expiration, inlined_value);

  const Slice value_slice(value);

  DeltaLogGarbageMeter deltaLog_garbage_meter;

  ASSERT_NOK(deltaLog_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(deltaLog_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
