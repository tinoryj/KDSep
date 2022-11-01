//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dbformat.h"
#include "db/deltaLog/deltaLog_garbage_meter.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogGarbageMeter::ProcessInFlow(const Slice& key,
                                           const Slice& value) {
  uint64_t deltaLog_file_number = kInvalidDeltaLogFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &deltaLog_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (deltaLog_file_number == kInvalidDeltaLogFileNumber) {
    return Status::OK();
  }

  flows_[deltaLog_file_number].AddInFlow(bytes);

  return Status::OK();
}

Status DeltaLogGarbageMeter::ProcessOutFlow(const Slice& key,
                                            const Slice& value) {
  uint64_t deltaLog_file_number = kInvalidDeltaLogFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &deltaLog_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (deltaLog_file_number == kInvalidDeltaLogFileNumber) {
    return Status::OK();
  }

  // Note: in order to measure the amount of additional garbage, we only need to
  // track the outflow for preexisting files, i.e. those that also had inflow.
  // (Newly written files would only have outflow.)
  auto it = flows_.find(deltaLog_file_number);
  if (it == flows_.end()) {
    return Status::OK();
  }

  it->second.AddOutFlow(bytes);

  return Status::OK();
}

Status DeltaLogGarbageMeter::Parse(const Slice& key, const Slice& value,
                                   uint64_t* deltaLog_file_number,
                                   uint64_t* bytes) {
  assert(deltaLog_file_number);
  assert(*deltaLog_file_number == kInvalidDeltaLogFileNumber);
  assert(bytes);
  assert(*bytes == 0);

  ParsedInternalKey ikey;

  {
    constexpr bool log_err_key = false;
    const Status s = ParseInternalKey(key, &ikey, log_err_key);
    if (!s.ok()) {
      return s;
    }
  }

  if (ikey.type != kTypeDeltaLogIndex) {
    return Status::OK();
  }

  DeltaLogIndex deltaLog_index;

  {
    const Status s = deltaLog_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }
  }

  if (deltaLog_index.IsInlined() || deltaLog_index.HasTTL()) {
    return Status::Corruption("Unexpected TTL/inlined deltaLog index");
  }

  *deltaLog_file_number = deltaLog_index.file_number();
  *bytes = deltaLog_index.size() +
           DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
               ikey.user_key.size());

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
