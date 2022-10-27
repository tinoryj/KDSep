//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dbformat.h"
#include "db/delta/delta_garbage_meter.h"
#include "db/delta/delta_index.h"
#include "db/delta/delta_log_format.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaGarbageMeter::ProcessInFlow(const Slice& key, const Slice& value) {
  uint64_t delta_file_number = kInvalidDeltaFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &delta_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (delta_file_number == kInvalidDeltaFileNumber) {
    return Status::OK();
  }

  flows_[delta_file_number].AddInFlow(bytes);

  return Status::OK();
}

Status DeltaGarbageMeter::ProcessOutFlow(const Slice& key, const Slice& value) {
  uint64_t delta_file_number = kInvalidDeltaFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &delta_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (delta_file_number == kInvalidDeltaFileNumber) {
    return Status::OK();
  }

  // Note: in order to measure the amount of additional garbage, we only need to
  // track the outflow for preexisting files, i.e. those that also had inflow.
  // (Newly written files would only have outflow.)
  auto it = flows_.find(delta_file_number);
  if (it == flows_.end()) {
    return Status::OK();
  }

  it->second.AddOutFlow(bytes);

  return Status::OK();
}

Status DeltaGarbageMeter::Parse(const Slice& key, const Slice& value,
                                uint64_t* delta_file_number, uint64_t* bytes) {
  assert(delta_file_number);
  assert(*delta_file_number == kInvalidDeltaFileNumber);
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

  if (ikey.type != kTypeDeltaIndex) {
    return Status::OK();
  }

  DeltaIndex delta_index;

  {
    const Status s = delta_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }
  }

  if (delta_index.IsInlined() || delta_index.HasTTL()) {
    return Status::Corruption("Unexpected TTL/inlined delta index");
  }

  *delta_file_number = delta_index.file_number();
  *bytes =
      delta_index.size() +
      DeltaLogRecord::CalculateAdjustmentForRecordHeader(ikey.user_key.size());

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
