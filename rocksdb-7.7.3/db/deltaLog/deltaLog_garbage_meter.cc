//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_garbage_meter.h"

#include "db/dbformat.h"
#include "db/deltaLog/deltaLog_index.h"
#include "db/deltaLog/deltaLog_log_format.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogGarbageMeter::ProcessInFlow(const Slice& key,
                                           const Slice& value) {
  uint64_t deltaLog_file_id = kGCSelectedDeltaLogFileID;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &deltaLog_file_id, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (deltaLog_file_id == kGCSelectedDeltaLogFileID) {
    return Status::OK();
  }

  flows_[deltaLog_file_id].AddInFlow(bytes);

  return Status::OK();
}

Status DeltaLogGarbageMeter::ProcessOutFlow(const Slice& key,
                                            const Slice& value) {
  uint64_t deltaLog_file_id = kGCSelectedDeltaLogFileID;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &deltaLog_file_id, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (deltaLog_file_id == kGCSelectedDeltaLogFileID) {
    return Status::OK();
  }

  // Note: in order to measure the amount of additional garbage, we only need to
  // track the outflow for preexisting files, i.e. those that also had inflow.
  // (Newly written files would only have outflow.)
  auto it = flows_.find(deltaLog_file_id);
  if (it == flows_.end()) {
    return Status::OK();
  }

  it->second.AddOutFlow(bytes);

  return Status::OK();
}

Status DeltaLogGarbageMeter::Parse(const Slice& key, const Slice& value,
                                   uint64_t* deltaLog_file_id,
                                   uint64_t* bytes) {
  assert(deltaLog_file_id);
  assert(*deltaLog_file_id == kGCSelectedDeltaLogFileID);
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
    const Status s = deltaLog_index.GenerateFullFileHashFromKey(key);
    if (!s.ok()) {
      return s;
    }
  }

  *deltaLog_file_id = deltaLog_index.getFileID();
  *bytes = deltaLog_index.getFileSize() + ikey.user_key.size();

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
