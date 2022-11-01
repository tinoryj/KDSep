//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_fetcher.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogFetcher::FetchDeltaLog(const Slice& user_key,
                                      const Slice& deltaLog_index_slice,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      PinnableSlice* deltaLog_value,
                                      uint64_t* bytes_read) const {
  assert(version_);

  return version_->GetDeltaLog(read_options_, user_key, deltaLog_index_slice,
                               prefetch_buffer, deltaLog_value, bytes_read);
}

Status DeltaLogFetcher::FetchDeltaLog(const Slice& user_key,
                                      const DeltaLogIndex& deltaLog_index,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      PinnableSlice* deltaLog_value,
                                      uint64_t* bytes_read) const {
  assert(version_);

  return version_->GetDeltaLog(read_options_, user_key, deltaLog_index,
                               prefetch_buffer, deltaLog_value, bytes_read);
}

}  // namespace ROCKSDB_NAMESPACE
