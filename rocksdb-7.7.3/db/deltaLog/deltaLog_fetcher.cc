//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_fetcher.h"

#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogFetcher::FetchDeltaLog(const Slice& user_key,
                                      const DeltaLogIndex& deltaLog_index,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      vector<PinnableSlice*> deltaLog_value_vec,
                                      uint64_t* bytes_read) const {
  const uint64_t deltaLog_file_full_hash = deltaLog_index.getFilePrefixHash();

  assert(deltaLog_source_);
  deltaLog_value_vec.clear();
  const Status s = deltaLog_source_->GetDeltaLog(
      read_options, user_key, deltaLog_file_full_hash,
      deltaLog_file_meta->GetDeltaLogFileSize(), prefetch_buffer, value,
      bytes_read);

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
