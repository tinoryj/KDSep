//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "file/filename.h"
#include "logging/logging.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "utilities/delta_db/delta_db_impl.h"

// DeltaDBImpl methods to get snapshot of files, e.g. for replication.

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

Status DeltaDBImpl::DisableFileDeletions() {
  // Disable base DB file deletions.
  Status s = db_impl_->DisableFileDeletions();
  if (!s.ok()) {
    return s;
  }

  int count = 0;
  {
    // Hold delete_file_mutex_ to make sure no DeleteObsoleteFiles job
    // is running.
    MutexLock l(&delete_file_mutex_);
    count = ++disable_file_deletions_;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Disabled delta file deletions. count: %d", count);
  return Status::OK();
}

Status DeltaDBImpl::EnableFileDeletions(bool force) {
  // Enable base DB file deletions.
  Status s = db_impl_->EnableFileDeletions(force);
  if (!s.ok()) {
    return s;
  }

  int count = 0;
  {
    MutexLock l(&delete_file_mutex_);
    if (force) {
      disable_file_deletions_ = 0;
    } else if (disable_file_deletions_ > 0) {
      count = --disable_file_deletions_;
    }
    assert(count >= 0);
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Enabled delta file deletions. count: %d", count);
  // Consider trigger DeleteobsoleteFiles once after re-enabled, if we are to
  // make DeleteobsoleteFiles re-run interval configuration.
  return Status::OK();
}

Status DeltaDBImpl::GetLiveFiles(std::vector<std::string>& ret,
                                 uint64_t* manifest_file_size,
                                 bool flush_memtable) {
  if (!bdb_options_.path_relative) {
    return Status::NotSupported(
        "Not able to get relative delta file path from absolute delta_dir.");
  }
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  Status s = db_->GetLiveFiles(ret, manifest_file_size, flush_memtable);
  if (!s.ok()) {
    return s;
  }
  ret.reserve(ret.size() + delta_files_.size());
  for (auto bfile_pair : delta_files_) {
    auto delta_file = bfile_pair.second;
    // Path should be relative to db_name, but begin with slash.
    ret.emplace_back(DeltaFileName("", bdb_options_.delta_dir,
                                   delta_file->DeltaFileNumber()));
  }
  return Status::OK();
}

void DeltaDBImpl::GetLiveFilesMetaData(
    std::vector<LiveFileMetaData>* metadata) {
  // Path should be relative to db_name.
  assert(bdb_options_.path_relative);
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  db_->GetLiveFilesMetaData(metadata);
  for (auto bfile_pair : delta_files_) {
    auto delta_file = bfile_pair.second;
    LiveFileMetaData filemetadata;
    filemetadata.size = delta_file->GetFileSize();
    const uint64_t file_number = delta_file->DeltaFileNumber();
    // Path should be relative to db_name, but begin with slash.
    filemetadata.name = DeltaFileName("", bdb_options_.delta_dir, file_number);
    filemetadata.file_number = file_number;
    if (delta_file->HasTTL()) {
      filemetadata.oldest_ancester_time =
          delta_file->GetExpirationRange().first;
    }
    auto cfh =
        static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily());
    filemetadata.column_family_name = cfh->GetName();
    metadata->emplace_back(filemetadata);
  }
}

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
