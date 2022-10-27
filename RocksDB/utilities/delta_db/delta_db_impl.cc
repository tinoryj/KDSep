
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/delta_db/delta_db_impl.h"

#include <algorithm>
#include <cinttypes>
#include <iomanip>
#include <memory>
#include <sstream>

#include "db/db_impl/db_impl.h"
#include "db/delta/delta_index.h"
#include "db/write_batch_internal.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/statistics.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_builder.h"
#include "table/meta_blocks.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stop_watch.h"
#include "util/timer_queue.h"
#include "utilities/delta_db/delta_compaction_filter.h"
#include "utilities/delta_db/delta_db_iterator.h"
#include "utilities/delta_db/delta_db_listener.h"

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // end namespace

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

bool DeltaFileComparator::operator()(
    const std::shared_ptr<DeltaFile>& lhs,
    const std::shared_ptr<DeltaFile>& rhs) const {
  return lhs->DeltaFileNumber() > rhs->DeltaFileNumber();
}

bool DeltaFileComparatorTTL::operator()(
    const std::shared_ptr<DeltaFile>& lhs,
    const std::shared_ptr<DeltaFile>& rhs) const {
  assert(lhs->HasTTL() && rhs->HasTTL());
  if (lhs->expiration_range_.first < rhs->expiration_range_.first) {
    return true;
  }
  if (lhs->expiration_range_.first > rhs->expiration_range_.first) {
    return false;
  }
  return lhs->DeltaFileNumber() < rhs->DeltaFileNumber();
}

DeltaDBImpl::DeltaDBImpl(const std::string& dbname,
                         const DeltaDBOptions& delta_db_options,
                         const DBOptions& db_options,
                         const ColumnFamilyOptions& cf_options)
    : DeltaDB(),
      dbname_(dbname),
      db_impl_(nullptr),
      env_(db_options.env),
      bdb_options_(delta_db_options),
      db_options_(db_options),
      cf_options_(cf_options),
      file_options_(db_options),
      statistics_(db_options_.statistics.get()),
      next_file_number_(1),
      flush_sequence_(0),
      closed_(true),
      open_file_count_(0),
      total_delta_size_(0),
      live_sst_size_(0),
      fifo_eviction_seq_(0),
      evict_expiration_up_to_(0),
      debug_level_(0) {
  clock_ = env_->GetSystemClock().get();
  delta_dir_ = (bdb_options_.path_relative)
                   ? dbname + "/" + bdb_options_.delta_dir
                   : bdb_options_.delta_dir;
  file_options_.bytes_per_sync = delta_db_options.bytes_per_sync;
}

DeltaDBImpl::~DeltaDBImpl() {
  tqueue_.shutdown();
  // CancelAllBackgroundWork(db_, true);
  Status s __attribute__((__unused__)) = Close();
  assert(s.ok());
}

Status DeltaDBImpl::Close() {
  if (closed_) {
    return Status::OK();
  }
  closed_ = true;

  // Close base DB before DeltaDBImpl destructs to stop event listener and
  // compaction filter call.
  Status s = db_->Close();
  // delete db_ anyway even if close failed.
  delete db_;
  // Reset pointers to avoid StackableDB delete the pointer again.
  db_ = nullptr;
  db_impl_ = nullptr;
  if (!s.ok()) {
    return s;
  }

  s = SyncDeltaFiles();
  return s;
}

DeltaDBOptions DeltaDBImpl::GetDeltaDBOptions() const { return bdb_options_; }

Status DeltaDBImpl::Open(std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  assert(db_ == nullptr);

  if (delta_dir_.empty()) {
    return Status::NotSupported("No delta directory in options");
  }

  if (bdb_options_.garbage_collection_cutoff < 0.0 ||
      bdb_options_.garbage_collection_cutoff > 1.0) {
    return Status::InvalidArgument(
        "Garbage collection cutoff must be in the interval [0.0, 1.0]");
  }

  // Temporarily disable compactions in the base DB during open; save the user
  // defined value beforehand so we can restore it once DeltaDB is initialized.
  // Note: this is only needed if garbage collection is enabled.
  const bool disable_auto_compactions = cf_options_.disable_auto_compactions;

  if (bdb_options_.enable_garbage_collection) {
    cf_options_.disable_auto_compactions = true;
  }

  Status s;

  // Create info log.
  if (db_options_.info_log == nullptr) {
    s = CreateLoggerFromOptions(dbname_, db_options_, &db_options_.info_log);
    if (!s.ok()) {
      return s;
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log, "Opening DeltaDB...");

  if ((cf_options_.compaction_filter != nullptr ||
       cf_options_.compaction_filter_factory != nullptr)) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "DeltaDB only support compaction filter on non-TTL values.");
  }

  // Open delta directory.
  s = env_->CreateDirIfMissing(delta_dir_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to create delta_dir %s, status: %s",
                    delta_dir_.c_str(), s.ToString().c_str());
  }
  s = env_->GetFileSystem()->NewDirectory(delta_dir_, IOOptions(), &dir_ent_,
                                          nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to open delta_dir %s, status: %s",
                    delta_dir_.c_str(), s.ToString().c_str());
    return s;
  }

  // Open delta files.
  s = OpenAllDeltaFiles();
  if (!s.ok()) {
    return s;
  }

  // Update options
  if (bdb_options_.enable_garbage_collection) {
    db_options_.listeners.push_back(std::make_shared<DeltaDBListenerGC>(this));
    cf_options_.compaction_filter_factory =
        std::make_shared<DeltaIndexCompactionFilterFactoryGC>(
            this, clock_, cf_options_, statistics_);
  } else {
    db_options_.listeners.push_back(std::make_shared<DeltaDBListener>(this));
    cf_options_.compaction_filter_factory =
        std::make_shared<DeltaIndexCompactionFilterFactory>(
            this, clock_, cf_options_, statistics_);
  }

  // Reset user compaction filter after building into compaction factory.
  cf_options_.compaction_filter = nullptr;

  // Open base db.
  ColumnFamilyDescriptor cf_descriptor(kDefaultColumnFamilyName, cf_options_);
  s = DB::Open(db_options_, dbname_, {cf_descriptor}, handles, &db_);
  if (!s.ok()) {
    return s;
  }
  db_impl_ = static_cast_with_check<DBImpl>(db_->GetRootDB());

  // Sanitize the delta_dir provided. Using a directory where the
  // base DB stores its files for the default CF is not supported.
  const ColumnFamilyData* const cfd =
      static_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->cfd();
  assert(cfd);

  const ImmutableCFOptions* const ioptions = cfd->ioptions();
  assert(ioptions);

  assert(env_);

  for (const auto& cf_path : ioptions->cf_paths) {
    bool delta_dir_same_as_cf_dir = false;
    s = env_->AreFilesSame(delta_dir_, cf_path.path, &delta_dir_same_as_cf_dir);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Error while sanitizing delta_dir %s, status: %s",
                      delta_dir_.c_str(), s.ToString().c_str());
      return s;
    }

    if (delta_dir_same_as_cf_dir) {
      return Status::NotSupported(
          "Using the base DB's storage directories for DeltaDB files is not "
          "supported.");
    }
  }

  // Initialize SST file <-> oldest delta file mapping if garbage collection
  // is enabled.
  if (bdb_options_.enable_garbage_collection) {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);

    InitializeDeltaFileToSstMapping(live_files);

    MarkUnreferencedDeltaFilesObsoleteDuringOpen();

    if (!disable_auto_compactions) {
      s = db_->EnableAutoCompaction(*handles);
      if (!s.ok()) {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "Failed to enable automatic compactions during open, status: %s",
            s.ToString().c_str());
        return s;
      }
    }
  }

  // Add trash files in delta dir to file delete scheduler.
  SstFileManagerImpl* sfm = static_cast<SstFileManagerImpl*>(
      db_impl_->immutable_db_options().sst_file_manager.get());
  DeleteScheduler::CleanupDirectory(env_, sfm, delta_dir_);

  UpdateLiveSSTSize();

  // Start background jobs.
  if (!bdb_options_.disable_background_tasks) {
    StartBackgroundTasks();
  }

  ROCKS_LOG_INFO(db_options_.info_log, "DeltaDB pointer %p", this);
  bdb_options_.Dump(db_options_.info_log.get());
  closed_ = false;
  return s;
}

void DeltaDBImpl::StartBackgroundTasks() {
  // store a call to a member function and object
  tqueue_.add(
      kReclaimOpenFilesPeriodMillisecs,
      std::bind(&DeltaDBImpl::ReclaimOpenFiles, this, std::placeholders::_1));
  tqueue_.add(kDeleteObsoleteFilesPeriodMillisecs,
              std::bind(&DeltaDBImpl::DeleteObsoleteFiles, this,
                        std::placeholders::_1));
  tqueue_.add(
      kSanityCheckPeriodMillisecs,
      std::bind(&DeltaDBImpl::SanityCheck, this, std::placeholders::_1));
  tqueue_.add(
      kEvictExpiredFilesPeriodMillisecs,
      std::bind(&DeltaDBImpl::EvictExpiredFiles, this, std::placeholders::_1));
}

Status DeltaDBImpl::GetAllDeltaFiles(std::set<uint64_t>* file_numbers) {
  assert(file_numbers != nullptr);
  std::vector<std::string> all_files;
  Status s = env_->GetChildren(delta_dir_, &all_files);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get list of delta files, status: %s",
                    s.ToString().c_str());
    return s;
  }

  for (const auto& file_name : all_files) {
    uint64_t file_number;
    FileType type;
    bool success = ParseFileName(file_name, &file_number, &type);
    if (success && type == kDeltaFile) {
      file_numbers->insert(file_number);
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Skipping file in delta directory: %s", file_name.c_str());
    }
  }

  return s;
}

Status DeltaDBImpl::OpenAllDeltaFiles() {
  std::set<uint64_t> file_numbers;
  Status s = GetAllDeltaFiles(&file_numbers);
  if (!s.ok()) {
    return s;
  }

  if (!file_numbers.empty()) {
    next_file_number_.store(*file_numbers.rbegin() + 1);
  }

  std::ostringstream delta_file_oss;
  std::ostringstream live_imm_oss;
  std::ostringstream obsolete_file_oss;

  for (auto& file_number : file_numbers) {
    std::shared_ptr<DeltaFile> delta_file = std::make_shared<DeltaFile>(
        this, delta_dir_, file_number, db_options_.info_log.get());
    delta_file->MarkImmutable(/* sequence */ 0);

    // Read file header and footer
    Status read_metadata_status =
        delta_file->ReadMetadata(env_->GetFileSystem(), file_options_);
    if (read_metadata_status.IsCorruption()) {
      // Remove incomplete file.
      if (!obsolete_files_.empty()) {
        obsolete_file_oss << ", ";
      }
      obsolete_file_oss << file_number;

      ObsoleteDeltaFile(delta_file, 0 /*obsolete_seq*/, false /*update_size*/);
      continue;
    } else if (!read_metadata_status.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Unable to read metadata of delta file %" PRIu64
                      ", status: '%s'",
                      file_number, read_metadata_status.ToString().c_str());
      return read_metadata_status;
    }

    total_delta_size_ += delta_file->GetFileSize();

    if (!delta_files_.empty()) {
      delta_file_oss << ", ";
    }
    delta_file_oss << file_number;

    delta_files_[file_number] = delta_file;

    if (!delta_file->HasTTL()) {
      if (!live_imm_non_ttl_delta_files_.empty()) {
        live_imm_oss << ", ";
      }
      live_imm_oss << file_number;

      live_imm_non_ttl_delta_files_[file_number] = delta_file;
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Found %" ROCKSDB_PRIszt " delta files: %s",
                 delta_files_.size(), delta_file_oss.str().c_str());
  ROCKS_LOG_INFO(
      db_options_.info_log, "Found %" ROCKSDB_PRIszt " non-TTL delta files: %s",
      live_imm_non_ttl_delta_files_.size(), live_imm_oss.str().c_str());
  ROCKS_LOG_INFO(db_options_.info_log,
                 "Found %" ROCKSDB_PRIszt
                 " incomplete or corrupted delta files: %s",
                 obsolete_files_.size(), obsolete_file_oss.str().c_str());
  return s;
}

template <typename Linker>
void DeltaDBImpl::LinkSstToDeltaFileImpl(uint64_t sst_file_number,
                                         uint64_t delta_file_number,
                                         Linker linker) {
  assert(bdb_options_.enable_garbage_collection);
  assert(delta_file_number != kInvalidDeltaFileNumber);

  auto it = delta_files_.find(delta_file_number);
  if (it == delta_files_.end()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Delta file %" PRIu64
                   " not found while trying to link "
                   "SST file %" PRIu64,
                   delta_file_number, sst_file_number);
    return;
  }

  DeltaFile* const delta_file = it->second.get();
  assert(delta_file);

  linker(delta_file, sst_file_number);

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Delta file %" PRIu64 " linked to SST file %" PRIu64,
                 delta_file_number, sst_file_number);
}

void DeltaDBImpl::LinkSstToDeltaFile(uint64_t sst_file_number,
                                     uint64_t delta_file_number) {
  auto linker = [](DeltaFile* delta_file, uint64_t sst_file) {
    WriteLock file_lock(&delta_file->mutex_);
    delta_file->LinkSstFile(sst_file);
  };

  LinkSstToDeltaFileImpl(sst_file_number, delta_file_number, linker);
}

void DeltaDBImpl::LinkSstToDeltaFileNoLock(uint64_t sst_file_number,
                                           uint64_t delta_file_number) {
  auto linker = [](DeltaFile* delta_file, uint64_t sst_file) {
    delta_file->LinkSstFile(sst_file);
  };

  LinkSstToDeltaFileImpl(sst_file_number, delta_file_number, linker);
}

void DeltaDBImpl::UnlinkSstFromDeltaFile(uint64_t sst_file_number,
                                         uint64_t delta_file_number) {
  assert(bdb_options_.enable_garbage_collection);
  assert(delta_file_number != kInvalidDeltaFileNumber);

  auto it = delta_files_.find(delta_file_number);
  if (it == delta_files_.end()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Delta file %" PRIu64
                   " not found while trying to unlink "
                   "SST file %" PRIu64,
                   delta_file_number, sst_file_number);
    return;
  }

  DeltaFile* const delta_file = it->second.get();
  assert(delta_file);

  {
    WriteLock file_lock(&delta_file->mutex_);
    delta_file->UnlinkSstFile(sst_file_number);
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Delta file %" PRIu64 " unlinked from SST file %" PRIu64,
                 delta_file_number, sst_file_number);
}

void DeltaDBImpl::InitializeDeltaFileToSstMapping(
    const std::vector<LiveFileMetaData>& live_files) {
  assert(bdb_options_.enable_garbage_collection);

  for (const auto& live_file : live_files) {
    const uint64_t sst_file_number = live_file.file_number;
    const uint64_t delta_file_number = live_file.oldest_delta_file_number;

    if (delta_file_number == kInvalidDeltaFileNumber) {
      continue;
    }

    LinkSstToDeltaFileNoLock(sst_file_number, delta_file_number);
  }
}

void DeltaDBImpl::ProcessFlushJobInfo(const FlushJobInfo& info) {
  assert(bdb_options_.enable_garbage_collection);

  WriteLock lock(&mutex_);

  if (info.oldest_delta_file_number != kInvalidDeltaFileNumber) {
    LinkSstToDeltaFile(info.file_number, info.oldest_delta_file_number);
  }

  assert(flush_sequence_ < info.largest_seqno);
  flush_sequence_ = info.largest_seqno;

  MarkUnreferencedDeltaFilesObsolete();
}

void DeltaDBImpl::ProcessCompactionJobInfo(const CompactionJobInfo& info) {
  assert(bdb_options_.enable_garbage_collection);

  if (!info.status.ok()) {
    return;
  }

  // Note: the same SST file may appear in both the input and the output
  // file list in case of a trivial move. We walk through the two lists
  // below in a fashion that's similar to merge sort to detect this.

  auto cmp = [](const CompactionFileInfo& lhs, const CompactionFileInfo& rhs) {
    return lhs.file_number < rhs.file_number;
  };

  auto inputs = info.input_file_infos;
  auto iit = inputs.begin();
  const auto iit_end = inputs.end();

  std::sort(iit, iit_end, cmp);

  auto outputs = info.output_file_infos;
  auto oit = outputs.begin();
  const auto oit_end = outputs.end();

  std::sort(oit, oit_end, cmp);

  WriteLock lock(&mutex_);

  while (iit != iit_end && oit != oit_end) {
    const auto& input = *iit;
    const auto& output = *oit;

    if (input.file_number == output.file_number) {
      ++iit;
      ++oit;
    } else if (input.file_number < output.file_number) {
      if (input.oldest_delta_file_number != kInvalidDeltaFileNumber) {
        UnlinkSstFromDeltaFile(input.file_number,
                               input.oldest_delta_file_number);
      }

      ++iit;
    } else {
      assert(output.file_number < input.file_number);

      if (output.oldest_delta_file_number != kInvalidDeltaFileNumber) {
        LinkSstToDeltaFile(output.file_number, output.oldest_delta_file_number);
      }

      ++oit;
    }
  }

  while (iit != iit_end) {
    const auto& input = *iit;

    if (input.oldest_delta_file_number != kInvalidDeltaFileNumber) {
      UnlinkSstFromDeltaFile(input.file_number, input.oldest_delta_file_number);
    }

    ++iit;
  }

  while (oit != oit_end) {
    const auto& output = *oit;

    if (output.oldest_delta_file_number != kInvalidDeltaFileNumber) {
      LinkSstToDeltaFile(output.file_number, output.oldest_delta_file_number);
    }

    ++oit;
  }

  MarkUnreferencedDeltaFilesObsolete();
}

bool DeltaDBImpl::MarkDeltaFileObsoleteIfNeeded(
    const std::shared_ptr<DeltaFile>& delta_file, SequenceNumber obsolete_seq) {
  assert(delta_file);
  assert(!delta_file->HasTTL());
  assert(delta_file->Immutable());
  assert(bdb_options_.enable_garbage_collection);

  // Note: FIFO eviction could have marked this file obsolete already.
  if (delta_file->Obsolete()) {
    return true;
  }

  // We cannot mark this file (or any higher-numbered files for that matter)
  // obsolete if it is referenced by any memtables or SSTs. We keep track of
  // the SSTs explicitly. To account for memtables, we keep track of the highest
  // sequence number received in flush notifications, and we do not mark the
  // delta file obsolete if there are still unflushed memtables from before
  // the time the delta file was closed.
  if (delta_file->GetImmutableSequence() > flush_sequence_ ||
      !delta_file->GetLinkedSstFiles().empty()) {
    return false;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Delta file %" PRIu64 " is no longer needed, marking obsolete",
                 delta_file->DeltaFileNumber());

  ObsoleteDeltaFile(delta_file, obsolete_seq, /* update_size */ true);
  return true;
}

template <class Functor>
void DeltaDBImpl::MarkUnreferencedDeltaFilesObsoleteImpl(
    Functor mark_if_needed) {
  assert(bdb_options_.enable_garbage_collection);

  // Iterate through all live immutable non-TTL delta files, and mark them
  // obsolete assuming no SST files or memtables rely on the deltas in them.
  // Note: we need to stop as soon as we find a delta file that has any
  // linked SSTs (or one potentially referenced by memtables).

  uint64_t obsoleted_files = 0;

  auto it = live_imm_non_ttl_delta_files_.begin();
  while (it != live_imm_non_ttl_delta_files_.end()) {
    const auto& delta_file = it->second;
    assert(delta_file);
    assert(delta_file->DeltaFileNumber() == it->first);
    assert(!delta_file->HasTTL());
    assert(delta_file->Immutable());

    // Small optimization: Obsolete() does an atomic read, so we can do
    // this check without taking a lock on the delta file's mutex.
    if (delta_file->Obsolete()) {
      it = live_imm_non_ttl_delta_files_.erase(it);
      continue;
    }

    if (!mark_if_needed(delta_file)) {
      break;
    }

    it = live_imm_non_ttl_delta_files_.erase(it);

    ++obsoleted_files;
  }

  if (obsoleted_files > 0) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "%" PRIu64 " delta file(s) marked obsolete by GC",
                   obsoleted_files);
    RecordTick(statistics_, DELTA_DB_GC_NUM_FILES, obsoleted_files);
  }
}

void DeltaDBImpl::MarkUnreferencedDeltaFilesObsolete() {
  const SequenceNumber obsolete_seq = GetLatestSequenceNumber();

  MarkUnreferencedDeltaFilesObsoleteImpl(
      [this, obsolete_seq](const std::shared_ptr<DeltaFile>& delta_file) {
        WriteLock file_lock(&delta_file->mutex_);
        return MarkDeltaFileObsoleteIfNeeded(delta_file, obsolete_seq);
      });
}

void DeltaDBImpl::MarkUnreferencedDeltaFilesObsoleteDuringOpen() {
  MarkUnreferencedDeltaFilesObsoleteImpl(
      [this](const std::shared_ptr<DeltaFile>& delta_file) {
        return MarkDeltaFileObsoleteIfNeeded(delta_file, /* obsolete_seq */ 0);
      });
}

void DeltaDBImpl::CloseRandomAccessLocked(
    const std::shared_ptr<DeltaFile>& bfile) {
  bfile->CloseRandomAccessLocked();
  open_file_count_--;
}

Status DeltaDBImpl::GetDeltaFileReader(
    const std::shared_ptr<DeltaFile>& delta_file,
    std::shared_ptr<RandomAccessFileReader>* reader) {
  assert(reader != nullptr);
  bool fresh_open = false;
  Status s = delta_file->GetReader(env_, file_options_, reader, &fresh_open);
  if (s.ok() && fresh_open) {
    assert(*reader != nullptr);
    open_file_count_++;
  }
  return s;
}

std::shared_ptr<DeltaFile> DeltaDBImpl::NewDeltaFile(
    bool has_ttl, const ExpirationRange& expiration_range,
    const std::string& reason) {
  assert(has_ttl == (expiration_range.first || expiration_range.second));

  uint64_t file_num = next_file_number_++;

  const uint32_t column_family_id =
      static_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  auto delta_file = std::make_shared<DeltaFile>(
      this, delta_dir_, file_num, db_options_.info_log.get(), column_family_id,
      bdb_options_.compression, has_ttl, expiration_range);

  ROCKS_LOG_DEBUG(db_options_.info_log,
                  "New delta file created: %s reason='%s'",
                  delta_file->PathName().c_str(), reason.c_str());
  LogFlush(db_options_.info_log);

  return delta_file;
}

void DeltaDBImpl::RegisterDeltaFile(std::shared_ptr<DeltaFile> delta_file) {
  const uint64_t delta_file_number = delta_file->DeltaFileNumber();

  auto it = delta_files_.lower_bound(delta_file_number);
  assert(it == delta_files_.end() || it->first != delta_file_number);

  delta_files_.insert(
      it, std::map<uint64_t, std::shared_ptr<DeltaFile>>::value_type(
              delta_file_number, std::move(delta_file)));
}

Status DeltaDBImpl::CreateWriterLocked(
    const std::shared_ptr<DeltaFile>& bfile) {
  std::string fpath(bfile->PathName());
  std::unique_ptr<FSWritableFile> wfile;
  const auto& fs = env_->GetFileSystem();

  Status s = fs->ReopenWritableFile(fpath, file_options_, &wfile, nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to open delta file for write: %s status: '%s'"
                    " exists: '%s'",
                    fpath.c_str(), s.ToString().c_str(),
                    fs->FileExists(fpath, file_options_.io_options, nullptr)
                        .ToString()
                        .c_str());
    return s;
  }

  std::unique_ptr<WritableFileWriter> fwriter;
  fwriter.reset(new WritableFileWriter(std::move(wfile), fpath, file_options_));

  uint64_t boffset = bfile->GetFileSize();
  if (debug_level_ >= 2 && boffset) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Open delta file: %s with offset: %" PRIu64, fpath.c_str(),
                    boffset);
  }

  DeltaLogWriter::ElemType et = DeltaLogWriter::kEtNone;
  if (bfile->file_size_ == DeltaLogHeader::kSize) {
    et = DeltaLogWriter::kEtFileHdr;
  } else if (bfile->file_size_ > DeltaLogHeader::kSize) {
    et = DeltaLogWriter::kEtRecord;
  } else if (bfile->file_size_) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Open delta file: %s with wrong size: %" PRIu64,
                   fpath.c_str(), boffset);
    return Status::Corruption("Invalid delta file size");
  }

  constexpr bool do_flush = true;

  bfile->log_writer_ = std::make_shared<DeltaLogWriter>(
      std::move(fwriter), clock_, statistics_, bfile->file_number_,
      db_options_.use_fsync, do_flush, boffset);
  bfile->log_writer_->last_elem_type_ = et;

  return s;
}

std::shared_ptr<DeltaFile> DeltaDBImpl::FindDeltaFileLocked(
    uint64_t expiration) const {
  if (open_ttl_files_.empty()) {
    return nullptr;
  }

  std::shared_ptr<DeltaFile> tmp = std::make_shared<DeltaFile>();
  tmp->SetHasTTL(true);
  tmp->expiration_range_ = std::make_pair(expiration, 0);
  tmp->file_number_ = std::numeric_limits<uint64_t>::max();

  auto citr = open_ttl_files_.equal_range(tmp);
  if (citr.first == open_ttl_files_.end()) {
    assert(citr.second == open_ttl_files_.end());

    std::shared_ptr<DeltaFile> check = *(open_ttl_files_.rbegin());
    return (check->expiration_range_.second <= expiration) ? nullptr : check;
  }

  if (citr.first != citr.second) {
    return *(citr.first);
  }

  auto finditr = citr.second;
  if (finditr != open_ttl_files_.begin()) {
    --finditr;
  }

  bool b2 = (*finditr)->expiration_range_.second <= expiration;
  bool b1 = (*finditr)->expiration_range_.first > expiration;

  return (b1 || b2) ? nullptr : (*finditr);
}

Status DeltaDBImpl::CheckOrCreateWriterLocked(
    const std::shared_ptr<DeltaFile>& delta_file,
    std::shared_ptr<DeltaLogWriter>* writer) {
  assert(writer != nullptr);
  *writer = delta_file->GetWriter();
  if (*writer != nullptr) {
    return Status::OK();
  }
  Status s = CreateWriterLocked(delta_file);
  if (s.ok()) {
    *writer = delta_file->GetWriter();
  }
  return s;
}

Status DeltaDBImpl::CreateDeltaFileAndWriter(
    bool has_ttl, const ExpirationRange& expiration_range,
    const std::string& reason, std::shared_ptr<DeltaFile>* delta_file,
    std::shared_ptr<DeltaLogWriter>* writer) {
  TEST_SYNC_POINT("DeltaDBImpl::CreateDeltaFileAndWriter");
  assert(has_ttl == (expiration_range.first || expiration_range.second));
  assert(delta_file);
  assert(writer);

  *delta_file = NewDeltaFile(has_ttl, expiration_range, reason);
  assert(*delta_file);

  // file not visible, hence no lock
  Status s = CheckOrCreateWriterLocked(*delta_file, writer);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get writer for delta file: %s, error: %s",
                    (*delta_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  assert(*writer);

  s = (*writer)->WriteHeader((*delta_file)->header_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to write header to new delta file: %s"
                    " status: '%s'",
                    (*delta_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  (*delta_file)->SetFileSize(DeltaLogHeader::kSize);
  total_delta_size_ += DeltaLogHeader::kSize;

  return s;
}

Status DeltaDBImpl::SelectDeltaFile(std::shared_ptr<DeltaFile>* delta_file) {
  assert(delta_file);

  {
    ReadLock rl(&mutex_);

    if (open_non_ttl_file_) {
      assert(!open_non_ttl_file_->Immutable());
      *delta_file = open_non_ttl_file_;
      return Status::OK();
    }
  }

  // Check again
  WriteLock wl(&mutex_);

  if (open_non_ttl_file_) {
    assert(!open_non_ttl_file_->Immutable());
    *delta_file = open_non_ttl_file_;
    return Status::OK();
  }

  std::shared_ptr<DeltaLogWriter> writer;
  const Status s = CreateDeltaFileAndWriter(
      /* has_ttl */ false, ExpirationRange(),
      /* reason */ "SelectDeltaFile", delta_file, &writer);
  if (!s.ok()) {
    return s;
  }

  RegisterDeltaFile(*delta_file);
  open_non_ttl_file_ = *delta_file;

  return s;
}

Status DeltaDBImpl::SelectDeltaFileTTL(uint64_t expiration,
                                       std::shared_ptr<DeltaFile>* delta_file) {
  assert(delta_file);
  assert(expiration != kNoExpiration);

  {
    ReadLock rl(&mutex_);

    *delta_file = FindDeltaFileLocked(expiration);
    if (*delta_file != nullptr) {
      assert(!(*delta_file)->Immutable());
      return Status::OK();
    }
  }

  // Check again
  WriteLock wl(&mutex_);

  *delta_file = FindDeltaFileLocked(expiration);
  if (*delta_file != nullptr) {
    assert(!(*delta_file)->Immutable());
    return Status::OK();
  }

  const uint64_t exp_low =
      (expiration / bdb_options_.ttl_range_secs) * bdb_options_.ttl_range_secs;
  const uint64_t exp_high = exp_low + bdb_options_.ttl_range_secs;
  const ExpirationRange expiration_range(exp_low, exp_high);

  std::ostringstream oss;
  oss << "SelectDeltaFileTTL range: [" << exp_low << ',' << exp_high << ')';

  std::shared_ptr<DeltaLogWriter> writer;
  const Status s =
      CreateDeltaFileAndWriter(/* has_ttl */ true, expiration_range,
                               /* reason */ oss.str(), delta_file, &writer);
  if (!s.ok()) {
    return s;
  }

  RegisterDeltaFile(*delta_file);
  open_ttl_files_.insert(*delta_file);

  return s;
}

class DeltaDBImpl::DeltaInserter : public WriteBatch::Handler {
 private:
  const WriteOptions& options_;
  DeltaDBImpl* delta_db_impl_;
  uint32_t default_cf_id_;
  WriteBatch batch_;

 public:
  DeltaInserter(const WriteOptions& options, DeltaDBImpl* delta_db_impl,
                uint32_t default_cf_id)
      : options_(options),
        delta_db_impl_(delta_db_impl),
        default_cf_id_(default_cf_id) {}

  WriteBatch* batch() { return &batch_; }

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Delta DB doesn't support non-default column family.");
    }
    Status s = delta_db_impl_->PutDeltaValue(options_, key, value,
                                             kNoExpiration, &batch_);
    return s;
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Delta DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::Delete(&batch_, column_family_id, key);
    return s;
  }

  virtual Status DeleteRange(uint32_t column_family_id, const Slice& begin_key,
                             const Slice& end_key) {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Delta DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::DeleteRange(&batch_, column_family_id,
                                               begin_key, end_key);
    return s;
  }

  Status SingleDeleteCF(uint32_t /*column_family_id*/,
                        const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in delta db.");
  }

  Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                 const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in delta db.");
  }

  void LogData(const Slice& delta) override { batch_.PutLogData(delta); }
};

Status DeltaDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  StopWatch write_sw(clock_, statistics_, DELTA_DB_WRITE_MICROS);
  RecordTick(statistics_, DELTA_DB_NUM_WRITE);
  uint32_t default_cf_id =
      static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily())
          ->GetID();
  Status s;
  DeltaInserter delta_inserter(options, this, default_cf_id);
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // delta files.
    MutexLock l(&write_mutex_);
    s = updates->Iterate(&delta_inserter);
  }
  if (!s.ok()) {
    return s;
  }
  return db_->Write(options, delta_inserter.batch());
}

Status DeltaDBImpl::Put(const WriteOptions& options, const Slice& key,
                        const Slice& value) {
  return PutUntil(options, key, value, kNoExpiration);
}

Status DeltaDBImpl::PutWithTTL(const WriteOptions& options, const Slice& key,
                               const Slice& value, uint64_t ttl) {
  uint64_t now = EpochNow();
  uint64_t expiration = kNoExpiration - now > ttl ? now + ttl : kNoExpiration;
  return PutUntil(options, key, value, expiration);
}

Status DeltaDBImpl::PutUntil(const WriteOptions& options, const Slice& key,
                             const Slice& value, uint64_t expiration) {
  StopWatch write_sw(clock_, statistics_, DELTA_DB_WRITE_MICROS);
  RecordTick(statistics_, DELTA_DB_NUM_PUT);
  Status s;
  WriteBatch batch;
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // delta files.
    MutexLock l(&write_mutex_);
    s = PutDeltaValue(options, key, value, expiration, &batch);
  }
  if (s.ok()) {
    s = db_->Write(options, &batch);
  }
  return s;
}

Status DeltaDBImpl::PutDeltaValue(const WriteOptions& /*options*/,
                                  const Slice& key, const Slice& value,
                                  uint64_t expiration, WriteBatch* batch) {
  write_mutex_.AssertHeld();
  Status s;
  std::string index_entry;
  uint32_t column_family_id =
      static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily())
          ->GetID();
  if (value.size() < bdb_options_.min_delta_size) {
    if (expiration == kNoExpiration) {
      // Put as normal value
      s = batch->Put(key, value);
      RecordTick(statistics_, DELTA_DB_WRITE_INLINED);
    } else {
      // Inlined with TTL
      DeltaIndex::EncodeInlinedTTL(&index_entry, expiration, value);
      s = WriteBatchInternal::PutDeltaIndex(batch, column_family_id, key,
                                            index_entry);
      RecordTick(statistics_, DELTA_DB_WRITE_INLINED_TTL);
    }
  } else {
    std::string compression_output;
    Slice value_compressed = GetCompressedSlice(value, &compression_output);

    std::string headerbuf;
    DeltaLogWriter::ConstructDeltaHeader(&headerbuf, key, value_compressed,
                                         expiration);

    // Check DB size limit before selecting delta file to
    // Since CheckSizeAndEvictDeltaFiles() can close delta files, it needs to be
    // done before calling SelectDeltaFile().
    s = CheckSizeAndEvictDeltaFiles(headerbuf.size() + key.size() +
                                    value_compressed.size());
    if (!s.ok()) {
      return s;
    }

    std::shared_ptr<DeltaFile> delta_file;
    if (expiration != kNoExpiration) {
      s = SelectDeltaFileTTL(expiration, &delta_file);
    } else {
      s = SelectDeltaFile(&delta_file);
    }
    if (s.ok()) {
      assert(delta_file != nullptr);
      assert(delta_file->GetCompressionType() == bdb_options_.compression);
      s = AppendDelta(delta_file, headerbuf, key, value_compressed, expiration,
                      &index_entry);
    }
    if (s.ok()) {
      if (expiration != kNoExpiration) {
        WriteLock file_lock(&delta_file->mutex_);
        delta_file->ExtendExpirationRange(expiration);
      }
      s = CloseDeltaFileIfNeeded(delta_file);
    }
    if (s.ok()) {
      s = WriteBatchInternal::PutDeltaIndex(batch, column_family_id, key,
                                            index_entry);
    }
    if (s.ok()) {
      if (expiration == kNoExpiration) {
        RecordTick(statistics_, DELTA_DB_WRITE_DELTA);
      } else {
        RecordTick(statistics_, DELTA_DB_WRITE_DELTA_TTL);
      }
    } else {
      ROCKS_LOG_ERROR(
          db_options_.info_log,
          "Failed to append delta to FILE: %s: KEY: %s VALSZ: %" ROCKSDB_PRIszt
          " status: '%s' delta_file: '%s'",
          delta_file->PathName().c_str(), key.ToString().c_str(), value.size(),
          s.ToString().c_str(), delta_file->DumpState().c_str());
    }
  }

  RecordTick(statistics_, DELTA_DB_NUM_KEYS_WRITTEN);
  RecordTick(statistics_, DELTA_DB_BYTES_WRITTEN, key.size() + value.size());
  RecordInHistogram(statistics_, DELTA_DB_KEY_SIZE, key.size());
  RecordInHistogram(statistics_, DELTA_DB_VALUE_SIZE, value.size());

  return s;
}

Slice DeltaDBImpl::GetCompressedSlice(const Slice& raw,
                                      std::string* compression_output) const {
  if (bdb_options_.compression == kNoCompression) {
    return raw;
  }
  StopWatch compression_sw(clock_, statistics_, DELTA_DB_COMPRESSION_MICROS);
  CompressionType type = bdb_options_.compression;
  CompressionOptions opts;
  CompressionContext context(type);
  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(), type,
                       0 /* sample_for_compression */);
  CompressBlock(raw, info, &type, kBlockBasedTableVersionFormat, false,
                compression_output, nullptr, nullptr);
  return *compression_output;
}

Status DeltaDBImpl::DecompressSlice(const Slice& compressed_value,
                                    CompressionType compression_type,
                                    PinnableSlice* value_output) const {
  assert(compression_type != kNoCompression);

  BlockContents contents;
  auto cfh = static_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily());

  {
    StopWatch decompression_sw(clock_, statistics_,
                               DELTA_DB_DECOMPRESSION_MICROS);
    UncompressionContext context(compression_type);
    UncompressionInfo info(context, UncompressionDict::GetEmptyDict(),
                           compression_type);
    Status s = UncompressBlockContentsForCompressionType(
        info, compressed_value.data(), compressed_value.size(), &contents,
        kBlockBasedTableVersionFormat, *(cfh->cfd()->ioptions()));
    if (!s.ok()) {
      return Status::Corruption("Unable to decompress delta.");
    }
  }

  value_output->PinSelf(contents.data);

  return Status::OK();
}

Status DeltaDBImpl::CompactFiles(
    const CompactionOptions& compact_options,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  // Note: we need CompactionJobInfo to be able to track updates to the
  // delta file <-> SST mappings, so we provide one if the user hasn't,
  // assuming that GC is enabled.
  CompactionJobInfo info{};
  if (bdb_options_.enable_garbage_collection && !compaction_job_info) {
    compaction_job_info = &info;
  }

  const Status s =
      db_->CompactFiles(compact_options, input_file_names, output_level,
                        output_path_id, output_file_names, compaction_job_info);
  if (!s.ok()) {
    return s;
  }

  if (bdb_options_.enable_garbage_collection) {
    assert(compaction_job_info);
    ProcessCompactionJobInfo(*compaction_job_info);
  }

  return s;
}

void DeltaDBImpl::GetCompactionContextCommon(DeltaCompactionContext* context) {
  assert(context);

  context->delta_db_impl = this;
  context->next_file_number = next_file_number_.load();
  context->current_delta_files.clear();
  for (auto& p : delta_files_) {
    context->current_delta_files.insert(p.first);
  }
  context->fifo_eviction_seq = fifo_eviction_seq_;
  context->evict_expiration_up_to = evict_expiration_up_to_;
}

void DeltaDBImpl::GetCompactionContext(DeltaCompactionContext* context) {
  assert(context);

  ReadLock l(&mutex_);
  GetCompactionContextCommon(context);
}

void DeltaDBImpl::GetCompactionContext(DeltaCompactionContext* context,
                                       DeltaCompactionContextGC* context_gc) {
  assert(context);
  assert(context_gc);

  ReadLock l(&mutex_);
  GetCompactionContextCommon(context);

  if (!live_imm_non_ttl_delta_files_.empty()) {
    auto it = live_imm_non_ttl_delta_files_.begin();
    std::advance(it, bdb_options_.garbage_collection_cutoff *
                         live_imm_non_ttl_delta_files_.size());
    context_gc->cutoff_file_number = it != live_imm_non_ttl_delta_files_.end()
                                         ? it->first
                                         : std::numeric_limits<uint64_t>::max();
  }
}

void DeltaDBImpl::UpdateLiveSSTSize() {
  uint64_t live_sst_size = 0;
  bool ok = GetIntProperty(DB::Properties::kLiveSstFilesSize, &live_sst_size);
  if (ok) {
    live_sst_size_.store(live_sst_size);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Updated total SST file size: %" PRIu64 " bytes.",
                   live_sst_size);
  } else {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "Failed to update total SST file size after flush or compaction.");
  }
  {
    // Trigger FIFO eviction if needed.
    MutexLock l(&write_mutex_);
    Status s = CheckSizeAndEvictDeltaFiles(0, true /*force*/);
    if (s.IsNoSpace()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "DB grow out-of-space after SST size updated. Current live"
                     " SST size: %" PRIu64
                     " , current delta files size: %" PRIu64 ".",
                     live_sst_size_.load(), total_delta_size_.load());
    }
  }
}

Status DeltaDBImpl::CheckSizeAndEvictDeltaFiles(uint64_t delta_size,
                                                bool force_evict) {
  write_mutex_.AssertHeld();

  uint64_t live_sst_size = live_sst_size_.load();
  if (bdb_options_.max_db_size == 0 ||
      live_sst_size + total_delta_size_.load() + delta_size <=
          bdb_options_.max_db_size) {
    return Status::OK();
  }

  if (bdb_options_.is_fifo == false ||
      (!force_evict && live_sst_size + delta_size > bdb_options_.max_db_size)) {
    // FIFO eviction is disabled, or no space to insert new delta even we evict
    // all delta files.
    return Status::NoSpace(
        "Write failed, as writing it would exceed max_db_size limit.");
  }

  std::vector<std::shared_ptr<DeltaFile>> candidate_files;
  CopyDeltaFiles(&candidate_files);
  std::sort(candidate_files.begin(), candidate_files.end(),
            DeltaFileComparator());
  fifo_eviction_seq_ = GetLatestSequenceNumber();

  WriteLock l(&mutex_);

  while (!candidate_files.empty() &&
         live_sst_size + total_delta_size_.load() + delta_size >
             bdb_options_.max_db_size) {
    std::shared_ptr<DeltaFile> delta_file = candidate_files.back();
    candidate_files.pop_back();
    WriteLock file_lock(&delta_file->mutex_);
    if (delta_file->Obsolete()) {
      // File already obsoleted by someone else.
      assert(delta_file->Immutable());
      continue;
    }
    // FIFO eviction can evict open delta files.
    if (!delta_file->Immutable()) {
      Status s = CloseDeltaFile(delta_file);
      if (!s.ok()) {
        return s;
      }
    }
    assert(delta_file->Immutable());
    auto expiration_range = delta_file->GetExpirationRange();
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Evict oldest delta file since DB out of space. Current "
                   "live SST file size: %" PRIu64 ", total delta size: %" PRIu64
                   ", max db size: %" PRIu64 ", evicted delta file #%" PRIu64
                   ".",
                   live_sst_size, total_delta_size_.load(),
                   bdb_options_.max_db_size, delta_file->DeltaFileNumber());
    ObsoleteDeltaFile(delta_file, fifo_eviction_seq_, true /*update_size*/);
    evict_expiration_up_to_ = expiration_range.first;
    RecordTick(statistics_, DELTA_DB_FIFO_NUM_FILES_EVICTED);
    RecordTick(statistics_, DELTA_DB_FIFO_NUM_KEYS_EVICTED,
               delta_file->DeltaCount());
    RecordTick(statistics_, DELTA_DB_FIFO_BYTES_EVICTED,
               delta_file->GetFileSize());
    TEST_SYNC_POINT("DeltaDBImpl::EvictOldestDeltaFile:Evicted");
  }
  if (live_sst_size + total_delta_size_.load() + delta_size >
      bdb_options_.max_db_size) {
    return Status::NoSpace(
        "Write failed, as writing it would exceed max_db_size limit.");
  }
  return Status::OK();
}

Status DeltaDBImpl::AppendDelta(const std::shared_ptr<DeltaFile>& bfile,
                                const std::string& headerbuf, const Slice& key,
                                const Slice& value, uint64_t expiration,
                                std::string* index_entry) {
  Status s;
  uint64_t delta_offset = 0;
  uint64_t key_offset = 0;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    std::shared_ptr<DeltaLogWriter> writer;
    s = CheckOrCreateWriterLocked(bfile, &writer);
    if (!s.ok()) {
      return s;
    }

    // write the delta to the delta log.
    s = writer->EmitPhysicalRecord(headerbuf, key, value, &key_offset,
                                   &delta_offset);
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Invalid status in AppendDelta: %s status: '%s'",
                    bfile->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  uint64_t size_put = headerbuf.size() + key.size() + value.size();
  bfile->DeltaRecordAdded(size_put);
  total_delta_size_ += size_put;

  if (expiration == kNoExpiration) {
    DeltaIndex::EncodeDelta(index_entry, bfile->DeltaFileNumber(), delta_offset,
                            value.size(), bdb_options_.compression);
  } else {
    DeltaIndex::EncodeDeltaTTL(index_entry, expiration,
                               bfile->DeltaFileNumber(), delta_offset,
                               value.size(), bdb_options_.compression);
  }

  return s;
}

std::vector<Status> DeltaDBImpl::MultiGet(const ReadOptions& read_options,
                                          const std::vector<Slice>& keys,
                                          std::vector<std::string>* values) {
  StopWatch multiget_sw(clock_, statistics_, DELTA_DB_MULTIGET_MICROS);
  RecordTick(statistics_, DELTA_DB_NUM_MULTIGET);
  // Get a snapshot to avoid delta file get deleted between we
  // fetch and index entry and reading from the file.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  std::vector<Status> statuses;
  statuses.reserve(keys.size());
  values->clear();
  values->reserve(keys.size());
  PinnableSlice value;
  for (size_t i = 0; i < keys.size(); i++) {
    statuses.push_back(Get(ro, DefaultColumnFamily(), keys[i], &value));
    values->push_back(value.ToString());
    value.Reset();
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  return statuses;
}

bool DeltaDBImpl::SetSnapshotIfNeeded(ReadOptions* read_options) {
  assert(read_options != nullptr);
  if (read_options->snapshot != nullptr) {
    return false;
  }
  read_options->snapshot = db_->GetSnapshot();
  return true;
}

Status DeltaDBImpl::GetDeltaValue(const Slice& key, const Slice& index_entry,
                                  PinnableSlice* value, uint64_t* expiration) {
  assert(value);

  DeltaIndex delta_index;
  Status s = delta_index.DecodeFrom(index_entry);
  if (!s.ok()) {
    return s;
  }

  if (delta_index.HasTTL() && delta_index.expiration() <= EpochNow()) {
    return Status::NotFound("Key expired");
  }

  if (expiration != nullptr) {
    if (delta_index.HasTTL()) {
      *expiration = delta_index.expiration();
    } else {
      *expiration = kNoExpiration;
    }
  }

  if (delta_index.IsInlined()) {
    // TODO(yiwu): If index_entry is a PinnableSlice, we can also pin the same
    // memory buffer to avoid extra copy.
    value->PinSelf(delta_index.value());
    return Status::OK();
  }

  CompressionType compression_type = kNoCompression;
  s = GetRawDeltaFromFile(key, delta_index.file_number(), delta_index.offset(),
                          delta_index.size(), value, &compression_type);
  if (!s.ok()) {
    return s;
  }

  if (compression_type != kNoCompression) {
    s = DecompressSlice(*value, compression_type, value);
    if (!s.ok()) {
      if (debug_level_ >= 2) {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "Uncompression error during delta read from file: %" PRIu64
            " delta_offset: %" PRIu64 " delta_size: %" PRIu64
            " key: %s status: '%s'",
            delta_index.file_number(), delta_index.offset(), delta_index.size(),
            key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());
      }
      return s;
    }
  }

  return Status::OK();
}

Status DeltaDBImpl::GetRawDeltaFromFile(const Slice& key, uint64_t file_number,
                                        uint64_t offset, uint64_t size,
                                        PinnableSlice* value,
                                        CompressionType* compression_type) {
  assert(value);
  assert(compression_type);
  assert(*compression_type == kNoCompression);

  if (!size) {
    value->PinSelf("");
    return Status::OK();
  }

  // offset has to have certain min, as we will read CRC
  // later from the Delta Header, which needs to be also a
  // valid offset.
  if (offset <
      (DeltaLogHeader::kSize + DeltaLogRecord::kHeaderSize + key.size())) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Invalid delta index file_number: %" PRIu64
                      " delta_offset: %" PRIu64 " delta_size: %" PRIu64
                      " key: %s",
                      file_number, offset, size,
                      key.ToString(/* output_hex */ true).c_str());
    }

    return Status::NotFound("Invalid delta offset");
  }

  std::shared_ptr<DeltaFile> delta_file;

  {
    ReadLock rl(&mutex_);
    auto it = delta_files_.find(file_number);

    // file was deleted
    if (it == delta_files_.end()) {
      return Status::NotFound("Delta Not Found as delta file missing");
    }

    delta_file = it->second;
  }

  *compression_type = delta_file->GetCompressionType();

  // takes locks when called
  std::shared_ptr<RandomAccessFileReader> reader;
  Status s = GetDeltaFileReader(delta_file, &reader);
  if (!s.ok()) {
    return s;
  }

  assert(offset >= key.size() + sizeof(uint32_t));
  const uint64_t record_offset = offset - key.size() - sizeof(uint32_t);
  const uint64_t record_size = sizeof(uint32_t) + key.size() + size;

  // Allocate the buffer. This is safe in C++11
  std::string buf;
  AlignedBuf aligned_buf;

  // A partial delta record contain checksum, key and value.
  Slice delta_record;

  {
    StopWatch read_sw(clock_, statistics_, DELTA_DB_DELTA_FILE_READ_MICROS);
    // TODO: rate limit old delta DB file reads.
    if (reader->use_direct_io()) {
      s = reader->Read(IOOptions(), record_offset,
                       static_cast<size_t>(record_size), &delta_record, nullptr,
                       &aligned_buf, Env::IO_TOTAL /* rate_limiter_priority */);
    } else {
      buf.reserve(static_cast<size_t>(record_size));
      s = reader->Read(IOOptions(), record_offset,
                       static_cast<size_t>(record_size), &delta_record, &buf[0],
                       nullptr, Env::IO_TOTAL /* rate_limiter_priority */);
    }
    RecordTick(statistics_, DELTA_DB_DELTA_FILE_BYTES_READ,
               delta_record.size());
  }

  if (!s.ok()) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Failed to read delta from delta file %" PRIu64
                    ", delta_offset: %" PRIu64 ", delta_size: %" PRIu64
                    ", key_size: %" ROCKSDB_PRIszt ", status: '%s'",
                    file_number, offset, size, key.size(),
                    s.ToString().c_str());
    return s;
  }

  if (delta_record.size() != record_size) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Failed to read delta from delta file %" PRIu64
                    ", delta_offset: %" PRIu64 ", delta_size: %" PRIu64
                    ", key_size: %" ROCKSDB_PRIszt ", read %" ROCKSDB_PRIszt
                    " bytes, expected %" PRIu64 " bytes",
                    file_number, offset, size, key.size(), delta_record.size(),
                    record_size);

    return Status::Corruption("Failed to retrieve delta from delta index.");
  }

  Slice crc_slice(delta_record.data(), sizeof(uint32_t));
  Slice delta_value(delta_record.data() + sizeof(uint32_t) + key.size(),
                    static_cast<size_t>(size));

  uint32_t crc_exp = 0;
  if (!GetFixed32(&crc_slice, &crc_exp)) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Unable to decode CRC from delta file %" PRIu64
                    ", delta_offset: %" PRIu64 ", delta_size: %" PRIu64
                    ", key size: %" ROCKSDB_PRIszt ", status: '%s'",
                    file_number, offset, size, key.size(),
                    s.ToString().c_str());
    return Status::Corruption("Unable to decode checksum.");
  }

  uint32_t crc = crc32c::Value(delta_record.data() + sizeof(uint32_t),
                               delta_record.size() - sizeof(uint32_t));
  crc = crc32c::Mask(crc);  // Adjust for storage
  if (crc != crc_exp) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(
          db_options_.info_log,
          "Delta crc mismatch file: %" PRIu64 " delta_offset: %" PRIu64
          " delta_size: %" PRIu64 " key: %s status: '%s'",
          file_number, offset, size,
          key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());
    }

    return Status::Corruption("Corruption. Delta CRC mismatch");
  }

  value->PinSelf(delta_value);

  return Status::OK();
}

Status DeltaDBImpl::Get(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        PinnableSlice* value) {
  return Get(read_options, column_family, key, value,
             static_cast<uint64_t*>(nullptr) /*expiration*/);
}

Status DeltaDBImpl::Get(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        PinnableSlice* value, uint64_t* expiration) {
  StopWatch get_sw(clock_, statistics_, DELTA_DB_GET_MICROS);
  RecordTick(statistics_, DELTA_DB_NUM_GET);
  return GetImpl(read_options, column_family, key, value, expiration);
}

Status DeltaDBImpl::GetImpl(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableSlice* value, uint64_t* expiration) {
  if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
    return Status::NotSupported(
        "Delta DB doesn't support non-default column family.");
  }
  // Get a snapshot to avoid delta file get deleted between we
  // fetch and index entry and reading from the file.
  // TODO(yiwu): For Get() retry if file not found would be a simpler strategy.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  PinnableSlice index_entry;
  Status s;
  bool is_delta_index = false;
  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = &index_entry;
  get_impl_options.is_delta_index = &is_delta_index;
  s = db_impl_->GetImpl(ro, key, get_impl_options);
  if (expiration != nullptr) {
    *expiration = kNoExpiration;
  }
  RecordTick(statistics_, DELTA_DB_NUM_KEYS_READ);
  if (s.ok()) {
    if (is_delta_index) {
      s = GetDeltaValue(key, index_entry, value, expiration);
    } else {
      // The index entry is the value itself in this case.
      value->PinSelf(index_entry);
    }
    RecordTick(statistics_, DELTA_DB_BYTES_READ, value->size());
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  return s;
}

std::pair<bool, int64_t> DeltaDBImpl::SanityCheck(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  ReadLock rl(&mutex_);

  ROCKS_LOG_INFO(db_options_.info_log, "Starting Sanity Check");
  ROCKS_LOG_INFO(db_options_.info_log, "Number of files %" ROCKSDB_PRIszt,
                 delta_files_.size());
  ROCKS_LOG_INFO(db_options_.info_log, "Number of open files %" ROCKSDB_PRIszt,
                 open_ttl_files_.size());

  for (const auto& delta_file : open_ttl_files_) {
    (void)delta_file;
    assert(!delta_file->Immutable());
  }

  for (const auto& pair : live_imm_non_ttl_delta_files_) {
    const auto& delta_file = pair.second;
    (void)delta_file;
    assert(!delta_file->HasTTL());
    assert(delta_file->Immutable());
  }

  uint64_t now = EpochNow();

  for (auto delta_file_pair : delta_files_) {
    auto delta_file = delta_file_pair.second;
    std::ostringstream buf;

    buf << "Delta file " << delta_file->DeltaFileNumber() << ", size "
        << delta_file->GetFileSize() << ", delta count "
        << delta_file->DeltaCount() << ", immutable "
        << delta_file->Immutable();

    if (delta_file->HasTTL()) {
      ExpirationRange expiration_range;
      {
        ReadLock file_lock(&delta_file->mutex_);
        expiration_range = delta_file->GetExpirationRange();
      }
      buf << ", expiration range (" << expiration_range.first << ", "
          << expiration_range.second << ")";

      if (!delta_file->Obsolete()) {
        buf << ", expire in " << (expiration_range.second - now) << "seconds";
      }
    }
    if (delta_file->Obsolete()) {
      buf << ", obsolete at " << delta_file->GetObsoleteSequence();
    }
    buf << ".";
    ROCKS_LOG_INFO(db_options_.info_log, "%s", buf.str().c_str());
  }

  // reschedule
  return std::make_pair(true, -1);
}

Status DeltaDBImpl::CloseDeltaFile(std::shared_ptr<DeltaFile> bfile) {
  TEST_SYNC_POINT("DeltaDBImpl::CloseDeltaFile");
  assert(bfile);
  assert(!bfile->Immutable());
  assert(!bfile->Obsolete());

  if (bfile->HasTTL() || bfile == open_non_ttl_file_) {
    write_mutex_.AssertHeld();
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Closing delta file %" PRIu64 ". Path: %s",
                 bfile->DeltaFileNumber(), bfile->PathName().c_str());

  const SequenceNumber sequence = GetLatestSequenceNumber();

  const Status s = bfile->WriteFooterAndCloseLocked(sequence);

  if (s.ok()) {
    total_delta_size_ += DeltaLogFooter::kSize;
  } else {
    bfile->MarkImmutable(sequence);

    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to close delta file %" PRIu64 "with error: %s",
                    bfile->DeltaFileNumber(), s.ToString().c_str());
  }

  if (bfile->HasTTL()) {
    size_t erased __attribute__((__unused__));
    erased = open_ttl_files_.erase(bfile);
  } else {
    if (bfile == open_non_ttl_file_) {
      open_non_ttl_file_ = nullptr;
    }

    const uint64_t delta_file_number = bfile->DeltaFileNumber();
    auto it = live_imm_non_ttl_delta_files_.lower_bound(delta_file_number);
    assert(it == live_imm_non_ttl_delta_files_.end() ||
           it->first != delta_file_number);
    live_imm_non_ttl_delta_files_.insert(
        it, std::map<uint64_t, std::shared_ptr<DeltaFile>>::value_type(
                delta_file_number, bfile));
  }

  return s;
}

Status DeltaDBImpl::CloseDeltaFileIfNeeded(std::shared_ptr<DeltaFile>& bfile) {
  write_mutex_.AssertHeld();

  // atomic read
  if (bfile->GetFileSize() < bdb_options_.delta_file_size) {
    return Status::OK();
  }

  WriteLock lock(&mutex_);
  WriteLock file_lock(&bfile->mutex_);

  assert(!bfile->Obsolete() || bfile->Immutable());
  if (bfile->Immutable()) {
    return Status::OK();
  }

  return CloseDeltaFile(bfile);
}

void DeltaDBImpl::ObsoleteDeltaFile(std::shared_ptr<DeltaFile> delta_file,
                                    SequenceNumber obsolete_seq,
                                    bool update_size) {
  assert(delta_file->Immutable());
  assert(!delta_file->Obsolete());

  // Should hold write lock of mutex_ or during DB open.
  delta_file->MarkObsolete(obsolete_seq);
  obsolete_files_.push_back(delta_file);
  assert(total_delta_size_.load() >= delta_file->GetFileSize());
  if (update_size) {
    total_delta_size_ -= delta_file->GetFileSize();
  }
}

bool DeltaDBImpl::VisibleToActiveSnapshot(
    const std::shared_ptr<DeltaFile>& bfile) {
  assert(bfile->Obsolete());

  // We check whether the oldest snapshot is no less than the last sequence
  // by the time the delta file become obsolete. If so, the delta file is not
  // visible to all existing snapshots.
  //
  // If we keep track of the earliest sequence of the keys in the delta file,
  // we could instead check if there's a snapshot falls in range
  // [earliest_sequence, obsolete_sequence). But doing so will make the
  // implementation more complicated.
  SequenceNumber obsolete_sequence = bfile->GetObsoleteSequence();
  SequenceNumber oldest_snapshot = kMaxSequenceNumber;
  {
    // Need to lock DBImpl mutex before access snapshot list.
    InstrumentedMutexLock l(db_impl_->mutex());
    auto& snapshots = db_impl_->snapshots();
    if (!snapshots.empty()) {
      oldest_snapshot = snapshots.oldest()->GetSequenceNumber();
    }
  }
  bool visible = oldest_snapshot < obsolete_sequence;
  if (visible) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Obsolete delta file %" PRIu64 " (obsolete at %" PRIu64
                   ") visible to oldest snapshot %" PRIu64 ".",
                   bfile->DeltaFileNumber(), obsolete_sequence,
                   oldest_snapshot);
  }
  return visible;
}

std::pair<bool, int64_t> DeltaDBImpl::EvictExpiredFiles(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  TEST_SYNC_POINT("DeltaDBImpl::EvictExpiredFiles:0");
  TEST_SYNC_POINT("DeltaDBImpl::EvictExpiredFiles:1");

  std::vector<std::shared_ptr<DeltaFile>> process_files;
  uint64_t now = EpochNow();
  {
    ReadLock rl(&mutex_);
    for (auto p : delta_files_) {
      auto& delta_file = p.second;
      ReadLock file_lock(&delta_file->mutex_);
      if (delta_file->HasTTL() && !delta_file->Obsolete() &&
          delta_file->GetExpirationRange().second <= now) {
        process_files.push_back(delta_file);
      }
    }
  }

  TEST_SYNC_POINT("DeltaDBImpl::EvictExpiredFiles:2");
  TEST_SYNC_POINT("DeltaDBImpl::EvictExpiredFiles:3");
  TEST_SYNC_POINT_CALLBACK("DeltaDBImpl::EvictExpiredFiles:cb", nullptr);

  SequenceNumber seq = GetLatestSequenceNumber();
  {
    MutexLock l(&write_mutex_);
    WriteLock lock(&mutex_);
    for (auto& delta_file : process_files) {
      WriteLock file_lock(&delta_file->mutex_);

      // Need to double check if the file is obsolete.
      if (delta_file->Obsolete()) {
        assert(delta_file->Immutable());
        continue;
      }

      if (!delta_file->Immutable()) {
        CloseDeltaFile(delta_file);
      }

      assert(delta_file->Immutable());

      ObsoleteDeltaFile(delta_file, seq, true /*update_size*/);
    }
  }

  return std::make_pair(true, -1);
}

Status DeltaDBImpl::SyncDeltaFiles() {
  MutexLock l(&write_mutex_);

  std::vector<std::shared_ptr<DeltaFile>> process_files;
  {
    ReadLock rl(&mutex_);
    for (auto fitr : open_ttl_files_) {
      process_files.push_back(fitr);
    }
    if (open_non_ttl_file_ != nullptr) {
      process_files.push_back(open_non_ttl_file_);
    }
  }

  Status s;
  for (auto& delta_file : process_files) {
    s = delta_file->Fsync();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to sync delta file %" PRIu64 ", status: %s",
                      delta_file->DeltaFileNumber(), s.ToString().c_str());
      return s;
    }
  }

  s = dir_ent_->FsyncWithDirOptions(IOOptions(), nullptr, DirFsyncOptions());
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to sync delta directory, status: %s",
                    s.ToString().c_str());
  }
  return s;
}

std::pair<bool, int64_t> DeltaDBImpl::ReclaimOpenFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  if (open_file_count_.load() < kOpenFilesTrigger) {
    return std::make_pair(true, -1);
  }

  // in the future, we should sort by last_access_
  // instead of closing every file
  ReadLock rl(&mutex_);
  for (auto const& ent : delta_files_) {
    auto bfile = ent.second;
    if (bfile->last_access_.load() == -1) continue;

    WriteLock lockbfile_w(&bfile->mutex_);
    CloseRandomAccessLocked(bfile);
  }

  return std::make_pair(true, -1);
}

std::pair<bool, int64_t> DeltaDBImpl::DeleteObsoleteFiles(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  MutexLock delete_file_lock(&delete_file_mutex_);
  if (disable_file_deletions_ > 0) {
    return std::make_pair(true, -1);
  }

  std::list<std::shared_ptr<DeltaFile>> tobsolete;
  {
    WriteLock wl(&mutex_);
    if (obsolete_files_.empty()) {
      return std::make_pair(true, -1);
    }
    tobsolete.swap(obsolete_files_);
  }

  bool file_deleted = false;
  for (auto iter = tobsolete.begin(); iter != tobsolete.end();) {
    auto bfile = *iter;
    {
      ReadLock lockbfile_r(&bfile->mutex_);
      if (VisibleToActiveSnapshot(bfile)) {
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Could not delete file due to snapshot failure %s",
                       bfile->PathName().c_str());
        ++iter;
        continue;
      }
    }
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Will delete file due to snapshot success %s",
                   bfile->PathName().c_str());

    {
      WriteLock wl(&mutex_);
      delta_files_.erase(bfile->DeltaFileNumber());
    }

    Status s = DeleteDBFile(&(db_impl_->immutable_db_options()),
                            bfile->PathName(), delta_dir_, true,
                            /*force_fg=*/false);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "File failed to be deleted as obsolete %s",
                      bfile->PathName().c_str());
      ++iter;
      continue;
    }

    file_deleted = true;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "File deleted as obsolete from delta dir %s",
                   bfile->PathName().c_str());

    iter = tobsolete.erase(iter);
  }

  // directory change. Fsync
  if (file_deleted) {
    Status s = dir_ent_->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kFileDeleted));
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log, "Failed to sync dir %s: %s",
                      delta_dir_.c_str(), s.ToString().c_str());
    }
  }

  // put files back into obsolete if for some reason, delete failed
  if (!tobsolete.empty()) {
    WriteLock wl(&mutex_);
    for (auto bfile : tobsolete) {
      delta_files_.insert(std::make_pair(bfile->DeltaFileNumber(), bfile));
      obsolete_files_.push_front(bfile);
    }
  }

  return std::make_pair(!aborted, -1);
}

void DeltaDBImpl::CopyDeltaFiles(
    std::vector<std::shared_ptr<DeltaFile>>* bfiles_copy) {
  ReadLock rl(&mutex_);
  for (auto const& p : delta_files_) {
    bfiles_copy->push_back(p.second);
  }
}

Iterator* DeltaDBImpl::NewIterator(const ReadOptions& read_options) {
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily())
          ->cfd();
  // Get a snapshot to avoid delta file get deleted between we
  // fetch and index entry and reading from the file.
  ManagedSnapshot* own_snapshot = nullptr;
  const Snapshot* snapshot = read_options.snapshot;
  if (snapshot == nullptr) {
    own_snapshot = new ManagedSnapshot(db_);
    snapshot = own_snapshot->snapshot();
  }
  auto* iter = db_impl_->NewIteratorImpl(
      read_options, cfd, snapshot->GetSequenceNumber(),
      nullptr /*read_callback*/, true /*expose_delta_index*/);
  return new DeltaDBIterator(own_snapshot, iter, this, clock_, statistics_);
}

Status DestroyDeltaDB(const std::string& dbname, const Options& options,
                      const DeltaDBOptions& bdb_options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;

  Status status;
  std::string deltadir;
  deltadir = (bdb_options.path_relative) ? dbname + "/" + bdb_options.delta_dir
                                         : bdb_options.delta_dir;

  std::vector<std::string> filenames;
  if (env->GetChildren(deltadir, &filenames).ok()) {
    for (const auto& f : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(f, &number, &type) && type == kDeltaFile) {
        Status del = DeleteDBFile(&soptions, deltadir + "/" + f, deltadir, true,
                                  /*force_fg=*/false);
        if (status.ok() && !del.ok()) {
          status = del;
        }
      }
    }
    // TODO: What to do if we cannot delete the directory?
    env->DeleteDir(deltadir).PermitUncheckedError();
  }
  Status destroy = DestroyDB(dbname, options);
  if (status.ok() && !destroy.ok()) {
    status = destroy;
  }

  return status;
}

#ifndef NDEBUG
Status DeltaDBImpl::TEST_GetDeltaValue(const Slice& key,
                                       const Slice& index_entry,
                                       PinnableSlice* value) {
  return GetDeltaValue(key, index_entry, value);
}

void DeltaDBImpl::TEST_AddDummyDeltaFile(uint64_t delta_file_number,
                                         SequenceNumber immutable_sequence) {
  auto delta_file = std::make_shared<DeltaFile>(
      this, delta_dir_, delta_file_number, db_options_.info_log.get());
  delta_file->MarkImmutable(immutable_sequence);

  delta_files_[delta_file_number] = delta_file;
  live_imm_non_ttl_delta_files_[delta_file_number] = delta_file;
}

std::vector<std::shared_ptr<DeltaFile>> DeltaDBImpl::TEST_GetDeltaFiles()
    const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<DeltaFile>> delta_files;
  for (auto& p : delta_files_) {
    delta_files.emplace_back(p.second);
  }
  return delta_files;
}

std::vector<std::shared_ptr<DeltaFile>>
DeltaDBImpl::TEST_GetLiveImmNonTTLFiles() const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<DeltaFile>> live_imm_non_ttl_files;
  for (const auto& pair : live_imm_non_ttl_delta_files_) {
    live_imm_non_ttl_files.emplace_back(pair.second);
  }
  return live_imm_non_ttl_files;
}

std::vector<std::shared_ptr<DeltaFile>> DeltaDBImpl::TEST_GetObsoleteFiles()
    const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<DeltaFile>> obsolete_files;
  for (auto& bfile : obsolete_files_) {
    obsolete_files.emplace_back(bfile);
  }
  return obsolete_files;
}

void DeltaDBImpl::TEST_DeleteObsoleteFiles() {
  DeleteObsoleteFiles(false /*abort*/);
}

Status DeltaDBImpl::TEST_CloseDeltaFile(std::shared_ptr<DeltaFile>& bfile) {
  MutexLock l(&write_mutex_);
  WriteLock lock(&mutex_);
  WriteLock file_lock(&bfile->mutex_);

  return CloseDeltaFile(bfile);
}

void DeltaDBImpl::TEST_ObsoleteDeltaFile(std::shared_ptr<DeltaFile>& delta_file,
                                         SequenceNumber obsolete_seq,
                                         bool update_size) {
  return ObsoleteDeltaFile(delta_file, obsolete_seq, update_size);
}

void DeltaDBImpl::TEST_EvictExpiredFiles() {
  EvictExpiredFiles(false /*abort*/);
}

uint64_t DeltaDBImpl::TEST_live_sst_size() { return live_sst_size_.load(); }

void DeltaDBImpl::TEST_InitializeDeltaFileToSstMapping(
    const std::vector<LiveFileMetaData>& live_files) {
  InitializeDeltaFileToSstMapping(live_files);
}

void DeltaDBImpl::TEST_ProcessFlushJobInfo(const FlushJobInfo& info) {
  ProcessFlushJobInfo(info);
}

void DeltaDBImpl::TEST_ProcessCompactionJobInfo(const CompactionJobInfo& info) {
  ProcessCompactionJobInfo(info);
}

#endif  //  !NDEBUG

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
