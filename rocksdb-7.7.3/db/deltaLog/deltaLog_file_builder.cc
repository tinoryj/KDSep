//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_builder.h"

#include <cassert>

#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_file_addition.h"
#include "db/deltaLog/deltaLog_file_completion_callback.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/deltaLog_log_writer.h"
#include "db/event_helpers.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"

namespace ROCKSDB_NAMESPACE {

DeltaLogFileBuilder::DeltaLogFileBuilder(
    VersionSet* versions, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    std::string db_id, std::string db_session_id, int job_id,
    uint32_t column_family_id, const std::string& column_family_name,
    Env::IOPriority io_priority, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    DeltaLogFileCompletionCallback* deltaLog_callback,
    DeltaLogFileCreationReason creation_reason,
    std::vector<std::string>* deltaLog_file_paths,
    std::vector<DeltaLogFileAddition>* deltaLog_file_additions)
    : DeltaLogFileBuilder(
          [versions]() { return versions->NewFileNumber(); }, fs,
          immutable_options, mutable_cf_options, file_options, db_id,
          db_session_id, job_id, column_family_id, column_family_name,
          io_priority, write_hint, io_tracer, deltaLog_callback,
          creation_reason, deltaLog_file_paths, deltaLog_file_additions) {}

DeltaLogFileBuilder::DeltaLogFileBuilder(
    std::function<uint64_t()> file_id_generator, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    std::string db_id, std::string db_session_id, int job_id,
    uint32_t column_family_id, const std::string& column_family_name,
    Env::IOPriority io_priority, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    DeltaLogFileCompletionCallback* deltaLog_callback,
    DeltaLogFileCreationReason creation_reason,
    std::vector<std::string>* deltaLog_file_paths,
    std::vector<DeltaLogFileAddition>* deltaLog_file_additions)
    : file_id_generator_(std::move(file_id_generator)),
      fs_(fs),
      immutable_options_(immutable_options),
      min_deltaLog_size_(mutable_cf_options->min_deltaLog_size),
      deltaLog_file_size_(mutable_cf_options->deltaLog_file_size),
      prepopulate_deltaLog_cache_(
          mutable_cf_options->prepopulate_deltaLog_cache),
      file_options_(file_options),
      db_id_(std::move(db_id)),
      db_session_id_(std::move(db_session_id)),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      io_priority_(io_priority),
      write_hint_(write_hint),
      io_tracer_(io_tracer),
      deltaLog_callback_(deltaLog_callback),
      creation_reason_(creation_reason),
      deltaLog_file_paths_(deltaLog_file_paths),
      deltaLog_file_additions_(deltaLog_file_additions),
      deltaLog_count_(0),
      deltaLog_bytes_(0) {
  assert(file_id_generator_);
  assert(fs_);
  assert(immutable_options_);
  assert(file_options_);
  assert(deltaLog_file_paths_);
  assert(deltaLog_file_paths_->empty());
  assert(deltaLog_file_additions_);
  assert(deltaLog_file_additions_->empty());
}

DeltaLogFileBuilder::~DeltaLogFileBuilder() = default;

Status DeltaLogFileBuilder::Add(const Slice& key, const Slice& value,
                                bool is_anchor) {
  if (value.size() < min_deltaLog_size_) {
    return Status::OK();
  }

  {
    const Status s = OpenDeltaLogFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  Slice deltaLog = value;
  {
    const Status s = WriteDeltaLogToFile(key, deltaLog, is_anchor);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = CloseDeltaLogFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = PutDeltaLogIntoCacheIfNeeded(key, value);
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          immutable_options_->info_log,
          "Failed to pre-populate the deltaLog into deltaLog cache: %s",
          s.ToString().c_str());
    }
  }
  return Status::OK();
}

Status DeltaLogFileBuilder::Finish() {
  if (!IsDeltaLogFileOpen()) {
    return Status::OK();
  }

  return CloseDeltaLogFile();
}

bool DeltaLogFileBuilder::IsDeltaLogFileOpen() const { return !!writer_; }

Status DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded() {
  if (IsDeltaLogFileOpen()) {
    return Status::OK();
  }

  assert(!deltaLog_count_);
  assert(!deltaLog_bytes_);
  assert(file_id_generator_);

  deltaLog_file_id_ = file_id_generator_();

  assert(immutable_options_);
  assert(!immutable_options_->cf_paths.empty());
  std::string deltaLog_file_path = DeltaLogFileName(
      immutable_options_->cf_paths.front().path, deltaLog_file_id_);

  if (deltaLog_callback_) {
    deltaLog_callback_->OnDeltaLogFileCreationStarted(
        deltaLog_file_path, column_family_name_, job_id_, creation_reason_);
  }

  std::unique_ptr<FSWritableFile> file;

  {
    assert(file_options_);
    Status s = NewWritableFile(fs_, deltaLog_file_path, &file, *file_options_);

    TEST_SYNC_POINT_CALLBACK(
        "DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded:NewWritableFile", &s);

    if (!s.ok()) {
      return s;
    }
  }

  // Note: files get added to deltaLog_file_paths_ right after the open, so they
  // can be cleaned up upon failure. Contrast this with
  // deltaLog_file_additions_, which only contains successfully written files.
  assert(deltaLog_file_paths_);
  deltaLog_file_paths_->emplace_back(std::move(deltaLog_file_path));

  assert(file);
  file->SetIOPriority(io_priority_);
  file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = immutable_options_->checksum_handoff_file_types;
  Statistics* const statistics = immutable_options_->stats;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), deltaLog_file_paths_->back(), *file_options_,
      immutable_options_->clock, io_tracer_, statistics,
      immutable_options_->listeners,
      immutable_options_->file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kDeltaLogFile), false));

  constexpr bool do_flush = false;

  std::unique_ptr<DeltaLogLogWriter> deltaLog_log_writer(new DeltaLogLogWriter(
      std::move(file_writer), immutable_options_->clock, statistics,
      deltaLog_file_id_, immutable_options_->use_fsync, do_flush));

  DeltaLogHeader header(column_family_id_);

  {
    Status s = deltaLog_log_writer->WriteHeader(header);

    TEST_SYNC_POINT_CALLBACK(
        "DeltaLogFileBuilder::OpenDeltaLogFileIfNeeded:WriteHeader", &s);

    if (!s.ok()) {
      return s;
    }
  }

  writer_ = std::move(deltaLog_log_writer);

  assert(IsDeltaLogFileOpen());

  return Status::OK();
}

Status DeltaLogFileBuilder::WriteDeltaLogToFile(const Slice& key,
                                                const Slice& deltaLog,
                                                bool is_anchor) {
  assert(IsDeltaLogFileOpen());

  Status s = writer_->AddRecord(key, deltaLog, is_anchor);

  TEST_SYNC_POINT_CALLBACK("DeltaLogFileBuilder::WriteDeltaLogToFile:AddRecord",
                           &s);

  if (!s.ok()) {
    return s;
  }

  ++deltaLog_count_;
  deltaLog_bytes_ +=
      DeltaLogRecord::kHeaderSize_ + key.size() + deltaLog.size();

  return Status::OK();
}

Status DeltaLogFileBuilder::CloseDeltaLogFile() {
  assert(IsDeltaLogFileOpen());

  Status OnDeltaLogFileCompletedStatus, s;
  if (deltaLog_callback_) {
    s = deltaLog_callback_->OnDeltaLogFileCompleted(
        deltaLog_file_paths_->back(), column_family_name_, job_id_,
        deltaLog_file_id_, creation_reason_, OnDeltaLogFileCompletedStatus,
        deltaLog_count_, deltaLog_bytes_);
  }

  assert(deltaLog_file_additions_);
  deltaLog_file_additions_->emplace_back(deltaLog_file_id_, deltaLog_count_,
                                         deltaLog_bytes_);

  assert(immutable_options_);
  ROCKS_LOG_INFO(immutable_options_->logger,
                 "[%s] [JOB %d] Generated deltaLog file #%" PRIu64 ": %" PRIu64
                 " total deltaLogs, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, deltaLog_file_id_,
                 deltaLog_count_, deltaLog_bytes_);

  writer_.reset();
  deltaLog_count_ = 0;
  deltaLog_bytes_ = 0;

  return s;
}

Status DeltaLogFileBuilder::CloseDeltaLogFileIfNeeded() {
  assert(IsDeltaLogFileOpen());

  const WritableFileWriter* const file_writer = writer_->file();
  assert(file_writer);

  if (file_writer->GetFileSize() < deltaLog_file_size_) {
    return Status::OK();
  }

  return CloseDeltaLogFile();
}

void DeltaLogFileBuilder::Abandon(const Status& s) {
  if (!IsDeltaLogFileOpen()) {
    return;
  }
  if (deltaLog_callback_) {
    // DeltaLogFileBuilder::Abandon() is called because of error while writing
    // to DeltaLog files. So we can ignore the below error.
    deltaLog_callback_
        ->OnDeltaLogFileCompleted(deltaLog_file_paths_->back(),
                                  column_family_name_, job_id_,
                                  writer_->get_log_number(), creation_reason_,
                                  s, deltaLog_count_, deltaLog_bytes_)
        .PermitUncheckedError();
  }

  writer_.reset();
  deltaLog_count_ = 0;
  deltaLog_bytes_ = 0;
}

Status DeltaLogFileBuilder::PutDeltaLogIntoCacheIfNeeded(
    const Slice& key, const Slice& deltaLog) const {
  Status s = Status::OK();

  auto deltaLog_cache = immutable_options_->deltaLog_cache;
  auto statistics = immutable_options_->statistics.get();
  bool warm_cache =
      prepopulate_deltaLog_cache_ == PrepopulateDeltaLogCache::kFlushOnly &&
      creation_reason_ == DeltaLogFileCreationReason::kFlush;

  if (deltaLog_cache && warm_cache) {
    const OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                            deltaLog_file_id_);
    const CacheKey cache_key = base_cache_key.WithOffset(0);
    const Slice key = cache_key.AsSlice();

    const Cache::Priority priority = Cache::Priority::BOTTOM;

    // Objects to be put into the cache have to be heap-allocated and
    // self-contained, i.e. own their contents. The Cache has to be able to
    // take unique ownership of them.
    CacheAllocationPtr allocation =
        AllocateBlock(deltaLog.size(), deltaLog_cache->memory_allocator());
    memcpy(allocation.get(), deltaLog.data(), deltaLog.size());
    std::unique_ptr<DeltaLogContents> buf =
        DeltaLogContents::Create(std::move(allocation), deltaLog.size());

    Cache::CacheItemHelper* const cache_item_helper =
        DeltaLogContents::GetCacheItemHelper();
    assert(cache_item_helper);

    if (immutable_options_->lowest_used_cache_tier ==
        CacheTier::kNonVolatileBlockTier) {
      s = deltaLog_cache->Insert(key, buf.get(), cache_item_helper,
                                 buf->ApproximateMemoryUsage(),
                                 nullptr /* cache_handle */, priority);
    } else {
      s = deltaLog_cache->Insert(key, buf.get(), buf->ApproximateMemoryUsage(),
                                 cache_item_helper->del_cb,
                                 nullptr /* cache_handle */, priority);
    }

    if (s.ok()) {
      RecordTick(statistics, DELTALOG_DB_CACHE_ADD);
      RecordTick(statistics, DELTALOG_DB_CACHE_BYTES_WRITE, buf->size());
      buf.release();
    } else {
      RecordTick(statistics, DELTALOG_DB_CACHE_ADD_FAILURES);
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
