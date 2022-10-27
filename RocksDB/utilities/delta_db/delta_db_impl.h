//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_iter.h"
#include "db/delta/delta_log_format.h"
#include "db/delta/delta_log_writer.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/wal_filter.h"
#include "util/mutexlock.h"
#include "util/timer_queue.h"
#include "utilities/delta_db/delta_db.h"
#include "utilities/delta_db/delta_file.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class ColumnFamilyHandle;
class ColumnFamilyData;
class SystemClock;

struct FlushJobInfo;

namespace delta_db {

struct DeltaCompactionContext;
struct DeltaCompactionContextGC;
class DeltaDBImpl;
class DeltaFile;

// Comparator to sort "TTL" aware Delta files based on the lower value of
// TTL range.
struct DeltaFileComparatorTTL {
  bool operator()(const std::shared_ptr<DeltaFile>& lhs,
                  const std::shared_ptr<DeltaFile>& rhs) const;
};

struct DeltaFileComparator {
  bool operator()(const std::shared_ptr<DeltaFile>& lhs,
                  const std::shared_ptr<DeltaFile>& rhs) const;
};

/**
 * The implementation class for DeltaDB. It manages the delta logs, which
 * are sequentially written files. Delta logs can be of the TTL or non-TTL
 * varieties; the former are cleaned up when they expire, while the latter
 * are (optionally) garbage collected.
 */
class DeltaDBImpl : public DeltaDB {
  friend class DeltaFile;
  friend class DeltaDBIterator;
  friend class DeltaDBListener;
  friend class DeltaDBListenerGC;
  friend class DeltaIndexCompactionFilterBase;
  friend class DeltaIndexCompactionFilterGC;

 public:
  // deletions check period
  static constexpr uint32_t kDeleteCheckPeriodMillisecs = 2 * 1000;

  // sanity check task
  static constexpr uint32_t kSanityCheckPeriodMillisecs = 20 * 60 * 1000;

  // how many random access open files can we tolerate
  static constexpr uint32_t kOpenFilesTrigger = 100;

  // how often to schedule reclaim open files.
  static constexpr uint32_t kReclaimOpenFilesPeriodMillisecs = 1 * 1000;

  // how often to schedule delete obs files periods
  static constexpr uint32_t kDeleteObsoleteFilesPeriodMillisecs = 10 * 1000;

  // how often to schedule expired files eviction.
  static constexpr uint32_t kEvictExpiredFilesPeriodMillisecs = 10 * 1000;

  // when should oldest file be evicted:
  // on reaching 90% of delta_dir_size
  static constexpr double kEvictOldestFileAtSize = 0.9;

  using DeltaDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using DeltaDB::Get;
  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;

  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             uint64_t* expiration) override;

  using DeltaDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& read_options) override;

  using DeltaDB::NewIterators;
  virtual Status NewIterators(
      const ReadOptions& /*read_options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/,
      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not implemented");
  }

  using DeltaDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& read_options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using DeltaDB::Write;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  virtual Status Close() override;

  using DeltaDB::PutWithTTL;
  Status PutWithTTL(const WriteOptions& options, const Slice& key,
                    const Slice& value, uint64_t ttl) override;

  using DeltaDB::PutUntil;
  Status PutUntil(const WriteOptions& options, const Slice& key,
                  const Slice& value, uint64_t expiration) override;

  using DeltaDB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  DeltaDBOptions GetDeltaDBOptions() const override;

  DeltaDBImpl(const std::string& dbname, const DeltaDBOptions& bdb_options,
              const DBOptions& db_options,
              const ColumnFamilyOptions& cf_options);

  virtual Status DisableFileDeletions() override;

  virtual Status EnableFileDeletions(bool force) override;

  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual void GetLiveFilesMetaData(std::vector<LiveFileMetaData>*) override;

  ~DeltaDBImpl();

  Status Open(std::vector<ColumnFamilyHandle*>* handles);

  Status SyncDeltaFiles() override;

  // Common part of the two GetCompactionContext methods below.
  // REQUIRES: read lock on mutex_
  void GetCompactionContextCommon(DeltaCompactionContext* context);

  void GetCompactionContext(DeltaCompactionContext* context);
  void GetCompactionContext(DeltaCompactionContext* context,
                            DeltaCompactionContextGC* context_gc);

#ifndef NDEBUG
  Status TEST_GetDeltaValue(const Slice& key, const Slice& index_entry,
                            PinnableSlice* value);

  void TEST_AddDummyDeltaFile(uint64_t delta_file_number,
                              SequenceNumber immutable_sequence);

  std::vector<std::shared_ptr<DeltaFile>> TEST_GetDeltaFiles() const;

  std::vector<std::shared_ptr<DeltaFile>> TEST_GetLiveImmNonTTLFiles() const;

  std::vector<std::shared_ptr<DeltaFile>> TEST_GetObsoleteFiles() const;

  Status TEST_CloseDeltaFile(std::shared_ptr<DeltaFile>& bfile);

  void TEST_ObsoleteDeltaFile(std::shared_ptr<DeltaFile>& delta_file,
                              SequenceNumber obsolete_seq = 0,
                              bool update_size = true);

  void TEST_EvictExpiredFiles();

  void TEST_DeleteObsoleteFiles();

  uint64_t TEST_live_sst_size();

  const std::string& TEST_delta_dir() const { return delta_dir_; }

  void TEST_InitializeDeltaFileToSstMapping(
      const std::vector<LiveFileMetaData>& live_files);

  void TEST_ProcessFlushJobInfo(const FlushJobInfo& info);

  void TEST_ProcessCompactionJobInfo(const CompactionJobInfo& info);

#endif  //  !NDEBUG

 private:
  class DeltaInserter;

  // Create a snapshot if there isn't one in read options.
  // Return true if a snapshot is created.
  bool SetSnapshotIfNeeded(ReadOptions* read_options);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value, uint64_t* expiration = nullptr);

  Status GetDeltaValue(const Slice& key, const Slice& index_entry,
                       PinnableSlice* value, uint64_t* expiration = nullptr);

  Status GetRawDeltaFromFile(const Slice& key, uint64_t file_number,
                             uint64_t offset, uint64_t size,
                             PinnableSlice* value,
                             CompressionType* compression_type);

  Slice GetCompressedSlice(const Slice& raw,
                           std::string* compression_output) const;

  Status DecompressSlice(const Slice& compressed_value,
                         CompressionType compression_type,
                         PinnableSlice* value_output) const;

  // Close a file by appending a footer, and removes file from open files list.
  // REQUIRES: lock held on write_mutex_, write lock held on both the db mutex_
  // and the delta file's mutex_. If called on a delta file which is visible
  // only to a single thread (like in the case of new files written during
  // compaction/GC), the locks on write_mutex_ and the delta file's mutex_ can
  // be avoided.
  Status CloseDeltaFile(std::shared_ptr<DeltaFile> bfile);

  // Close a file if its size exceeds delta_file_size
  // REQUIRES: lock held on write_mutex_.
  Status CloseDeltaFileIfNeeded(std::shared_ptr<DeltaFile>& bfile);

  // Mark file as obsolete and move the file to obsolete file list.
  //
  // REQUIRED: hold write lock of mutex_ or during DB open.
  void ObsoleteDeltaFile(std::shared_ptr<DeltaFile> delta_file,
                         SequenceNumber obsolete_seq, bool update_size);

  Status PutDeltaValue(const WriteOptions& options, const Slice& key,
                       const Slice& value, uint64_t expiration,
                       WriteBatch* batch);

  Status AppendDelta(const std::shared_ptr<DeltaFile>& bfile,
                     const std::string& headerbuf, const Slice& key,
                     const Slice& value, uint64_t expiration,
                     std::string* index_entry);

  // Create a new delta file and associated writer.
  Status CreateDeltaFileAndWriter(bool has_ttl,
                                  const ExpirationRange& expiration_range,
                                  const std::string& reason,
                                  std::shared_ptr<DeltaFile>* delta_file,
                                  std::shared_ptr<DeltaLogWriter>* writer);

  // Get the open non-TTL delta log file, or create a new one if no such file
  // exists.
  Status SelectDeltaFile(std::shared_ptr<DeltaFile>* delta_file);

  // Get the open TTL delta log file for a certain expiration, or create a new
  // one if no such file exists.
  Status SelectDeltaFileTTL(uint64_t expiration,
                            std::shared_ptr<DeltaFile>* delta_file);

  std::shared_ptr<DeltaFile> FindDeltaFileLocked(uint64_t expiration) const;

  // periodic sanity check. Bunch of checks
  std::pair<bool, int64_t> SanityCheck(bool aborted);

  // Delete files that have been marked obsolete (either because of TTL
  // or GC). Check whether any snapshots exist which refer to the same.
  std::pair<bool, int64_t> DeleteObsoleteFiles(bool aborted);

  // periodically check if open delta files and their TTL's has expired
  // if expired, close the sequential writer and make the file immutable
  std::pair<bool, int64_t> EvictExpiredFiles(bool aborted);

  // if the number of open files, approaches ULIMIT's this
  // task will close random readers, which are kept around for
  // efficiency
  std::pair<bool, int64_t> ReclaimOpenFiles(bool aborted);

  std::pair<bool, int64_t> RemoveTimerQ(TimerQueue* tq, bool aborted);

  // Adds the background tasks to the timer queue
  void StartBackgroundTasks();

  // add a new Delta File
  std::shared_ptr<DeltaFile> NewDeltaFile(
      bool has_ttl, const ExpirationRange& expiration_range,
      const std::string& reason);

  // Register a new delta file.
  // REQUIRES: write lock on mutex_.
  void RegisterDeltaFile(std::shared_ptr<DeltaFile> delta_file);

  // collect all the delta log files from the delta directory
  Status GetAllDeltaFiles(std::set<uint64_t>* file_numbers);

  // Open all delta files found in delta_dir.
  Status OpenAllDeltaFiles();

  // Link an SST to a delta file. Comes in locking and non-locking varieties
  // (the latter is used during Open).
  template <typename Linker>
  void LinkSstToDeltaFileImpl(uint64_t sst_file_number,
                              uint64_t delta_file_number, Linker linker);

  void LinkSstToDeltaFile(uint64_t sst_file_number, uint64_t delta_file_number);

  void LinkSstToDeltaFileNoLock(uint64_t sst_file_number,
                                uint64_t delta_file_number);

  // Unlink an SST from a delta file.
  void UnlinkSstFromDeltaFile(uint64_t sst_file_number,
                              uint64_t delta_file_number);

  // Initialize the mapping between delta files and SSTs during Open.
  void InitializeDeltaFileToSstMapping(
      const std::vector<LiveFileMetaData>& live_files);

  // Update the mapping between delta files and SSTs after a flush and mark
  // any unneeded delta files obsolete.
  void ProcessFlushJobInfo(const FlushJobInfo& info);

  // Update the mapping between delta files and SSTs after a compaction and
  // mark any unneeded delta files obsolete.
  void ProcessCompactionJobInfo(const CompactionJobInfo& info);

  // Mark an immutable non-TTL delta file obsolete assuming it has no more SSTs
  // linked to it, and all memtables from before the delta file became immutable
  // have been flushed. Note: should only be called if the condition holds for
  // all lower-numbered non-TTL delta files as well.
  bool MarkDeltaFileObsoleteIfNeeded(
      const std::shared_ptr<DeltaFile>& delta_file,
      SequenceNumber obsolete_seq);

  // Mark all immutable non-TTL delta files that aren't needed by any SSTs as
  // obsolete. Comes in two varieties; the version used during Open need not
  // worry about locking or snapshots.
  template <class Functor>
  void MarkUnreferencedDeltaFilesObsoleteImpl(Functor mark_if_needed);

  void MarkUnreferencedDeltaFilesObsolete();
  void MarkUnreferencedDeltaFilesObsoleteDuringOpen();

  void UpdateLiveSSTSize();

  Status GetDeltaFileReader(const std::shared_ptr<DeltaFile>& delta_file,
                            std::shared_ptr<RandomAccessFileReader>* reader);

  // hold write mutex on file and call.
  // Close the above Random Access reader
  void CloseRandomAccessLocked(const std::shared_ptr<DeltaFile>& bfile);

  // hold write mutex on file and call
  // creates a sequential (append) writer for this deltafile
  Status CreateWriterLocked(const std::shared_ptr<DeltaFile>& bfile);

  // returns a DeltaLogWriter object for the file. If writer is not
  // already present, creates one. Needs Write Mutex to be held
  Status CheckOrCreateWriterLocked(const std::shared_ptr<DeltaFile>& delta_file,
                                   std::shared_ptr<DeltaLogWriter>* writer);

  // checks if there is no snapshot which is referencing the
  // deltas
  bool VisibleToActiveSnapshot(const std::shared_ptr<DeltaFile>& file);
  bool FileDeleteOk_SnapshotCheckLocked(
      const std::shared_ptr<DeltaFile>& bfile);

  void CopyDeltaFiles(std::vector<std::shared_ptr<DeltaFile>>* bfiles_copy);

  uint64_t EpochNow() { return clock_->NowMicros() / 1000000; }

  // Check if inserting a new delta will make DB grow out of space.
  // If is_fifo = true, FIFO eviction will be triggered to make room for the
  // new delta. If force_evict = true, FIFO eviction will evict delta files
  // even eviction will not make enough room for the new delta.
  Status CheckSizeAndEvictDeltaFiles(uint64_t delta_size,
                                     bool force_evict = false);

  // name of the database directory
  std::string dbname_;

  // the base DB
  DBImpl* db_impl_;
  Env* env_;
  SystemClock* clock_;
  // the options that govern the behavior of Delta Storage
  DeltaDBOptions bdb_options_;
  DBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  FileOptions file_options_;

  // Raw pointer of statistic. db_options_ has a std::shared_ptr to hold
  // ownership.
  Statistics* statistics_;

  // by default this is "delta_dir" under dbname_
  // but can be configured
  std::string delta_dir_;

  // pointer to directory
  std::unique_ptr<FSDirectory> dir_ent_;

  // Read Write Mutex, which protects all the data structures
  // HEAVILY TRAFFICKED
  mutable port::RWMutex mutex_;

  // Writers has to hold write_mutex_ before writing.
  mutable port::Mutex write_mutex_;

  // counter for delta file number
  std::atomic<uint64_t> next_file_number_;

  // entire metadata of all the DELTA files memory
  std::map<uint64_t, std::shared_ptr<DeltaFile>> delta_files_;

  // All live immutable non-TTL delta files.
  std::map<uint64_t, std::shared_ptr<DeltaFile>> live_imm_non_ttl_delta_files_;

  // The largest sequence number that has been flushed.
  SequenceNumber flush_sequence_;

  // opened non-TTL delta file.
  std::shared_ptr<DeltaFile> open_non_ttl_file_;

  // all the delta files which are currently being appended to based
  // on variety of incoming TTL's
  std::set<std::shared_ptr<DeltaFile>, DeltaFileComparatorTTL> open_ttl_files_;

  // Flag to check whether Close() has been called on this DB
  bool closed_;

  // timer based queue to execute tasks
  TimerQueue tqueue_;

  // number of files opened for random access/GET
  // counter is used to monitor and close excess RA files.
  std::atomic<uint32_t> open_file_count_;

  // Total size of all live delta files (i.e. exclude obsolete files).
  std::atomic<uint64_t> total_delta_size_;

  // total size of SST files.
  std::atomic<uint64_t> live_sst_size_;

  // Latest FIFO eviction timestamp
  //
  // REQUIRES: access with metex_ lock held.
  uint64_t fifo_eviction_seq_;

  // The expiration up to which latest FIFO eviction evicts.
  //
  // REQUIRES: access with metex_ lock held.
  uint64_t evict_expiration_up_to_;

  std::list<std::shared_ptr<DeltaFile>> obsolete_files_;

  // DeleteObsoleteFiles, DiableFileDeletions and EnableFileDeletions block
  // on the mutex to avoid contention.
  //
  // While DeleteObsoleteFiles hold both mutex_ and delete_file_mutex_, note
  // the difference. mutex_ only needs to be held when access the
  // data-structure, and delete_file_mutex_ needs to be held the whole time
  // during DeleteObsoleteFiles to avoid being run simultaneously with
  // DisableFileDeletions.
  //
  // If both of mutex_ and delete_file_mutex_ needs to be held, it is adviced
  // to hold delete_file_mutex_ first to avoid deadlock.
  mutable port::Mutex delete_file_mutex_;

  // Each call of DisableFileDeletions will increase disable_file_deletion_
  // by 1. EnableFileDeletions will either decrease the count by 1 or reset
  // it to zeor, depending on the force flag.
  //
  // REQUIRES: access with delete_file_mutex_ held.
  int disable_file_deletions_ = 0;

  uint32_t debug_level_;
};

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
