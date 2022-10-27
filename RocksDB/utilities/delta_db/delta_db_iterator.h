//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "db/arena_wrapped_db_iter.h"
#include "rocksdb/iterator.h"
#include "util/stop_watch.h"
#include "utilities/delta_db/delta_db_impl.h"

namespace ROCKSDB_NAMESPACE {
class Statistics;
class SystemClock;

namespace delta_db {

using ROCKSDB_NAMESPACE::ManagedSnapshot;

class DeltaDBIterator : public Iterator {
 public:
  DeltaDBIterator(ManagedSnapshot* snapshot, ArenaWrappedDBIter* iter,
                  DeltaDBImpl* delta_db, SystemClock* clock,
                  Statistics* statistics)
      : snapshot_(snapshot),
        iter_(iter),
        delta_db_(delta_db),
        clock_(clock),
        statistics_(statistics) {}

  virtual ~DeltaDBIterator() = default;

  bool Valid() const override {
    if (!iter_->Valid()) {
      return false;
    }
    return status_.ok();
  }

  Status status() const override {
    if (!iter_->status().ok()) {
      return iter_->status();
    }
    return status_;
  }

  void SeekToFirst() override {
    StopWatch seek_sw(clock_, statistics_, DELTA_DB_SEEK_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_SEEK);
    iter_->SeekToFirst();
    while (UpdateDeltaValue()) {
      iter_->Next();
    }
  }

  void SeekToLast() override {
    StopWatch seek_sw(clock_, statistics_, DELTA_DB_SEEK_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_SEEK);
    iter_->SeekToLast();
    while (UpdateDeltaValue()) {
      iter_->Prev();
    }
  }

  void Seek(const Slice& target) override {
    StopWatch seek_sw(clock_, statistics_, DELTA_DB_SEEK_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_SEEK);
    iter_->Seek(target);
    while (UpdateDeltaValue()) {
      iter_->Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    StopWatch seek_sw(clock_, statistics_, DELTA_DB_SEEK_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_SEEK);
    iter_->SeekForPrev(target);
    while (UpdateDeltaValue()) {
      iter_->Prev();
    }
  }

  void Next() override {
    assert(Valid());
    StopWatch next_sw(clock_, statistics_, DELTA_DB_NEXT_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_NEXT);
    iter_->Next();
    while (UpdateDeltaValue()) {
      iter_->Next();
    }
  }

  void Prev() override {
    assert(Valid());
    StopWatch prev_sw(clock_, statistics_, DELTA_DB_PREV_MICROS);
    RecordTick(statistics_, DELTA_DB_NUM_PREV);
    iter_->Prev();
    while (UpdateDeltaValue()) {
      iter_->Prev();
    }
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    if (!iter_->IsDelta()) {
      return iter_->value();
    }
    return value_;
  }

  // Iterator::Refresh() not supported.

 private:
  // Return true if caller should continue to next value.
  bool UpdateDeltaValue() {
    value_.Reset();
    status_ = Status::OK();
    if (iter_->Valid() && iter_->status().ok() && iter_->IsDelta()) {
      Status s =
          delta_db_->GetDeltaValue(iter_->key(), iter_->value(), &value_);
      if (s.IsNotFound()) {
        return true;
      } else {
        if (!s.ok()) {
          status_ = s;
        }
        return false;
      }
    } else {
      return false;
    }
  }

  std::unique_ptr<ManagedSnapshot> snapshot_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  DeltaDBImpl* delta_db_;
  SystemClock* clock_;
  Statistics* statistics_;
  Status status_;
  PinnableSlice value_;
};
}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
