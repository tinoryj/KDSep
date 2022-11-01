//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "db/deltaLog/deltaLog_garbage_meter.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// An internal iterator that passes each key-value encountered to
// DeltaLogGarbageMeter as inflow in order to measure the total number and size
// of deltaLogs in the compaction input on a per-deltaLog file basis.
class DeltaLogCountingIterator : public InternalIterator {
 public:
  DeltaLogCountingIterator(InternalIterator* iter,
                           DeltaLogGarbageMeter* deltaLog_garbage_meter)
      : iter_(iter), deltaLog_garbage_meter_(deltaLog_garbage_meter) {
    assert(iter_);
    assert(deltaLog_garbage_meter_);

    UpdateAndCountDeltaLogIfNeeded();
  }

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    UpdateAndCountDeltaLogIfNeeded();
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    UpdateAndCountDeltaLogIfNeeded();
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    UpdateAndCountDeltaLogIfNeeded();
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    UpdateAndCountDeltaLogIfNeeded();
  }

  void Next() override {
    assert(Valid());

    iter_->Next();
    UpdateAndCountDeltaLogIfNeeded();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());

    const bool res = iter_->NextAndGetResult(result);
    UpdateAndCountDeltaLogIfNeeded();
    return res;
  }

  void Prev() override {
    assert(Valid());

    iter_->Prev();
    UpdateAndCountDeltaLogIfNeeded();
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice user_key() const override {
    assert(Valid());
    return iter_->user_key();
  }

  Slice value() const override {
    assert(Valid());
    return iter_->value();
  }

  Status status() const override { return status_; }

  bool PrepareValue() override {
    assert(Valid());
    return iter_->PrepareValue();
  }

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return iter_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return iter_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

 private:
  void UpdateAndCountDeltaLogIfNeeded() {
    assert(!iter_->Valid() || iter_->status().ok());

    if (!iter_->Valid()) {
      status_ = iter_->status();
      return;
    }

    TEST_SYNC_POINT(
        "DeltaLogCountingIterator::UpdateAndCountDeltaLogIfNeeded:"
        "ProcessInFlow");

    status_ = deltaLog_garbage_meter_->ProcessInFlow(key(), value());
  }

  InternalIterator* iter_;
  DeltaLogGarbageMeter* deltaLog_garbage_meter_;
  Status status_;
};

}  // namespace ROCKSDB_NAMESPACE
