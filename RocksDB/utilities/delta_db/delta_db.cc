//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/delta_db/delta_db.h"

#include <cinttypes>

#include "logging/logging.h"
#include "utilities/delta_db/delta_db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

Status DeltaDB::Open(const Options& options, const DeltaDBOptions& bdb_options,
                     const std::string& dbname, DeltaDB** delta_db) {
  *delta_db = nullptr;
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DeltaDB::Open(db_options, bdb_options, dbname, column_families,
                           &handles, delta_db);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DeltaDB::Open(const DBOptions& db_options,
                     const DeltaDBOptions& bdb_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     DeltaDB** delta_db) {
  assert(handles);

  if (column_families.size() != 1 ||
      column_families[0].name != kDefaultColumnFamilyName) {
    return Status::NotSupported(
        "Delta DB doesn't support non-default column family.");
  }

  DeltaDBImpl* delta_db_impl = new DeltaDBImpl(dbname, bdb_options, db_options,
                                               column_families[0].options);
  Status s = delta_db_impl->Open(handles);
  if (s.ok()) {
    *delta_db = static_cast<DeltaDB*>(delta_db_impl);
  } else {
    if (!handles->empty()) {
      for (ColumnFamilyHandle* cfh : *handles) {
        delta_db_impl->DestroyColumnFamilyHandle(cfh);
      }

      handles->clear();
    }

    delete delta_db_impl;
    *delta_db = nullptr;
  }
  return s;
}

DeltaDB::DeltaDB() : StackableDB(nullptr) {}

void DeltaDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(
      log, "                                  DeltaDBOptions.delta_dir: %s",
      delta_dir.c_str());
  ROCKS_LOG_HEADER(
      log, "                             DeltaDBOptions.path_relative: %d",
      path_relative);
  ROCKS_LOG_HEADER(
      log, "                                   DeltaDBOptions.is_fifo: %d",
      is_fifo);
  ROCKS_LOG_HEADER(
      log,
      "                               DeltaDBOptions.max_db_size: %" PRIu64,
      max_db_size);
  ROCKS_LOG_HEADER(
      log,
      "                            DeltaDBOptions.ttl_range_secs: %" PRIu64,
      ttl_range_secs);
  ROCKS_LOG_HEADER(
      log,
      "                             DeltaDBOptions.min_delta_size: %" PRIu64,
      min_delta_size);
  ROCKS_LOG_HEADER(
      log,
      "                            DeltaDBOptions.bytes_per_sync: %" PRIu64,
      bytes_per_sync);
  ROCKS_LOG_HEADER(
      log,
      "                            DeltaDBOptions.delta_file_size: %" PRIu64,
      delta_file_size);
  ROCKS_LOG_HEADER(
      log, "                               DeltaDBOptions.compression: %d",
      static_cast<int>(compression));
  ROCKS_LOG_HEADER(
      log, "                 DeltaDBOptions.enable_garbage_collection: %d",
      enable_garbage_collection);
  ROCKS_LOG_HEADER(
      log, "                 DeltaDBOptions.garbage_collection_cutoff: %f",
      garbage_collection_cutoff);
  ROCKS_LOG_HEADER(
      log, "                  DeltaDBOptions.disable_background_tasks: %d",
      disable_background_tasks);
}

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif
