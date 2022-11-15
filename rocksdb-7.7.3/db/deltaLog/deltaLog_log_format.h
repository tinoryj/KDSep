//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Log format information shared by reader and writer.

#pragma once

#include <memory>
#include <utility>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint32_t kMagicNumberDeltaLog_ = 2395959;  // 0x00248f37
constexpr uint32_t kVersion1DeltaLog_ = 1;

// Format of deltaLog log file header (30 bytes):
//
//    +--------------+---------+---------+
//    | magic number | version |  cf id  |
//    +--------------+---------+---------+
//    |   Fixed32    | Fixed32 | Fixed32 |
//    +--------------+---------+---------+
//
// List of flags:
//   has_ttl: Whether the file contain TTL data.
//
// Expiration range in the header is a rough range based on
// deltaLog_db_options.ttl_range_secs.
struct DeltaLogHeader {
  static constexpr size_t kSize_ = 12;

  DeltaLogHeader() = default;
  DeltaLogHeader(uint32_t _column_family_id_)
      : column_family_id_(_column_family_id_) {}

  uint32_t version_ = kVersion1DeltaLog_;
  uint32_t column_family_id_ = 0;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// Format of deltaLog log file footer (32 bytes):
//
//    +--------------+----------------+
//    | magic number | deltaLog count |
//    +--------------+----------------+
//    |   Fixed32    |  Fixed64       |
//    +--------------+----------------+
//
// The footer will be presented only when the deltaLog file is properly closed.
//
// Unlike the same field in file header, expiration range in the footer is the
// range of smallest and largest expiration of the data in this file.
struct DeltaLogFooter {
  static constexpr size_t kSize_ = 12;

  uint64_t deltaLog_count = 0;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// DeltaLog record format (32 bytes header + key + value):
//
//    +------------+--------------+--------+---------+---------+-----------+
//    | key length | value length | anchor | Seq ID  | key     | value     |
//    +------------+--------------+--------+---------+---------+-----------+
//    |   Fixed64  |   Fixed64    | char   | Fixed64 | key len | value len |
//    +------------+--------------+--------+---------+---------+-----------+
//
// If file has has_ttl = false, expiration field is always 0, and the deltaLog
// doesn't has expiration.
//
// Also note that if compression is used, value is compressed value and value
// length is compressed value length.
//
// Header CRC is the checksum of (key_len + val_len + expiration), while
// deltaLog CRC is the checksum of (key + value).
//
// We could use variable length encoding (Varint64) to save more space, but it
// make reader more complicated.
struct DeltaLogRecord {
  // header include fields up to deltaLog CRC
  static constexpr size_t kHeaderSize_ = 25;

  uint64_t key_size_ = 0;
  uint64_t value_size_ = 0;
  uint64_t sequence_number_ = 0;
  bool is_anchor_ = false;
  Slice key_;
  Slice value_;
  std::unique_ptr<char[]> key_buf_;
  std::unique_ptr<char[]> value_buf_;

  uint64_t record_size() const {
    return kHeaderSize_ + key_size_ + value_size_;
  }

  void EncodeHeaderTo(std::string* dst);

  Status DecodeHeaderFrom(Slice src);
};

}  // namespace ROCKSDB_NAMESPACE
