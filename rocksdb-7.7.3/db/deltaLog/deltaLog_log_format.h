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

constexpr uint32_t kMagicNumberDeltaLog = 2395959;  // 0x00248f37
constexpr uint32_t kVersion1DeltaLog = 1;

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
struct DeltaLogLogHeader {
  static constexpr size_t kSize = 12;

  DeltaLogLogHeader() = default;
  DeltaLogLogHeader(uint32_t _column_family_id)
      : column_family_id(_column_family_id) {}

  uint32_t version = kVersion1DeltaLog;
  uint32_t column_family_id = 0;

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
struct DeltaLogLogFooter {
  static constexpr size_t kSize = 12;

  uint64_t deltaLog_count = 0;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// DeltaLog record format (32 bytes header + key + value):
//
//    +------------+--------------+---------+---------+-----------+
//    | key length | value length | anchor  | key     |   value   |
//    +------------+--------------+---------+---------+-----------+
//    |   Fixed64  |   Fixed64    | char    | key len | value len |
//    +------------+--------------+---------+---------+-----------+
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
struct DeltaLogLogRecord {
  // header include fields up to deltaLog CRC
  static constexpr size_t kHeaderSize = 17;

  uint64_t key_size = 0;
  uint64_t value_size = 0;
  char anchor = '0';  // 0-> not anchor, 1-> anchor
  Slice key;
  Slice value;
  std::unique_ptr<char[]> key_buf;
  std::unique_ptr<char[]> value_buf;

  uint64_t record_size() const { return kHeaderSize + key_size + value_size; }

  void EncodeHeaderTo(std::string* dst, bool is_anchor);

  Status DecodeHeaderFrom(Slice src, bool& is_anchor);
};

}  // namespace ROCKSDB_NAMESPACE
