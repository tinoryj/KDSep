//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/deltaLog/deltaLog_log_format.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

void DeltaLogLogHeader::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogLogHeader::kSize);
  PutFixed32(dst, kMagicNumberDeltaLog);
  PutFixed32(dst, version);
  PutFixed32(dst, column_family_id);
}

Status DeltaLogLogHeader::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding deltaLog log header";
  if (src.size() != DeltaLogLogHeader::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog file header size");
  }
  uint32_t magic_number;
  unsigned char flags;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed32(&src, &column_family_id)) {
    return Status::Corruption(
        kErrorMessage,
        "Error decoding magic number, version and column family id");
  }
  if (magic_number != kMagicNumberDeltaLog) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (version != kVersion1DeltaLog) {
    return Status::Corruption(kErrorMessage, "Unknown header version");
  }
  return Status::OK();
}

void DeltaLogLogFooter::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogLogFooter::kSize);
  PutFixed32(dst, kMagicNumberDeltaLog);
  PutFixed64(dst, deltaLog_count);
}

Status DeltaLogLogFooter::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding deltaLog log footer";
  if (src.size() != DeltaLogLogFooter::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog file footer size");
  }
  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed64(&src, &deltaLog_count)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (magic_number != kMagicNumberDeltaLog) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  return Status::OK();
}

void DeltaLogLogRecord::EncodeHeaderTo(std::string* dst, bool is_anchor) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogLogRecord::kHeaderSize + key.size() + value.size());
  PutFixed64(dst, key.size());
  PutFixed64(dst, value.size());
  unsigned char is_anchor_flag = (is_anchor ? 1 : 0);
  dst->push_back(is_anchor_flag);
}

Status DeltaLogLogRecord::DecodeHeaderFrom(Slice src, bool& is_anchor) {
  const char* kErrorMessage = "Error while decoding deltaLog record";
  if (src.size() != DeltaLogLogRecord::kHeaderSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog record header size");
  }
  if (!GetFixed64(&src, &key_size) || !GetFixed64(&src, &value_size)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  unsigned char is_anchor_flag = src.data()[0];
  is_anchor = (is_anchor_flag & 1) == 1;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
