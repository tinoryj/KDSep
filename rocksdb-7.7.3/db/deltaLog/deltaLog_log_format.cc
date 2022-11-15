//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/deltaLog/deltaLog_log_format.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

void DeltaLogHeader::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogHeader::kSize_);
  PutFixed32(dst, kMagicNumberDeltaLog_);
  PutFixed32(dst, version_);
  PutFixed32(dst, column_family_id_);
}

Status DeltaLogHeader::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding deltaLog log header";
  if (src.size() != DeltaLogHeader::kSize_) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog file header size");
  }
  uint32_t magic_number;
  unsigned char flags;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version_) ||
      !GetFixed32(&src, &column_family_id_)) {
    return Status::Corruption(
        kErrorMessage,
        "Error decoding magic number, version_ and column family id");
  }
  if (magic_number != kMagicNumberDeltaLog_) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (version_ != kVersion1DeltaLog_) {
    return Status::Corruption(kErrorMessage, "Unknown header version_");
  }
  return Status::OK();
}

void DeltaLogFooter::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogFooter::kSize_);
  PutFixed32(dst, kMagicNumberDeltaLog_);
  PutFixed64(dst, deltaLog_count);
}

Status DeltaLogFooter::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding deltaLog log footer";
  if (src.size() != DeltaLogFooter::kSize_) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog file footer size");
  }
  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed64(&src, &deltaLog_count)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (magic_number != kMagicNumberDeltaLog_) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  return Status::OK();
}

void DeltaLogRecord::EncodeHeaderTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(DeltaLogRecord::kHeaderSize_ + key_.size() + value_.size());
  PutFixed64(dst, key_.size());
  PutFixed64(dst, value_.size());
  PutFixed64(dst, sequence_number_);
  unsigned char is_anchor_flag = (is_anchor_ ? 1 : 0);
  dst->push_back(is_anchor_flag);
}

Status DeltaLogRecord::DecodeHeaderFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding deltaLog record";
  if (src.size() != DeltaLogRecord::kHeaderSize_) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected deltaLog record header size");
  }
  if (!GetFixed64(&src, &key_size_) || !GetFixed64(&src, &value_size_) ||
      !GetFixed64(&src, &sequence_number_)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  unsigned char is_anchor_flag = src.data()[0];
  is_anchor_ = (is_anchor_flag & 1) == 1;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
