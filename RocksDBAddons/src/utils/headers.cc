#include "utils/headers.hh"

namespace DELTAKV_NAMESPACE {

inline char* EncodeVarint32(char* dst, uint32_t v) {
    // Operate on characters as unsigneds
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline size_t PutVarint32(char* buf, uint32_t v) {
  char* ptr = EncodeVarint32(buf, v);
  return static_cast<size_t>(ptr - buf);
}

inline size_t PutVarint32Varint32(char* buf, uint32_t v1, uint32_t v2) {
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  return static_cast<size_t>(ptr - buf);
}

inline size_t PutVarint32Varint32Varint32(char* buf, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  return static_cast<size_t>(ptr - buf);
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline size_t PutVarint64(char* buf, uint64_t v) {
  char* ptr = EncodeVarint64(buf, v);
  return static_cast<size_t>(ptr - buf);
}

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

inline size_t GetVarint32(const char* input, uint32_t* value) {
  const char* p = input;
  const char* limit = p + 5;
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return 0;
  } else {
    return static_cast<size_t>(q - p);
  }
}

inline size_t PutVlogIndexVarint(char* buf, externalIndexInfo index) {
    return PutVarint32Varint32Varint32(buf, 
            index.externalFileID_, index.externalFileOffset_,
            index.externalContentSize_);
}

inline externalIndexInfo GetVlogIndexVarint(char* buf, size_t& offset) {
    offset = 0;
    externalIndexInfo index;
    size_t sz1 = GetVarint32(buf, &index.externalFileID_);
    size_t sz2 = GetVarint32(buf + sz1, &index.externalFileOffset_);
    size_t sz3 = GetVarint32(buf + sz1 + sz2, &index.externalContentSize_);
    if (sz1 == 0 || sz2 == 0 || sz3 == 0) {
        fprintf(stderr, "ERROR: GetVariant32 fault. %lu %lu %lu\n",
                sz1, sz2, sz3);
        exit(1);
    }
    return index;
}

}
