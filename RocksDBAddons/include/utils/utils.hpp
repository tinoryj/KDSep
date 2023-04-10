#pragma once

#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

inline static uint64_t prefixSubstr(uint64_t prefix, uint64_t len) {
    return prefix & ((1 << len) - 1);
} 

inline static uint64_t prefixStrToU64(const string& prefix) {
    uint64_t prefix_u64 = 0;
    for (auto i = prefix.size() - 1; i >= 0; i--) {
        prefix_u64 = (prefix_u64 << 1) + (prefix[i] - '0');
    }
    return prefix_u64;
}

inline static uint64_t prefixExtract(uint64_t k) {
    return k & ((1ull << 56) - 1);
}

inline static uint64_t prefixLenExtract(uint64_t k) {
    return k >> 56;
}

inline static void copyInc(char* buf, uint64_t& index, 
        const void* read_buf, size_t size) {
    memcpy(buf + index, (char*)read_buf, size);
    index += size;
}

unsigned long long inline timevalToMicros(struct timeval& res) {
    return res.tv_sec * 1000000ull + res.tv_usec;
}

}
