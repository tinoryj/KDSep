#pragma once

#include "common/dataStructure.hpp"

namespace DELTAKV_NAMESPACE {

char* EncodeVarint32(char* dst, uint32_t v); 
char* EncodeVarint64(char* dst, uint64_t v); 
void PutVarint32(std::string* dst, uint32_t v); 
void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2); 
void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint32_t v3); 
size_t PutVarint32(char* buf, uint32_t v); 
size_t PutVarint32Varint32(char* buf, uint32_t v1, uint32_t v2); 
size_t PutVarint32Varint32Varint32(char* buf, uint32_t v1,
                                        uint32_t v2, uint32_t v3); 
void PutVarint64(std::string* dst, uint64_t v); 
size_t PutVarint64(char* buf, uint64_t v); 

}
