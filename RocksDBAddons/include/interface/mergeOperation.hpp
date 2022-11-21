#pragma once

#include "common/dataStructure.hpp"
#include "common/rocksdbHeaders.hpp"
#include <bits/stdc++.h>
using namespace std;

namespace DELTAKV_NAMESPACE {

class RocksDBInternalMergeOperator : public rocksdb::MergeOperator {
public:
    bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, rocksdb::Logger* logger) const override;

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
        const rocksdb::Slice& right_operand, std::string* new_value,
        rocksdb::Logger* logger) const override;

    static const char* kClassName() { return "RocksDBInternalMergeOperator"; }
    const char* Name() const override { return kClassName(); }
};

class FieldUpdateMergeOperator : public rocksdb::MergeOperator {
public:
    bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, rocksdb::Logger* logger) const override;

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
        const rocksdb::Slice& right_operand, std::string* new_value,
        rocksdb::Logger* logger) const override;

    static const char* kClassName() { return "FieldUpdateMergeOperator"; }
    const char* Name() const override { return kClassName(); }

    vector<string> stringSplit(string str, string token) const;
};

} // namespace DELTAKV_NAMESPACE