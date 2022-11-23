#pragma once

#include "common/dataStructure.hpp"
#include "common/rocksdbHeaders.hpp"
#include <bits/stdc++.h>
using namespace std;
using namespace rocksdb;

namespace DELTAKV_NAMESPACE {

class FieldUpdateMergeOperator : public MergeOperator {
public:
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override;

    static const char* kClassName() { return "FieldUpdateMergeOperator"; }
    const char* Name() const override { return kClassName(); }

    vector<string> stringSplit(string str, string token) const;
};

} // namespace DELTAKV_NAMESPACE