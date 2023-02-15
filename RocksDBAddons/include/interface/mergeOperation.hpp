#pragma once

#include "common/dataStructure.hpp"
#include "common/rocksdbHeaders.hpp"
#include "utils/debug.hpp"
#include <bits/stdc++.h>
using namespace std;
using namespace rocksdb;

namespace DELTAKV_NAMESPACE {

class DeltaKVMergeOperator {
public:
    virtual bool Merge(string rawValue, vector<string> operandList, string* finalValue) = 0;
    virtual bool PartialMerge(vector<string> operandList, vector<string>& finalOperandList) = 0;
    virtual string kClassName() = 0;
};

class DeltaKVFieldUpdateMergeOperator : public DeltaKVMergeOperator {
public:
    bool Merge(string rawValue, vector<string> operandList, string* finalValue);
    bool PartialMerge(vector<string> operandList, vector<string>& finalOperandList);
    string kClassName();
};

class RocksDBInternalMergeOperator : public MergeOperator {
public:
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override;

    static const char* kClassName() { return "RocksDBInternalMergeOperator"; }
    const char* Name() const override { return kClassName(); }

private:
    bool FullMergeFieldUpdates(string rawValue, vector<string>& operandList, string* finalValue) const;
    bool PartialMergeFieldUpdates(vector<pair<internalValueType, string>> batchedOperandVec, string& finalDeltaListStr) const;
};

} // namespace DELTAKV_NAMESPACE
