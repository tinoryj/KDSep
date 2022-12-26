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

} // namespace DELTAKV_NAMESPACE