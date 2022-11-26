#pragma once

#include "common/dataStructure.hpp"
#include "common/rocksdbHeaders.hpp"
#include "utils/loggerColor.hpp"
#include <bits/stdc++.h>
using namespace std;
using namespace rocksdb;

namespace DELTAKV_NAMESPACE {

class DeltaKVMergeOperator {
public:
    virtual bool Merge(string rawValue, vector<string> operandList, string* finalValue) = 0;
    virtual string kClassName() = 0;
};

class FieldUpdateMergeOperator : public DeltaKVMergeOperator {
public:
    bool Merge(string rawValue, vector<string> operandList, string* finalValue);
    string kClassName();
};

} // namespace DELTAKV_NAMESPACE