#pragma once

#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, rocksdb::Logger* logger) const
{
    externalValueType tempExternalValueTypeStructForCheck;
    memcpy(&tempExternalValueTypeStructForCheck, existing_value->data(), sizeof(externalValueType));
    if (tempExternalValueTypeStructForCheck.mergeFlag_ == false) {
        cerr << "[ERROR]:[Addons]-[RocksDBInternalMergeOperator]-[FullMerge] find object request merge without correct merge flag" << endl;
        return false;
    }
    new_value->assign(existing_value->data());
    return true;
};

bool RocksDBInternalMergeOperator::PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
    const rocksdb::Slice& right_operand, std::string* new_value,
    rocksdb::Logger* logger) const
{
    string emptyValueStr = "";
    new_value->assign(emptyValueStr);
    return true;
};

vector<string> FieldUpdateMergeOperator::stringSplit(string str, string token) const
{
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0)
                result.push_back(str);
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
};

bool FieldUpdateMergeOperator::FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, rocksdb::Logger* logger) const
{
    vector<std::string> words = stringSplit(existing_value->ToString(), ",");
    for (auto q : operand_list) {
        vector<string> operandVector = stringSplit(q, ",");
        for (long unsigned int i = 0; i < operandVector.size(); i += 2) {
            words[stoi(operandVector[i])] = operandVector[i + 1];
        }
    }
    string temp;
    for (long unsigned int i = 0; i < words.size() - 1; i++) {
        temp += words[i] + ",";
    }
    temp += words[words.size() - 1];
    new_value->assign(temp);
    return true;
};

bool FieldUpdateMergeOperator::PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
    const rocksdb::Slice& right_operand, std::string* new_value,
    rocksdb::Logger* logger) const
{
    new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
    return true;
};

} // namespace DELTAKV_NAMESPACE