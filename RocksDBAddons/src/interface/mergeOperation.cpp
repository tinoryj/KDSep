#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

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

bool FieldUpdateMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
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

bool FieldUpdateMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    new_value->assign(left_operand.ToString() + "," + right_operand.ToString());
    return true;
};

} // namespace DELTAKV_NAMESPACE