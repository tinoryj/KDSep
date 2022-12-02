#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

vector<string> stringSplit(string str, string token)
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

bool DeltaKVFieldUpdateMergeOperator::Merge(string rawValue, vector<string> operandList, string* finalValue)
{
    vector<string> rawValueFieldsVec = stringSplit(rawValue, ",");
    for (auto q : operandList) {
        string indexStr = q.substr(0, q.find(","));
        int index = stoi(indexStr);
        string updateContentStr = q.substr(q.find(",") + 1, q.size());
        debug_trace("merge operand = %s, current index =  %d, content = %s\n", q.c_str(), index, updateContentStr.c_str());
        rawValueFieldsVec[index - 1].assign(updateContentStr);
    }

    string temp;
    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        finalValue->append(rawValueFieldsVec[i]);
        finalValue->append(",");
    }
    finalValue->append(rawValueFieldsVec[rawValueFieldsVec.size() - 1]);
    return true;
}

string DeltaKVFieldUpdateMergeOperator::kClassName()
{
    return "DeltaKVFieldUpdateMergeOperator";
}

} // namespace DELTAKV_NAMESPACE