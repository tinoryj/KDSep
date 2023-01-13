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
    unordered_map<int, string> operandMap;
    string overallOperandListStr = "";
    for (auto it : operandList) {
        overallOperandListStr.append(it);
        overallOperandListStr.append(",");
    }
    overallOperandListStr = overallOperandListStr.substr(0, overallOperandListStr.size() - 1);
    // cerr << "overallOperandListStr = " << overallOperandListStr << endl;
    vector<string> rawOperandListVec = stringSplit(overallOperandListStr, ",");
    // cerr << "rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    vector<string> rawValueFieldsVec = stringSplit(rawValue, ",");
    for (auto q : operandMap) {
        // debug_trace("merge operand = %s, current index =  %d, content = %s, rawValue at indx = %s\n", q.c_str(), index, updateContentStr.c_str(), rawValueFieldsVec[index].c_str());
        rawValueFieldsVec[q.first].assign(q.second);
    }

    string temp;
    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        finalValue->append(rawValueFieldsVec[i]);
        finalValue->append(",");
    }
    finalValue->append(rawValueFieldsVec[rawValueFieldsVec.size() - 1]);
    return true;
}

bool DeltaKVFieldUpdateMergeOperator::PartialMerge(vector<string> operandList, vector<string>& finalOperandList)
{
    unordered_map<int, string> operandMap;
    string overallOperandListStr;
    for (auto it : operandList) {
        overallOperandListStr.append(it);
        overallOperandListStr.append(",");
    }
    overallOperandListStr = overallOperandListStr.substr(0, overallOperandListStr.size() - 1);
    // cerr << "[Partial] overallOperandListStr = " << overallOperandListStr << endl;
    vector<string> rawOperandListVec = stringSplit(overallOperandListStr, ",");
    // cerr << "[Partial] rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "[Partial] rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    string finalOperator = "";
    for (auto it : operandMap) {
        finalOperator.append(to_string(it.first) + "," + it.second + ",");
    }
    finalOperator = finalOperator.substr(0, finalOperator.size() - 1);
    finalOperandList.push_back(finalOperator);
    return true;
}

bool MergeStrCpy(str_cpy_t rawValue, vector<str_cpy_t> operandList, str_cpy_t* finalValue)
{
    unordered_map<int, str_cpy_t> operandMap;
    string overallOperandListStr = "";
    for (auto it : operandList) {
        overallOperandListStr.append(it);
        overallOperandListStr.append(",");
    }
    overallOperandListStr = overallOperandListStr.substr(0, overallOperandListStr.size() - 1);
    // cerr << "overallOperandListStr = " << overallOperandListStr << endl;
    vector<string> rawOperandListVec = stringSplit(overallOperandListStr, ",");
    // cerr << "rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    vector<string> rawValueFieldsVec = stringSplit(rawValue, ",");
    for (auto q : operandMap) {
        // debug_trace("merge operand = %s, current index =  %d, content = %s, rawValue at indx = %s\n", q.c_str(), index, updateContentStr.c_str(), rawValueFieldsVec[index].c_str());
        rawValueFieldsVec[q.first].assign(q.second);
    }

    string temp;
    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        finalValue->append(rawValueFieldsVec[i]);
        finalValue->append(",");
    }
    finalValue->append(rawValueFieldsVec[rawValueFieldsVec.size() - 1]);
    return true;
}

bool PartialMergeStrCpy(vector<str_cpy_t> operandList, vector<str_cpy_t>& finalOperandList)
{
    unordered_map<int, string> operandMap;
    string overallOperandListStr;
    for (auto it : operandList) {
        overallOperandListStr.append(it);
        overallOperandListStr.append(",");
    }
    overallOperandListStr = overallOperandListStr.substr(0, overallOperandListStr.size() - 1);
    // cerr << "[Partial] overallOperandListStr = " << overallOperandListStr << endl;
    vector<string> rawOperandListVec = stringSplit(overallOperandListStr, ",");
    // cerr << "[Partial] rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "[Partial] rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    string finalOperator = "";
    for (auto it : operandMap) {
        finalOperator.append(to_string(it.first) + "," + it.second + ",");
    }
    finalOperator = finalOperator.substr(0, finalOperator.size() - 1);
    finalOperandList.push_back(finalOperator);
    return true;
}

string DeltaKVFieldUpdateMergeOperator::kClassName()
{
    return "DeltaKVFieldUpdateMergeOperator";
}

} // namespace DELTAKV_NAMESPACE
