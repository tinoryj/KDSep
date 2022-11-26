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

bool FieldUpdateMergeOperator::Merge(string rawValue, vector<string> operandList, string* finalValue)
{
    vector<string> rawValueFieldsVec = stringSplit(rawValue, ",");
    for (auto q : operandList) {
        cout << BLUE << "[DEBUG-LOG]:[Addons]-[FieldUpdateMergeOperator]-[Merge] merge operand = " << q << RESET << endl;
        string indexStr = q.substr(0, q.find(","));
        cout << BLUE << "[DEBUG-LOG]:[Addons]-[FieldUpdateMergeOperator]-[Merge] merge operand current indexStr = " << indexStr << RESET << endl;
        int index = stoi(indexStr);
        cout << BLUE << "[DEBUG-LOG]:[Addons]-[FieldUpdateMergeOperator]-[Merge] merge operand current index = " << index << RESET << endl;
        string updateContentStr = q.substr(q.find(",") + 1, q.size());
        cout << BLUE << "[DEBUG-LOG]:[Addons]-[FieldUpdateMergeOperator]-[Merge] merge operand current update target content = " << updateContentStr << RESET << endl;
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

string FieldUpdateMergeOperator::kClassName()
{
    return "FieldUpdateMergeOperator";
}

} // namespace DELTAKV_NAMESPACE