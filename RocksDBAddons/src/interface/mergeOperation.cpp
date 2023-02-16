#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

vector<string> stringSplit(string str, const string& token)
{
    vector<string> result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
};

bool stringSplit(string str, const string& token, vector<string>& result)
{
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
        } else {
            result.push_back(str);
            str = "";
        }
    }
    return true;
};

bool stringSplit(const str_t& str, const char& tokenChar, vector<str_t>& result)
{
    char* data = str.data_;
    char* anchor = data;
    int anchorIndex = 0;

    for (int i = 0; i < str.size_; i++) {
        if (data[i] == tokenChar) {
            if (i > anchorIndex) {
                result.push_back(str_t(anchor, i - anchorIndex));
            }
            if (i + 1 < str.size_) {
                anchorIndex = i + 1;
                anchor = data + anchorIndex;
            } else {
                break;
            }
        } else if (i == str.size_ - 1) {
            result.push_back(str_t(anchor, i + 1 - anchorIndex));
        }
    }
    return true;
};

int str_t_stoi(const str_t& str) {
    int ret = 0;

    for (int i = 0; i < str.size_; i++) {
        ret = ret * 10 + (str.data_[i] - '0');
    }
    return ret;
}

bool DeltaKVFieldUpdateMergeOperator::Merge(string rawValue, const vector<string>& operandList, string* finalValue)
{
    unordered_map<int, string> operandMap;
    vector<string> rawOperandListVec;
    for (auto& it : operandList) {
        stringSplit(it, ",", rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap.at(index).assign(rawOperandListVec[it + 1]);
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    vector<string> rawValueFieldsVec = stringSplit(rawValue, ",");
    if (rawValueFieldsVec.size() > 10) {
        debug_error("rawValueFieldsVec.size() %lu rawValue %.*s\n", rawValueFieldsVec.size(), 20, rawValue.c_str());
    }
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
    if (finalValue->size() > 4400) {
        debug_error("rawOperandListVec.size() %lu rawValueFieldsVec.size() %lu rawValue %s\n", rawOperandListVec.size(), rawValueFieldsVec.size(), rawValue.c_str());
    }
    return true;
}

bool DeltaKVFieldUpdateMergeOperator::PartialMerge(const vector<string>& operandList, vector<string>& finalOperandList)
{
    unordered_map<int, string> operandMap;
    vector<string> rawOperandListVec;
    for (auto& it : operandList) {
        stringSplit(it, ",", rawOperandListVec);
    }
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

bool DeltaKVFieldUpdateMergeOperator::PartialMerge(const vector<str_t>& operandList, vector<str_t>& finalOperandList)
{
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
    for (auto& it : operandList) {
        stringSplit(it, ',', rawOperandListVec);
    }
    // cerr << "[Partial] rawOperandListVec size = " << rawOperandListVec.size() << endl;
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        // cerr << "[Partial] rawOperandListVec[" << it << "] = " << rawOperandListVec[it] << endl;
        int index = str_t_stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap[index] = rawOperandListVec[it + 1];
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    bool first = true;
    str_t result;
    result.size_ = 0;

    for (auto& it : operandMap) {
        if (first) {
            result.size_ += 3 + it.second.size_;
            first = false;
        } else {
            result.size_ += 4 + it.second.size_;
        }
    }

    result.data_ = new char[result.size_];
    first = true;
    int resultIndex = 0;

    for (auto& it : operandMap) {
        if (first) {
            sprintf(result.data_ + resultIndex, "%d,%.*s", it.first, it.second.size_, it.second.data_); 
            first = false;
        } else {
            sprintf(result.data_ + resultIndex, ",%d,%.*s", it.first, it.second.size_, it.second.data_); 
        }
        // replace strlen()
        resultIndex += it.second.size_ + 1; 
        while (result.data_[resultIndex]) resultIndex++;
    }
    result.size_ = resultIndex;
    finalOperandList.push_back(result);
    return true;
}

string DeltaKVFieldUpdateMergeOperator::kClassName()
{
    return "DeltaKVFieldUpdateMergeOperator";
}

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // request merge operation when the value is found
    debug_info("Full merge for key = %s, value size = %lu, content = %s\n", key.ToString().c_str(), existing_value->size(), existing_value->ToString().c_str());
    string newValueIndexStr;
    string filteredOperandStr;
    int headerSize = sizeof(internalValueType), valueIndexSize = sizeof(externalIndexInfo);

    internalValueType existingValueType;
    internalValueType outputValueType;
    memcpy(&existingValueType, existing_value->ToString().c_str(), headerSize);

    int operandIndex = 0;
    bool findUpdatedValueIndex = false;
    vector<string> leadingRawDeltas;
    string operand;

    // Output format:
    // If value is separated:    [internalValueType] [externalIndexInfo] [appended deltas if any]
    // If value is not separated:[internalValueType] [   raw   value   ] [appended deltas if any]

    // Step 1. Scan the operand list
    for (auto operandListIt : operand_list) {
        uint64_t deltaOffset = 0;

        while (deltaOffset < operandListIt.size()) {
            internalValueType tempInternalValueType;
            memcpy(&tempInternalValueType, operandListIt.c_str() + deltaOffset, headerSize);

            // extract the oprand
            if (tempInternalValueType.mergeFlag_ == true) {
                // index update
                assert(tempInternalValueType.valueSeparatedFlag_ == true && deltaOffset + headerSize + valueIndexSize <= operandListIt.size());
                operand.assign(operandListIt.c_str() + deltaOffset, headerSize + valueIndexSize);
                deltaOffset += headerSize + valueIndexSize;
            } else {
                if (tempInternalValueType.valueSeparatedFlag_ == false) {
                    // raw delta
                    assert(deltaOffset + headerSize + tempInternalValueType.rawValueSize_ <= operandListIt.size());
                    operand.assign(operandListIt.c_str() + deltaOffset, headerSize + tempInternalValueType.rawValueSize_);
                    deltaOffset += headerSize + tempInternalValueType.rawValueSize_;
                } else {
                    // separated delta
                    assert(deltaOffset + headerSize <= operandListIt.size());
                    operand.assign(operandListIt.c_str() + deltaOffset, headerSize);
                    deltaOffset += headerSize;
                }
            }

            // Find a delta from normal merge operator
            if (tempInternalValueType.mergeFlag_ == false) {
                // Check whether we need to collect the raw deltas for immediate merging.
                // 1. The value should be not separated (i.e., should be raw value)
                // 2. The previous deltas (if exists) should also be raw deltas
                // 3. The current deltas should be a raw delta
                if (existingValueType.valueSeparatedFlag_ == false && (int)leadingRawDeltas.size() == operandIndex && tempInternalValueType.valueSeparatedFlag_ == false) {
                    // Extract the raw delta, prepare for field updates
                    leadingRawDeltas.push_back(operand.substr(headerSize));
                } else {
                    // Append to the string
                    filteredOperandStr.append(operand);
                }
            } else { // Find a delta from vLog GC
                if (existingValueType.valueSeparatedFlag_ == false) {
                    debug_error("[ERROR] updating a value index but the value is not separated! key [%s]\n", key.ToString().c_str());
                    exit(1);
                }
                findUpdatedValueIndex = true;
                newValueIndexStr.assign(operand);
            }
            operandIndex++;
        }
    }

    // Step 2. Check index updates and output
    //         output format     [internalValueType] [externalIndexInfo] [appended deltas]
    if (findUpdatedValueIndex == true) {
        memcpy(&outputValueType, newValueIndexStr.c_str(), headerSize);
        if (filteredOperandStr.empty()) {
            outputValueType.mergeFlag_ = false;
            new_value->assign(std::string((char*)(&outputValueType), headerSize)); // internalValueType
            new_value->append(newValueIndexStr.substr(headerSize)); // externalIndexInfo
        } else {
            new_value->assign(newValueIndexStr); // internalValueType + externalIndexInfo
        }
        new_value->append(filteredOperandStr);
        return true;
    }

    // Step 3.1 Prepare the header
    outputValueType = existingValueType;
    if (!filteredOperandStr.empty()) {
        outputValueType.mergeFlag_ = true;
    }

    // Step 3.2 Prepare the value, if some merges on raw deltas can be performed
    string mergedValueWithoutValueType;
    string rawValue(existing_value->data_ + headerSize, existing_value->size_ - headerSize);
    if (!leadingRawDeltas.empty()) {
        FullMergeFieldUpdates(rawValue, leadingRawDeltas, &mergedValueWithoutValueType);
        if (mergedValueWithoutValueType.size() != existingValueType.rawValueSize_) {
            debug_error("[ERROR] value size differs after merging: %lu v.s. %u\n", mergedValueWithoutValueType.size(), existingValueType.rawValueSize_);
        }
    } else {
        mergedValueWithoutValueType.assign(rawValue);
    }

    // Step 3.3 Prepare the following deltas (whether raw or not raw)
    //          Already prepared, don't need to do anything

    // Step 3.4 Append everything

    new_value->assign(string((char*)&outputValueType, headerSize));
    new_value->append(mergedValueWithoutValueType);
    new_value->append(filteredOperandStr);

    return true;
}

bool RocksDBInternalMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    string operandStr;
    operandStr.assign(left_operand.ToString());
    operandStr.append(right_operand.ToString());
    auto deltaOffset = 0;
    string newValueIndexStr = "";
    vector<pair<internalValueType, string>> batchedOperandVec;
    bool findRawDeltaFlag = false;
    while (deltaOffset < operandStr.size()) {
        internalValueType tempInternalValueType;
        memcpy(&tempInternalValueType, operandStr.c_str() + deltaOffset, sizeof(internalValueType));
        // extract the oprand
        if (tempInternalValueType.mergeFlag_ == true) {
            // index update
            assert(tempInternalValueType.valueSeparatedFlag_ == true && (deltaOffset + sizeof(internalValueType) + sizeof(externalIndexInfo)) <= operandStr.size());
            newValueIndexStr.assign(operandStr.substr(deltaOffset, sizeof(internalValueType) + sizeof(externalIndexInfo)));
            deltaOffset += (sizeof(internalValueType) + sizeof(externalIndexInfo));
            batchedOperandVec.clear(); // clear since new value
        } else {
            if (tempInternalValueType.valueSeparatedFlag_ == false) {
                // raw delta
                assert(deltaOffset + sizeof(internalValueType) + tempInternalValueType.rawValueSize_ <= operandStr.size());
                batchedOperandVec.push_back(make_pair(tempInternalValueType, operandStr.substr(deltaOffset + sizeof(internalValueType), tempInternalValueType.rawValueSize_)));
                deltaOffset += (sizeof(internalValueType) + tempInternalValueType.rawValueSize_);
                findRawDeltaFlag = true;
            } else {
                // separated delta
                assert(deltaOffset + sizeof(internalValueType) <= operandStr.size());
                batchedOperandVec.push_back(make_pair(tempInternalValueType, ""));
                deltaOffset += sizeof(internalValueType);
            }
        }
    }
    if (findRawDeltaFlag == true) {
        string finalDeltaListStr = "";
        PartialMergeFieldUpdates(batchedOperandVec, finalDeltaListStr);
        if (newValueIndexStr.size() > 0) {
            new_value->assign(newValueIndexStr);
            new_value->append(finalDeltaListStr);
        } else {
            new_value->assign(finalDeltaListStr);
        }
    } else {
        string finalDeltaListStr;
        for (auto i = 0; i < batchedOperandVec.size(); i++) {
            if (batchedOperandVec[i].first.valueSeparatedFlag_ == true) {
                char buffer[sizeof(internalValueType)];
                memcpy(buffer, &batchedOperandVec[i].first, sizeof(internalValueType));
                string headerStr(buffer, sizeof(internalValueType));
                finalDeltaListStr.append(headerStr);
            } else {
                char buffer[sizeof(internalValueType) + batchedOperandVec[i].first.rawValueSize_];
                memcpy(buffer, &batchedOperandVec[i].first, sizeof(internalValueType));
                memcpy(buffer + sizeof(internalValueType), batchedOperandVec[i].second.c_str(), batchedOperandVec[i].first.rawValueSize_);
                string contentStr(buffer, sizeof(internalValueType) + batchedOperandVec[i].first.rawValueSize_);
                finalDeltaListStr.append(contentStr);
            }
        }
        if (newValueIndexStr.size() > 0) {
            new_value->assign(newValueIndexStr);
            new_value->append(finalDeltaListStr);
        } else {
            new_value->assign(finalDeltaListStr);
        }
    }
    return true;
}

bool RocksDBInternalMergeOperator::PartialMergeFieldUpdates(vector<pair<internalValueType, string>> batchedOperandVec, string& finalDeltaListStr) const
{
    unordered_set<int> findIndexSet;
    stack<pair<internalValueType, string>> finalResultStack;
    for (auto i = batchedOperandVec.size() - 1; i != 0; i--) {
        if (batchedOperandVec[i].first.valueSeparatedFlag_ == false) {
            int index = stoi(batchedOperandVec[i].second.substr(0, batchedOperandVec[i].second.find(",")));
            if (findIndexSet.find(index) == findIndexSet.end()) {
                findIndexSet.insert(index);
                finalResultStack.push(batchedOperandVec[i]);
            }
        } else {
            finalResultStack.push(batchedOperandVec[i]);
        }
    }
    debug_info("PartialMerge raw delta number = %lu, valid delta number = %lu", batchedOperandVec.size(), finalResultStack.size());
    while (finalResultStack.empty() == false) {
        if (finalResultStack.top().first.valueSeparatedFlag_ == true) {
            char buffer[sizeof(internalValueType)];
            memcpy(buffer, &finalResultStack.top().first, sizeof(internalValueType));
            string headerStr(buffer, sizeof(internalValueType));
            finalDeltaListStr.append(headerStr);
        } else {
            char buffer[sizeof(internalValueType) + finalResultStack.top().first.rawValueSize_];
            memcpy(buffer, &finalResultStack.top().first, sizeof(internalValueType));
            memcpy(buffer + sizeof(internalValueType), finalResultStack.top().second.c_str(), finalResultStack.top().first.rawValueSize_);
            string contentStr(buffer, sizeof(internalValueType) + finalResultStack.top().first.rawValueSize_);
            finalDeltaListStr.append(contentStr);
        }
        finalResultStack.pop();
    }
    return true;
}

bool RocksDBInternalMergeOperator::FullMergeFieldUpdates(string& rawValue, vector<string>& operandList, string* finalValue) const
{
    size_t pos = 0;
    char delimiter = ',';
    int bufferSize = -1;

    str_t str(rawValue.data(), rawValue.size()); 
    vector<str_t> rawValueFieldsVec;
    stringSplit(str, delimiter, rawValueFieldsVec);

    vector<str_t> rawOperandsVec;

    for (auto& it : operandList) {
        str_t str_t_it(it.data(), it.size());
        stringSplit(str_t_it, ',', rawOperandsVec); 
    }

    for (auto it = 0; it < rawOperandsVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandsVec[it]);
        rawValueFieldsVec[index] = rawOperandsVec[it+1];
    }

    for (auto& it : rawValueFieldsVec) {
        bufferSize += it.size_ + 1;
    }

    char buffer[bufferSize];
    int bufferIndex = 0;

    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        memcpy(buffer + bufferIndex, rawValueFieldsVec[i].data_, rawValueFieldsVec[i].size_);
        buffer[bufferIndex + rawValueFieldsVec[i].size_] = ',';
        bufferIndex += rawValueFieldsVec[i].size_ + 1; 
    }
    memcpy(buffer + bufferIndex, rawValueFieldsVec[rawValueFieldsVec.size()-1].data_, rawValueFieldsVec[rawValueFieldsVec.size()-1].size_);
    finalValue->assign(buffer, bufferSize);
    return true;
}


} // namespace DELTAKV_NAMESPACE
