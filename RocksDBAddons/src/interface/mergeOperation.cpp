#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

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
}

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
}

vector<string> stringSplit(string str, const string& token) {
    vector<string> result;
    stringSplit(str, token, result);
    return result;
}

vector<str_t> stringSplit(const str_t& str, const char tokenChar) {
    vector<str_t> result;
    stringSplit(str, tokenChar, result);
    return result;
}

int str_t_stoi(const str_t& str) {
    int ret = 0;

    for (int i = 0; i < str.size_; i++) {
        ret = ret * 10 + (str.data_[i] - '0');
    }
    return ret;
}

bool DeltaKVFieldUpdateMergeOperator::Merge(const string& rawValue, const vector<string>& operandList, string* finalValue)
{
    str_t rawValueStrT(const_cast<char*>(rawValue.data()), rawValue.size());
    vector<str_t> operandListStrT;
    for (auto& it : operandList) {
        operandListStrT.push_back(str_t(const_cast<char*>(it.data()), it.size()));
    }
    return Merge(rawValueStrT, operandListStrT, finalValue);
}

bool DeltaKVFieldUpdateMergeOperator::Merge(const str_t& rawValue, const vector<str_t>& operandList, string* finalValue) {
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
//    struct timeval tv;
//    gettimeofday(&tv, 0);
    for (auto& it : operandList) {
        stringSplit(it, ',', rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandListVec[it]);
        operandMap[index] = rawOperandListVec[it + 1];
    }
    vector<str_t> rawValueFieldsVec = stringSplit(rawValue, ',');
    for (auto q : operandMap) {
        // debug_trace("merge operand = %s, current index =  %d, content = %s, rawValue at indx = %s\n", q.c_str(), index, updateContentStr.c_str(), rawValueFieldsVec[index].c_str());
        rawValueFieldsVec[q.first] = q.second;
    }

//    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_SPLIT, &tv);
//    gettimeofday(&tv, 0);

    int resultSize = -1;
    int index = 0;
    for (auto& it : rawValueFieldsVec) {
        resultSize += it.size_ + 1;
    }

    finalValue->resize(resultSize);
    for (auto i = 0; i < rawValueFieldsVec.size(); i++) {
        auto& it = rawValueFieldsVec[i];
        memcpy(finalValue->data() + index, it.data_, it.size_); 
        if (i < rawValueFieldsVec.size() - 1) {
            finalValue->data()[index + it.size_] = ',';
            index += it.size_ + 1;
        } else {
            index += it.size_;
        }
    }
    if (index != resultSize) {
        debug_error("value size error %d v.s. %d\n", index, resultSize);
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

bool DeltaKVFieldUpdateMergeOperator::PartialMerge(const vector<str_t>& operandList, str_t& result)
{
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
    for (auto& it : operandList) {
        stringSplit(it, ',', rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandListVec[it]);
        if (operandMap.find(index) != operandMap.end()) {
            operandMap[index] = rawOperandListVec[it + 1];
        } else {
            operandMap.insert(make_pair(index, rawOperandListVec[it + 1]));
        }
    }
    bool first = true;
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
    // request meRGE Operation when the value is found
    debug_info("Full merge for key = %s, value size = %lu, content = %s\n", key.ToString().c_str(), existing_value->size(), existing_value->ToString().c_str());
//    debug_error("Full merge for key = %s, value size = %lu\n", key.ToString().c_str(), existing_value->size());
    str_t newValueIndexStr;
    str_t filteredOperandStr;
    vector<str_t> filteredOperandStrVec;
    int headerSize = sizeof(internalValueType), valueIndexSize = sizeof(externalIndexInfo);
    int filteredOperandStrVecTotalSize = 0;

    internalValueType* existingValueTypePtr; 
    internalValueType outputValueType;

    existingValueTypePtr = (internalValueType*)const_cast<char*>(existing_value->data());
    int operandIndex = 0;
    bool findUpdatedValueIndex = false;
    vector<str_t> leadingRawDeltas;
    str_t operand;

    // Output format:
    // If value is separated:    [internalValueType] [externalIndexInfo] [appended deltas if any]
    // If value is not separated:[internalValueType] [   raw   value   ] [appended deltas if any]

    // Step 1. Scan the operand list
    for (auto& operandListIt : operand_list) {
        uint64_t deltaOffset = 0;

        while (deltaOffset < operandListIt.size()) {
            internalValueType tempInternalValueType;
            memcpy(&tempInternalValueType, operandListIt.c_str() + deltaOffset, headerSize);

            // extract the oprand
            if (tempInternalValueType.mergeFlag_ == true) {
                // index update
                assert(tempInternalValueType.valueSeparatedFlag_ == true && deltaOffset + headerSize + valueIndexSize <= operandListIt.size());
                operand = str_t(const_cast<char*>(operandListIt.data()) + deltaOffset, headerSize + valueIndexSize);
                deltaOffset += headerSize + valueIndexSize;
            } else {
                if (tempInternalValueType.valueSeparatedFlag_ == false) {
                    // raw delta
                    assert(deltaOffset + headerSize + tempInternalValueType.rawValueSize_ <= operandListIt.size());
                    operand = str_t(const_cast<char*>(operandListIt.data()) + deltaOffset, headerSize + tempInternalValueType.rawValueSize_);
                    deltaOffset += headerSize + tempInternalValueType.rawValueSize_;
                } else {
                    // separated delta
                    assert(deltaOffset + headerSize <= operandListIt.size());
                    operand = str_t(const_cast<char*>(operandListIt.data()) + deltaOffset, headerSize);
                    deltaOffset += headerSize;
                }
            }

            // Find a delta from normal merge operator
            if (tempInternalValueType.mergeFlag_ == false) {
                // Check whether we need to collect the raw deltas for immediate merging.
                // 1. The value should be not separated (i.e., should be raw value)
                // 2. The previous deltas (if exists) should also be raw deltas
                // 3. The current deltas should be a raw delta
                if (existingValueTypePtr->valueSeparatedFlag_ == false && (int)leadingRawDeltas.size() == operandIndex && tempInternalValueType.valueSeparatedFlag_ == false) {
                    // Extract the raw delta, prepare for field updates
                    leadingRawDeltas.push_back(str_t(operand.data_ + headerSize, operand.size_ - headerSize));
                } else {
                    // Append to the string
                    filteredOperandStrVec.push_back(str_t(operand.data_, operand.size_));
                    filteredOperandStrVecTotalSize += operand.size_;
                }
            } else { // Find a delta from vLog GC
                if (existingValueTypePtr->valueSeparatedFlag_ == false) {
                    debug_error("[ERROR] updating a value index but the value is not separated! key [%s]\n", key.ToString().c_str());
                    exit(1);
                }
                findUpdatedValueIndex = true;
                newValueIndexStr = str_t(operand.data_, operand.size_);
            }
            operandIndex++;
        }
    }

    // Step 2. Check index updates and output
    //         output format     [internalValueType] [externalIndexInfo] [appended deltas]
    if (findUpdatedValueIndex == true) {
        memcpy(&outputValueType, newValueIndexStr.data_, headerSize);

        new_value->resize(newValueIndexStr.size_ + filteredOperandStrVecTotalSize);
        char* valueBuffer = new_value->data();
        if (filteredOperandStrVecTotalSize == 0) {
            outputValueType.mergeFlag_ = false;
            memcpy(valueBuffer, &outputValueType, headerSize);
            memcpy(valueBuffer + headerSize, newValueIndexStr.data_ + headerSize, newValueIndexStr.size_ - headerSize); 
        } else {
            memcpy(valueBuffer, newValueIndexStr.data_, newValueIndexStr.size_);
            int i = newValueIndexStr.size_;
            for (auto& it : filteredOperandStrVec) {
                memcpy(valueBuffer + i, it.data_, it.size_);
                i += it.size_; 
            }
        }

        debug_error("Full merge finished for key = %s, value size = %lu\n", key.ToString().c_str(), existing_value->size());
        return true;
    }

    // Step 3.1 Prepare the header
    outputValueType = *existingValueTypePtr;
    if (filteredOperandStrVecTotalSize > 0) {
        outputValueType.mergeFlag_ = true;
    }

    // Step 3.2 Prepare the value, if some merges on raw deltas can be performed
    str_t mergedValueWithoutValueType;
    str_t rawValue(const_cast<char*>(existing_value->data() + headerSize), existing_value->size() - headerSize);
    bool needFree = false;
    if (!leadingRawDeltas.empty()) {
        FullMergeFieldUpdates(rawValue, leadingRawDeltas, &mergedValueWithoutValueType);
        needFree = true;
        if (mergedValueWithoutValueType.size_ != existingValueTypePtr->rawValueSize_) {
            debug_error("[ERROR] value size differs after merging: %u v.s. %u\n", mergedValueWithoutValueType.size_, existingValueTypePtr->rawValueSize_);
        }
    } else {
        mergedValueWithoutValueType = rawValue;
    }

    // Step 3.3 Prepare the following deltas (whether raw or not raw)
    //          Already prepared, don't need to do anything

    // Step 3.4 Append everything

    new_value->resize(headerSize + mergedValueWithoutValueType.size_ + filteredOperandStrVecTotalSize);
    char* buffer = new_value->data();
    memcpy(buffer, &outputValueType, headerSize);
    if (mergedValueWithoutValueType.size_ != 0) {
        memcpy(buffer + headerSize, mergedValueWithoutValueType.data_, mergedValueWithoutValueType.size_);
        if (needFree) {
            delete[] mergedValueWithoutValueType.data_;
        }
    }
    if (filteredOperandStrVecTotalSize != 0) {
        int i = headerSize + mergedValueWithoutValueType.size_;
//        debug_error("i = %d, headerSize = %d, mergedValueWithoutValueType.size_ = %d\n", i, headerSize, mergedValueWithoutValueType.size_);
        for (auto& it : filteredOperandStrVec) {
            memcpy(buffer + i, it.data_, it.size_);
//            debug_error("i = %d\n", i);
            i += it.size_;
        }
    }


//    debug_error("Full merge finished for key = %s, value size = %lu\n", key.ToString().c_str(), existing_value->size());
    return true;
}

bool RocksDBInternalMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    vector<str_t> operandStrs;
    operandStrs.push_back(str_t(const_cast<char*>(left_operand.data()), left_operand.size()));
    operandStrs.push_back(str_t(const_cast<char*>(right_operand.data()), right_operand.size()));
    str_t newValueIndexStrT(nullptr, 0);
    vector<pair<internalValueType*, str_t>> batchedOperandVec;
    for (auto& it : operandStrs) {
        uint64_t deltaOffset = 0;
        while (deltaOffset < it.size_) {
            internalValueType* tempInternalValueType = (internalValueType*)it.data_ + deltaOffset;
            // extract the oprand
            if (tempInternalValueType->mergeFlag_ == true) {
                // index update
                assert(tempInternalValueType->valueSeparatedFlag_ == true && (deltaOffset + sizeof(internalValueType) + sizeof(externalIndexInfo)) <= it.size_);
                newValueIndexStrT.data_ = it.data_ + deltaOffset; 
                newValueIndexStrT.size_ = sizeof(internalValueType) + sizeof(externalIndexInfo);
                deltaOffset += (sizeof(internalValueType) + sizeof(externalIndexInfo));
//                batchedOperandVec.clear(); // clear since new value
            } else {
                if (tempInternalValueType->valueSeparatedFlag_ == false) {
                    // raw delta
                    assert(deltaOffset + sizeof(internalValueType) + tempInternalValueType->rawValueSize_ <= it.size_);
                    batchedOperandVec.push_back(make_pair(tempInternalValueType, str_t(it.data_ + deltaOffset + sizeof(internalValueType), tempInternalValueType->rawValueSize_)));
                    deltaOffset += (sizeof(internalValueType) + tempInternalValueType->rawValueSize_);
                } else {
                    // separated delta
                    assert(deltaOffset + sizeof(internalValueType) <= it.size_);
                    batchedOperandVec.push_back(make_pair(tempInternalValueType, str_t(it.data_, 0)));
                    deltaOffset += sizeof(internalValueType);
                }
            }
        }
    }

    str_t finalDeltaListStrT;
    PartialMergeFieldUpdates(batchedOperandVec, finalDeltaListStrT);
    if (newValueIndexStrT.size_ > 0) {
        new_value->resize(newValueIndexStrT.size_ + finalDeltaListStrT.size_);
        char* buffer = new_value->data();
        memcpy(buffer, newValueIndexStrT.data_, newValueIndexStrT.size_);
        memcpy(buffer + newValueIndexStrT.size_, finalDeltaListStrT.data_, finalDeltaListStrT.size_);
    } else {
        new_value->assign(finalDeltaListStrT.data_, finalDeltaListStrT.size_);
    }
    delete[] finalDeltaListStrT.data_;
    return true;
}

// batchedOperandVec[i].second do not have the header
// If all of them are raw values, can do the partial merge.
// Otherwise, do the partial merge only when the raw values are continuous.
// Example: [separated] [1,A] [2,B] [1,C] [separated] -> [separated] [2,B] [1,C] [separated]
// The original implementation is incorrect; it keeps only the earliest one, and the sequence is all reversed
bool RocksDBInternalMergeOperator::PartialMergeFieldUpdates(vector<pair<internalValueType*, str_t>>& batchedOperandVec, str_t& finalDeltaListStr) const
{
    vector<pair<internalValueType*, str_t>> finalResultVec;
    unordered_map<int, pair<internalValueType*, str_t>> rawFields;

    int finalDeltaSize = 0;

    for (auto i = batchedOperandVec.size() - 1; i != 0; i--) {
        if (batchedOperandVec[i].first->valueSeparatedFlag_ == false) {
            int commaIndex = 0;
            str_t& it = batchedOperandVec[i].second;
            while (commaIndex < it.size_ && it.data_[commaIndex] != ',') {
                commaIndex++;
            }
            int index = str_t_stoi(str_t(it.data_, commaIndex)); 
           
            rawFields[index] = batchedOperandVec[i];
        } else {
            if (!rawFields.empty()) {
                for (auto& it : rawFields) {
                    finalResultVec.push_back(it.second);
                    finalDeltaSize += sizeof(internalValueType) + it.second.second.size_;
                }
                rawFields.clear();
            }
            finalResultVec.push_back(batchedOperandVec[i]);
            finalDeltaSize += sizeof(internalValueType) + batchedOperandVec[i].second.size_;
        }
    }

    if (!rawFields.empty()) {
        for (auto& it : rawFields) {
            finalResultVec.push_back(it.second);
            finalDeltaSize += sizeof(internalValueType) + it.second.second.size_;
        }
    }

    debug_info("PartialMerge raw delta number = %lu, valid delta number = %lu", batchedOperandVec.size(), finalResultVec.size());
    vector<str_t> finalDeltaList;

    char* result = new char[finalDeltaSize];
    int resultIndex = 0;

    for (auto& it : finalResultVec) {
        memcpy(result + resultIndex, it.first, sizeof(internalValueType));
        if (it.second.size_ > 0) {
            memcpy(result + resultIndex + sizeof(internalValueType), it.second.data_, it.second.size_);
        }
        resultIndex += sizeof(internalValueType) + it.second.size_;
    }

    finalDeltaListStr = str_t(result, finalDeltaSize);
    return true;
}

bool RocksDBInternalMergeOperator::FullMergeFieldUpdates(str_t& rawValue, vector<str_t>& operandList, str_t* finalValue) const
{
    size_t pos = 0;
    char delimiter = ',';
    int bufferSize = -1;

    vector<str_t> rawValueFieldsVec;
    stringSplit(rawValue, delimiter, rawValueFieldsVec);

    vector<str_t> rawOperandsVec;

    for (auto& it : operandList) {
        stringSplit(it, ',', rawOperandsVec); 
    }

    for (auto it = 0; it < rawOperandsVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandsVec[it]);
        rawValueFieldsVec[index] = rawOperandsVec[it+1];
    }

    for (auto& it : rawValueFieldsVec) {
        bufferSize += it.size_ + 1;
    }

    char* buffer = new char[bufferSize];
    int bufferIndex = 0;

    for (auto i = 0; i < rawValueFieldsVec.size() - 1; i++) {
        memcpy(buffer + bufferIndex, rawValueFieldsVec[i].data_, rawValueFieldsVec[i].size_);
        buffer[bufferIndex + rawValueFieldsVec[i].size_] = ',';
        bufferIndex += rawValueFieldsVec[i].size_ + 1; 
    }
    memcpy(buffer + bufferIndex, rawValueFieldsVec[rawValueFieldsVec.size()-1].data_, rawValueFieldsVec[rawValueFieldsVec.size()-1].size_);
    finalValue->data_ = buffer;
    finalValue->size_ = bufferSize;
    return true;
}


} // namespace DELTAKV_NAMESPACE
