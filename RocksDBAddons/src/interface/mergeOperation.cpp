#include "interface/mergeOperation.hpp"

namespace DELTAKV_NAMESPACE {

bool StringSplit(string str, const string& token, vector<string>& result)
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

bool StringSplit(const str_t& str, const char& tokenChar, vector<str_t>& result)
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

vector<string> StringSplit(string str, const string& token) {
    vector<string> result;
    StringSplit(str, token, result);
    return result;
}

vector<str_t> StringSplit(const str_t& str, const char tokenChar) {
    vector<str_t> result;
    StringSplit(str, tokenChar, result);
    return result;
}

int str_t_stoi(const str_t& str) {
    int ret = 0;

    for (int i = 0; i < str.size_; i++) {
        ret = ret * 10 + (str.data_[i] - '0');
    }
    return ret;
}

int IntToStringSize(int num) {
    int result = 1;
    while (num >= 10) {
        result++;
        num /= 10;
    }
    return result;
}

bool DeltaKVFieldUpdateMergeOperator::Merge(const string& raw_value, const vector<string>& operands, string* result)
{
    str_t rawValueStrT(const_cast<char*>(raw_value.data()), raw_value.size());
    vector<str_t> operandListStrT;
    for (auto& it : operands) {
        operandListStrT.push_back(str_t(const_cast<char*>(it.data()), it.size()));
    }
    return Merge(rawValueStrT, operandListStrT, result);
}

bool DeltaKVFieldUpdateMergeOperator::Merge(const str_t& raw_value, const vector<str_t>& operands, string* result) {
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
//    struct timeval tv;
//    gettimeofday(&tv, 0);
    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandListVec);
    }
    for (auto it = 0; it < rawOperandListVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandListVec[it]);
        operandMap[index] = rawOperandListVec[it + 1];
    }
    vector<str_t> raw_value_fields = StringSplit(raw_value, ',');
    for (auto q : operandMap) {
        // debug_trace("merge operand = %s, current index =  %d, content = %s, raw_value at indx = %s\n", q.c_str(), index, updateContentStr.c_str(), raw_value_fields[index].c_str());
        raw_value_fields[q.first] = q.second;
    }

//    StatsRecorder::getInstance()->timeProcess(StatsType::DKV_MERGE_SPLIT, &tv);
//    gettimeofday(&tv, 0);

    int resultSize = -1;
    int index = 0;
    for (auto& it : raw_value_fields) {
        resultSize += it.size_ + 1;
    }

    result->resize(resultSize);
    for (auto i = 0; i < raw_value_fields.size(); i++) {
        auto& it = raw_value_fields[i];
        memcpy(result->data() + index, it.data_, it.size_); 
        if (i < raw_value_fields.size() - 1) {
            result->data()[index + it.size_] = ',';
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

// All the operands are raw deltas (without headers)
bool DeltaKVFieldUpdateMergeOperator::PartialMerge(const vector<string>& operands, vector<string>& finalOperandList)
{
    unordered_map<int, string> operandMap;
    vector<string> rawOperandListVec;
    for (auto& it : operands) {
        StringSplit(it, ",", rawOperandListVec);
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

// All the operands are raw deltas (without headers)
bool DeltaKVFieldUpdateMergeOperator::PartialMerge(const vector<str_t>& operands, str_t& result)
{
    unordered_map<int, str_t> operandMap;
    vector<str_t> rawOperandListVec;
    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandListVec);
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
            result.size_ += IntToStringSize(it.first) + it.second.size_ + 2;
            first = false;
        } else {
            result.size_ += IntToStringSize(it.first) + it.second.size_ + 2;
        }
    }

    // TODO may allocate a 1-byte buffer if the operands are empty
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

inline bool RocksDBInternalMergeOperator::ExtractDeltas(bool value_separated,
        str_t& operand, uint64_t& delta_off, vector<str_t>& deltas, str_t&
        new_value_index, int& leading_index) const {
    const int header_size = sizeof(internalValueType);
    const int value_index_size = sizeof(externalIndexInfo);
    str_t operand_delta;

    while (delta_off < operand.size_) {
        internalValueType* header_ptr = (internalValueType*)(operand.data_ + delta_off);

        // extract the oprand
        if (header_ptr->mergeFlag_ == true) {
            // index update
            if (use_varint_index == false) {
                assert(header_ptr->valueSeparatedFlag_ == true && delta_off +
                        header_size + value_index_size <= operand.size_);
                new_value_index = str_t(operand.data_ + delta_off + header_size, value_index_size);
                delta_off += header_size + value_index_size;
            } else {
                assert(header_ptr->valueSeparatedFlag_ == true &&
                        delta_off + header_size <= operand.size_);
                char* buf = operand.data_ + delta_off + header_size;
                int index_size = GetVlogIndexVarintSize(buf);
                debug_error("extract, index_size %d\n", index_size);
                new_value_index = str_t(buf, index_size);
                delta_off += header_size + index_size; 
            }
        } else {
            // Check whether we need to collect the raw deltas for immediate merging.
            // 1. The value should be not separated (i.e., should be raw value)
            // 2. The previous deltas (if exists) should also be raw deltas
            // 3. The current deltas should be a raw delta

            if (header_ptr->valueSeparatedFlag_ == false) {
                // raw delta
                auto& delta_sz = header_ptr->rawValueSize_;
                assert(delta_off + header_size + delta_sz <= operand.size_);
                deltas.push_back(str_t(operand.data_ + delta_off, 
                            header_size + delta_sz));
                delta_off += header_size + delta_sz;

                if (value_separated == false && (int)deltas.size() == leading_index + 1) {
                    // Extract the raw delta, prepare for field updates
                    leading_index++;
                }
            } else {
                // separated delta
                assert(delta_off + header_size <= operand.size_);
                deltas.push_back(str_t(operand.data_ + delta_off, header_size));
                delta_off += header_size;
            }

        }
    }
    return true;
}

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    // request meRGE Operation when the value is found
    debug_info("Full merge for key = %s, value size = %lu\n",
            key.ToString().c_str(), existing_value->size());
    str_t new_value_index(nullptr, 0);
    const int header_size = sizeof(internalValueType);
    const int value_index_size = sizeof(externalIndexInfo);

    internalValueType value_header; 

    memcpy(&value_header, existing_value->data(), header_size);

    vector<str_t> leadingRawDeltas;
    vector<str_t> deltas;
    str_t operand;

    bool value_separated = value_header.valueSeparatedFlag_;
    int leading_index = 0;

    // Output format:
    // If value is separated:    
    //      [internalValueType] [externalIndexInfo] [appended deltas if any]
    // If value is not separated:
    //      [internalValueType] [   raw   value   ] [appended deltas if any]

    // Step 1. Scan the deltas in the value 
    {
        uint64_t delta_off = 0;
        if (value_header.valueSeparatedFlag_ == false) {
            delta_off = header_size + value_header.rawValueSize_;
        } else if (use_varint_index == false) {
            delta_off = header_size + value_index_size;
        } else {
            delta_off = header_size + GetVlogIndexVarintSize(
                    const_cast<char*>(existing_value->data()) + header_size);
        }
        str_t operand_it(const_cast<char*>(existing_value->data()),
                existing_value->size());

        ExtractDeltas(value_separated, operand_it,
                delta_off, deltas, new_value_index, leading_index);
    }

    // Step 2. Scan the deltas in the operand list
    for (auto& operand_list_it : operand_list) {
        uint64_t delta_off = 0;
        str_t operand_it(const_cast<char*>(operand_list_it.data()),
                operand_list_it.size());
        ExtractDeltas(value_separated, operand_it,
                delta_off, deltas, new_value_index, leading_index);
    }

    // Step 3. Do full merge on the value
    str_t merged_raw_value(nullptr, 0);
    bool need_free = false;
    str_t raw_value(const_cast<char*>(existing_value->data()) + header_size, value_header.rawValueSize_);
    if (leading_index > 0) {
        vector<str_t> raw_deltas;
        for (int i = 0; i < leading_index; i++) {
            raw_deltas.push_back(str_t(deltas[i].data_ + header_size, deltas[i].size_ - header_size));
        }
        FullMergeFieldUpdates(raw_value, raw_deltas, &merged_raw_value);
        // need to free the space for full merge later
        need_free = true;

        value_header.rawValueSize_ = merged_raw_value.size_;
    } else if (value_separated) {
        if (new_value_index.data_ != nullptr) {
            merged_raw_value = new_value_index;
        } else {
            if (use_varint_index) {
                merged_raw_value = str_t(raw_value.data_,
                        GetVlogIndexVarintSize(raw_value.data_));
            } else {
                merged_raw_value = str_t(raw_value.data_, value_index_size); 
            }
        }
    } else {
        merged_raw_value = raw_value; 
    }

    // Step 4. Do partial merge on the remaining deltas
    str_t partial_merged_delta(nullptr, 0);
    bool need_free_partial = false;
    if (deltas.size() - leading_index > 0) {
        // There are deltas to merge 
        if (deltas.size() - leading_index == 1) {
            // Only one delta. Directly append
            partial_merged_delta = deltas[leading_index]; 
        } else {
            // TODO manage the sequence numbers
            // copy the headers and the contents to the vector; then perform
            // partial merge
            vector<pair<internalValueType*, str_t>> operand_type_vec;
            operand_type_vec.resize(deltas.size() - leading_index);
            uint64_t total_delta_size = 0;
            for (int i = leading_index; i < (int)deltas.size(); i++) {
                operand_type_vec[i - leading_index] = 
                    make_pair((internalValueType*)deltas[i].data_, 
                            str_t(deltas[i].data_ + header_size, 
                                deltas[i].size_ - header_size)); 
                total_delta_size += deltas[i].size_;
            }

            // partial merged delta include the header
            PartialMergeFieldUpdates(operand_type_vec, 
                    partial_merged_delta);

            debug_info("After partial merge: num deltas %lu, tot sz %lu, "
                    "merged delta size %u\n",
                    operand_type_vec.size(), total_delta_size, 
                    partial_merged_delta.size_);

            // need to free the partial merged delta later
            need_free_partial = true;
        }
    }

    // Step 5. Update header
    // Reuse value_header as an output
    if (partial_merged_delta.size_ > 0) {
        value_header.mergeFlag_ = true;
    }

    new_value->resize(header_size + merged_raw_value.size_ + partial_merged_delta.size_);
    char* buffer = new_value->data();
    memcpy(buffer, &value_header, header_size);
    if (merged_raw_value.size_ > 0) {
        memcpy(buffer + header_size, merged_raw_value.data_, merged_raw_value.size_);
        if (need_free) {
            delete[] merged_raw_value.data_;
        }
    }
    if (partial_merged_delta.size_ > 0) {
        memcpy(buffer + header_size + merged_raw_value.size_, 
                partial_merged_delta.data_, 
                partial_merged_delta.size_);
        if (need_free_partial) {
            delete[] partial_merged_delta.data_;
        }
    }

    debug_info("Full merge finished for key = %s, value size = %lu, "
            "number of deltas = %lu, num of leading %u, final size = %lu\n", 
            key.ToString().c_str(), existing_value->size(), 
            deltas.size(), leading_index, new_value->size());
    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_ROCKSDB_FULLMERGE, tv);
    return true;
}

bool RocksDBInternalMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    const int header_size = sizeof(internalValueType);
    const int value_index_size = sizeof(externalIndexInfo);
    vector<str_t> operandStrs;
    operandStrs.push_back(str_t(const_cast<char*>(left_operand.data()), left_operand.size()));
    operandStrs.push_back(str_t(const_cast<char*>(right_operand.data()), right_operand.size()));
    str_t new_value_index(nullptr, 0);
    vector<pair<internalValueType*, str_t>> operand_type_vec;

    // An simplified version of ExtractDeltas()
    for (auto& it : operandStrs) {
        uint64_t delta_off = 0;
        while (delta_off < it.size_) {
            internalValueType* header_ptr = (internalValueType*)(it.data_ + delta_off);
            // extract the oprand
            if (header_ptr->mergeFlag_ == true) {
                // index update
                if (use_varint_index == false) {
                    assert(header_ptr->valueSeparatedFlag_ == true && 
                            delta_off + header_size + value_index_size <=
                            it.size_);
                    new_value_index = str_t(it.data_ + delta_off, header_size +
                            value_index_size);
                    delta_off += header_size + value_index_size;
                } else {
                    assert(header_ptr->valueSeparatedFlag_ == true && 
                            delta_off + header_size < it.size_);
                    int index_size = GetVlogIndexVarintSize(it.data_ + delta_off);
                    new_value_index = str_t(it.data_ + delta_off, header_size +
                            index_size);
                    delta_off += header_size + index_size; 
                }
            } else {
                if (header_ptr->valueSeparatedFlag_ == false) {
                    // raw delta
                    auto& delta_sz = header_ptr->rawValueSize_;
                    assert(delta_off + header_size + delta_sz <= it.size_);
                    operand_type_vec.push_back(make_pair(header_ptr, 
                                str_t(it.data_ + delta_off + header_size, delta_sz)));
                    delta_off += header_size + delta_sz;
                } else {
                    // separated delta
                    assert(delta_off + header_size <= it.size_);
                    operand_type_vec.push_back(make_pair(header_ptr, 
                                str_t(it.data_ + delta_off, 0)));
                    delta_off += header_size;
                }
            }
        }
    }

    str_t finalDeltaListStrT;
    PartialMergeFieldUpdates(operand_type_vec, finalDeltaListStrT);
    if (new_value_index.size_ > 0) {
        new_value->resize(new_value_index.size_ + finalDeltaListStrT.size_);
        char* buffer = new_value->data();
        memcpy(buffer, new_value_index.data_, new_value_index.size_);
        memcpy(buffer + new_value_index.size_, finalDeltaListStrT.data_, finalDeltaListStrT.size_);
    } else {
        new_value->assign(finalDeltaListStrT.data_, finalDeltaListStrT.size_);
    }
    delete[] finalDeltaListStrT.data_;
    StatsRecorder::getInstance()->timeProcess(StatsType::LSM_FLUSH_ROCKSDB_PARTIALMERGE, tv);
    return true;
}

// operand_type_vec[i].second do not have the header
// If all of them are raw values, can do the partial merge.
// Otherwise, do the partial merge only when the raw values are continuous.
// Example: [separated] [1,A] [2,B] [1,C] [separated] -> [separated] [2,B] [1,C] [separated]
// The original implementation is incorrect; it keeps only the earliest one, and the sequence is all reversed
inline bool RocksDBInternalMergeOperator::PartialMergeFieldUpdates(vector<pair<internalValueType*, str_t>>& operand_type_vec, str_t& final_result) const
{
    vector<internalValueType*> separated_headers;
    unordered_map<int, str_t> raw_fields;

    // empty - separated deltas; not empty, raw deltas
    vector<unordered_map<int, str_t>> raw_fields_result; 

    int final_result_size = 0;
    const int header_size = sizeof(internalValueType);

    // Extract the raw fields 
    for (auto i = 0; i < operand_type_vec.size(); i++) {
        if (operand_type_vec[i].first->valueSeparatedFlag_ == false) {
            vector<str_t> operand_list;
            StringSplit(operand_type_vec[i].second, ',', operand_list);

            for (auto j = 0; j < operand_list.size(); j += 2) {
                int index = str_t_stoi(operand_list[j]);
                raw_fields[index] = operand_list[j+1];
            }
        } else {
            if (!raw_fields.empty()) {
                raw_fields_result.push_back(raw_fields);
                raw_fields.clear();
            }
            raw_fields_result.push_back({});
            separated_headers.push_back(operand_type_vec[i].first);
        }
    }

    if (!raw_fields.empty()) {
        raw_fields_result.push_back(raw_fields);
        raw_fields.clear();
    }

    // Calculate the final delta size
    for (auto& it : raw_fields_result) {
        final_result_size += header_size;
        if (it.empty() == false) {
            for (auto& it0 : it) {
                final_result_size += IntToStringSize(it0.first) + it0.second.size_ + 2;
            }
            final_result_size--;
        }
    }

    debug_info("PartialMerge raw delta number %lu, "
            "separated deltas %lu, final result size %d\n", 
            operand_type_vec.size(), separated_headers.size(),
            final_result_size);

    char* result = new char[final_result_size + 1];
    int result_i = 0;

    // Build the final delta
    // the index of separated deltas
    int i = 0;

    for (auto& it : raw_fields_result) {
        internalValueType header;
        if (it.empty() == false) {
            bool first = true;
            int tmp_i = result_i;
            result_i += header_size;
            for (auto& it0 : it) {
                if (first) {
                    sprintf(result + result_i, "%d,%.*s", it0.first,
                            it0.second.size_, it0.second.data_);
                    first = false;
                } else {
                    sprintf(result + result_i, ",%d,%.*s", it0.first,
                            it0.second.size_, it0.second.data_);
                }
                result_i += it0.second.size_ + 1;
                while (result[result_i]) 
                    result_i++;
            }
            header.rawValueSize_ = result_i - tmp_i - header_size;
            memcpy(result + tmp_i, &header, header_size);
        } else {
            memcpy(result + result_i, separated_headers[i],
                    header_size);
            result_i += header_size;
            i++;
        }
    }

    debug_info("result_i %d final_result_size %d header %d\n", 
            result_i, final_result_size, header_size);

    final_result = str_t(result, final_result_size);
    return true;
}

inline bool RocksDBInternalMergeOperator::FullMergeFieldUpdates(str_t& raw_value, vector<str_t>& operands, str_t* result) const
{
    int buffer_size = -1;

    vector<str_t> raw_value_fields;
    StringSplit(raw_value, ',', raw_value_fields);

    vector<str_t> rawOperandsVec;

    for (auto& it : operands) {
        StringSplit(it, ',', rawOperandsVec); 
    }

    for (auto it = 0; it < rawOperandsVec.size(); it += 2) {
        int index = str_t_stoi(rawOperandsVec[it]);
        raw_value_fields[index] = rawOperandsVec[it+1];
    }

    for (auto& it : raw_value_fields) {
        buffer_size += it.size_ + 1;
    }

    char* buffer = new char[buffer_size];
    int buffer_index = 0;

    for (auto i = 0; i < raw_value_fields.size() - 1; i++) {
        memcpy(buffer + buffer_index, raw_value_fields[i].data_, raw_value_fields[i].size_);
        buffer[buffer_index + raw_value_fields[i].size_] = ',';
        buffer_index += raw_value_fields[i].size_ + 1; 
    }
    memcpy(buffer + buffer_index, 
            raw_value_fields[raw_value_fields.size()-1].data_, 
            raw_value_fields[raw_value_fields.size()-1].size_);
    result->data_ = buffer;
    result->size_ = buffer_size;
    return true;
}


} // namespace DELTAKV_NAMESPACE
