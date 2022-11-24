#pragma once

#include "common/rocksdbHeaders.hpp"
#include "hashBasedStore/hashStoreFileManager.hpp"
#include "hashBasedStore/hashStoreFileOperator.hpp"
#include "hashBasedStore/hashStoreGCManager.hpp"
#include "hashBasedStore/hashStoreInterface.hpp"
#include "indexBasedStore/indexStoreInterface.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"
#include "utils/loggerColor.hpp"
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>

using namespace std;

namespace DELTAKV_NAMESPACE {

class RocksDBInternalMergeOperator : public MergeOperator {
public:
    bool FullMerge(const Slice& key, const Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value, Logger* logger) const override;

    bool PartialMerge(const Slice& key, const Slice& left_operand,
        const Slice& right_operand, std::string* new_value,
        Logger* logger) const override;

    static const char* kClassName() { return "RocksDBInternalMergeOperator"; }
    const char* Name() const override { return kClassName(); }
};

class DeltaKV {
public:
    rocksdb::DB* pointerToRawRocksDB_;
    // Abstract class ctor
    DeltaKV() = default;
    DeltaKV(DeltaKVOptions& options, const string& name);
    // No copying allowed
    DeltaKV(const DeltaKV&) = delete;
    void operator=(const DeltaKV&) = delete;
    // Abstract class dector
    ~DeltaKV();

    bool Open(DeltaKVOptions& options, const string& name);
    bool Close();

    bool Put(const string& key, const string& value);
    bool Get(const string& key, string* value);
    bool Merge(const string& key, const string& value);
    vector<bool> MultiGet(const vector<string>& keys, vector<string>* values);
    vector<bool> GetByPrefix(const string& targetKeyPrefix, vector<string>* keys, vector<string>* values);
    vector<bool> GetByTargetNumber(const uint64_t& targetGetNumber, vector<string>* keys, vector<string>* values);
    bool SingleDelete(const string& key);

private:
    boost::asio::thread_pool* threadpool_;
    bool launchThreadPool(uint64_t totalThreadNumber);
    bool deleteThreadPool();
    // for separated operations
    bool processValueWithMergeRequestToValueAndMergeOperations(string internalValue, uint64_t skipSize, vector<pair<bool, string>>* mergeOperatorsVec); // mergeOperatorsVec contains is_separted flag and related values if it is not separated.
    // Storage component for delta store
    HashStoreInterface* HashStoreInterfaceObjPtr_;
    HashStoreFileManager* hashStoreFileManagerPtr_;
    HashStoreFileOperator* hashStoreFileOperatorPtr_;
    HashStoreGCManager* hashStoreGCManagerPtr_;
    shared_ptr<DeltaKVMergeOperator> deltaKVMergeOperatorPtr_;
    // Storage component for value store
    IndexStoreInterface* IndexStoreInterfaceObjPtr_;
};

} // namespace DELTAKV_NAMESPACE