#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

DeltaKV::DeltaKV()
{
}

DeltaKV::~DeltaKV()
{
    delete pointerToRawRocksDB_;
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    options.rocksdbRawOptions_.merge_operator.reset(
        new RocksDBInternalMergeOperator);
    cerr << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, name = " << name << endl;
    cerr << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, pointerToRawRocksDB_ = " << &pointerToRawRocksDB_ << endl;
    rocksdb::Status s = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!s.ok()) {
        cerr << "[ERROR]:[Addons]-[DeltaKVInterface]-[Construction] Can't open underlying rocksdb" << endl;
        return false;
    } else {
        cerr << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb success" << endl;
        return true;
    }
}

bool DeltaKV::Close()
{
    delete pointerToRawRocksDB_;
    if (pointerToRawRocksDB_) {
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::Put(const string& key, const string& value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::Get(const string& key, string* value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), key, value);
    if (!s.ok()) {
        return false;
    } else {
        return true;
    }
}

bool DeltaKV::Merge(const string& key, const string& value)
{
    rocksdb::Status s = pointerToRawRocksDB_->Merge(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        return false;
    } else {
        return true;
    }
}

vector<bool> DeltaKV::MultiGet(const vector<string>& keys, vector<string>* values)
{
    vector<bool> queryStatus;
    for (auto currentKey : keys) {
        string tempValue;
        rocksdb::Status s = pointerToRawRocksDB_->Get(rocksdb::ReadOptions(), currentKey, &tempValue);
        values->push_back(tempValue);
        if (!s.ok()) {
            queryStatus.push_back(false);
        } else {
            queryStatus.push_back(true);
        }
    }
    return queryStatus;
}

vector<bool> DeltaKV::GetByPrefix(const string& targetKeyPrefix, vector<string>* keys, vector<string>* values)
{
    auto it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    it->Seek(targetKeyPrefix);
    vector<bool> queryStatus;
    for (int i = 0; it->Valid(); i++) {
        string tempKey, tempValue;
        tempKey = it->key().ToString();
        tempValue = it->value().ToString();
        it->Next();
        keys->push_back(tempKey);
        values->push_back(tempValue);
        if (tempValue.empty()) {
            queryStatus.push_back(false);
        } else {
            queryStatus.push_back(true);
        }
    }
    delete it;
    return queryStatus;
}
vector<bool> DeltaKV::GetByTargetNumber(const uint64_t& targetGetNumber, vector<string>* keys, vector<string>* values)
{
    vector<bool> queryStatus;
    rocksdb::Iterator* it = pointerToRawRocksDB_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        keys->push_back(it->key().ToString());
        values->push_back(it->value().ToString());
        queryStatus.push_back(true);
    }
    if (queryStatus.size() < targetGetNumber) {
        for (int i = 0; i < targetGetNumber - queryStatus.size(); i++) {
            queryStatus.push_back(false);
        }
    }
    delete it;
    return queryStatus;
}

bool DeltaKV::SingleDelete(const string& key)
{
    rocksdb::Status s = pointerToRawRocksDB_->SingleDelete(rocksdb::WriteOptions(), key);
    if (!s.ok()) {
        return false;
    } else {
        return true;
    }
}

} // namespace DELTAKV_NAMESPACE