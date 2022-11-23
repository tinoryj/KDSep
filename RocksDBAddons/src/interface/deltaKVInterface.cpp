#include "interface/deltaKVInterface.hpp"

namespace DELTAKV_NAMESPACE {

bool RocksDBInternalMergeOperator::FullMerge(const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value, Logger* logger) const
{
    // externalValueType tempExternalValueTypeStructForCheck;
    // memcpy(&tempExternalValueTypeStructForCheck, existing_value->data(), sizeof(externalValueType));
    // if (tempExternalValueTypeStructForCheck.mergeFlag_ == false) {
    //     cerr << RED << "[ERROR]:[Addons]-[RocksDBInternalMergeOperator]-[FullMerge] find object request merge without correct merge flag" << RESET << endl;
    //     return false;
    // }
    new_value->assign(existing_value->data());
    return true;
};

bool RocksDBInternalMergeOperator::PartialMerge(const Slice& key, const Slice& left_operand,
    const Slice& right_operand, std::string* new_value,
    Logger* logger) const
{
    string emptyValueStr = "";
    new_value->assign(emptyValueStr);
    return true;
};

DeltaKV::DeltaKV(DeltaKVOptions& options, const string& name)
{
    // Create objects
    if (options.enable_deltaStore == true) {
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, hashStoreGCManagerPtr_);
    }
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] add HashStoreInterfaceObjPtr_ success" << RESET << endl;
    // start threadPool, memPool, etc.
    launchThreadPool(options.deltaKV_thread_number_limit);
    ioService_.post(boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] start thread for HashStoreFileOperator::operationWorker success" << RESET << endl;
}

DeltaKV::~DeltaKV()
{
    if (pointerToRawRocksDB_ != nullptr) {
        delete pointerToRawRocksDB_;
    }
    if (HashStoreInterfaceObjPtr_ != nullptr) {
        delete HashStoreInterfaceObjPtr_;
        // delete related object pointers
        delete hashStoreFileManagerPtr_;
        delete hashStoreFileOperatorPtr_;
        delete hashStoreGCManagerPtr_;
    }
    deleteThreadPool();
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Deconstruction] delete thread pool and underlying rocksdb success" << RESET << endl;
}

bool DeltaKV::Open(DeltaKVOptions& options, const string& name)
{
    options.rocksdbRawOptions_.merge_operator.reset(
        new RocksDBInternalMergeOperator);
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, name = " << name << RESET << endl;
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb, pointerToRawRocksDB_ = " << &pointerToRawRocksDB_ << RESET << endl;
    rocksdb::Status s = rocksdb::DB::Open(options.rocksdbRawOptions_, name, &pointerToRawRocksDB_);
    if (!s.ok()) {
        cerr << RED << "[ERROR]:[Addons]-[DeltaKVInterface]-[Construction] Can't open underlying rocksdb" << RESET << endl;
        return false;
    } else {
        cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] Open underlying rocksdb success" << RESET << endl;
    }
    // Create objects
    if (options.enable_deltaStore == true && HashStoreInterfaceObjPtr_ == nullptr) {
        HashStoreInterfaceObjPtr_ = new HashStoreInterface(&options, name, hashStoreFileManagerPtr_, hashStoreFileOperatorPtr_, hashStoreGCManagerPtr_);
    }
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] add HashStoreInterfaceObjPtr_ success" << RESET << endl;
    // start threadPool, memPool, etc.
    launchThreadPool(options.deltaKV_thread_number_limit);
    ioService_.post(boost::bind(&HashStoreFileOperator::operationWorker, hashStoreFileOperatorPtr_));
    cerr << GREEN << "[INFO]:[Addons]-[DeltaKVInterface]-[Construction] start thread for HashStoreFileOperator::operationWorker success" << RESET << endl;
    return true;
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

bool DeltaKV::launchThreadPool(uint64_t totalThreadNumber)
{
    /*
     * This will start the ioService_ processing loop. All tasks
     * assigned with ioService_.post() will start executing.
     */
    boost::asio::io_service::work work(ioService_);

    /*
     * This will add totalThreadNumber threads to the thread pool.
     */
    for (uint64_t i = 0; i < totalThreadNumber; i++) {
        threadpool_.create_thread(
            boost::bind(&boost::asio::io_service::run, &ioService_));
    }
    return true;
}

bool DeltaKV::deleteThreadPool()
{
    /*
     * This will stop the ioService_ processing loop. Any tasks
     * you add behind this point will not execute.
     */
    ioService_.stop();

    /*
     * Will wait till all the threads in the thread pool are finished with
     * their assigned tasks and 'join' them. Just assume the threads inside
     * the threadpool_ will be destroyed by this method.
     */
    threadpool_.join_all();
    return true;
}

} // namespace DELTAKV_NAMESPACE