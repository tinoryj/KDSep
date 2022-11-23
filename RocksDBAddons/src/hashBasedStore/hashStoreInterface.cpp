#include "hashBasedStore/hashStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr)
{
    internalOptionsPtr_ = options;
    fileManagerNotifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;
    GCNotifyFileMetaDataUpdateMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;

    hashStoreFileManager_ = new HashStoreFileManager(internalOptionsPtr_->hashStore_init_prefix_bit_number, internalOptionsPtr_->hashStore_max_prefix_bit_number, internalOptionsPtr_->deltaStore_garbage_collection_start_single_file_minimum_occupancy * internalOptionsPtr_->deltaStore_single_file_maximum_size, internalOptionsPtr_->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * internalOptionsPtr_->deltaStore_total_storage_maximum_size, workingDirStr, fileManagerNotifyGCMQ_, GCNotifyFileMetaDataUpdateMQ_);
    hashStoreGCManager_ = new HashStoreGCManager(workingDirStr, fileManagerNotifyGCMQ_, GCNotifyFileMetaDataUpdateMQ_);
    hashStoreFileOperator_ = new HashStoreFileOperator(options, fileManagerNotifyGCMQ_);
    if (!hashStoreFileManager_) {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreInterface]-[Construction] Create HashStoreFileManager error" << RESET << endl;
    }
    if (!hashStoreFileOperator_) {
        cerr << RED << "[ERROR]:[Addons]-[HashStoreInterface]-[Construction] Create HashStoreFileOperator error" << RESET << endl;
    }
}

HashStoreInterface::~HashStoreInterface()
{
}

bool HashStoreInterface::put(const string& keyStr, const string& valueStr)
{
    return true;
}

vector<bool> HashStoreInterface::multiPut(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool HashStoreInterface::get(const string& keyStr, string* valueStrPtr)
{
    return true;
}

vector<bool> HashStoreInterface::multiGet(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool HashStoreInterface::forcedManualGarbageCollection()
{
    return true;
}

}