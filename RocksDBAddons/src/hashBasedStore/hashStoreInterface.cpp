#include "hashBasedStore/hashStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

HashStoreInterface::HashStoreInterface(DeltaKVOptions* options, const string& workingDirStr, HashStoreFileManager*& hashStoreFileManager,
    HashStoreFileOperator*& hashStoreFileOperator)
{
    internalOptionsPtr_ = options;
    extractValueSizeThreshold_ = options->extract_to_deltaStore_size_lower_bound;

    notifyGCMQ_ = new messageQueue<hashStoreFileMetaDataHandler*>;

    uint64_t singleFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_single_file_minimum_occupancy * internalOptionsPtr_->deltaStore_single_file_maximum_size;
    uint64_t totalHashStoreFileGCThreshold = internalOptionsPtr_->deltaStore_garbage_collection_start_total_storage_minimum_occupancy * internalOptionsPtr_->deltaStore_total_storage_maximum_size;

    hashStoreFileManager = new HashStoreFileManager(internalOptionsPtr_->hashStore_init_prefix_bit_number, internalOptionsPtr_->hashStore_max_prefix_bit_number, singleFileGCThreshold, totalHashStoreFileGCThreshold, workingDirStr, notifyGCMQ_, options->fileOperationMethod_);
    hashStoreFileOperator = new HashStoreFileOperator(options, notifyGCMQ_);
    if (!hashStoreFileManager) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Create HashStoreFileManager error" << RESET << endl;
    }
    if (!hashStoreFileOperator) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): Create HashStoreFileOperator error" << RESET << endl;
    }
    hashStoreFileManagerPtr_ = hashStoreFileManager;
    hashStoreFileOperatorPtr_ = hashStoreFileOperator;
    unordered_map<string, vector<pair<bool, string>>> targetListForRedo;
    hashStoreFileManagerPtr_->recoveryFromFailure(targetListForRedo);
    hashStoreFileManagerPtr_->setOperationNumberThresholdForMetadataUpdata(options->deltaStore_operationNumberForMetadataCommitThreshold_);
}

HashStoreInterface::~HashStoreInterface()
{
    delete notifyGCMQ_;
}

bool HashStoreInterface::setJobDone()
{
    notifyGCMQ_->done_ = true;
    if (hashStoreFileOperatorPtr_->setJobDone() == true) {
        return true;
    } else {
        return false;
    }
}

uint64_t HashStoreInterface::getExtractSizeThreshold()
{
    return extractValueSizeThreshold_;
}

bool HashStoreInterface::put(const string& keyStr, const string& valueStr, bool isAnchor)
{
    hashStoreFileMetaDataHandler* tempFileHandler;
    //  cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): start get fileHandler from file manager" << RESET << endl;
    if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kPut, tempFileHandler) != true) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get fileHandler from file manager error" << RESET << endl;

        return false;
    } else {
        //  cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): get fileHandler from file manager success, handler address = " << tempFileHandler << " file id = " << tempFileHandler->target_file_id_ << RESET << endl;
        if (hashStoreFileOperatorPtr_->putWriteOperationIntoJobQueue(tempFileHandler, keyStr, valueStr, isAnchor) != true) {

            cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write to dLog error" << RESET << endl;

            return false;
        } else {
            //  cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write to dLog success" << RESET << endl;
            return true;
        }
    }
}

bool HashStoreInterface::multiPut(vector<string> keyStrVec, vector<string> valueStrPtrVec, vector<bool> isAnchorVec)
{
    unordered_map<hashStoreFileMetaDataHandler*, tuple<vector<string>, vector<string>, vector<bool>>> tempFileHandlerMap;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr;
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStrVec[i], kPut, currentFileHandlerPtr) != true) {
            return false;
        } else {
            if (tempFileHandlerMap.find(currentFileHandlerPtr) != tempFileHandlerMap.end()) {
                std::get<0>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(keyStrVec[i]);
                std::get<1>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(valueStrPtrVec[i]);
                std::get<2>(tempFileHandlerMap.at(currentFileHandlerPtr)).push_back(isAnchorVec[i]);
            } else {
            }
        }
    }
    if (hashStoreFileOperatorPtr_->putWriteOperationsVectorIntoJobQueue(tempFileHandlerMap) != true) {

        cerr << BOLDRED << "[ERROR]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write to dLog error" << RESET << endl;

        return false;
    } else {
        //  cout << BLUE << "[DEBUG-LOG]:" << __STR_FILE__ << "<->" << __STR_FUNCTIONP__ << "<->(line " << __LINE__ << "): write to dLog success" << RESET << endl;
        return true;
    }
}

bool HashStoreInterface::get(const string& keyStr, vector<string>*& valueStrVec)
{
    hashStoreFileMetaDataHandler* tempFileHandler;
    if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStr, kGet, tempFileHandler) != true) {
        return false;
    } else {
        if (hashStoreFileOperatorPtr_->putReadOperationIntoJobQueue(tempFileHandler, keyStr, valueStrVec) != true) {
            return false;
        } else {
            return true;
        }
    }
}

bool HashStoreInterface::multiGet(vector<string> keyStrVec, vector<vector<string>*>*& valueStrVecVec)
{
    vector<hashStoreFileMetaDataHandler*> tempFileHandlerVec;
    for (auto i = 0; i < keyStrVec.size(); i++) {
        hashStoreFileMetaDataHandler* currentFileHandlerPtr;
        if (hashStoreFileManagerPtr_->getHashStoreFileHandlerByInputKeyStr(keyStrVec[i], kGet, currentFileHandlerPtr) != true) {
            return false;
        } else {
            tempFileHandlerVec.push_back(currentFileHandlerPtr);
        }
    }
    if (hashStoreFileOperatorPtr_->putReadOperationsVectorIntoJobQueue(tempFileHandlerVec, keyStrVec, valueStrVecVec) != true) {
        return false;
    } else {
        return true;
    }
}

bool HashStoreInterface::forcedManualGarbageCollection()
{
    bool forcedGCStatus = hashStoreFileManagerPtr_->forcedManualGCAllFiles();
    if (forcedGCStatus == true) {
        return true;
    } else {
        return false;
    }
}

}