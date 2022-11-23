#include "hashBasedStore/hashStoreInterface.hpp"

namespace DELTAKV_NAMESPACE {

hashStoreInterface::hashStoreInterface(DeltaKVOptions* options)
{
    internalOptionsPtr_ = options;
}

hashStoreInterface::~hashStoreInterface()
{
}

bool hashStoreInterface::put(const string& keyStr, const string& valueStr)
{
    return true;
}

vector<bool> hashStoreInterface::multiPut(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool hashStoreInterface::get(const string& keyStr, string* valueStrPtr)
{
    return true;
}

vector<bool> hashStoreInterface::multiGet(vector<string> keyStrVec, vector<string*> valueStrPtrVec)
{
    vector<bool> resultBoolVec;
    return resultBoolVec;
}

bool hashStoreInterface::forcedManualGarbageCollection()
{
    return true;
}

}