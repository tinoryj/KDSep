#include "interface/deltaKVOptions.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

class indexStoreInterface {
public:
    indexStoreInterface(DeltaKVOptions* options);
    ~indexStoreInterface();
    bool put(const string& keyStr, const string& valueStr);
    vector<bool> multiPut(vector<string> keyStrVec, vector<string*> valueStrPtrVec);
    bool get(const string& keyStr, string* valueStrPtr);
    vector<bool> multiGet(vector<string> keyStrVec, vector<string*> valueStrPtrVec);
    bool forcedManualGarbageCollection();

private:
    DeltaKVOptions* internalOptionsPtr_;
};

}