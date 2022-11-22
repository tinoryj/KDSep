#include "common/rocksdbHeaders.hpp"
#include "interface/deltaKVOptions.hpp"
#include "interface/mergeOperation.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

class DeltaKV {
public:
    // Abstract class ctor
    DeltaKV();
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
    rocksdb::DB* pointerToRawRocksDB_;
};

} // namespace DELTAKV_NAMESPACE