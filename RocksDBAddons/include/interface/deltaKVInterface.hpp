#include "common/rocksdbHeaders.hpp"
#include "interface/deltaKVOptions.hpp"

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

    static bool Open(const DeltaKVOptions& options, const string& name);
    static bool Close();

    bool Put(const string& key, const string& value);
    bool Get(const string& key, string* value);
    bool Merge(const string& key, const string& value);
    vector<bool> MultiGet(const vector<string>& keys, vector<string>* values);
    vector<bool> GetByPrefix(const string& key, vector<string>* keys, vector<string>* values);
    vector<bool> GetByTargetNumber(const uint64_t& targetGetNumber, vector<string>* keys, vector<string>* values);
    bool SingleDelete(const string& key);

private:
    static rocksdb::DB* pointerToRawRocksDB_;
};

}