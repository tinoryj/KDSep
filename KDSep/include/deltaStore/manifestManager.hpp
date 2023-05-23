#pragma once

#include "common/dataStructure.hpp"

namespace KDSEP_NAMESPACE {

class ManifestManager {
public:
    ManifestManager();
    ~ManifestManager();
    ManifestManager(const string& working_dir);
    bool RetrieveFileMetadata(bool& should_recover, 
	unordered_map<uint64_t, uint64_t>& id2prefixes); 

    void InitialSnapshot(BucketHandler* bucket); 
    void UpdateGCMetadata(
	const vector<BucketHandler*>& old_buckets,
	const vector<BucketHandler*>& new_buckets); 
    void UpdateGCMetadata(const vector<uint64_t>& old_ids,
	const vector<uint64_t>& old_prefixes,
	const vector<uint64_t>& new_ids,
	const vector<uint64_t>& new_prefixes); 
    void UpdateGCMetadata(const uint64_t old_id, const uint64_t old_prefix,
	    const uint64_t new_id, const uint64_t new_prefix); 
    bool CreateManifestIfNotExist();

private:
    bool enable_pointer_ = false;
    uint64_t pointer_int_ = 0;
    shared_mutex mtx_;
    ofstream manifest_fs_;
    ofstream pointer_fs_;
    string working_dir_;
};
}
