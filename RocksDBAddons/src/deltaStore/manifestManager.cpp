#include "deltaStore/manifestManager.hpp"

namespace KDSEP_NAMESPACE {

ManifestManager::ManifestManager(const string& working_dir) {
    working_dir_ = working_dir;
}

bool ManifestManager::CreateManifestIfNotExist() {

    if (enable_pointer_) {
	fstream pointer_fs;
	pointer_fs.open(working_dir_ + "/deltaStoreManifest.pointer", ios::out);
	pointer_int_ = 0;

	if (pointer_fs.is_open()) {
	    pointer_fs << pointer_int_ << endl;
	    pointer_fs.flush();
	    pointer_fs.close();

	    manifest_fs_.open(working_dir_ + "/deltaStoreManifest." +
		    to_string(pointer_int_), ios::out);
	    manifest_fs_.flush();
	    return true;
	} else {
	    debug_error("[ERROR] Open pointer %lu failed\n", pointer_int_);
	    return false;
	}
    }

    manifest_fs_.open(working_dir_ + "/deltaStoreManifest");

    return true;
}

bool ManifestManager::RetrieveFileMetadata(bool& should_recover, 
	unordered_map<uint64_t, uint64_t>& id2prefixes) {
    string path = working_dir_ + "/deltaStoreManifest";
    should_recover = true;
    id2prefixes.clear();

    // check the pointer
    if (enable_pointer_) {
	fstream pointer_fs;
	pointer_fs.open(path + ".pointer", ios::in);

	// check whether need to do recovery
	if (pointer_fs.is_open()) {
	    // pointer exist

	    string pointer;
	    getline(pointer_fs, pointer);
	    // TODO
	    string closeFlagStr;
	    getline(pointer_fs, closeFlagStr);

	    if (closeFlagStr.size() > 0) {
		should_recover = false;
		return true;
	    }
	    pointer_fs.close();

	    path = path + "." + pointer;
	} else {
	    if (CreateManifestIfNotExist()) {
		// first load, not need to recovery
		should_recover = false;
		return true;
	    } else { 
		return false;
	    }
	}
    }

    ifstream manifest_fs;
    manifest_fs.open(path);

    if (manifest_fs.is_open() == false) {
	should_recover = false;
	return true;
    }

    string line_str;
    id2prefixes.clear();
    while (getline(manifest_fs, line_str)) {
	// Case 1: From snapshot
	if (line_str == "add") {
	    uint64_t prefix, file_id;;
	    if (!getline(manifest_fs, line_str)) {
		debug_error("recover manifest stop at prefix: get %lu\n",
			id2prefixes.size());
		return true;
	    }
	    prefix = stoull(line_str);

	    if (!getline(manifest_fs, line_str)) {
		debug_error("recover manifest stop at file id: get %lu\n",
			id2prefixes.size());
		return true;
	    }
	    file_id = stoull(line_str);

	    if (!getline(manifest_fs, line_str)) {
		debug_error("recover manifest stop at end label: get %lu\n",
			id2prefixes.size());
		return true;
	    }
	    
	    if (line_str != "add_end") {
		debug_error("recover manifest stop at end label: not add_end"
			"but %s get %lu\n",
			line_str.c_str(), id2prefixes.size());
		return true;
	    }

	    id2prefixes[file_id] = prefix;

	    continue;
	}

	// Case 2: From GC
	if (line_str == "gc_start") {
	    uint64_t prefix, file_id;
	    unordered_map<uint64_t, uint64_t> id2prefixes_del;
	    unordered_map<uint64_t, uint64_t> id2prefixes_add;

	    while (true) {
		// get prefix
		if (!getline(manifest_fs, line_str)) {
		    debug_error("recover manifest stop at prefix: get %lu\n",
			    id2prefixes.size());
		    return true;
		}
		if (line_str == "gc_new") {
		    break;
		}

		prefix = stoull(line_str); 

		// get file id 
		if (!getline(manifest_fs, line_str)) {
		    debug_error("recover manifest stop at fileid: get %lu\n",
			    id2prefixes.size());
		    return true;
		}
		file_id = stoull(line_str); 

		if (!id2prefixes.count(file_id)) {
		    debug_error("[ERROR] do not have %lu\n", file_id); 
		    return false;
		} else if (id2prefixes[file_id] != prefix) {
		    debug_error("[ERROR] prefix of %lu is not %lu but %lu\n", 
			    file_id, prefix, id2prefixes[file_id]); 
		    return false;
		}

		id2prefixes_del[file_id] = prefix; 
	    }

	    if (line_str != "gc_new") {
		debug_error("not gc new: %s\n", line_str.c_str());
		return true;
	    }

	    while (true) {
		if (!getline(manifest_fs, line_str)) {
		    debug_error("recover manifest stop at prefix: get %lu\n",
			    id2prefixes.size());
		    return true;
		}
		if (line_str == "gc_end") {
		    break;
		}
		prefix = stoull(line_str); 

		if (!getline(manifest_fs, line_str)) {
		    debug_error("recover manifest stop at prefix: get %lu\n",
			    id2prefixes.size());
		    return true;
		}
		file_id = stoull(line_str); 

		id2prefixes_add[file_id] = prefix; 
	    }

	    for (auto& it : id2prefixes_del) {
		id2prefixes.erase(it.first);
	    }
	    for (auto& it : id2prefixes_add) {
		id2prefixes.insert(it);
	    }
	}
    }

    manifest_fs.close();
    manifest_fs_.open(working_dir_ + "/deltaStoreManifest", ios::app);
    if (!manifest_fs_.is_open()) {
	debug_error("open failed - id2prefixes %lu\n", id2prefixes.size());
    }
    return true;
}

void ManifestManager::InitialSnapshot(BucketHandler* bucket) {
    scoped_lock<shared_mutex> lk(mtx_);

    if (!(manifest_fs_ << "add" << endl)) {
	debug_error("output error: %d\n", __LINE__);
    }	
    manifest_fs_ << bucket->prefix << endl;
    manifest_fs_ << bucket->file_id << endl;
    manifest_fs_ << "add_end" << endl;
}

void ManifestManager::UpdateGCMetadata(
	const vector<BucketHandler*>& old_buckets,
	const vector<BucketHandler*>& new_buckets) {

    scoped_lock<shared_mutex> lk(mtx_);

    if (!(manifest_fs_ << "gc_start" << endl)) {
	debug_error("output error: %d\n", __LINE__);
    }	
    // write old file handlers
    for (auto& bucket : old_buckets) {
	manifest_fs_ << bucket->prefix << endl;
	manifest_fs_ << bucket->file_id << endl;
    }

    manifest_fs_ << "gc_new" << endl;
    // write new file handlers
    for (auto& bucket : new_buckets) {
	manifest_fs_ << bucket->prefix << endl;
	manifest_fs_ << bucket->file_id << endl;
    }
    manifest_fs_ << "gc_end" << endl;
}

void ManifestManager::UpdateGCMetadata(const vector<uint64_t>& old_ids,
	const vector<uint64_t>& old_prefixes,
	const vector<uint64_t>& new_ids,
	const vector<uint64_t>& new_prefixes) {
    scoped_lock<shared_mutex> lk(mtx_);

    if (!(manifest_fs_ << "gc_start" << endl)) {
	debug_error("output error: %d\n", __LINE__);
    }	
    for (auto i = 0; i < old_ids.size(); i++) {
	manifest_fs_ << old_prefixes[i] << endl;
	manifest_fs_ << old_ids[i] << endl;
    }
    manifest_fs_ << "gc_new" << endl;
    for (auto i = 0; i < new_ids.size(); i++) {
	manifest_fs_ << new_prefixes[i] << endl;
	manifest_fs_ << new_ids[i] << endl;
    }
    manifest_fs_ << "gc_end" << endl;
}

void ManifestManager::UpdateGCMetadata(const uint64_t old_id, 
	const uint64_t old_prefix, const uint64_t new_id, 
	const uint64_t new_prefix) {
    scoped_lock<shared_mutex> lk(mtx_);

    if (!(manifest_fs_ << "gc_start" << endl)) {
	debug_error("output error: %d\n", __LINE__);
    }	
    manifest_fs_ << old_prefix << endl;
    manifest_fs_ << old_id << endl;
    manifest_fs_ << "gc_new" << endl;
    manifest_fs_ << new_prefix << endl;
    manifest_fs_ << new_id << endl;
    manifest_fs_ << "gc_end" << endl;
}

}
