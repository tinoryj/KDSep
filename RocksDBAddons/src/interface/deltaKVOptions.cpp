#include "interface/deltaKVOptions.hpp"

namespace DELTAKV_NAMESPACE {

bool DeltaKVOptions::dumpOptions(string dumpPath)
{
    ofstream dumpOptionsOutStream;
    dumpOptionsOutStream.open(dumpPath, ios::out);

    dumpOptionsOutStream << "DeltaStore options:" << endl;
    dumpOptionsOutStream << "\tenable_deltaStore = " << enable_deltaStore << endl;
    dumpOptionsOutStream << "\tenable_deltaStore_fileLvel_cache = " << enable_deltaStore_fileLvel_cache << endl;
    dumpOptionsOutStream << "\tenable_deltaStore_KDLevel_cahce = " << enable_deltaStore_KDLevel_cahce << endl;
    dumpOptionsOutStream << "\tenable_deltaStore_garbage_collection = " << enable_deltaStore_garbage_collection << endl;
    dumpOptionsOutStream << "\tdeltaStore_base_cache_mode = " << static_cast<typename std::underlying_type<contentCacheMode>::type>(deltaStore_base_cache_mode) << endl;
    dumpOptionsOutStream << "\tdeltaStore_base_store_mode = " << static_cast<typename std::underlying_type<contentStoreMode>::type>(deltaStore_base_store_mode) << endl;
    dumpOptionsOutStream << "\tdeltaStore_fileLvel_cache_size = " << deltaStore_fileLvel_cache_size << endl;
    dumpOptionsOutStream << "\tdeltaStore_KDLevel_cahce_size = " << deltaStore_KDLevel_cahce_size << endl;
    dumpOptionsOutStream << "\textract_to_deltaStore_size_lower_bound = " << extract_to_deltaStore_size_lower_bound << endl;
    dumpOptionsOutStream << "\textract_to_deltaStore_size_upper_bound = " << extract_to_deltaStore_size_upper_bound << endl;
    dumpOptionsOutStream << "\tdeltaStore_single_file_maximum_size = " << deltaStore_single_file_maximum_size << endl;
    dumpOptionsOutStream << "\tdeltaStore_total_storage_maximum_size = " << deltaStore_total_storage_maximum_size << endl;
    dumpOptionsOutStream << "\tdeltaStore_garbage_collection_start_single_file_minimum_occupancy = " << deltaStore_garbage_collection_start_single_file_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tdeltaStore_garbage_collection_start_total_storage_minimum_occupancy = " << deltaStore_garbage_collection_start_total_storage_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tdeltaStore_garbage_collection_force_single_file_minimum_occupancy = " << deltaStore_garbage_collection_force_single_file_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tdeltaStore_garbage_collection_force_total_storage_minimum_occupancy = " << deltaStore_garbage_collection_force_total_storage_minimum_occupancy << endl;

    dumpOptionsOutStream << "ValueStore options:" << endl;
    dumpOptionsOutStream << "\tenable_valueStore = " << enable_valueStore << endl;
    dumpOptionsOutStream << "\tenable_valueStore_fileLvel_cache = " << enable_valueStore_fileLvel_cache << endl;
    dumpOptionsOutStream << "\tenable_valueStore_KDLevel_cahce = " << enable_valueStore_KDLevel_cahce << endl;
    dumpOptionsOutStream << "\tenable_valueStore_garbage_collection = " << enable_valueStore_garbage_collection << endl;
    dumpOptionsOutStream << "\tvalueStore_base_cache_mode = " << static_cast<typename std::underlying_type<contentCacheMode>::type>(valueStore_base_cache_mode) << endl;
    dumpOptionsOutStream << "\tvalueStore_base_store_mode = " << static_cast<typename std::underlying_type<contentStoreMode>::type>(valueStore_base_store_mode) << endl;
    dumpOptionsOutStream << "\tvalueStore_fileLvel_cache_size = " << valueStore_fileLvel_cache_size << endl;
    dumpOptionsOutStream << "\tvalueStore_KDLevel_cahce_size = " << valueStore_KDLevel_cahce_size << endl;
    dumpOptionsOutStream << "\textract_to_valueStore_size_lower_bound = " << extract_to_valueStore_size_lower_bound << endl;
    dumpOptionsOutStream << "\textract_to_valueStore_size_upper_bound = " << extract_to_valueStore_size_upper_bound << endl;
    dumpOptionsOutStream << "\tvalueStore_single_file_maximum_size = " << valueStore_single_file_maximum_size << endl;
    dumpOptionsOutStream << "\tvalueStore_total_storage_maximum_size = " << valueStore_total_storage_maximum_size << endl;
    dumpOptionsOutStream << "\tvalueStore_garbage_collection_start_single_file_minimum_occupancy = " << valueStore_garbage_collection_start_single_file_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tvalueStore_garbage_collection_start_total_storage_minimum_occupancy = " << valueStore_garbage_collection_start_total_storage_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tvalueStore_garbage_collection_force_single_file_minimum_occupancy = " << valueStore_garbage_collection_force_single_file_minimum_occupancy << endl;
    dumpOptionsOutStream << "\tvalueStore_garbage_collection_force_total_storage_minimum_occupancy = " << valueStore_garbage_collection_force_total_storage_minimum_occupancy << endl;
    dumpOptionsOutStream.flush();
    dumpOptionsOutStream.close();
    return true;
}

} // namespace DELTAKV_NAMESPACE