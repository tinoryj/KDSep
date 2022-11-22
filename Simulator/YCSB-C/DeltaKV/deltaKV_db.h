#ifndef YCSB_C_DeltaKVDB_DB_H
#define YCSB_C_DeltaKVDB_DB_H

#include <assert.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "core/db.h"
#include "core/properties.h"
#include "interface/deltaKVInterface.hpp"
#include "interface/deltaKVOptions.hpp"

namespace ycsbc {
class DeltaKVDB : public YCSBDB {
   public:
    DeltaKVDB(const char *dbfilename, const std::string &config_file_path);

    int Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result);

    int Scan(const std::string &table, const std::string &key,
             int len, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result);

    int Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int OverWrite(const std::string &table, const std::string &key,
                  std::vector<KVPair> &values);

    int Delete(const std::string &table, const std::string &key);

    void printStats();

    ~DeltaKVDB();

   private:
    std::ofstream outputStream_;
    DeltaKV *db_;
    DeltaKVOptions options_;
};
}  // namespace ycsbc
#endif  // YCSB_C_DeltaKVDB_DB_H
