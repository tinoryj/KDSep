#include "db/db_factory.h"

#include <string>

#include "DeltaKV/deltaKV_db.h"
// #include "RocksDB/rocksdb_db.h"

using namespace std;
using ycsbc::YCSBDB;
using ycsbc::DBFactory;

YCSBDB *DBFactory::CreateDB(utils::Properties &props) {
    if (props["dbname"] == "rocksdb") {
        return new DeltaKVDB(props["dbfilename"].c_str(), props["configpath"]);
        // cerr << props["dbfilename"].c_str() << props["configpath"] << endl;
    } else {
        return NULL;
    }
}
