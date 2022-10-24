#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core/timer.h"
#include "core_workload.h"
#include "db.h"
#include "utils.h"
#include <iostream>
#include <string>

extern double ops_time[6];
extern long ops_cnt[6];

namespace ycsbc {

class Client {
public:
    Client(DB& db, CoreWorkload& wl)
        : db_(db)
        , workload_(wl)
    {
    }

    virtual Operation DoInsert();
    virtual Operation DoTransaction();

    virtual ~Client() { }

protected:
    virtual int TransactionRead();
    virtual int TransactionReadModifyWrite();
    virtual int TransactionScan();
    virtual int TransactionUpdate();
    virtual int TransactionOverWrite();
    virtual int TransactionInsert();

    DB& db_;
    CoreWorkload& workload_;
};

// FILE* fw = fopen("write_latencies", "a");
//FILE* fr = fopen("read_latencies","a");

inline Operation Client::DoInsert()
{
    std::string key = workload_.NextSequenceKey();
    std::vector<DB::KVPair> pairs;
    workload_.BuildValues(pairs);
    assert(db_.Insert(workload_.NextTable(), key, pairs) >= 0);
    return (Operation::INSERT);
}

inline Operation Client::DoTransaction()
{
    int status = -1;
    utils::Timer timer;
    Operation operation_type = workload_.NextOperation();
    timer.Start();
    switch (operation_type) {
    case READ:
        status = TransactionRead();
        ops_time[READ] += timer.End();
        ops_cnt[READ]++;
        //      fprintf(fr,"%.0f,",timer.End());
        break;
    case UPDATE:
        status = TransactionUpdate();
        ops_time[UPDATE] += timer.End();
        ops_cnt[UPDATE]++;
        // fprintf(fw, "%.0f,", timer.End());
        break;
    case OVERWRITE:
        status = TransactionOverWrite();
        ops_time[OVERWRITE] += timer.End();
        ops_cnt[OVERWRITE]++;
        // fprintf(fw, "%.0f,", timer.End());
        break;
    case INSERT:
        status = TransactionInsert();
        ops_time[INSERT] += timer.End();
        ops_cnt[INSERT]++;
        //      fprintf(fw,"%.0f,",timer.End());
        break;
    case SCAN:
        status = TransactionScan();
        ops_time[SCAN] += timer.End();
        ops_cnt[SCAN]++;
        break;
    case READMODIFYWRITE:
        status = TransactionReadModifyWrite();
        ops_time[READMODIFYWRITE] += timer.End();
        ops_cnt[READMODIFYWRITE]++;
        break;
    default:
        throw utils::Exception("Operation request is not recognized!");
    }
    assert(status >= 0);
    return (operation_type);
}

inline int Client::TransactionRead()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextTransactionKey();
    // std::cout << "Read transaction key = " << key << std::endl;
    std::vector<DB::KVPair> result;
    if (!workload_.read_all_fields()) {
        std::vector<std::string> fields;
        fields.push_back("field" + workload_.NextFieldName());
        return db_.Read(table, key, &fields, result);
    } else {
        return db_.Read(table, key, NULL, result);
    }
}

inline int Client::TransactionReadModifyWrite()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextTransactionKey();
    std::vector<DB::KVPair> result;

    if (!workload_.read_all_fields()) {
        std::vector<std::string> fields;
        fields.push_back("field" + workload_.NextFieldName());
        db_.Read(table, key, &fields, result);
    } else {
        db_.Read(table, key, NULL, result);
    }

    std::vector<DB::KVPair> values;
    // if (workload_.write_all_fields()) {
    //     workload_.BuildValues(values);
    // } else {
    //     workload_.BuildUpdate(values);
    // }
    // return db_.Update(table, key, values);
    workload_.BuildValues(values);
    return db_.Insert(table, key, values);
}

inline int Client::TransactionScan()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextTransactionKey();
    int len = workload_.NextScanLength();
    std::vector<std::vector<DB::KVPair>> result;
    if (!workload_.read_all_fields()) {
        std::vector<std::string> fields;
        fields.push_back("field" + workload_.NextFieldName());
        return db_.Scan(table, key, len, &fields, result);
    } else {
        return db_.Scan(table, key, len, NULL, result);
    }
}

inline int Client::TransactionUpdate()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextTransactionKey();
    std::vector<DB::KVPair> values;
    if (workload_.write_all_fields()) {
        workload_.BuildValues(values);
    } else {
        workload_.BuildUpdate(values);
    }
    // std::cout << "Update transaction key = " << key << std::endl;
    // for (long unsigned int i = 0; i < values.size(); i++) {
    //     std::cout << "Update transaction value = " << values[i].second << std::endl;
    // }
    return db_.Update(table, key, values);
}

inline int Client::TransactionOverWrite()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextTransactionKey();
    std::vector<DB::KVPair> values;
    workload_.BuildValues(values);
    // std::cout << "Update transaction key = " << key << std::endl;
    // for (long unsigned int i = 0; i < values.size(); i++) {
    //     std::cout << "Update transaction value = " << values[i].second << std::endl;
    // }
    return db_.OverWrite(table, key, values);
}

inline int Client::TransactionInsert()
{
    const std::string& table = workload_.NextTable();
    const std::string& key = workload_.NextSequenceKey();
    std::vector<DB::KVPair> values;
    workload_.BuildValues(values);
    return db_.Insert(table, key, values);
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
