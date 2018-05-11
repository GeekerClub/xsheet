// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_TABLE_H
#define  XSHEET_TABLE_H

#include <stdint.h>
#include <string>
#include <vector>

#include "error_code.h"
#include "mutation.h"
#include "reader.h"
#include "scan.h"
#include "table_descriptor.h"

#pragma GCC visibility push(default)
namespace xsheet {

struct TableInfo {
    TableDescriptor* table_desc;
    std::string status;
};

// struct TabletInfo {
//     std::string table_name;
//     std::string path;
//     std::string server_addr;
//     std::string start_key;
//     std::string end_key;
//     int64_t data_size;
//     std::string status;
// };

class RowMutation;
class RowReader;
class Transaction;
class Table {
public:
    virtual const std::string GetName() = 0;

    virtual RowMutation* NewRowMutation(const std::string& row_key) = 0;
    virtual void Put(RowMutation* row_mutation) = 0;
    virtual void Put(const std::vector<RowMutation*>& row_mutations) = 0;
    virtual bool IsPutFinished() = 0;
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) = 0;
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err) = 0;
    virtual bool Add(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) = 0;
    virtual bool PutIfAbsent(const std::string& row_key, const std::string& family,
                             const std::string& qualifier, const std::string& value,
                             ErrorCode* err) = 0;
    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err) = 0;

    // Return a row reader handle. User should delete it when it is no longer
    // needed.
    virtual RowReader* NewRowReader(const std::string& row_key) = 0;
    virtual void Get(RowReader* row_reader) = 0;
    virtual void Get(const std::vector<RowReader*>& row_readers) = 0;
    virtual bool IsGetFinished() = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err) = 0;
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err) = 0;

    // Return a result stream described by "desc".
    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err) = 0;

    // EXPERIMENTAL
    // Return a row transaction handle.
    virtual Transaction* StartRowTransaction(const std::string& row_key) = 0;
    // Commit a row transaction.
    virtual void CommitRowTransaction(Transaction* transaction) = 0;

    Table() {}
    virtual ~Table() {}

private:
    Table(const Table&);
    void operator=(const Table&);
};

} // namespace xsheet
#pragma GCC visibility pop

#endif  // XSHEET_TABLE_H
