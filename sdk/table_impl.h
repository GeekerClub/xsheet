// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_TABLE_IMPLH
#define  XSHEET_TABLE_IMPLH

#include <stdint.h>
#include <string>
#include <vector>

#include "xsheet/error_code.h"
#include "xsheet/mutation.h"
#include "xsheet/reader.h"
#include "xsheet/scan.h"

#include "engine/tablet_schema.pb.h"

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
    TableImpl(const TabletSchema& tablet_schema, KvBase* kvbase);
    virtual ~TableImpl();

    virtual const std::string GetName();

    virtual RowMutation* NewRowMutation(const std::string& row_key);
    virtual void Put(RowMutation* row_mutation);
    virtual void Put(const std::vector<RowMutation*>& row_mutations);
    virtual bool IsPutFinished();
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err);
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err);
    virtual bool Add(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err);
    virtual bool PutIfAbsent(const std::string& row_key, const std::string& family,
                             const std::string& qualifier, const std::string& value,
                             ErrorCode* err);
    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err);

    // Return a row reader handle. User should delete it when it is no longer
    // needed.
    virtual RowReader* NewRowReader(const std::string& row_key);
    virtual void Get(RowReader* row_reader);
    virtual void Get(const std::vector<RowReader*>& row_readers);
    virtual bool IsGetFinished();
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err);
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err);

    // Return a result stream described by "desc".
    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err);

    // EXPERIMENTAL
    // Return a row transaction handle.
    virtual Transaction* StartRowTransaction(const std::string& row_key);
    // Commit a row transaction.
    virtual void CommitRowTransaction(Transaction* transaction);

private:
    void ApplyMutation(const std::vector<RowMutation*>& row_mutations);
    void CommitMutation(const std::vector<RowMutation*>& row_mutations);
    void MutationCallback(std::vector<const RowMutationSequence*>* row_mutation_vec,
                          std::vector<StatusCode>* status_vec);


private:
    TableImpl(const TableImpl&);
    void operator=(const TableImpl&);

    TabletSchema tablet_schema_;
    KvBase* kvbase_;
    TabletWriter tablet_writer_;
    TabletScanner tablet_scanner_;

    toft::scopted_ptr<toft::ThreadPool> thread_pool_;
};

} // namespace xsheet
#pragma GCC visibility pop

#endif  // XSHEET_TABLE_IMPLH
