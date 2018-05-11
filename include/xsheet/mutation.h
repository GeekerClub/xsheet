// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_MUTATION_H
#define  XSHEET_MUTATION_H

#include <stdint.h>
#include <string>

#include "error_code.h"

namespace xsheet {

class Table;
class Transaction;
class RowLock;

class RowMutation {
public:
    // Return row key.
    virtual const std::string& RowKey() = 0;

    // Set the database entry for "key" to "value". The database should be
    // created as a key-value storage.
    // "ttl"(time-to-live) is optional, "value" will expire after "ttl"
    // second. If ttl <= 0, "value" never expire.
    virtual void Put(const std::string& value, int32_t ttl = -1) = 0;

    // Set the database entry for the specified column to "value".
    // "timestamp"(us) is optional, current time by default.
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value, int64_t timestamp = -1) = 0;
    // Put an integer into a cell. This cell can be used as a counter.
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const int64_t value, int64_t timestamp = -1) = 0;
    // Add "delta" to a specified cell. "delta" can be negative.
    virtual void Add(const std::string& family, const std::string& qualifier,
                     const int64_t delta) = 0;
    // "value" will take effect when specified cell does not exist.
    // Otherwise, "value" will be discarded.
    virtual void PutIfAbsent(const std::string& family, const std::string& qualifier,
                             const std::string& value) = 0;
    // Append "value" to a specified cell.
    virtual void Append(const std::string& family, const std::string& qualifier,
                        const std::string& value) = 0;

    // Delete updates of a specified row/columnfamily/qualifier before "timestamp"(us).
    // Delete all versions by default.
    // "timestamp" will be ignored in key-value mode.
    virtual void DeleteRow(int64_t timestamp = -1) = 0;
    virtual void DeleteFamily(const std::string& family, int64_t timestamp = -1) = 0;
    virtual void DeleteColumns(const std::string& family, const std::string& qualifier,
                               int64_t timestamp = -1) = 0;
    // Delete the cell specified by "family"&"qualifier"&"timestamp".
    virtual void DeleteColumn(const std::string& family, const std::string& qualifier,
                              int64_t timestamp) = 0;

    // The status of this row mutation. Returns kOK on success and a non-OK
    // status on error.
    virtual const ErrorCode& GetError() = 0;

    // Users are allowed to register callback/context a two-tuples that
    // will be invoked when this mutation is finished.
    typedef void (*Callback)(RowMutation* param);
    virtual void SetCallBack(Callback callback) = 0;
    virtual Callback GetCallBack() = 0;
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;

    // Set/get timeout(ms).
    virtual void SetTimeOut(int64_t timeout_ms) = 0;
    virtual int64_t TimeOut() = 0;

    // EXPRIMENTAL
    // Returns transaction if exists.
    virtual Transaction* GetTransaction() = 0;

    // Each mutation has an operation. A row mutation is a series of mutations.
    enum Type {
        kPut,
        kDeleteColumn,
        kDeleteColumns,
        kDeleteFamily,
        kDeleteRow,
        kAdd,
        kPutIfAbsent,
        kAppend,
        kAddInt64
    };
    struct Mutation {
        Type type;
        std::string family;
        std::string qualifier;
        std::string value;
        int64_t timestamp;
        int32_t ttl;
    };

    // Get the mutation count of this row mutaion.
    virtual uint32_t MutationNum() = 0;
    // Get total size of all mutations, including size of rowkey, columnfamily,
    // qualifier, value and timestamp.
    virtual uint32_t Size() = 0;
    // Get a mutation specified by "index".
    virtual const RowMutation::Mutation& GetMutation(uint32_t index) = 0;
//     virtual Table* GetTable() = 0;

    RowMutation() {};
    virtual ~RowMutation() {};

private:
    RowMutation(const RowMutation&);
    void operator=(const RowMutation&);
};

class RowLock {};
} // namespace xsheet

#endif  // XSHEET_MUTATION_H
