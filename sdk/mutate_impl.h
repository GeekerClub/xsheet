// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_SDK_MUTATION_IPML_H
#define  XSHEET_SDK_MUTATION_IPML_H

#include <stdint.h>
#include <string>
#include <vector>

#include "xsheet/error_code.h"
#include "xsheet/mutation.h"

namespace xsheet {

class RowMutationImpl : public RowMutation {
public:
    RowMutationImpl();
    virtual ~RowMutationImpl();

    // Return row key.
    virtual const std::string& RowKey();

    // Set the database entry for "key" to "value". The database should be
    // created as a key-value storage.
    // "ttl"(time-to-live) is optional, "value" will expire after "ttl"
    // second. If ttl <= 0, "value" never expire.
    virtual void Put(const std::string& value, int32_t ttl = -1);

    // Set the database entry for the specified column to "value".
    // "timestamp"(us) is optional, current time by default.
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value, int64_t timestamp = -1);
    // Put an integer into a cell. This cell can be used as a counter.
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const int64_t value, int64_t timestamp = -1);
    // Add "delta" to a specified cell. "delta" can be negative.
    virtual void Add(const std::string& family, const std::string& qualifier,
                     const int64_t delta);
    // "value" will take effect when specified cell does not exist.
    // Otherwise, "value" will be discarded.
    virtual void PutIfAbsent(const std::string& family, const std::string& qualifier,
                             const std::string& value);
    // Append "value" to a specified cell.
    virtual void Append(const std::string& family, const std::string& qualifier,
                        const std::string& value);

    // Delete updates of a specified row/columnfamily/qualifier before "timestamp"(us).
    // Delete all versions by default.
    // "timestamp" will be ignored in key-value mode.
    virtual void DeleteRow(int64_t timestamp = -1);
    virtual void DeleteFamily(const std::string& family, int64_t timestamp = -1);
    virtual void DeleteColumns(const std::string& family, const std::string& qualifier,
                               int64_t timestamp = -1);
    // Delete the cell specified by "family"&"qualifier"&"timestamp".
    virtual void DeleteColumn(const std::string& family, const std::string& qualifier,
                              int64_t timestamp);

    // The status of this row mutation. Returns kOK on success and a non-OK
    // status on error.
    virtual const ErrorCode& GetError();

    // Users are allowed to register callback/context a two-tuples that
    // will be invoked when this mutation is finished.
    typedef void (*Callback)(RowMutation* param);
    virtual void SetCallBack(Callback callback);
    virtual Callback GetCallBack();
    virtual void SetContext(void* context);
    virtual void* GetContext();

    // Set/get timeout(ms).
    virtual void SetTimeOut(int64_t timeout_ms);
    virtual int64_t TimeOut();

    // EXPRIMENTAL
    // Returns transaction if exists.
    virtual Transaction* GetTransaction();

    // Get the mutation count of this row mutaion.
    virtual uint32_t MutationNum();
    // Get total size of all mutations, including size of rowkey, columnfamily,
    // qualifier, value and timestamp.
    virtual uint32_t Size();
    // Get a mutation specified by "index".
    virtual const RowMutation::Mutation& GetMutation(uint32_t index);

private:
    RowMutationImpl(const RowMutationImpl&);
    void operator=(const RowMutationImpl&);

    std::string row_key_;
    Mutation mutation_;
    ErrorCode error_;
    RowMutation::Callback callback_;
    void* user_context_;

    std::vector<RowMutation::Mutation> mu_seq_;
};


} // namespace xsheet

#endif  // XSHEET_SDK_MUTATION_IPML_H
