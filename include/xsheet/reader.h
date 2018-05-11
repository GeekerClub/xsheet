// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_READER_H
#define  XSHEET_READER_H

#include <stdint.h>
#include <map>
#include <set>
#include <string>

#include "error_code.h"

namespace xsheet {

class RowReaderImpl;
class Transaction;
class Table;
class RowReader {
public:

    // Set request filter conditions. If none, returns all cells of this row.
    // Only returns cells in these columns.
    virtual void AddColumnFamily(const std::string& family) = 0;
    virtual void AddColumn(const std::string& family, const std::string& qualifier) = 0;
    // Set the maximum number of versions of each column.
    virtual void SetMaxVersions(uint32_t max_version) = 0;
    // If set, only returns cells of which update timestamp is within [ts_start, ts_end].
    virtual void SetTimeRange(int64_t ts_start, int64_t ts_end) = 0;

    // Access received data.
    // Use RowReader as an iterator. While Done() returns false, one cell is
    // accessible.
    virtual bool Done() = 0;
    virtual void Next() = 0;
    // Access present cell.
    // Only RowKey&Value are effective in key-value storage.
    virtual const std::string& RowKey() = 0;
    virtual std::string Value() = 0;
    virtual std::string Family() = 0;
    virtual std::string Qualifier() = 0;
    virtual int64_t Timestamp() = 0;

    // Returns all cells in this row as a nested std::map.
    typedef std::map<int64_t, std::string> TColumn;
    typedef std::map<std::string, TColumn> TColumnFamily;
    typedef std::map<std::string, TColumnFamily> TRow;
    virtual void ToMap(TRow* rowmap) = 0;

    // The status of this row reader. Returns kOK on success and a non-OK
    // status on error.
    virtual ErrorCode GetError() = 0;

    // Users are allowed to register a callback/context two-tuples that
    // will be invoked when this reader is finished.
    typedef void (*Callback)(RowReader* param);
    virtual void SetCallBack(Callback callback) = 0;
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;
    virtual void SetTimeOut(int64_t timeout_ms) = 0;
//     virtual Table* GetTable() = 0;

    // Get column filters map.
    typedef std::map<std::string, std::set<std::string> >ReadColumnList;
//     virtual const ReadColumnList& GetReadColumnList() = 0;

    // EXPERIMENTAL
    // Returns transaction if exists.
//     virtual Transaction* GetTransaction() = 0;


    RowReader() {};
    virtual ~RowReader() {};

private:
    RowReader(const RowReader&);
    void operator=(const RowReader&);
};

} // namespace xsheet

#endif  // XSHEET_READER_H
