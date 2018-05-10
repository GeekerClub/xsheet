// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEE_SDK_READ_IMPL_H
#define XSHEE_SDK_READ_IMPL_H

#include "engine/tablet_scanner.h"
#include "xsheet/error_code.h"
#include "xsheet/reader.h"

namespace xsheet {

class RowReaderImpl : public RowReader {
public:
    RowReaderImpl(TabletScanner* tablet_scanner);
    ~RowReaderImpl();

    virtual void AddColumnFamily(const std::string& family);
    virtual void AddColumn(const std::string& family, const std::string& qualifier);
    virtual void SetMaxVersions(uint32_t max_version);
    virtual void SetTimeRange(int64_t ts_start, int64_t ts_end);

    virtual bool Done();
    virtual void Next();
    virtual const std::string& RowKey();
    virtual std::string Value();
    virtual std::string Family();
    virtual std::string Qualifier();
    virtual int64_t Timestamp();

    // Returns all cells in this row as a nested std::map.
//     virtual void ToMap(TRow* rowmap);

    virtual ErrorCode GetError();

    virtual void SetCallBack(Callback callback);
    virtual void SetContext(void* context);
    virtual void* GetContext();
    virtual void SetTimeOut(int64_t timeout_ms);

    // Get column filters map.
//     typedef std::map<std::string, std::set<std::string> >ReadColumnList;
//     virtual const ReadColumnList& GetReadColumnList();

    // EXPERIMENTAL
    // Returns transaction if exists.
//     virtual Transaction* GetTransaction();

private:
    TabletScanner* tablet_scanner_;
    ErrorCode error_;
};

} // namespace xsheet

#endif // XSHEE_SDK_READ_IMPL_H
