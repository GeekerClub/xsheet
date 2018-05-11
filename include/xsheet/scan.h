// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_SCAN_H
#define  XSHEET_SCAN_H

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "error_code.h"

#pragma GCC visibility push(default)
namespace xsheet {

class ResultStream {
public:
    ResultStream() {}
    virtual ~ResultStream() {}

    virtual bool Done(ErrorCode* err = NULL) = 0;

    virtual void Next() = 0;

    virtual std::string RowName() const = 0;
    virtual std::string Family() const = 0;
    virtual std::string Qualifier() const = 0;
    virtual int64_t Timestamp() const = 0;
    virtual std::string Value() const = 0;
    virtual int64_t ValueInt64() const = 0;

private:
    ResultStream(const ResultStream&);
    void operator=(const ResultStream&);
};

class ScanDescImpl;
class ScanDescriptor {
public:
    ScanDescriptor(const std::string& rowkey);
    ~ScanDescriptor();
    void SetEnd(const std::string& rowkey);

    void AddColumnFamily(const std::string& cf);
    void AddColumn(const std::string& cf, const std::string& qualifier);
    void SetMaxVersions(int32_t versions);
    void SetTimeRange(int64_t ts_end, int64_t ts_start);
    void SetAsync(bool async);
    bool IsAsync() const;

    // Set timeout for each internal scan jobs, which avoids long-term scan jobs to trigger rpc's timeout.
    // Not required.
    void SetPackInterval(int64_t timeout);

    // Set buffersize for each internal scan jobs, which avoids scan result buffer growing too much.
    // Default: 64KB
    void SetBufferSize(int64_t buf_size);

    // Set the limitation of cell number for each internal scan jobs,
    // which acquires lower latency in interactive scan task.
    // Not required.
    void SetNumberLimit(int64_t number_limit);
    int64_t GetNumberLimit();

private:
    ScanDescriptor(const ScanDescriptor&);
    void operator=(const ScanDescriptor&);
    ScanDescImpl* impl_;
};

} // namespace xsheet
#pragma GCC visibility pop

#endif  // XSHEET_SCAN_H
