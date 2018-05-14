// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_SDK_SCAN_IMPL_H
#define  XSHEET_SDK_SCAN_IMPL_H

#include <list>
#include <queue>
#include <string>
#include <vector>

#include "toft/system/threading/event.h"
#include "toft/system/threading/thread.h"

#include "engine/tablet_scanner.h"
#include "engine/types.h"
#include "proto/tablet.pb.h"
#include "xsheet/scan.h"

namespace xsheet {

class TabletScanner;

class ResultStreamImpl : public ResultStream {
public:
    ResultStreamImpl(TabletScanner* tablet_scanner, ScanDescImpl* scan_desc_impl);
    virtual ~ResultStreamImpl();

    bool LookUp(const std::string& row_key);
    bool Done(ErrorCode* err);
    void Next();

    std::string RowName() const;
    std::string Family() const;
    std::string ColumnName() const;
    std::string Qualifier() const;
    int64_t Timestamp() const;
    std::string Value() const;
    int64_t ValueInt64() const;

public:
    ScanDescImpl* GetScanDesc();
    std::string GetNextStartPoint(const std::string& str);

private:
    ResultStreamImpl(const ResultStreamImpl&);
    void operator=(const ResultStreamImpl&);

    void FillScanOptions(ScanDescImpl* desc_impl, ScanOptions* scan_options);
    void ScanSessionReset();

protected:
    xsheet::ScanDescImpl* scan_desc_impl_;
    TabletScanner* tablet_scanner_;

    ScanContext* scan_context_;
    ScanStats* scan_stats_;

    int32_t next_idx_;
};

class ScanDescImpl {
public:
    ScanDescImpl(const std::string& rowkey);

    ScanDescImpl(const ScanDescImpl& impl);

    ~ScanDescImpl();

    void SetEnd(const std::string& rowkey);

    void AddColumnFamily(const std::string& cf);

    void AddColumn(const std::string& cf, const std::string& qualifier);

    void SetMaxVersions(int32_t versions);

    void SetPackInterval(int64_t timeout);

    void SetTimeRange(int64_t ts_end, int64_t ts_start);

    bool SetFilter(const std::string& schema);

//     void SetValueConverter(ValueConverter converter);

    void SetSnapshot(uint64_t snapshot_id);

    void SetBufferSize(int64_t buf_size);

    void SetNumberLimit(int64_t number_limit);

    void SetAsync(bool async);

    void SetStart(const std::string& row_key, const std::string& column_family = "",
                  const std::string& qualifier = "", int64_t time_stamp = kLatestTimestamp);

    const std::string& GetStartRowKey() const;

    const std::string& GetEndRowKey() const;

    const std::string& GetStartColumnFamily() const;

    const std::string& GetStartQualifier() const;

    int64_t GetStartTimeStamp() const;

    int32_t GetMaxVersion() const;

    int64_t GetPackInterval() const;

    uint64_t GetSnapshot() const;

    int64_t GetBufferSize() const;

    int64_t GetNumberLimit();

public:
    struct ColumnFamily {
        std::string family_name;
        std::vector<std::string> qualifier_list;
    };

private:
    std::string start_key_;
    std::string end_key_;
    std::string start_column_family_;
    std::string start_qualifier_;
    int64_t start_timestamp_;
    std::vector<ColumnFamily*> cf_list_;
    xsheet::TimeRange* timer_range_;
    int64_t buf_size_;
    int64_t number_limit_;
    bool is_async_;
    int32_t max_version_;
    int64_t pack_interval_;
    uint64_t snapshot_;
//     std::string filter_string_;
//     FilterList filter_list_;
//     ValueConverter value_converter_;
//     TableSchema table_schema_;
};

} // namespace xsheet

#endif  // XSHEET_SDK_SCAN_IMPL_H
