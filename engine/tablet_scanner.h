// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_SCANNER_H
#define XSHEET_ENGINE_TABLET_SCANNER_H

#include <limits>

#include "engine/kvbase/kv_base.h"
#include "engine/types.h"
#include "proto/status_code.pb.h"
#include "proto/tablet.pb.h"

namespace xsheet {

typedef std::map< std::string, std::set<std::string> > ColumnFamilyMap;

struct ScanOptions {
    uint32_t max_versions;
    uint32_t max_size;
    int64_t number_limit; // kv number > number_limit, return to user
    int64_t timestamp_start;
    int64_t timestamp_end;
    uint64_t snapshot_id;
    ColumnFamilyMap column_family_list;
    std::set<std::string> iter_cf_set;
    int64_t timeout;

    ScanOptions()
            : max_versions(std::numeric_limits<uint32_t>::max()),
              max_size(std::numeric_limits<uint32_t>::max()),
              number_limit(std::numeric_limits<int64_t>::max()),
              timestamp_start(kOldestTimestamp), timestamp_end(kLatestTimestamp),
              snapshot_id(0), timeout(std::numeric_limits<int64_t>::max() / 2)
    {}
};

struct ScanContext {
    int64_t session_id;

    std::string start_raw_key;
    std::string end_user_key;
    ScanOptions scan_options;
    KvIterator* it; // init to NULL
//     leveldb::CompactStrategy* compact_strategy;
    uint32_t version_num;
    std::string last_key;
    std::string last_col;
    std::string last_qual;

    // use for reture
    StatusCode ret_code; // set by lowlevelscan
    bool completed; // test this flag know whether scan finish or not
    RowResult* results; // scan result for one round
};

struct ScanStats {
    uint32_t read_row_count;
    uint32_t read_bytes;
};

class TabletScanner {
public:
    TabletScanner(cosnt TabletSchema& tablet_schema, KvBase* kvbase);
    ~TabletScanner();

private:
    StatusCode ScanImpl(const ScanOptions& scan_options,
                        ScanContext* scan_context,
                        ScanStats* scan_stats);

private:
   TabletSchema tablet_schema_;
   KvBase* kvbase_;
};

} // namespace xsheet

#endif  // XSHEET_ENGINE_TABLET_SCANNER_H
