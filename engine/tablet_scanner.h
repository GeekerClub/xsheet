// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_SCANNER_H
#define XSHEET_ENGINE_TABLET_SCANNER_H

#include <limits>
#include <set>

#include "engine/codec/raw_key.h"
#include "engine/kvbase/kv_base.h"
#include "engine/tablet_schema.pb.h"
#include "engine/types.h"
#include "proto/status_code.pb.h"
#include "proto/tablet.pb.h"

namespace xsheet {

class DropChecker;

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

    std::string start_user_key;
    std::string end_user_key;
    ScanOptions scan_options;
    KvIterator* it;
    DropChecker* drop_checker;
    uint32_t version_num;
    std::string prev_key;
    std::string prev_col;
    std::string prev_qual;

    // use for reture
    StatusCode ret_code;
    bool completed;
    RowResult* results;

    ScanContext()
        : it(NULL), ret_code(kTabletOk), completed(false), results(NULL) {}
};

struct ScanStats {
    uint32_t read_row_count;
    uint32_t read_bytes;

    ScanStats() { Reset(); }
    void Reset() { read_row_count = 0; read_bytes = 0; }
};

class TabletScanner {
public:
    TabletScanner(const TabletSchema& tablet_schema, KvBase* kvbase);
    ~TabletScanner();

    StatusCode Scan(const ScanOptions& scan_options,
                    ScanContext* scan_context,
                    ScanStats* scan_stats);

    TabletSchema GetTabletSchema() { return  tablet_schema_; }

private:
    StatusCode ScanImpl(const ScanOptions& scan_options,
                        ScanContext* scan_context,
                        ScanStats* scan_stats);
    bool FillBufferAndCheckLimit(const KeyValuePair& record,
                                 const ScanOptions& scan_options,
                                 RowResult* results_list,
                                 uint32_t *in_buffer_size);
    void PrepareContextForNextScan(const KeyValuePair& record,
                                   ScanContext* scan_context);

private:
   TabletSchema tablet_schema_;
   KvBase* kvbase_;
   const RawKeyOperator* key_operator_;
};

} // namespace xsheet

#endif  // XSHEET_ENGINE_TABLET_SCANNER_H
