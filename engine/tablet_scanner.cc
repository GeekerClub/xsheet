// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet_scanner.h"


namespace xsheet {


TabletScanner::TabletScanner(cosnt TabletSchema& tablet_schema, KvBase* kvbase)
    : tablet_schema_(tablet_schema), kvbase_(kvbase) {}

TabletScanner::~TabletScanner() {}

StatusCode TabletScanner::ScanImpl(const ScanOptions& scan_options,
                                   ScanContext* scan_context,
                                   ScanStats* scan_stats) {
    std::string& last_key = scan_context->last_key;
    std::string& last_col = scan_context->last_col;
    std::string& last_qual = scan_context->last_qual;
    uint32_t& version_num = scan_context->version_num;

    std::list<KeyValuePair> row_buf;
    uint32_t buffer_size = 0;
    int64_t number_limit = 0;
    value_list->clear_key_values();

    int64_t now_time = toft::GetTimestampInMs();
    int64_t time_out = now_time + scan_options.timeout;
    KeyValuePair next_start_kv_pair;

    scan_stats->read_row_count = 0;
    scan_stats->read_bytes = 0;
    scan_context->completed = false;

    for (KvIterator* it = scan_context->it; it->Valid();) {
        scan_stats->read_bytes += (it->Key().size() + it->Value().size());

        toft::StringPiece raw_key = it->key();
        toft::StringPiece value = it->value();
        toft::StringPiece key, col, qual;
        int64_t ts = 0;
        leveldb::RawKeyType type;
        if (!key_operator_->ExtractTeraKey(raw_key, &key, &col, &qual, &ts, &type)) {
            LOG(WARNING) << "invalid tera key: " << DebugString(tera_key.ToString());
            it->Next();
            continue;
        }

        if (scan_context->end_user_key.size()
            && key.compare(scan_context->end_user_key) >= 0) {
            scan_context->completed = true;
            break;
        }

        const std::set<std::string>& cf_set = scan_options.iter_cf_set;
        if (cf_set.size() > 0 &&
            cf_set.find(col.ToString()) == cf_set.end() && type != TKT_DEL) {
            it->Next();
            continue;
        }



    }
}

bool TabletScanner::IsDrop(const KvIterator& it, KeyValuePair* row_record,
                           bool* is_completed) {

}

} // namespace xsheet
