// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet_scanner.h"

#include "toft/system/time/timestamp.h"

#include "engine/drop_checker.h"
#include "engine/tablet_utils.h"

namespace xsheet {


TabletScanner::TabletScanner(const TabletSchema& tablet_schema, KvBase* kvbase)
    : tablet_schema_(tablet_schema), kvbase_(kvbase),
      key_operator_(utils::GetRawKeyOperatorFromSchema(tablet_schema_)) {}

TabletScanner::~TabletScanner() {}

StatusCode TabletScanner::Scan(const ScanOptions& scan_options,
                               ScanContext* scan_context,
                               ScanStats* scan_stats) {
    DropChecker* drop_checker = new DropChecker(tablet_schema_, scan_options,
                                                scan_context->end_user_key,
                                                key_operator_);
    scan_context->drop_checker = drop_checker;
    return ScanImpl(scan_options, scan_context, scan_stats);
}

StatusCode TabletScanner::ScanImpl(const ScanOptions& scan_options,
                                   ScanContext* scan_context,
                                   ScanStats* scan_stats) {
    DropChecker* drop_checker = scan_context->drop_checker;
    RowResult* results_list = scan_context->results;
    results_list->clear_key_values();

    uint32_t in_buffer_size = 0;

    int64_t now_time = toft::GetTimestampInMs();
    int64_t time_out = now_time + scan_options.timeout;
    KeyValuePair next_start_kv_pair;

    scan_stats->read_row_count = 0;
    scan_stats->read_bytes = 0;
    scan_context->completed = false;

    for (KvIterator* it = scan_context->it; it->Valid();) {
        scan_stats->read_bytes += (it->Key().size() + it->Value().size());
        scan_stats->read_row_count++;

        toft::StringPiece raw_key = it->Key();
        toft::StringPiece value = it->Value();

        KeyValuePair row_record;
        if (!drop_checker->DropCheck(raw_key, &row_record)) {
            it->Next();
        }

        if (drop_checker->IsCompleted()) {
            scan_context->completed = true;
            break;
        }

        if (drop_checker->IsDrop()) {
            it->Next();
            continue;
        }

        row_record.set_value(value.data(), value.size());
        if (!FillBufferAndCheckLimit(row_record, scan_options,
                                     results_list, &in_buffer_size)) {
            it->Next();
            PrepareContextForNextScan(row_record, scan_context);
            break;
        }
    }

    if (!scan_context->it->Valid()) {
        scan_context->completed = true;
    }

    return kTabletOk;
}

bool TabletScanner::FillBufferAndCheckLimit(const KeyValuePair& record,
                                            const ScanOptions& scan_options,
                                            RowResult* results_list,
                                            uint32_t *in_buffer_size) {
    uint32_t record_size = record.key().size() + record.value().size()
        + record.column_family().size() + record.qualifier().size()
        + sizeof(record.timestamp());
    if (*in_buffer_size + record_size > scan_options.max_size) {
        LOG(INFO) << "buffer-fill reaches the size limit";
        return false;
    } else if (results_list->key_values_size() + 1 > scan_options.number_limit) {
        LOG(INFO) << "buffer-fill reaches the number limit";
        return false;
    }
    results_list->add_key_values()->CopyFrom(record);
    *in_buffer_size += record_size;
    return true;
}

void TabletScanner::PrepareContextForNextScan(const KeyValuePair& record,
                                              ScanContext* scan_context) {
    scan_context->prev_key = record.key();
    scan_context->prev_col = record.column_family();
    scan_context->prev_qual = record.qualifier();
}

} // namespace xsheet
