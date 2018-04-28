// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/drop_checker.h"

#include "thirdparty/glog/logging.h"
#include "toft/system/time/timestamp.h"

#include "engine/codec/string_utils.h"

namespace xsheet {

DropChecker::DropChecker(const TabletSchema& tablet_schema,
                         const ScanOptions& options, const std::string& end_user_key,
                         const RawKeyOperator* key_operator)
    : tablet_schema_(tablet_schema), scan_options_(options), end_user_key_(end_user_key),
      key_operator_(key_operator), is_drop_(true), is_completed_(false),
      prev_ts_(-1), prev_type_(TKT_FORSEEK), cur_type_(TKT_FORSEEK),
      del_row_ts_(-1), del_col_ts_(-1), del_qual_ts_(-1), cur_ts_(-1),
      del_row_seq_(0), del_col_seq_(0), del_qual_seq_(0), version_num_(0) {

    for (int32_t i = 0; i < tablet_schema_.column_families_size(); ++i) {
        const std::string name = tablet_schema_.column_families(i).name();
        cf_indexs_[name] = i;
    }
    has_put_ = false;
}

DropChecker::~DropChecker() {}

bool DropChecker::DropCheck(toft::StringPiece raw_key, KeyValuePair* row_record) {
    toft::StringPiece key, col, qual;
    int64_t ts = -1;
    RawKeyType type;

    if (!key_operator_->ExtractRawKey(raw_key, &key, &col, &qual, &ts, &type)) {
        LOG(WARNING) << "invalid tera key: " << DebugString(raw_key.as_string());
        return false;
    }

    // check whether key arrives boudary
    if (end_user_key_.size() && key.compare(end_user_key_) >= 0) {
        LOG(INFO) << "scan is completed";
        is_completed_ = true;
        return true;
    }

    if (type != TKT_DEL && IsUnselectedColumnFamily(col.as_string())) {
        LOG(INFO) << "drop record for unselected column-family";
        return true;
    }

    cur_type_ = type;
    cur_ts_ = ts;
    int32_t cf_id = -1;
    // check whether is illegal column family
    if (type != TKT_DEL && IsInvalidColumnFamily(col.as_string(), &cf_id)) {
        LOG(INFO) << "drop record for invalid column-family";
        return true;
    }

    // check whether is out of timelife
    if (type >= TKT_VALUE && IsOutofLifeTime(cf_id, ts)) {
        LOG(INFO) << "drop record for out of timelift";
        return true;
    }

    if (key.compare(prev_key_) != 0) {
        // reach a new row
        prev_key_ = key.as_string();
        prev_col_ = col.as_string();
        prev_qual_ = qual.as_string();
        prev_type_ = type;
        version_num_ = 0;
        del_row_ts_ = del_col_ts_ = del_qual_ts_ = -1;
        has_put_ = false;

        // no break in switch: need to set multiple variables
        switch (type) {
            case TKT_DEL:
                del_row_ts_ = ts;
            case TKT_DEL_COLUMN:
                del_col_ts_ = ts;
            case TKT_DEL_QUALIFIERS:
                del_qual_ts_ = ts;
            default:;
        }
    } else if (del_row_ts_ >= ts) {
        // skip deleted row and the same row_del mark
        return true;
    } else if (col.compare(prev_col_) != 0) {
        // reach a new column family
        prev_col_ = col.as_string();
        prev_qual_ = qual.as_string();
        prev_type_ = type;
        version_num_ = 0;
        del_col_ts_ = del_qual_ts_ = -1;
        has_put_ = false;
        // set both variables when type is TKT_DEL_COLUMN
        switch (type) {
            case TKT_DEL_COLUMN:
                del_col_ts_ = ts;
            case TKT_DEL_QUALIFIERS:
                del_qual_ts_ = ts;
            default:;
        }
    } else if (del_col_ts_ > ts) {
        // skip deleted column family
        return true;
    } else if (qual.compare(prev_qual_) != 0) {
        // reach a new qualifier
        prev_qual_ = qual.as_string();
        prev_type_ = type;
        version_num_ = 0;
        del_qual_ts_ = -1;
        has_put_ = false;
        if (type == TKT_DEL_QUALIFIERS) {
            del_qual_ts_ = ts;
        }
    } else if (del_qual_ts_ > ts) {
        // skip deleted qualifier
        return true;
    } else if (type == TKT_DEL_QUALIFIERS) {
        // reach a delete-all-qualifier mark
        del_qual_ts_ = ts;
    } else if (prev_type_ == TKT_DEL_QUALIFIER) {
        // skip latest deleted version
        prev_type_ = type;
        if (type == TKT_VALUE) {
            version_num_++;
        }
        return true;
    } else {
        prev_type_ = type;
    }

    if (type != TKT_VALUE && !IsCommonOP(type)) {
        return true;
    }

    if (type == TKT_VALUE) {
        has_put_ = true;
    }

    if (IsCommonOP(type) && has_put_) {
        return true;
    }

    if (type == TKT_VALUE) {
        if (cur_ts_ == prev_ts_ && prev_qual_ == qual.as_string() &&
            prev_col_ == col.as_string() && prev_key_ == key.as_string()) {
            // this is the same key, do not chang version num
        } else {
            version_num_++;
        }
        if (version_num_ >
            static_cast<uint32_t>(tablet_schema_.column_families(cf_id).max_versions())) {
            // drop out-of-range version
            VLOG(20) << "drop invalid record: " << key.as_string()
                << ", version " << version_num_
                << ", timestamp " << ts;
            return true;
        } else if (version_num_ > scan_options_.max_versions) {
            VLOG(20) << "drop unselected record: " << key.as_string()
                << ", version " << version_num_ << ", timestamp " << ts;
            return true;
        }
    }

    row_record->set_key(key.data(), key.size());
    row_record->set_column_family(col.data(), col.size());
    row_record->set_qualifier(qual.data(), qual.size());
    row_record->set_timestamp(ts);
    is_drop_ = false;

    return true;
}

bool DropChecker::IsUnselectedColumnFamily(const std::string& column_family) const {
    const std::set<std::string>& cf_set = scan_options_.iter_cf_set;
    if (cf_set.size() > 0 && cf_set.find(column_family) == cf_set.end()) {
        return true;
    }
    return false;
}

bool DropChecker::IsInvalidColumnFamily(const std::string& column_family,
                                          int32_t* cf_idx) const {
    std::map<std::string, int32_t>::const_iterator it =
        cf_indexs_.find(column_family);
    if (it == cf_indexs_.end()) {
        return true;
    }
    if (cf_idx) {
        *cf_idx = it->second;
    }
    return false;
}

bool DropChecker::IsOutofLifeTime(int32_t cf_idx, int64_t timestamp) const {
    int64_t ttl = tablet_schema_.column_families(cf_idx).time_to_live() * 1000000LL;
    if (ttl <= 0) {
        // do not drop
        return false;
    }
    int64_t cur_time = toft::GetTimestampInMs();
    if (timestamp + ttl > cur_time) {
        return false;
    } else {
        return true;
    }
}

bool DropChecker::IsCommonOP(RawKeyType keyType) {
    if (keyType == TKT_ADD ||
        keyType == TKT_ADDINT64 ||
        keyType == TKT_PUT_IFABSENT ||
        keyType == TKT_APPEND) {
        return true;
    }
    return false;
}


} // namespace xsheet
