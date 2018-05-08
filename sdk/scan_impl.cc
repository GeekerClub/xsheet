// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/scan_impl.h"

#include <functional>

#include "thirdparty/glog/logging.h"

#include "proto/status_code.pb.h"

DECLARE_bool(xsheet_sdk_batch_scan_enabled);
DECLARE_int64(xsheet_sdk_scan_number_limit);
DECLARE_int64(xsheet_sdk_scan_buffer_size);
DECLARE_int32(xsheet_sdk_max_batch_scan_req);
DECLARE_int32(xsheet_sdk_batch_scan_max_retry);
DECLARE_int64(xsheet_sdk_scan_timeout);
DECLARE_int64(batch_scan_delay_retry_in_us);

namespace xsheet {

ResultStreamImpl::ResultStreamImpl(TabletScanner* scanner,
                                   ScanDescImpl* scan_desc_impl)
    : scan_desc_impl_(new ScanDescImpl(*scan_desc_impl)),
      tablet_scanner_(scanner), scan_context_(NULL), scan_stats_(NULL),
      next_idx_(0) {
}

ResultStreamImpl::~ResultStreamImpl() {
    if (scan_desc_impl_ != NULL) {
        delete scan_desc_impl_;
    }
}

ScanDescImpl* ResultStreamImpl::GetScanDesc() {
    return scan_desc_impl_;
}

std::string ResultStreamImpl::GetNextStartPoint(const std::string& str) {
    const static std::string x0("\x0", 1);
    const static std::string x1("\x1");
    RawKey rawkey_type = table_ptr_->GetTableSchema().raw_key();
    return rawkey_type == Readable ? str + x1 : str + x0;
}


bool ResultStreamImpl::LookUp(const std::string& row_key) {
    ScanOptions scan_options;
    FillScanOptions(scan_desc_impl_, &scan_options);

    if (!scan_context_) {
        scan_context_ = new ScanContext;
        scan_stats_ = new ScanStats;
    } else if (scan_context_->session_id != UnInvalidSessionId) {
        LOG(ERROR) << "scan is working for session: " << scan_context_->session_id;
        return false;
    }

    scan_context_->session_id++;
    scan_context_->start_user_key = row_key;
    scan_context_->end_user_key = scan_desc_impl_->GetEndRowKey();
    scan_context_->scan_options = scan_options;

    scan_stats_->Reset();

    StatusCode ret = tablet_scanner_->Scan(scan_options, scan_context_, scan_stats_);
    return ret == kTabletOk;
}

bool ResultStreamImpl::Done(ErrorCode* err) {
    if (err) {
        error->SetFailed(ErrorCode::kOK);
    }
    while (true) {

    }
}

void ResultStreamImpl::Next() {
    next_idx_++;
}

std::string ResultStreamImpl::RowName() const {
    const KeyValuePair& pair = scan_context_->results->key_values(next_idx_);
    return std::string(pair.key().data(), pair.key().size());
}

std::string ResultStreamImpl::Family() const {
    const KeyValuePair& pair = scan_context_->results->key_values(next_idx_);
    return std::string(pair.column_family().data(), pair.column_family().size());
}

std::string ResultStreamImpl::ColumnName() const {
}

std::string ResultStreamImpl::Qualifier() const {

    const KeyValuePair& pair = scan_context_->results->key_values(next_idx_);
    return std::string(pair.qualifier().data(), pair.qualifier().size());
}

int64_t ResultStreamImpl::Timestamp() const {
    return 0;
}

std::string ResultStreamImpl::Value() const {
    const KeyValuePair& pair = scan_context_->results->key_values(next_idx_);
    return std::string(pair.value().data(), pair.value().size());
}

int64_t ResultStreamImpl::ValueInt64() const {
    return 0;
}


void ResultStreamImpl::FillScanOptions(ScanDescImpl* desc_impl,
                                       ScanOptions* scan_options) {
    scan_options->max_versions = desc_impl->GetMaxVersion();
    scan_options->max_size = desc_impl->GetBufferSize();
    scan_options->number_limit = desc_impl->GetNumberLimit();
    scan_options->timestamp_start = desc_impl->GetStartTimeStamp()
    scan_options->snapshot_id; = desc_impl->GetSnapshot();
}

///////////////////////// ScanDescImpl ///////////////////////

ScanDescImpl::ScanDescImpl(const string& rowkey)
    : start_timestamp_(0),
      timer_range_(NULL),
      buf_size_(FLAGS_xsheet_sdk_scan_buffer_size),
      number_limit_(FLAGS_xsheet_sdk_scan_number_limit),
      is_async_(FLAGS_xsheet_sdk_batch_scan_enabled),
      max_version_(1),
      pack_interval_(FLAGS_xsheet_sdk_scan_timeout),
      snapshot_(0) {
    SetStart(rowkey);
}

ScanDescImpl::~ScanDescImpl() {
    if (timer_range_ != NULL) {
        delete timer_range_;
    }
    for (uint32_t i = 0; i < cf_list_.size(); ++i) {
        delete cf_list_[i];
    }
}

void ScanDescImpl::SetStart(const string& row_key, const string& column_family,
                            const string& qualifier, int64_t time_stamp)
{
    start_key_ = row_key;
    start_column_family_ = column_family;
    start_qualifier_ = qualifier;
    start_timestamp_ = time_stamp;
}

void ScanDescImpl::SetEnd(const string& rowkey) {
    end_key_ = rowkey;
}

void ScanDescImpl::AddColumnFamily(const string& cf) {
    AddColumn(cf, "");
}

void ScanDescImpl::AddColumn(const string& cf, const string& qualifier) {
    for (uint32_t i = 0; i < cf_list_.size(); ++i) {
        if (cf_list_[i]->family_name() == cf) {
            if (qualifier != "") {
                cf_list_[i]->add_qualifier_list(qualifier);
            }
            return;
        }
    }
    tera::ColumnFamily* column_family = new tera::ColumnFamily;
    column_family->set_family_name(cf);
    if (qualifier != "") {
        column_family->add_qualifier_list(qualifier);
    }
    cf_list_.push_back(column_family);
}

void ScanDescImpl::SetMaxVersions(int32_t versions) {
    max_version_ = versions;
}

void ScanDescImpl::SetPackInterval(int64_t interval) {
    pack_interval_ = interval;
}

void ScanDescImpl::SetTimeRange(int64_t ts_end, int64_t ts_start) {
    if (timer_range_ == NULL) {
        timer_range_ = new tera::TimeRange;
    }
    timer_range_->set_ts_start(ts_start);
    timer_range_->set_ts_end(ts_end);
}

void ScanDescImpl::SetSnapshot(uint64_t snapshot_id) {
    snapshot_ = snapshot_id;
}

uint64_t ScanDescImpl::GetSnapshot() const {
    return snapshot_;
}

void ScanDescImpl::SetBufferSize(int64_t buf_size) {
    buf_size_ = buf_size;
}

void ScanDescImpl::SetNumberLimit(int64_t number_limit) {
    number_limit_ = number_limit;
}

void ScanDescImpl::SetAsync(bool async) {
    is_async_ = async;
}

const string& ScanDescImpl::GetStartRowKey() const {
    return start_key_;
}

const string& ScanDescImpl::GetEndRowKey() const {
    return end_key_;
}

const string& ScanDescImpl::GetStartColumnFamily() const {
    return start_column_family_;
}

const string& ScanDescImpl::GetStartQualifier() const {
    return start_qualifier_;
}

int64_t ScanDescImpl::GetStartTimeStamp() const {
    return start_timestamp_;
}

int32_t ScanDescImpl::GetSizeofColumnFamilyList() const {
    return cf_list_.size();
}

const tera::ColumnFamily* ScanDescImpl::GetColumnFamily(int32_t num) const {
    if (static_cast<uint64_t>(num) >= cf_list_.size()) {
        return NULL;
    }
    return cf_list_[num];
}

int32_t ScanDescImpl::GetMaxVersion() const {
    return max_version_;
}

int64_t ScanDescImpl::GetPackInterval() const {
    return pack_interval_;
}

const tera::TimeRange* ScanDescImpl::GetTimerRange() const {
    return timer_range_;
}

const string& ScanDescImpl::GetFilterString() const {
    return filter_string_;
}

const FilterList& ScanDescImpl::GetFilterList() const {
    return filter_list_;
}

const ValueConverter ScanDescImpl::GetValueConverter() const {
    return value_converter_;
}

int64_t ScanDescImpl::GetBufferSize() const {
    return buf_size_;
}

int64_t ScanDescImpl::GetNumberLimit() {
    return number_limit_;
}

bool ScanDescImpl::IsAsync() const {
    return is_async_;
}

void ScanDescImpl::SetTableSchema(const TableSchema& schema) {
    table_schema_ = schema;
}

bool ScanDescImpl::IsKvOnlyTable() {
    return IsKvTable(table_schema_);
}

// SELECT * WHERE <type> <cf0> <op0> <value0> AND <type> <cf1> <op1> <value1>
bool ScanDescImpl::SetFilter(const std::string& schema) {
    std::string select;
    std::string where;
    std::string::size_type pos;
    if ((pos = schema.find("SELECT ")) != 0) {
        LOG(ERROR) << "illegal scan expression: should be begin with \"SELECT\"";
        return false;
    }
    if ((pos = schema.find(" WHERE ")) != string::npos) {
        select = schema.substr(7, pos - 7);
        where = schema.substr(pos + 7, schema.size() - pos - 7);
    } else {
        select = schema.substr(7);
    }
    // parse select
    {
        select = RemoveInvisibleChar(select);
        if (select != "*") {
            std::vector<string> cfs;
            SplitString(select, ",", &cfs);
            for (size_t i = 0; i < cfs.size(); ++i) {
                // add columnfamily
                AddColumnFamily(cfs[i]);
                VLOG(10) << "add cf: " << cfs[i] << " to scan descriptor";
            }
        }
    }
    // parse where
    if (where != "") {
        filter_string_ = where;
        if (!ParseFilterString()) {
            return false;
        }
    }
    return true;
}

bool ScanDescImpl::ParseFilterString() {
    const char* and_op = " AND ";
    filter_list_.Clear();
    std::vector<string> filter_v;
    SplitString(filter_string_, and_op, &filter_v);
    for (size_t i = 0; i < filter_v.size(); ++i) {
        Filter filter;
        if (ParseSubFilterString(filter_v[i], &filter)) {
            Filter* pf = filter_list_.add_filter();
            pf->CopyFrom(filter);
        } else {
            LOG(ERROR) << "fail to parse expression: " << filter_v[i];
            return false;
        }
    }

    return true;
}

bool ScanDescImpl::ParseSubFilterString(const string& filter_str,
                                        Filter* filter) {
    string filter_t = RemoveInvisibleChar(filter_str);
    if (filter_t.size() < 3) {
        LOG(ERROR) << "illegal filter expression: " << filter_t;
        return false;
    }
    if (filter_t.find("@") == string::npos) {
        // default filter, value compare filter
        if (!ParseValueCompareFilter(filter_t, filter)) {
            return false;
        }
    } else {
        // TODO: other filter
        LOG(ERROR) << "illegal filter expression: " << filter_t;
        return false;
    }
    return true;
}

bool ScanDescImpl::ParseValueCompareFilter(const string& filter_str,
                                           Filter* filter) {
    if (filter == NULL) {
        LOG(ERROR) << "filter ptr is NULL.";
        return false;
    }

    if (max_version_ != 1) {
        LOG(ERROR) << "only support 1 version scan if there is a value filter: "
            << filter_str;
        return false;
    }
    string::size_type type_pos;
    string::size_type cf_pos;
    if ((type_pos = filter_str.find("int64")) != string::npos) {
        filter->set_value_type(kINT64);
        cf_pos = type_pos + 5;
    } else {
        LOG(ERROR) << "only support int64 value filter, but got: "
            << filter_str;
        return false;
    }

    string cf_name, value;
    string::size_type op_pos;
    BinCompOp comp_op = UNKNOWN;
    if ((op_pos = filter_str.find(">=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = GE;
    } else if ((op_pos = filter_str.find(">")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 1, filter_str.size() - op_pos - 1);
        comp_op = GT;
    } else if ((op_pos = filter_str.find("<=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = LE;
    } else if ((op_pos = filter_str.find("<")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 1, filter_str.size() - op_pos - 1);
        comp_op = LT;
    } else if ((op_pos = filter_str.find("==")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = EQ;
    } else if ((op_pos = filter_str.find("!=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = NE;
    } else {
        LOG(ERROR) << "fail to parse expression: " << filter_str;
        return false;
    }
    string type;
    if (filter->value_type() == kINT64) {
        type = "int64";
    } else {
        assert(false);
    }

    string value_internal;
    if (!value_converter_(value, type, &value_internal)) {
        LOG(ERROR) << "fail to convert value: \""<< value << "\"(" << type << ")";
        return false;
    }

    filter->set_type(BinComp);
    filter->set_bin_comp_op(comp_op);
    filter->set_field(ValueFilter);
    filter->set_content(cf_name);
    filter->set_ref_value(value_internal);
    return true;
}

} // namespace xsheet

