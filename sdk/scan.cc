// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/scan_impl.h"

namespace xsheet {

ScanDescriptor::ScanDescriptor(const std::string& rowkey) {
    impl_ = new ScanDescImpl(rowkey);
}

ScanDescriptor::~ScanDescriptor() {
    delete impl_;
}

void ScanDescriptor::SetEnd(const std::string& rowkey) {
    impl_->SetEnd(rowkey);
}

void ScanDescriptor::AddColumnFamily(const std::string& cf) {
    impl_->AddColumnFamily(cf);
}

void ScanDescriptor::AddColumn(const std::string& cf, const std::string& qualifier) {
    impl_->AddColumn(cf, qualifier);
}

void ScanDescriptor::SetMaxVersions(int32_t versions) {
    impl_->SetMaxVersions(versions);
}

void ScanDescriptor::SetPackInterval(int64_t interval) {
    impl_->SetPackInterval(interval);
}

void ScanDescriptor::SetTimeRange(int64_t ts_end, int64_t ts_start) {
    impl_->SetTimeRange(ts_end, ts_start);
}

// bool ScanDescriptor::SetFilter(const std::string& filter_string) {
//     return impl_->SetFilter(filter_string);
// }

// void ScanDescriptor::SetValueConverter(ValueConverter converter) {
//     impl_->SetValueConverter(converter);
// }

// void ScanDescriptor::SetSnapshot(uint64_t snapshot_id) {
//     return impl_->SetSnapshot(snapshot_id);
// }

void ScanDescriptor::SetBufferSize(int64_t buf_size) {
    impl_->SetBufferSize(buf_size);
}

void ScanDescriptor::SetNumberLimit(int64_t number_limit) {
    impl_->SetNumberLimit(number_limit);
}

int64_t ScanDescriptor::GetNumberLimit() {
    return impl_->GetNumberLimit();
}

// void ScanDescriptor::SetAsync(bool async) {
//     impl_->SetAsync(async);
// }

// bool ScanDescriptor::IsAsync() const {
//     return impl_->IsAsync();
// }

// ScanDescImpl* ScanDescriptor::GetImpl() const {
//     return impl_;
// }

} // namespace xsheet

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
