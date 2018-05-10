// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/read_impl.h"

namespace xsheet {


RowReaderImpl::RowReaderImpl(TabletScanner* tablet_scanner)
    : tablet_scanner_(tablet_scanner) {}

RowReaderImpl::~RowReaderImpl() {}

void RowReaderImpl::AddColumnFamily(const std::string& family) {

}

void RowReaderImpl::AddColumn(const std::string& family, const std::string& qualifier) {

}

void RowReaderImpl:: SetMaxVersions(uint32_t max_version) {

}

void RowReaderImpl::SetTimeRange(int64_t ts_start, int64_t ts_end) {

}

bool RowReaderImpl::Done() {
    return false;
}

void RowReaderImpl::Next() {

}

const std::string& RowReaderImpl::RowKey() {

}

std::string RowReaderImpl::Value() {

}

std::string RowReaderImpl::Family() {

}

std::string RowReaderImpl::Qualifier() {

}

int64_t RowReaderImpl::Timestamp() {

}

ErrorCode RowReaderImpl::GetError() {
    return error_;
}

void RowReaderImpl::SetCallBack(Callback callback) {

}

void RowReaderImpl::SetContext(void* context) {

}

void* RowReaderImpl::GetContext() {
    return NULL;
}

void RowReaderImpl::SetTimeOut(int64_t timeout_ms) {

}



} // namespace xsheet
