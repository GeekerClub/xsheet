// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "sdk/tablet_impl.h"

namespace xsheet {

TableImpl::TableImpl(const TabletSchema& tablet_schema, KvBase* kvbase)
    : tablet_schema_(tablet_schema), kvbase_(kvbase),
      tablet_writer_(tablet_schema_, kvbase_),
      tablet_scanner_(tablet_schema_, kvbase_) {

}

TableImpl::~TableImpl() {}

const std::string TableImpl::GetName() {
    if (tablet_schema_.has_alias()) {
        return tablet_schema_.alias();
    }
    return tablet_schema_.name();
}

RowMutation* TableImpl::NewRowMutation(const std::string& row_key) {

}

void TableImpl::ApplyMutation(const std::vector<RowMutation*>& row_mutations) {

}

void TableImpl::Put(RowMutation* row_mutation) {
    std::vector<RowMutation*> row_mutations;
    row_mutations.push_back(row_mutation);
    ApplyMuation(row_mutations);
}

void TableImpl::Put(const std::vector<RowMutation*>& row_mutations) {

}

bool TableImpl::IsPutFinished() {

}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    ErrorCode* err) {

}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const int64_t value,
                    ErrorCode* err) {

}

bool TableImpl::Add(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta,
                    ErrorCode* err) {

}

bool TableImpl::PutIfAbsent(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const std::string& value,
                            ErrorCode* err) {

}

bool TableImpl::Append(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    ErrorCode* err) {

}


RowReader* TableImpl::NewRowReader(const std::string& row_key) {

}

void TableImpl::Get(RowReader* row_reader) {

}

void TableImpl::Get(const std::vector<RowReader*>& row_readers) {

}

bool TableImpl::IsGetFinished() {

}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                 const std::string& qualifier, std::string* value,
                 ErrorCode* err) {

}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                 const std::string& qualifier, int64_t* value,
                 ErrorCode* err) {

}

ResultStream* TableImpl::Scan(const ScanDescriptor& desc, ErrorCode* err) {

}

Transaction* TableImpl::StartRowTransaction(const std::string& row_key) {
    return NULL;
}

void TableImpl::CommitRowTransaction(Transaction* transaction) {

}


} // namespace xsheet
