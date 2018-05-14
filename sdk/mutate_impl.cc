// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/mutate_impl.h"

namespace xsheet {

RowMutationImpl::RowMutationImpl() {}

RowMutationImpl::~RowMutationImpl() {}

const std::string& RowMutationImpl::RowKey() {
    return row_key_;
}

void RowMutationImpl::Put(const std::string& value, int32_t ttl) {

}

void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                 const std::string& value, int64_t timestamp) {

}

void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                 const int64_t value, int64_t timestamp) {

}

void RowMutationImpl::Add(const std::string& family, const std::string& qualifier,
                 const int64_t delta) {

}
void RowMutationImpl::PutIfAbsent(const std::string& family, const std::string& qualifier,
                         const std::string& value) {

}

void RowMutationImpl::Append(const std::string& family, const std::string& qualifier,
                    const std::string& value) {

}

void RowMutationImpl::DeleteRow(int64_t timestamp) {

}

void RowMutationImpl::DeleteFamily(const std::string& family, int64_t timestamp) {

}

void RowMutationImpl::DeleteColumns(const std::string& family, const std::string& qualifier,
                           int64_t timestamp) {

}

void RowMutationImpl::DeleteColumn(const std::string& family, const std::string& qualifier,
                          int64_t timestamp) {

}

const ErrorCode& RowMutationImpl::GetError() {
    return error_;
}

void RowMutationImpl::SetCallBack(Callback callback) {

}

RowMutation::Callback RowMutationImpl::GetCallBack() {
    return callback_;
}

void RowMutationImpl::SetContext(void* context) {

}

void* RowMutationImpl::GetContext() {
    return user_context_;
}

void RowMutationImpl::SetTimeOut(int64_t timeout_ms) {

}

int64_t RowMutationImpl::TimeOut() {
    return -1;
}

Transaction* RowMutationImpl::GetTransaction() {
    return NULL;
}

uint32_t RowMutationImpl::MutationNum() {
    return 0;
}

uint32_t RowMutationImpl::Size() {
    return 0;
}

const RowMutation::Mutation& RowMutationImpl::GetMutation(uint32_t index) {
    return mutation_;
}


} // namespace xsheet
