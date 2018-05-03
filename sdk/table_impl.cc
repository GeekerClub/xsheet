// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "sdk/tablet_impl.h"


#include "toft/base/scopted_ptr.h"

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

void TableImpl::MutationCallback(std::vector<const RowMutationSequence*>* row_mutation_vec,
                                 std::vector<StatusCode>* status_vec) {
    LOG(INFO) << "MutationCallback()";
    delete row_mutation_vec;
    delete status_vec;
}

void TableImpl::ApplyMutation(const std::vector<RowMutation*>& row_mutations) {
//     toft::Closure<void>* callback =
//         toft::NewClosure(this, &TableImpl::CommitMutation, &row_mutations);
//     thread_pool_->AddTask(callback);
    CommitMutation(row_mutations);
}

void TableImpl::CommitMutation(const std::vector<RowMutation*>& row_mutations) {
    std::vector<const RowMutationSequence*>* row_mutation_vec
        = new std::vector<const RowMutationSequence*>;
    for (uint32_t i = 0; i < row_mutations.size(); ++i) {
        RowMutation* row_mutation = row_mutations[i];
        RowMutationSequence* mu_seq = new RowMutationSequence;
        mu_seq->set_row_key(row_mutation->RowKey());
        for (uint32_t j = 0; j < row_mutation->MutationNum(); ++j) {
            const RowMutation::Mutation& mu = row_mutation->GetMutation(j);
            Mutation* mutation = mu_seq->add_mutation_sequence();
            SerializeMutation(mu, mutation);
        }
        row_mutation_vec.push_back(mu_seq);
    }
    std::vector<StatusCode>* status_vec = new  std::vector<StatusCode>;
    TabletWriter::WriteCallback write_callback =
        std::bind(TableImpl::MutationCallback, this, std::placeholders::_1, std::placeholders::_2);
    tablet_writer_.Write(row_mutation_vec, status_vec, write_callback);
}

void TableImpl::Put(RowMutation* row_mutation) {
    std::vector<RowMutation*> row_mutations;
    row_mutations.push_back(row_mutation);
    Put(row_mutations);
}

void TableImpl::Put(const std::vector<RowMutation*>& row_mutations) {
    ApplyMuation(row_mutations);
}

bool TableImpl::IsPutFinished() {
    return false;
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const int64_t value,
                    ErrorCode* err) {
    std::string value_str((char*)&value, sizeof(int64_t));
    return Put(row_key, family, qualifier, value_str, err);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Add(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta,
                    ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Add(family, qualifier, delta);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::PutIfAbsent(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const std::string& value,
                            ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->PutIfAbsent(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);

}

bool TableImpl::Append(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Append(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
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
    return Get(row_key, family, qualifier, value, 0, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err) {
    return Get(row_key, family, qualifier, value, 0, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    uint64_t snapshot_id, ErrorCode* err) {
    std::string value_str;
    if (Get(row_key, family, qualifier, &value_str, err, snapshot_id)
        && value_str.size() == sizeof(int64_t)) {
        *value = *(int64_t*)value_str.c_str();
        return true;
    }
    return false;
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    uint64_t snapshot_id, ErrorCode* err) {
    RowReader* row_reader = NewRowReader(row_key);
    row_reader->AddColumn(family, qualifier);
    row_reader->SetSnapshot(snapshot_id);
    Get(row_reader);
    *err = row_reader->GetError();
    if (err->GetType() == ErrorCode::kOK) {
        *value = row_reader->Value();
        delete row_reader;
        return true;
    }
    delete row_reader;
    return false;
}

ResultStream* TableImpl::Scan(const ScanDescriptor& desc, ErrorCode* err) {

}

Transaction* TableImpl::StartRowTransaction(const std::string& row_key) {
    return NULL;
}

void TableImpl::CommitRowTransaction(Transaction* transaction) {

}


} // namespace xsheet
