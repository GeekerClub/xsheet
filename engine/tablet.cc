// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet.h"

#include "toft/storage/path/path.h"
#include "thirdparty/glog/logging.h"

#include "engine/kvbase/base_options.h"

namespace xsheet {


Tablet::Tablet(const std::string& db_path, const TabletSchema& schema)
    : db_path_(db_path), schema_(schema) {
    CHECK(toft::Path::GetBaseName(db_path) == schema_.name())
        << ", db name '" << schema_.name() << "' not same as path:"
        << db_path_;
    kvbase_.reset(KvBase::Open(db_path_, BaseOptions()));
    writer_.reset(new TabletWriter(schema_, kvbase_.get()));
    scanner_.reset(new TabletScanner(schema_, kvbase_.get()));

    writer_->Start();
}

Tablet::~Tablet() {
    writer_->Stop();
}

StatusCode Tablet::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                 std::vector<StatusCode>* status_vec,
                 TabletWriter::WriteCallback callback) {
    return writer_->Write(row_mutation_vec, status_vec, callback);
}

StatusCode Tablet::Read() {
    return kTabletOk;

}

StatusCode Tablet::Scan() {
    return kTabletOk;

}

StatusCode Tablet::Put(const std::string& row_key, const std::string& family,
               const std::string& qualifier, const std::string& value) {
    RowMutationSequence* mu_seq = new RowMutationSequence;
    mu_seq->set_row_key(row_key);
    Mutation* mutation = mu_seq->add_mutation_list();
    mutation->set_type(kPut);
    mutation->set_family(family);
    mutation->set_qualifier(qualifier);
    mutation->set_value(value);


    std::vector<const RowMutationSequence*> row_mutation_vec;
    row_mutation_vec.push_back(mu_seq);
    std::vector<StatusCode> status_vec;
    status_vec.push_back(kTabletOk);

    StatusCode ret = Write(&row_mutation_vec, &status_vec,
                           std::bind(&Tablet::PutCallback, this,
                                     std::placeholders::_1, std::placeholders::_2));

    put_event_.Wait();
    return ret == kTabletOk?status_vec[0]:ret;
}

StatusCode Tablet::Get(const std::string& row_key, const std::string& family,
               const std::string& qualifier, std::string* value) {
    ScanOptions scan_options;
    scan_options.column_family_list[family].insert(qualifier);

    ScanContext scan_context;
    scan_context.start_user_key = row_key;

    ScanStats scan_stats;

    StatusCode status = scanner_->Scan(scan_options, &scan_context, &scan_stats);
    if (status != kTabletOk) {
        LOG(ERROR) << "fail to scan: " << row_key;
        return status;
    }
    CHECK(scan_context.results);
    const KeyValuePair& pair = scan_context.results->key_values(0);
    *value = std::string(pair.value().data(), pair.value().size());
    return kTabletOk;

}

void Tablet::PutCallback(std::vector<const RowMutationSequence*>* row_mutation_vec,
                         std::vector<StatusCode>* status_vec) {
    LOG(INFO) << "PutCallback()";
    delete (*row_mutation_vec)[0];
    put_event_.Set();
}

} // namespace xsheet
