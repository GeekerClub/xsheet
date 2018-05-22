// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "sdk/meta_base.h"


#include "thirdparty/glog/logging.h"
#include "thirdparty/gflags/gflags.h"

#include "proto/status_code.pb.h"

DECLARE_string(xsheet_test_dir);
DECLARE_string(xsheet_kvbase_prefix);

namespace xsheet {


MetaBase::MetaBase(const std::string& db_path, const BaseOptions& options)
    : db_path_(db_path), options_(options) {

    kvbase_.reset(KvBase::Open(db_path_, options_));
}

MetaBase::~MetaBase() {}

bool MetaBase::Put(const std::string& db_name, const TabletSchema& schema) {
    std::string schema_str;
    if (!schema.SerializeToString(&schema_str)) {
        LOG(ERROR) << "fail to seralize pb: " << schema.DebugString();
        return false;
    }
    StatusCode status = kvbase_->Put(WriteOptions(), db_name, schema_str);
    if (status != kBaseOk) {
        LOG(ERROR) << "fail to write db info: " << db_name;
        return false;
    }
    return true;
}

bool MetaBase::Get(const std::string& db_name, TabletSchema* schema) {
    std::string value;
    StatusCode status = kvbase_->Get(ReadOptions(), db_name, &value);
    if (status != kBaseOk) {
        LOG(ERROR) << "fail to read db info: " << db_name;
        return false;
    }
    if (!schema->ParseFromString(value)) {
        LOG(ERROR) << "fail to parse pb from string";
        return false;
    }
    return true;
}

} // namespace xsheet
