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

bool MetaBase::Delete(const std::string& db_name) {
   StatusCode status = kvbase_->Delete(WriteOptions(), db_name);
   if (status != kBaseOk) {
       LOG(ERROR) << "fail to delete db: " << db_name;
       return false;
   }
   return true;
}

bool MetaBase::Get(std::vector<TabletSchema>* schema_list,
                   int32_t offset, int32_t payload_num) {
    KvIterator* iter = kvbase_->NewIterator(ReadOptions());

    int32_t count = 0;
    for (iter->SeekToFirst();
         iter->Valid() && count < offset + payload_num;
         iter->Next(), ++count) {
        if (count < offset) {
            continue;
        }
        TabletSchema schema;
        if (!schema.ParseFromString(iter->Value().as_string())) {
            LOG(WARNING) << "fail to parse schema string: " << iter->Value();
            continue;
        }
        schema_list->push_back(schema);
    }
    delete iter;
    return true;
}

} // namespace xsheet
