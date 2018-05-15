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

}

Tablet::~Tablet() {}

StatusCode Tablet::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                 std::vector<StatusCode>* status_vec,
                 TabletWriter::WriteCallback callback) {
    return kTabletOk;
}

StatusCode Tablet::Read() {
    return kTabletOk;

}

StatusCode Tablet::Scan() {
    return kTabletOk;

}

StatusCode Tablet::Put(const std::string& row_key, const std::string& family,
               const std::string& qualifier, const std::string& value) {
    return kTabletOk;

}

StatusCode Tablet::Get(const std::string& row_key, const std::string& family,
               const std::string& qualifier, std::string* value) {
    return kTabletOk;

}

} // namespace xsheet
