// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_H
#define XSHEET_ENGINE_TABLET_H

#include <vector>

#include "toft/base/scoped_ptr.h"

// #include "proto/tablet.pb.h"
#include "proto/status_code.pb.h"
#include "engine/tablet_schema.pb.h"
#include "engine/kvbase/kv_base.h"

#include "engine/tablet_writer.h"
#include "engine/tablet_scanner.h"

namespace xsheet {

// class TabletWriter;
// class TabletScanner;

class Tablet {
public:
    Tablet(const std::string& db_path, const TabletSchema& schema);
    ~Tablet();

    StatusCode Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                     std::vector<StatusCode>* status_vec,
                     TabletWriter::WriteCallback callback);

    StatusCode Read();
    StatusCode Scan();

    StatusCode Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const std::string& value);
    StatusCode Get(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, std::string* value);


private:
    std::string db_path_;
    TabletSchema schema_;
    toft::scoped_ptr<KvBase> kvbase_;

    toft::scoped_ptr<TabletWriter> writer_;
    toft::scoped_ptr<TabletScanner> scanner_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_TABLET_H
