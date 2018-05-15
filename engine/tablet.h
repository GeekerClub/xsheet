// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_H
#define XSHEET_ENGINE_TABLET_H


#include "proto/tablet.pb.h"
#include "proto/status_code.pb.h"

namespace xsheet {

class TabletWriter;
class TabletScanner;

class Tablet {
public:
    Tablet(const std::string& db_path);
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
    KvBase kvbase_;
    TabletSchema schema_;

    TabletWriter writer_;
    TabletScanner scanner_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_TABLET_H
