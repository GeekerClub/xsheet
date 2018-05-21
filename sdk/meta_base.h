// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef XSHEET_SDK_META_BASE_H
#define XSHEET_SDK_META_BASE_H

#include "toft/base/scoped_ptr.h"

#include "engine/kvbase/base_options.h"
#include "engine/kvbase/kv_base.h"
#include "engine/tablet_schema.pb.h"

namespace xsheet {

class MetaBase {
public:
    MetaBase(const std::string& db_path, const BaseOptions& options);
    ~MetaBase();

    bool Put(const std::string& db_path, const TabletSchema& schema);
    bool Get(const std::string& db_path, TabletSchema* schema);

private:
    std::string db_path_;
    BaseOptions options_;

    toft::scoped_ptr<KvBase> kvbase_;
};

} // namespace xsheet


#endif // XSHEET_SDK_META_BASE_H
