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

    bool Put(const std::string& db_name, const TabletSchema& schema);
    bool Get(const std::string& db_name, TabletSchema* schema);
    bool Delete(const std::string& db_name);

    bool Get(std::vector<TabletSchema>* schema_list, int32_t offset = 0,
             int32_t payload_num = 100);

private:
    std::string db_path_;
    BaseOptions options_;

    toft::scoped_ptr<KvBase> kvbase_;
};

} // namespace xsheet


#endif // XSHEET_SDK_META_BASE_H
