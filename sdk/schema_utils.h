// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef XSHEET_SDK_SCHEMA_UTILS_H
#define XSHEET_SDK_SCHEMA_UTILS_H


#include "engine/tablet_schema.pb.h"
#include "sdk/prop_tree.h"
#include "xsheet/error_code.h"

namespace xsheet {

void ShowTableSchema(const TabletSchema& schema, bool is_x = false);


bool ParseTableSchemaFile(const std::string& file, TabletSchema* tablet_schema);

} // namespace xsheet

#endif // XSHEET_SDK_SCHEMA_UTILS_H

