// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_UTILS_H
#define XSHEET_ENGINE_TABLET_UTILS_H

#include "engine/tablet_schema.pb.h"
#include "codec/raw_key_operator.h"

namespace xsheet {
namespace utils {

const RawKeyOperator* GetRawKeyOperatorFromSchema(TabletSchema& schema);


} // namespace utils
} // namespace xsheet


#endif // XSHEET_ENGINE_TABLET_UTILS_H
