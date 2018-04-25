// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet_utils.h"


namespace xsheet {
namespace utils {


const RawKeyOperator* GetRawKeyOperatorFromSchema(TabletSchema& schema) {
    switch (schema.raw_key_type()) {
        case Binary:
            return BinaryRawKeyOperator();
        case Readable:
            return ReadableRawKeyOperator();
        default:
            return KvRawKeyOperator();
    }
}

} // namespace utils
} // namespace xsheet
