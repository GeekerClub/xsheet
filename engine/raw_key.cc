// Copyright (C) 2018, For authors
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/raw_key.h"

#include <pthread.h>

#include "engine/key_coding.h"
#include "engine/string_utils.h"
#include "engine/raw_key_operator.h"

namespace xsheet {

bool RawKey::IsTypeAllowUserSetTimestamp(RawKeyType type) {
    bool is_allow = false;
    switch (type) {
    case TKT_DEL:
    case TKT_DEL_COLUMN:
    case TKT_DEL_QUALIFIERS:
    case TKT_DEL_QUALIFIER:
    case TKT_VALUE:
        is_allow = true;
        break;
    default:
        break;
    }
    return is_allow;
}

RawKey::RawKey(const RawKeyOperator* op)
    : operator_(op),
      timestamp_(-1),
      type_(TKT_FORSEEK),
      is_empty_(true) {
}

RawKey::RawKey(const RawKey& tk) {
    *this = tk;
    operator_->ExtractRawKey(raw_key_, &key_, &column_,
                              &qualifier_, &timestamp_, &type_);
}

RawKey::~RawKey() {}

bool RawKey::Encode(const std::string& key, const std::string& column,
                     const std::string& qualifier, int64_t timestamp,
                     RawKeyType type) {
    is_empty_ = false;
    operator_->EncodeRawKey(key, column, qualifier, timestamp, type, &raw_key_);
    return operator_->ExtractRawKey(raw_key_, &key_, &column_,
                                     &qualifier_, &timestamp_, &type_);
}

bool RawKey::Decode(const toft::StringPiece& raw_key) {
    raw_key_ = raw_key.as_string();
    bool res =
        operator_->ExtractRawKey(raw_key_, &key_, &column_, &qualifier_, &timestamp_, &type_);
    if (res) {
        is_empty_ = false;
        return true;
    } else {
        return false;
    }
}

bool RawKey::SameRow(const RawKey& tk) {
    return (key_.compare(tk.key()) == 0);
}

bool RawKey::SameColumn(const RawKey& tk) {
    return (key_.compare(tk.key()) == 0
            && column_.compare(tk.column()) == 0);
}

bool RawKey::SameQualifier(const RawKey& tk) {
    return (key_.compare(tk.key()) == 0
            && column_.compare(tk.column()) == 0
            && qualifier_.compare(tk.qualifier()) == 0);
}

bool RawKey::IsDel() {
    switch (type_) {
    case TKT_DEL:
    case TKT_DEL_COLUMN:
    case TKT_DEL_QUALIFIERS:
    case TKT_DEL_QUALIFIER:
        return true;
    default:
        return false;
    }
}

int RawKey::Compare(const RawKey& tk) {
    int res = key_.compare(tk.key());
    if (res != 0) {
        return res;
    }
    res = column_.compare(tk.column());
    if (res != 0) {
        return res;
    }
    res = qualifier_.compare(tk.qualifier());
    if (res != 0) {
        return res;
    }
    if (timestamp_ != tk.timestamp()) {
        return timestamp_ > tk.timestamp() ? 1 : -1;
    }
    if (type_ != tk.type()) {
        return type_ > tk.type() ? 1 : -1;
    } else {
        return 0;
    }
}

std::string RawKey::DebugString() {
    std::string r;
    r.append(EscapeString(key_) + " : ");
    r.append(EscapeString(column_) + " : ");
    r.append(EscapeString(qualifier_) + " : ");
    AppendNumberTo(&r, timestamp_);
    r.append(" : ");
    AppendNumberTo(&r, type_);
    return r;
}
} // namespace xsheet
