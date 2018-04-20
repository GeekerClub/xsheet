// Copyright (C) 2018, For authors
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_CODEC_RAW_KEY_H
#define XSHEET_ENGINE_CODEC_RAW_KEY_H

#include <stdint.h>

#include "toft/base/string/string_piece.h"

namespace xsheet {

enum RawKeyType {
    TKT_FORSEEK        = 0,
    TKT_DEL            = 1,
    TKT_DEL_COLUMN     = 2,
    TKT_DEL_QUALIFIERS = 3,
    TKT_DEL_QUALIFIER  = 4,
    TKT_VALUE          = 5,
    // 6 is reserved, do not use
    TKT_ADD            = 7,
    TKT_PUT_IFABSENT   = 8,
    TKT_APPEND         = 9,
    TKT_ADDINT64       = 10,
    TKT_TYPE_NUM       = 11
};

class RawKeyOperator;

class RawKey {
public:
    static bool IsTypeAllowUserSetTimestamp(RawKeyType type);

    explicit RawKey(const RawKeyOperator* op);
    explicit RawKey(const RawKey& tk);
    ~RawKey();

    bool Encode(const std::string& key, const std::string& column,
                const std::string& qualifier, int64_t timestamp,
                RawKeyType type);
    bool Decode(const toft::StringPiece& raw_key);

    bool SameRow(const RawKey& tk);
    bool SameColumn(const RawKey& tk);
    bool SameQualifier(const RawKey& tk);

    bool IsDel();
    int Compare(const RawKey& tk);
    std::string DebugString();

    bool empty() const { return is_empty_; }
    toft::StringPiece raw_key() const { return raw_key_; }
    toft::StringPiece key() const { return key_; }
    toft::StringPiece column() const { return column_; }
    toft::StringPiece qualifier() const { return qualifier_; }
    int64_t timestamp() const { return timestamp_; }
    RawKeyType type() const { return type_; }

private:
    RawKey();
    const RawKeyOperator* operator_;
    std::string raw_key_;
    toft::StringPiece key_;
    toft::StringPiece column_;
    toft::StringPiece qualifier_;
    int64_t timestamp_;
    RawKeyType type_;
    bool is_empty_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_CODEC_RAW_KEY_H
