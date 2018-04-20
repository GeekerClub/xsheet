// Copyright (C) 2018, For authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CODEC_RAW_KEY_OPERATOR_H
#define XSHEET_ENGINE_CODEC_RAW_KEY_OPERATOR_H

#include <stdint.h>

#include "toft/base/string/string_piece.h"

#include "engine/codec//raw_key.h"


namespace xsheet {

class RawKeyOperator {
public:
    virtual void EncodeRawKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               RawKeyType type,
                               std::string* raw_key) const = 0;

    virtual bool ExtractRawKey(const toft::StringPiece& raw_key,
                                toft::StringPiece* row_key,
                                toft::StringPiece* family,
                                toft::StringPiece* qualifier,
                                int64_t* timestamp,
                                RawKeyType* type) const = 0;
    virtual int Compare(const toft::StringPiece& key1,
                        const toft::StringPiece& key2) const = 0;
    virtual const char* Name() const = 0;
};

const RawKeyOperator* ReadableRawKeyOperator();
const RawKeyOperator* BinaryRawKeyOperator();
const RawKeyOperator* KvRawKeyOperator();

} // namespace xsheet

#endif //XSHEET_ENGINE_CODEC_RAW_KEY_OPERATOR_H
