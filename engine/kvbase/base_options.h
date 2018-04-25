// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_KVBASE_BASE_OPTIONS_H
#define XSHEET_ENGINE_KVBASE_BASE_OPTIONS_H

#include <vector>

#include "toft/base/string/string_piece.h"

#include "engine/tablet_schema.pb.h"

namespace xsheet {

struct BaseOptions {

};

struct ReadOptions {

};

struct WriteOptions {

};

struct WriteBatch {
    WriteBatch() {}
    ~WriteBatch() {

    }

    void Put(const toft::StringPiece& key, const toft::StringPiece& value) {
        KeyValuePair kv;
        kv.set_key(key.data(), key.size());
        kv.set_value(value.data(), value.size());
        kv.set_is_del(false);
        key_value_list_.push_back(kv);
    }

    void Delete(const toft::StringPiece& key) {
        KeyValuePair kv;
        kv.set_key(key.data(), key.size());
        kv.set_is_del(true);
        key_value_list_.push_back(kv);
    }

    void Clear() {
        key_value_list_.clear();
    }

    std::vector<KeyValuePair> key_value_list_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_KVBASE_BASE_OPTIONS_H
