// Copyright (C) 2018, For authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_KVBASE_BASE_SYSTEM_H
#define XSHEET_ENGINE_KVBASE_BASE_SYSTEM_H

#include "toft/base/string/string_piece.h"

#include "engine/kvbase/base_options.h"
#include "proto/status_code.pb.h"

namespace xsheet {

class KvIterator {
public:
    Iterator();
    virtual ~Iterator();

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const toft::StringPiece& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;

    virtual toft::StringPiece key() const = 0;
    virtual toft::StringPiece value() const = 0;

    virtual StatusCode status() const = 0;
};

class KvBase {
public:
    KvBase() {}
    virtual ~KvBase() {}

    virtual StatusCode Put(const WriteOptions& options,
                           const toft::StringPiece& key, const toft::StringPiece& value) = 0;
    virtual StatusCode Get(const ReadOptions& options,
                           const toft::StringPiece& key, std::string* value) = 0;

    virtual KvIterator* NewIterator(const ReadOptions& options) = 0;

    virtual StatusCode Write(const WriteOptions& options, WriteBatch* updates) = 0;
    virtual StatusCode Delete(const WriteOptions& options, const toft::StringPiece& key) = 0;

};

class BaseSystem {
public:
    BaseSystem() {}
    ~BaseSystem() {}

};

} // namespace xsheet

#endif // XSHEET_ENGINE_KVBASE_BASE_SYSTEM_H
