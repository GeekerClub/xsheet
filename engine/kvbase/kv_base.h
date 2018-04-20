// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_KVBASE_KV_BASE_H
#define XSHEET_ENGINE_KVBASE_KV_BASE_H

#include "toft/base/string/string_piece.h"
#include "toft/base/class_registry.h"
#include "toft/base/uncopyable.h"

#include "engine/kvbase/base_options.h"
#include "proto/status_code.pb.h"

namespace xsheet {

class KvIterator {
public:
    virtual ~KvIterator();

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const toft::StringPiece& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;

    virtual toft::StringPiece Key() const = 0;
    virtual toft::StringPiece Value() const = 0;

    virtual StatusCode Status() const = 0;
};

class KvBase {
public:
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

    virtual kvBase* Open(const std::string& db_path, const BaseOptions& options) = 0;
    virtual bool Exists(const std::string& db_path) = 0;
    virtual bool Delete(const std::string& db_path) = 0;
    virtual int64_t GetSize(const std::string& db_path) = 0;
};

TOFT_CLASS_REGISTRY_DEFINE_SINGLETON(base_system, BaseSystem);

} // namespace xsheet

#define TOFT_REGISTER_BASE_SYSTEM(prefix, class_name) \
    TOFT_CLASS_REGISTRY_REGISTER_CLASS_SINGLETON( \
        base_system, BaseSystem, prefix, class_name)

// Get BaseSystem singleton from prefix.
#define TOFT_GET_BASE_SYSTEM(prefix) \
    TOFT_CLASS_REGISTRY_GET_SINGLETON(base_system, prefix)

// Count of registed database systems.
#define TOFT_BASE_SYSTEM_COUNT() TOFT_CLASS_REGISTRY_CLASS_COUNT(base_system)

// Get database systems name by index
#define TOFT_BASE_SYSTEM_NAME(i) TOFT_CLASS_REGISTRY_CLASS_NAME(base_system, i)

#endif // XSHEET_ENGINE_KVBASE_KV_BASE_H
