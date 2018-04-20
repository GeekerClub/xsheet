// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
#define XSHEET_ENGINE_KVBASE_LEVEL_BASE_H

#include "toft/base/string/string_piece.h"
#include "toft/base/class_registry.h"
#include "toft/base/uncopyable.h"

#include "engine/kvbase/base_options.h"
#include "proto/status_code.pb.h"

namespace xsheet {

class LevelIterator : public KvIterator {
public:
    virtual ~LevelIterator();

    virtual bool Valid() const;
    virtual void SeekToFirst();
    virtual void SeekToLast();
    virtual void Seek(const toft::StringPiece& target);
    virtual void Next();
    virtual void Prev();

    virtual toft::StringPiece Key() const;
    virtual toft::StringPiece Value() const;

    virtual StatusCode Status() const;
};

class LevelBase : public KvBase {
public:
    virtual ~LevelBase() {}

    virtual StatusCode Put(const WriteOptions& options,
                           const toft::StringPiece& key, const toft::StringPiece& value);
    virtual StatusCode Get(const ReadOptions& options,
                           const toft::StringPiece& key, std::string* value);

    virtual KvIterator* NewIterator(const ReadOptions& options);

    virtual StatusCode Write(const WriteOptions& options, WriteBatch* updates);
    virtual StatusCode Delete(const WriteOptions& options, const toft::StringPiece& key);

};

class LevelSystem {
public:
    LevelSystem() {}
    virtual ~LevelSystem() {}

    virtual LevelBase* Open(const std::string& db_path, const BaseOptions& options);
    virtual bool Exists(const std::string& db_path);
    virtual bool Delete(const std::string& db_path);
    virtual int64_t GetSize(const std::string& db_path);
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

#endif // XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
