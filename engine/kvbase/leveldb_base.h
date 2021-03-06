// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
#define XSHEET_ENGINE_KVBASE_LEVEL_BASE_H

#include "thirdparty/leveldb/db.h"
#include "thirdparty/leveldb/env.h"
#include "thirdparty/leveldb/iterator.h"
#include "thirdparty/leveldb/options.h"
#include "thirdparty/leveldb/cache.h"
#include "toft/base/string/string_piece.h"
#include "toft/base/uncopyable.h"

#include "engine/kvbase/kv_base.h"
#include "engine/kvbase/base_options.h"
#include "proto/status_code.pb.h"

namespace xsheet {

class LevelIterator : public KvIterator {
public:
    LevelIterator(leveldb::Iterator* ldb_iter);
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

private:
    leveldb::Iterator* ldb_iter_;
};

class LevelBase : public KvBase {
public:
    LevelBase(leveldb::DB* db, leveldb::Options ldb_options,
              const BaseOptions& base_options, const std::string& db_path);
    virtual ~LevelBase();

    virtual StatusCode Put(const WriteOptions& options,
                           const toft::StringPiece& key, const toft::StringPiece& value);
    virtual StatusCode Get(const ReadOptions& options,
                           const toft::StringPiece& key, std::string* value);

    virtual KvIterator* NewIterator(const ReadOptions& options);

    virtual StatusCode Write(const WriteOptions& options, WriteBatch* updates);
    virtual StatusCode Delete(const WriteOptions& options, const toft::StringPiece& key);

private:
    void SetupOptions(const ReadOptions& x, leveldb::ReadOptions* l);
    void SetupOptions(const WriteOptions& x, leveldb::WriteOptions* l);
    void SetupBatchUpdates(WriteBatch* updates, leveldb::WriteBatch* ldb_updates);

private:
    leveldb::DB* db_;
    leveldb::Options options_;
//     std::string db_path_;
};

class LevelBaseSystem : public BaseSystem {
public:
    LevelBaseSystem();
    virtual ~LevelBaseSystem();

    virtual LevelBase* Open(const std::string& db_path, const BaseOptions& base_options);
    virtual bool Exists(const std::string& db_path);
    virtual bool Delete(const std::string& db_path);
    virtual int64_t GetSize(const std::string& db_path);

    static LevelBaseSystem* GetRegisteredBaseSystem() {
        return static_cast<LevelBaseSystem*>(TOFT_GET_BASE_SYSTEM(Level));
    }

public:
    static const char* Level;

private:
    void SetupOptions(const BaseOptions& base_options, leveldb::Options* ldb_options);
    void SetupCache(const BaseOptions& base_options, leveldb::Options* ldb_options);

private:
    std::string db_path_;
    BaseOptions base_options_;
    leveldb::Env* ldb_env_;
    leveldb::Cache* ldb_cache_;
};


} // namespace xsheet

#endif // XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
