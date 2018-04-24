// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
#define XSHEET_ENGINE_KVBASE_LEVEL_BASE_H

#include "thirdparty/leveldb/db.h"
#include "thirdparty/leveldb/env.h"
#include "toft/base/string/string_piece.h"
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
    LevelBase(leveldb::DB* db, leveldb::Options options, const std::string& db_path);
    virtual ~LevelBase() {}

    virtual StatusCode Put(const WriteOptions& options,
                           const toft::StringPiece& key, const toft::StringPiece& value);
    virtual StatusCode Get(const ReadOptions& options,
                           const toft::StringPiece& key, std::string* value);

    virtual KvIterator* NewIterator(const ReadOptions& options);

    virtual StatusCode Write(const WriteOptions& options, WriteBatch* updates);
    virtual StatusCode Delete(const WriteOptions& options, const toft::StringPiece& key);

private:
    leveldb::DB* db_;
    leveldb::Options* options_;
    std::string db_path_;
};

class LevelSystem : public BaseSystem {
public:
    virtual ~LevelSystem() {}

    virtual LevelBase* Open(const std::string& db_path, const BaseOptions& base_options);
    virtual bool Exists(const std::string& db_path);
    virtual bool Delete(const std::string& db_path);
    virtual int64_t GetSize(const std::string& db_path);

    static LevelSystem* GetRegisteredFileSystem() {
        return static_cast<LevelSystem*>(TOFT_GET_BASE_SYSTEM(Level));
    }

private:
    void SetupOptions(const BaseOptions& base_options, leveldb::Options* ldb_options);

private:
    std::string db_path_;
    BaseOptions base_options_;
    leveldb::Env* ldb_env_;
};


} // namespace xsheet

#endif // XSHEET_ENGINE_KVBASE_LEVEL_BASE_H
