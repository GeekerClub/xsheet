// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/kvbase/leveldb_base.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/leveldb/status.h"

#include "engine/kvbase/kv_base.h"

DECLARE_string(xsheet_leveldb_env_type);

namespace xsheet {

// Iterator

LevelIterator::LevelIterator() {}

LevelIterator::~LevelIterator() {}

bool LevelIterator::Valid() const {

}

void LevelIterator::eekToFirst() {

}

void LevelIterator::SeekToLast() {

}

void LevelIterator::Seek(const toft::StringPiece& target) {

}

void LevelIterator::Next() {

}

void LevelIterator::Prev() {

}

toft::StringPiece LevelIterator::Key() const {

}

toft::StringPiece LevelIterator::Value() const {

}

StatusCode LevelIterator::Status() const {

}

// KvBase

LevelBase::LevelBase(leveldb::DB* db, leveldb::Options options, const std::string& db_path)
    : db_(db), options_(options), db_path_(db_path_) {}


LevelBase::~LevelBase() {}

StatusCode LevelBase::Put(const WriteOptions& options,
                          const toft::StringPiece& key, const toft::StringPiece& value) {

}

StatusCode LevelBase::Get(const ReadOptions& options,
                          const toft::StringPiece& key, std::string* value) {

}


KvIterator* LevelBase::NewIterator(const ReadOptions& options) {

}

StatusCode LevelBase::Write(const WriteOptions& options, WriteBatch* updates) {

}

StatusCode LevelBase::Delete(const WriteOptions& options, const toft::StringPiece& key) {

}

// BaseSystem

LevelSystem::LevelSystem()
    : ldb_env_(NULL) {}

LevelBase* LevelSystem::Open(const std::string& db_path, const BaseOptions& options) {
    db_path_ = db_path;
    base_options_ = base_options;

    leveldb::DB* ldb = NULL;
    leveldb::Options ldb_options;
    SetupOptions(base_options, &ldb_options);
    leveldb::Status ldb_status = leveldb::DB::Open(ldb_options, db_path, &ldb);
    if (!ldb_status.ok()) {
        LOG(FATAL) << "fail to create leveldb on: " << db_path_;
        return NULL;
    }

    return new LevelBase(ldb, ldb_options, db_path_);
}

bool LevelSystem::Exists(const std::string& db_path) {
    return ldb_env_->FileExists(db_path);
}

bool LevelSystem::Delete(const std::string& db_path) {
    return false;
}

int64_t LevelSystem::GetSize(const std::string& db_path) {
    return false;
}

void LevelSystem::SetupOptions(const BaseOptions& base_options, leveldb::Options* ldb_options) {

    if (FLAGS_xsheet_leveldb_env_type == "local") {
        ldb_env_ = leveldb::Env::Default();
    } else {
        ldb_env_ = leveldb::Env::Default();
    }
    CHEKC(ldb_env_) << ", leveldb env pointer should not be null";
    ldb_options->env = ldb_env_;
}

} // namespace xsheet
