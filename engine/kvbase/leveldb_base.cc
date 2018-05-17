// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/kvbase/leveldb_base.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/leveldb/status.h"
#include "thirdparty/leveldb/write_batch.h"

#include "engine/kvbase/kv_base.h"

DECLARE_string(engine_leveldb_env_type);

namespace xsheet {

// Iterator

LevelIterator::LevelIterator(leveldb::Iterator* ldb_iter)
    : ldb_iter_(ldb_iter) {
    CHECK(ldb_iter_);
}

LevelIterator::~LevelIterator() {
    delete ldb_iter_;
}

bool LevelIterator::Valid() const {
    return ldb_iter_->Valid();
}

void LevelIterator::SeekToFirst() {
    ldb_iter_->SeekToFirst();
}

void LevelIterator::SeekToLast() {
    ldb_iter_->SeekToLast();
}

void LevelIterator::Seek(const toft::StringPiece& target) {
    ldb_iter_->Seek(target.as_string());
}

void LevelIterator::Next() {
    ldb_iter_->Next();
}

void LevelIterator::Prev() {
    ldb_iter_->Prev();
}

toft::StringPiece LevelIterator::Key() const {
    toft::StringPiece sp(ldb_iter_->key().data(),
                         ldb_iter_->key().size());
    return sp;
}

toft::StringPiece LevelIterator::Value() const {
    toft::StringPiece sp(ldb_iter_->value().data(),
                         ldb_iter_->value().size());
    return sp;
}

StatusCode LevelIterator::Status() const {

}

// KvBase

LevelBase::LevelBase(leveldb::DB* db, leveldb::Options ldb_options,
                     const BaseOptions& base_options, const std::string& db_path)
    : KvBase(db_path, base_options), db_(db), options_(ldb_options) {
    CHECK(db_);
}

LevelBase::~LevelBase() {
    delete db_;
}

StatusCode LevelBase::Put(const WriteOptions& options,
                          const toft::StringPiece& key, const toft::StringPiece& value) {
    leveldb::Status ldb_status = db_->Put(leveldb::WriteOptions(), key.as_string(), value.as_string());
    StatusCode status_code = kBaseOk;
    if (!ldb_status.ok()) {
        LOG(ERROR) << "fail to put kv pair";
        status_code = kBaseIOError;
    }
    return status_code;
}

StatusCode LevelBase::Get(const ReadOptions& options,
                          const toft::StringPiece& key, std::string* value) {
    leveldb::Status ldb_status = db_->Get(leveldb::ReadOptions(), key.as_string(), value);
    StatusCode status_code = kBaseOk;
    if (!ldb_status.ok()) {
        LOG(ERROR) << "fail to get kv pair";
        status_code = kBaseIOError;
    }
    return status_code;
}


KvIterator* LevelBase::NewIterator(const ReadOptions& options) {
    leveldb::Iterator* ldb_iter = db_->NewIterator(leveldb::ReadOptions());
    CHECK(ldb_iter);
    return new LevelIterator(ldb_iter);
}

StatusCode LevelBase::Write(const WriteOptions& options, WriteBatch* updates) {
    leveldb::WriteBatch ldb_updates;
    SetupBatchUpdates(updates, &ldb_updates);
    leveldb::Status ldb_status = db_->Write(leveldb::WriteOptions(), &ldb_updates);
    StatusCode status_code = kBaseOk;
    if (!ldb_status.ok()) {
        LOG(ERROR) << "fail to put kv pair";
        status_code = kBaseIOError;
    }
    return status_code;
}

StatusCode LevelBase::Delete(const WriteOptions& options, const toft::StringPiece& key) {
    return kBaseNotSupported;
}

void LevelBase::SetupOptions(const WriteOptions& x, leveldb::WriteOptions* l) {

}

void LevelBase::SetupOptions(const ReadOptions& x, leveldb::ReadOptions* l) {

}

void LevelBase::SetupBatchUpdates(WriteBatch* updates, leveldb::WriteBatch* ldb_updates) {
    std::vector<KeyValuePair>::iterator it = updates->key_value_list_.begin();
    for (; it != updates->key_value_list_.end(); ++it) {
        if (it->del()) {
            ldb_updates->Delete(it->key());
        } else {
            ldb_updates->Put(it->key(), it->value());
        }
    }
}

// BaseSystem


LevelBaseSystem::LevelBaseSystem()
    : ldb_env_(NULL) {}


LevelBaseSystem::~LevelBaseSystem() {}

LevelBase* LevelBaseSystem::Open(const std::string& db_path, const BaseOptions& base_options) {
    db_path_ = db_path;
    base_options_ = base_options;

    leveldb::DB* ldb = NULL;
    leveldb::Options ldb_options;
    ldb_options.create_if_missing = true;
    SetupOptions(base_options, &ldb_options);
    leveldb::Status ldb_status = leveldb::DB::Open(ldb_options, db_path, &ldb);
    if (!ldb_status.ok()) {
        LOG(FATAL) << "fail to create leveldb on: " << db_path_;
        return NULL;
    }

    return new LevelBase(ldb, ldb_options, base_options_, db_path_);
}

bool LevelBaseSystem::Exists(const std::string& db_path) {
    return ldb_env_->FileExists(db_path);
}

bool LevelBaseSystem::Delete(const std::string& db_path) {
    return false;
}

int64_t LevelBaseSystem::GetSize(const std::string& db_path) {
    return false;
}

void LevelBaseSystem::SetupOptions(const BaseOptions& base_options, leveldb::Options* ldb_options) {

    if (FLAGS_engine_leveldb_env_type == "local") {
        ldb_env_ = leveldb::Env::Default();
    } else {
        ldb_env_ = leveldb::Env::Default();
    }
    CHECK(ldb_env_) << ", leveldb env pointer should not be null";
    ldb_options->env = ldb_env_;
}

const char* LevelBaseSystem::Level = "leveldb";

TOFT_REGISTER_BASE_SYSTEM("leveldb", LevelBaseSystem);

} // namespace xsheet
