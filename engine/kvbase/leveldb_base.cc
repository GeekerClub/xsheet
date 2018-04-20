// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/kvbase/leveldb_base.h"

#include "engine/kvbase/kv_base.h"

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

LevelBase::LevelBase() {}

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


LevelBase* LevelSystem::Open(const std::string& db_path, const BaseOptions& options) {

}

bool LevelSystem::Exists(const std::string& db_path) {

}

bool LevelSystem::Delete(const std::string& db_path) {

}

int64_t LevelSystem::GetSize(const std::string& db_path) {

}

} // namespace xsheet
