// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/ldb_cache.h"

namespace leveldb {



PredictCache::PredictCache(xsheet::HRCache* hr_cache)
    : hr_cache_(hr_cache) {}

PredictCache::~PredictCache() {}

Cache::Handle* PredictCache::Insert(const Slice& key, void* value, size_t charge,
                     void (*deleter)(const Slice& key, void* value)) {

}

Handle* PredictCach::eLookup(const Slice& key) {

}

void PredictCache::Release(Handle* handle) {

}

void* PredictCache::Value(Handle* handle) {

}

void PredictCache::Erase(const Slice& key) {

}

uint64_t PredictCache::NewId() {

}

} // namespace leveldb
