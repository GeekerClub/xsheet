// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/ldb_cache.h"


namespace leveldb {



PredictCache::PredictCache(xsheet::HRCache* hr_cache)
    : hr_cache_(hr_cache), cache_id_(0) {}

PredictCache::~PredictCache() {}

Cache::Handle* PredictCache::Insert(const Slice& key, void* value, size_t charge,
                                    void (*deleter)(const Slice& key, void* value)) {
    PCHandle* handle = new PCHandle;

    handle->key_sp_.set(key.data(), key.size());
    handle->value_sp_.set(value, charge);
    handle->hr_result_ = hr_cache_->Insert(handle->key_sp_, handle->value_sp_);
    return handle;
}

Cache::Handle* PredictCache::Lookup(const Slice& key) {
    PCHandle* handle = new PCHandle;
    handle->key_sp_.set(key.data(), key.size());
    toft::StringPiece sp = hr_cache_->Lookup(handle->key_sp_);
    handle->value_sp_.set(sp.data(), sp.size());

    return handle;
}

void PredictCache::Release(Cache::Handle* handle) {
    PCHandle* pc_handle = reinterpret_cast<PCHandle*>(handle);
    delete pc_handle->hr_result_;
    delete handle;
}

void* PredictCache::Value(Handle* handle) {
    PCHandle* pc_handle = reinterpret_cast<PCHandle*>(handle);
    return const_cast<char*>(pc_handle->value_sp_.data());
}

void PredictCache::Erase(const Slice& key) {
    toft::StringPiece key_sp(key.data(), key.size());
    hr_cache_->Erase(key_sp, NULL);
}

uint64_t PredictCache::NewId() {
    return cache_id_++;
}


} // namespace leveldb
