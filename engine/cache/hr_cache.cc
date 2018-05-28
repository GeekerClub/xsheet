// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/hr_cache.h"

namespace xsheet {

HRCache::HRCache(const std::string& name, const CacheOptions& options)
    : Cache(name, options) {}

HRCache::~HRCache() {}


toft::StringPiece HRCache::Value() {

}

Resutl* HRCache::Insert(const toft::StringPiece& key,
                       const toft::StringPiece& value) {

}

Result* HRCache::Erase(const toft::StringPiece& key, Handle handle) {

}


const char* HRCacheSystem::Name = "hr";

TOFT_REGISTER_CACHE_SYSTEM("hr", HRCacheSystem);

HRCacheSystem::HRCacheSystem() {}

HRCacheSystem::~HRCacheSystem() {}

HRCache* HRCacheSystem::Open(const std::string& cache_path, const CacheOptions& options) {
    toft::MutexLocker lock(&list_mutex_);
    std::map<std::string, CacheNode>::iterator it = cache_list_.find(cache_path);
    if (it != cache_list_.end()) {
        LOG(WARNING) << "cache existed: " << cache_path;
        return it->second.second;
    }
    HRCache* cache = HRCache(cache_path, options);
    CacheNode node(options, cache);
    cache_list_[cache_path] = node;
    return cache;
}

bool HRCacheSystem::Exists(const std::string& cache_path) {
    toft::MutexLocker lock(&list_mutex_);
    std::map<std::string, CacheNode>::iterator it = cache_list_.find(cache_path);
    if (it != cache_list_.end()) {
        return true;
    }
    return false;
}

bool HRCacheSystem::Delete(const std::string& cache_path) {
    toft::MutexLocker lock(&list_mutex_);
    std::map<std::string, CacheNode>::iterator it = cache_list_.find(cache_path);
    if (it == cache_list_.end()) {
        return true;
    }
    delete it->second.second;
    cache_list_.erase(it);
    return true;
}

int64_t HRCacheSystem::GetSize(const std::string& cache_path) {
    return 0;
}


} // namespace xsheet