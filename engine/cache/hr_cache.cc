// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/hr_cache.h"

namespace xsheet {

HRCache::HRCache(const std::string& name, const CacheOptions& options)
    : Cache(name, options) {}

HRCache::~HRCache() {}


toft::StringPiece HRCache::Lookup(const toft::StringPiece& key) {
    toft::MutexLocker lock(&cache_mutex_);

    std::map<toft::StringPiece, CacheNode>::iterator it = cache_index_.find(key);
    if (it == cache_index_.end()) {
        return "";
    }
    return cache_[it->second].payload_;
}

Cache::Result* HRCache::Insert(const toft::StringPiece& key,
                        const toft::StringPiece& value) {
    toft::MutexLocker lock(&cache_mutex_);
    if (key.size() + value.size() + cur_load_ > options_.capacity_limit_) {
        LOG(ERROR) << "cache over load, status: " << StatusCode_Name(kCacheOverLoad);
        return new HrResult(kCacheOverLoad, "cache over load");
    }

    std::map<toft::StringPiece, CacheNode>::iterator it = cache_index_.find(key);
    if (it != cache_index_.end()) {
        cache_[it->second].hit_count_++;
        return new HrResult(kCacheOk, "");
    }

    CacheNode node;
    node.key_ = key;
    node.payload_ = value;
    cache_.push_back(node);
    cache_index_[key] = cache_.size() - 1;

    return new HrResult(kCacheOk, "");
}

Cache::Result* HRCache::Erase(const toft::StringPiece& key, Handle handle) {
    toft::MutexLocker lock(&cache_mutex_);

    std::map<toft::StringPiece, CacheNode>::iterator it = cache_index_.find(key);
    if (it == cache_index_.end()) {
        return new HrResult(kCacheOk, "");
    }
    cache_.erase(cache_.begin() + it->second);
    cache_index_.erase(it);
    return new HrResult(kCacheOk, "");
}


const char* HRCacheSystem::HR = "hr";

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
    HRCache* cache = new HRCache(cache_path, options);
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
