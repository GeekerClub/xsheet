// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/hr_cache.h"

#include <algorithm>

#include "toft/base/closure.h"

#include "engine/types.h"

namespace xsheet {

HRCache::HRCache(const std::string& name, const CacheOptions& options,
                 toft::TimerManager* timer_manager)
    : erase_timer_id_(kInvalidTimerId), timer_manager_(timer_manager),
      Cache(name, options) {
    EnableEraseTimer();
}

HRCache::~HRCache() {
    DisableEraseTimer();
    toft::MutexLocker lock(&cache_mutex_);
    ReleaseCacheNode(0, cache_.size());
}

void HRCache::PrintStat() {
    for (uint32_t i = 0; i < cache_.size(); ++i) {
        LOG(INFO) << "[" << cache_[i]->key_str_ << ", "
            << cache_[i]->hit_count_ << "]";
    }
}

toft::StringPiece HRCache::Lookup(const toft::StringPiece& key) {
    toft::MutexLocker lock(&cache_mutex_);

    std::map<std::string, uint32_t>::iterator it = cache_index_.find(key.as_string());
    if (it == cache_index_.end()) {
        return "";
    } else {
        cache_[it->second]->hit_count_++;
    }
    return cache_[it->second]->payload_;
}

Cache::Result* HRCache::Insert(const toft::StringPiece& key,
                        const toft::StringPiece& value) {
    LOG(INFO) << "insert: " << key.as_string();
    toft::MutexLocker lock(&cache_mutex_);
    std::map<std::string, uint32_t>::iterator it = cache_index_.find(key.as_string());
    if (it != cache_index_.end()) {
        cache_[it->second]->hit_count_++;
        return new HrResult(kCacheOk, "");
    }

    if (key.size() + value.size() + cur_load_ > options_.capacity_limit_) {
        LOG(ERROR) << "cache over load, status: " << StatusCode_Name(kCacheOverLoad);
        return new HrResult(kCacheOverLoad, "cache over load");
    }

    CacheNode* node = new CacheNode;
    node->key_ = key.as_string();
    node->key_str_ = key.as_string();
    node->hit_count_ = 1;
    node->payload_ = value.as_string();
    cache_.push_back(node);
    cache_index_[key.as_string()] = cache_.size() - 1;


    return new HrResult(kCacheOk, "");
}

Cache::Result* HRCache::Erase(const toft::StringPiece& key, Handle handle) {
    toft::MutexLocker lock(&cache_mutex_);

    std::map<std::string, uint32_t>::iterator it = cache_index_.find(key.as_string());
    if (it == cache_index_.end()) {
        return new HrResult(kCacheOk, "");
    }
    cache_[it->second]->hit_count_ = 0;
    return new HrResult(kCacheOk, "");
}

void HRCache::EraseElement(uint64_t timer_id) {
    LOG(INFO) << "EraseElement() timer is activated";
    toft::MutexLocker lock(&cache_mutex_);
    CHECK_EQ(timer_id, erase_timer_id_);

    PrintStat();

    std::sort(cache_.begin(), cache_.end());
    if (cache_.size() <= options_.erase_limit_) {
        return;
    }

    uint64_t erase_num = cache_.size() - options_.erase_limit_;
    ReleaseCacheNode(erase_num, cache_.size());
    cache_.erase(cache_.begin() + erase_num, cache_.end());
    cache_index_.clear();
    for (uint32_t i = 0; i < cache_.size(); ++i) {
        CacheNode* node = cache_[i];
        cache_index_[node->key_.as_string()] = i;
    }
}

bool HRCache::ReleaseCacheNode(uint32_t start, uint32_t end) {
    if (start < 0 || end > cache_.size()) {
        LOG(ERROR) << "wrong boundary, start: " << start
            << ", end: " << end;
        return false;
    }
    for (uint32_t i = start; i < end; ++i) {
        CacheNode* node = cache_[i];
        delete node;
    }
    return true;
}

void HRCache::EnableEraseTimer(int32_t expand_factor) {
    if (erase_timer_id_ == kInvalidTimerId) {
        toft::Closure<void (uint64_t)>* closure =
            toft::NewClosure(this, &HRCache::EraseElement);
        int64_t timeout_period = 1000 * expand_factor * options_.timer_in_sec_;
        erase_timer_id_ = timer_manager_->AddOneshotTimer(
            timeout_period, closure);
        if (erase_timer_id_ == kInvalidTimerId) {
            delete closure;
        }
    } else {
        timer_manager_->EnableTimer(erase_timer_id_);
    }
}

void HRCache::DisableEraseTimer() {
    if (erase_timer_id_ != kInvalidTimerId) {
        timer_manager_->DisableTimer(erase_timer_id_);
    }
}


const char* HRCacheSystem::HR = "hr";

TOFT_REGISTER_CACHE_SYSTEM("hr", HRCacheSystem);

HRCacheSystem::HRCacheSystem() {}

HRCacheSystem::~HRCacheSystem() {
    toft::MutexLocker lock(&list_mutex_);
    std::map<std::string, CacheNode>::iterator it = cache_list_.begin();
    for (; it != cache_list_.end(); ++it) {
        if (it->second.second) {
            delete it->second.second;
        }
    }

}

HRCache* HRCacheSystem::Open(const std::string& cache_path, const CacheOptions& options) {
    toft::MutexLocker lock(&list_mutex_);
    std::map<std::string, CacheNode>::iterator it = cache_list_.find(cache_path);
    if (it != cache_list_.end()) {
        LOG(WARNING) << "cache existed: " << cache_path;
        return it->second.second;
    }
    HRCache* cache = new HRCache(cache_path, options, &timer_manager_);
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
