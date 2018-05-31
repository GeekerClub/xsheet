// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CACHE_HR_CACHE_H
#define XSHEET_ENGINE_CACHE_HR_CACHE_H

#include "engine/cache/cache.h"

#include <map>
#include <vector>

#include "toft/system/threading/mutex.h"

#include "proto/status_code.pb.h"

namespace xsheet {

struct HrResult : public Cache::Result {
    StatusCode status_;
    std::string reason_;

    HrResult(const StatusCode& status = kCacheOk,
             const std::string& reason = "")
        : status_(status), reason_(reason) {}
};

class HRCache : public Cache {
public:
    HRCache(const std::string& name, const CacheOptions& options);
    virtual ~HRCache();


    virtual toft::StringPiece Lookup(const toft::StringPiece& key);

    virtual Result* Insert(const toft::StringPiece& key,
                           const toft::StringPiece& value);
    virtual Result* Erase(const toft::StringPiece& key, Handle handle);

private:
    struct CacheNode {
        toft::StringPiece key_;
        int64_t hit_count_;
        toft::StringPiece payload_;

        bool operator>(const CacheNode& rhs) {
            return hit_count_ > rhs.hit_count_;
        }

        CacheNode() : hit_count_(0) {}
    };

    std::map<toft::StringPiece, uint32_t> cache_index_;
    std::vector<CacheNode> cache_;
    toft::Mutex cache_mutex_;
};

class HRCacheSystem : public CacheSystem {
public:
    HRCacheSystem();
    virtual ~HRCacheSystem();

    virtual HRCache* Open(const std::string& cache_path, const CacheOptions& base_options);
    virtual bool Exists(const std::string& cache_path);
    virtual bool Delete(const std::string& cache_path);
    virtual int64_t GetSize(const std::string& cache_path);

    static HRCacheSystem* GetRegisteredCacheSystem() {
        return static_cast<HRCacheSystem*>(TOFT_GET_CACHE_SYSTEM(HR));
    }

public:
    static const char* HR;

private:
    typedef std::pair<CacheOptions, HRCache*> CacheNode;
    std::map<std::string, CacheNode> cache_list_;
    toft::Mutex list_mutex_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_CACHE_HR_CACHE_H
