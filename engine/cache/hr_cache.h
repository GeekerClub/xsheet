// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CACHE_HR_CACHE_H
#define XSHEET_ENGINE_CACHE_HR_CACHE_H

#include "engine/cache/cache.h"

namespace xsheet {

struct HrResult : public Cache::Result {

};

class HRCache : public Cache {
public:
    HRCache();
    virtual ~HRCache();


    virtual toft::StringPiece Value();

    virtual Resutl* Insert(const toft::StringPiece& key,
                           const toft::StringPiece& value);
    virtual Result* Erase(const toft::StringPiece& key, Handle handle);

private:
    uint32_t capality_limist_;

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
        return static_cast<HRCacheSystem*>(TOFT_GET_BASE_SYSTEM(Level));
    }

public:
    static const char* Level;

private:
    std::string cache_path_;
    CacheOptions base_options_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_CACHE_HR_CACHE_H
