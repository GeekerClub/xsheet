// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_CACHE_CACHE_H
#define XSHEET_ENGINE_CACHE_CACHE_H

#include <stdint.h>

#include "toft/base/string/string_piece.h"
#include "toft/base/class_registry.h"
#include "toft/base/uncopyable.h"

namespace xsheet {

class CacheSystem;

struct CacheOptions {
    uint64_t capacity_limit_;
    uint64_t erase_limit_;
    uint64_t timer_in_sec_;
};

class Cache {
public:
    typedef void (*Handle)(const toft::StringPiece& key, const toft::StringPiece& value);
    struct Result {};

public:
    Cache(const std::string& name, const CacheOptions& options);
    virtual ~Cache() {}

    virtual void PrintStat() = 0;

    virtual toft::StringPiece Lookup(const toft::StringPiece& key) = 0;
    virtual Result* Insert(const toft::StringPiece& key,
                           const toft::StringPiece& value) = 0;
    virtual Result* Erase(const toft::StringPiece& key, Handle handle) = 0;

    virtual toft::StringPiece Lookup(const std::string& file_path, int64_t offset);
    virtual Result* Insert(const std::string& file_path, int64_t offset,
                           const toft::StringPiece& value);
    virtual Result* Erase(const std::string& file_path, int64_t offset, Handle handle);

public:
    static Cache* Open(const std::string& name, const CacheOptions& options);
    static bool Exists(const std::string& name);
    static  bool Delete(const std::string& name);
    static int64_t GetSize(const std::string& name);

protected:
    static CacheSystem* GetCacheSystemByName(const std::string& name,
                                             std::string* real_path = NULL);
protected:
    std::string name_;
    CacheOptions options_;

    uint64_t cur_load_;
};


class CacheSystem {
public:
    CacheSystem() {}
    virtual ~CacheSystem() {}

    virtual Cache* Open(const std::string& path, const CacheOptions& options) = 0;
    virtual bool Exists(const std::string& path) = 0;
    virtual bool Delete(const std::string& path) = 0;
    virtual int64_t GetSize(const std::string& path) = 0;

};

TOFT_CLASS_REGISTRY_DEFINE_SINGLETON(cache_system, CacheSystem);

} // namespace xsheet


#define TOFT_REGISTER_CACHE_SYSTEM(prefix, class_name) \
    TOFT_CLASS_REGISTRY_REGISTER_CLASS_SINGLETON( \
        cache_system, CacheSystem, prefix, class_name)

// Get CacheSystem singleton from prefix.
#define TOFT_GET_CACHE_SYSTEM(prefix) \
    TOFT_CLASS_REGISTRY_GET_SINGLETON(cache_system, prefix)

// Count of registed database systems.
#define TOFT_CACHE_SYSTEM_COUNT() TOFT_CLASS_REGISTRY_CLASS_COUNT(cache_system)

// Get database systems name by index
#define TOFT_CACHE_SYSTEM_NAME(i) TOFT_CLASS_REGISTRY_CLASS_NAME(cache_system, i)

#endif // XSHEET_ENGINE_CACHE_CACHE_H
