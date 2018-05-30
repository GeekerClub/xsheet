// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/cache.h"


#include "toft/base/string/number.h"

namespace xsheet {

Cache::Cache(const std::string& name, CacheOptions options)
    : name_(name), options_(options), cur_load_(0) {}


Resutl* Cache::Insert(const std::string& file_path, int64_t offset,
                      const toft::StringPiece& value) {
    return Insert(file_name + "/" + toft::NumberToString(offset), value);
}

Result* Cache::Erase(const std::string& file_path, int64_t offset, Handle handle) {
    return Erase(file_name + "/" + toft::NumberToString(offset), handle);
}

CacheSystem* Cache::GetCacheSystemByName(const std::string& db_path, std::string* real_path) {
    // "/hot_predict/abc" -> "hot_predict"
    if (db_path[0] == '/') {
        size_t next_slash = db_path.find('/', 1);
        if (next_slash != std::string::npos) {
            std::string prefix = db_path.substr(1, next_slash - 1);
            *real_path = db_path.substr(next_slash + 1, db_path.length() - 1);
            CacheSystem* cs = TOFT_GET_CACHE_SYSTEM(prefix);
            if (fs != NULL)
                return fs;
        }
    }
    return TOFT_GET_CACHE_SYSTEM("default");
}

Cache* Cache::Open(const std::string& db_path, const CacheOptions& options) {
    std::string real_path = db_path;
    CacheSystem* cs = GetCacheSystemByName(db_path, &real_path);
    return cs->Open(real_path, options);
}

bool Cache::Exists(const std::string& db_path) {
    std::string real_path = db_path;
    CacheSystem* cs = GetCacheSystemByName(db_path);
    return cs->Exists(real_path);
}

bool Cache::Delete(const std::string& db_path) {
    std::string real_path = db_path;
    CacheSystem* cs = GetCacheSystemByName(db_path);
    return cs->Delete(real_path);
}

int64_t Cache::GetSize(const std::string& db_path) {
    std::string real_path = db_path;
    CacheSystem* cs = GetCacheSystemByName(db_path);
    return cs->GetSize(real_path);
}

} // namespace xsheet
