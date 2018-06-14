// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CACHE_LDB_CACHE_H
#define XSHEET_ENGINE_CACHE_LDB_CACHE_H

#include "engine/cache/hr_cache.h"

#include "toft/system/atomic/atomic.h"
#include "toft/base/string/string_piece.h"
#include "thirdparty/leveldb/slice.h"
#include "thirdparty/leveldb/cache.h"

namespace leveldb {


struct PCHandle : public Cache::Handle {
    toft::StringPiece key_sp_;
    toft::StringPiece value_sp_;
    xsheet::Cache::Result* hr_result_;

    PCHandle() : hr_result_(NULL) {}
};

class PredictCache : public Cache {
public:
    PredictCache(xsheet::HRCache* hr_cache);
    virtual ~PredictCache();

    virtual Cache::Handle* Insert(
        const Slice& key, void* value, size_t charge,
        void (*deleter)(const Slice& key, void* value));
    virtual Cache::Handle* Lookup(const Slice& key);
    virtual void Release(Handle* handle);
    virtual void* Value(Handle* handle);
    virtual void Erase(const Slice& key);
    virtual uint64_t NewId();

private:
    xsheet::HRCache* hr_cache_;
    toft::Atomic<uint64_t> cache_id_;
};

} // namespace leveldb


#endif // XSHEET_ENGINE_CACHE_LDB_CACHE_H
