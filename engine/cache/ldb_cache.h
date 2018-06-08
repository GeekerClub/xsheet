// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CACHE_LDB_CACHE_H
#define XSHEET_ENGINE_CACHE_LDB_CACHE_H

#include "engine/cache/hr_cache.h"

namespace leveldb {


struct PCHandle {

};

class PredictCache : public Cache {
public:
    PredictCache();
    virtual ~PredictCache();

    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value));
    virtual Handle* Lookup(const Slice& key);
    virtual void Release(Handle* handle);
    virtual void* Value(Handle* handle);
    virtual void Erase(const Slice& key);
    virtual uint64_t NewId();

private:
    toft::scoped_ptr<xsheet::HRCache> hr_cache_;
};

} // namespace leveldb


#endif // XSHEET_ENGINE_CACHE_LDB_CACHE_H
