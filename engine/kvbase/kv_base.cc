// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/kvbase/kv_base.h"

namespace xsheet {

KvBase::KvBase(const std::string& db_path, BaseOptions options)
    : m_db_path(db_path), m_options(options) {}

BaseSystem* KvBase::GetBaseSystemByPath(const std::string& db_path, std::string* real_path) {
    // "/leveldb/abc" -> "leveldb"
    if (db_path[0] == '/') {
        size_t next_slash = db_path.find('/', 1);
        if (next_slash != std::string::npos) {
            std::string prefix = db_path.substr(1, next_slash - 1);
            *real_path = db_path.substr(next_slash + 1, db_path.length() - 1);
            BaseSystem* fs = TOFT_GET_BASE_SYSTEM(prefix);
            if (fs != NULL)
                return fs;
        }
    }
    return TOFT_GET_BASE_SYSTEM("local");
}

KvBase* KvBase::Open(const std::string& db_path, const BaseOptions& options) {
    std::string real_path = db_path;
    BaseSystem* fs = GetBaseSystemByPath(db_path, &real_path);
    return fs->Open(real_path, options);
}

bool KvBase::Exists(const std::string& db_path) {
    std::string real_path = db_path;
    BaseSystem* fs = GetBaseSystemByPath(db_path);
    return fs->Exists(real_path);
}

bool KvBase::Delete(const std::string& db_path) {
    std::string real_path = db_path;
    BaseSystem* fs = GetBaseSystemByPath(db_path);
    return fs->Delete(real_path);
}

int64_t KvBase::GetSize(const std::string& db_path) {
    std::string real_path = db_path;
    BaseSystem* fs = GetBaseSystemByPath(db_path);
    return fs->GetSize(real_path);
}

} // namespace xsheet
