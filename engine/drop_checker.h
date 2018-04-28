// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#ifndef XSHEET_ENGINE_DROP_CHECKER_H
#define XSHEET_ENGINE_DROP_CHECKER_H

#include <map>

#include "toft/base/string/string_piece.h"

#include "engine/tablet_schema.pb.h"
#include "proto/tablet.pb.h"
#include "engine/codec/raw_key_operator.h"
#include "engine/codec/raw_key.h"
#include "engine/tablet_scanner.h"

namespace xsheet {

// class ScanOptions;

class DropChecker {
public:
    DropChecker(const TabletSchema& taclet_schema,
                const ScanOptions& options, const std::string& end_user_key,
                const RawKeyOperator* key_operator);
    ~DropChecker();

    bool DropCheck(toft::StringPiece raw_key, KeyValuePair* row_record);

    bool IsDrop() const { return is_drop_; }
    bool IsCompleted() const { return is_completed_; }

private:
    bool IsUnselectedColumnFamily(const std::string& column_family) const;
    bool IsInvalidColumnFamily(const std::string& column_family, int32_t* cf_idx) const;
    bool IsOutofLifeTime(int32_t cf_idx, int64_t timestamp) const;
    bool IsCommonOP(RawKeyType keyType);

private:
    TabletSchema tablet_schema_;
    ScanOptions scan_options_;
    std::string end_user_key_;
    const RawKeyOperator* key_operator_;
    bool is_drop_;
    bool is_completed_;
    std::map<std::string, int32_t> cf_indexs_;

    std::string prev_key_;
    std::string prev_col_;
    std::string prev_qual_;
    int64_t prev_ts_;
    RawKeyType prev_type_;
    RawKeyType cur_type_;

    int64_t del_row_ts_;
    int64_t del_col_ts_;
    int64_t del_qual_ts_;
    int64_t cur_ts_;
    uint64_t del_row_seq_;
    uint64_t del_col_seq_;
    uint64_t del_qual_seq_;
    uint32_t version_num_;
    uint64_t snapshot_;
    bool has_put_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_DROP_CHECKER_H
