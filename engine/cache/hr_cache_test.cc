// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/hr_cache.h"

#include "toft/base/string/string_piece.h"
#include "toft/base/scoped_ptr.h"
#include "thirdpartygflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

namespace xsheet {

const std::string cache_name = "test_cache";

class HRCacheTest : public ::testing::Test {
public:
    HRCacheTest() {}
    ~HRCacheTest() {}

    void SetupCache(const std::string& cache_name) {
        CacheOptions options;
        options.capacity_limit_ = 5;
        options.erase_limit_ = 3;
        options.timer_in_sec_ = 3;
        cache_.reset(Cache::Open(cache_name, options));
    }

    void CreateData(uint32_t num) {
        std::string payload = "payload";
        for (uint32_t i = 0; i < num; ++i) {
            cache_.Insert("file", i % 3, payload + toft::NumberToString(i));
        }
    }

    void HitData(uint32_t num) {
        for (uint32_t i = 0; i < num; ++i) {
            if (i % 2 == 0) {
                toft::StringPiece sp = cache_.Lookup("file", i % 3);
                EXPECT_TRUE(sp.as_string() != "");
            } else {
                Cache::Result* result = cache_.Insert("file", i % 3, "");
                EXPECT_TRUE(static_cast<HRCache*>(result)->status_ == kCacheOk);
                delete result;
            }
        }
    }

protected:
    toft::scoped_ptr<HRCache> cache_;
};

TEST_F(HRCacheTest, General) {

}

} // namespace xsheet
