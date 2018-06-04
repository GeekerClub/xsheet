// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/cache/hr_cache.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

namespace xsheet {

class HRCacheTest : public ::testing::Test {
public:
    HRCacheTest() {}
    ~HRCacheTest() {}

protected:
    HRCache* cache_;
};

TEST_F(HRCacheTest, General) {

}

} // namespace xsheet
