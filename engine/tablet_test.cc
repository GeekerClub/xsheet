// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"
#include "toft/base/scoped_ptr.h"
#include "toft/storage/path/path_ext.h"

#include "engine/tablet_schema.pb.h"

DECLARE_bool(engine_cache_activated);

namespace xsheet {

const std::string workspace_dir = "./testdata/";
const std::string test_db_name = "test.db";

class TabletTest : public ::testing::Test {
public:
    TabletTest() {
        if (!toft::IsExist(workspace_dir)) {
            toft::CreateDirWithRetry(workspace_dir);
        }
        CreateTablet();
    }
    ~TabletTest() {}

    void CreateSchema(const std::string& db_name) {
        tablet_schema_.set_name(db_name);
        tablet_schema_.set_raw_key_type(Readable);

        ColumnFamilySchema* cf1 = tablet_schema_.add_column_families();
        cf1->set_name("cf1");
        cf1->set_locality_group("lg1");
        ColumnFamilySchema* cf2 = tablet_schema_.add_column_families();
        cf2->set_name("cf2");
        cf2->set_locality_group("lg1");

        LocalityGroupSchema* lg1 = tablet_schema_.add_locality_groups();
        lg1->set_name("lg1");

        LOG(INFO) << "tablet schema: " << tablet_schema_.DebugString();
    }

    void CreateTablet() {
        std::string db_path = std::string("/leveldb/") + workspace_dir
            + test_db_name;
        CreateSchema(test_db_name);
        tablet_.reset(new Tablet(db_path, tablet_schema_));
    }

protected:
    TabletSchema tablet_schema_;
    toft::scoped_ptr<Tablet> tablet_;
};

TEST_F(TabletTest, Sample) {
    std::string row_key = "row_key_test";
    std::string family = "cf2";
    std::string qualifier = "qualifier_test";
    std::string value = "value_test";

    EXPECT_EQ(kBaseOk, tablet_->Put(row_key, family, qualifier, value));

    std::string get_value;
    EXPECT_EQ(kBaseOk, tablet_->Get(row_key, family, qualifier, &get_value));
    EXPECT_EQ(value, get_value);
}


TEST_F(TabletTest, CacheDisabled) {
    FLAGS_engine_cache_activated = false;

    std::string row_key = "row_key_test";
    std::string family = "cf2";
    std::string qualifier = "qualifier_test";
    std::string value = "value_test";

    EXPECT_EQ(kBaseOk, tablet_->Put(row_key, family, qualifier, value));

    std::string get_value;
    EXPECT_EQ(kBaseOk, tablet_->Get(row_key, family, qualifier, &get_value));
    EXPECT_EQ(value, get_value);
}

TEST_F(TabletTest, CacheEnabled) {
    FLAGS_engine_cache_activated = false;

    std::string row_key = "row_key_test";
    std::string family = "cf2";
    std::string qualifier = "qualifier_test";
    std::string value = "value_test";

    EXPECT_EQ(kBaseOk, tablet_->Put(row_key, family, qualifier, value));

    std::string get_value;
    EXPECT_EQ(kBaseOk, tablet_->Get(row_key, family, qualifier, &get_value));
    EXPECT_EQ(value, get_value);
}

} // namespace xsheet
