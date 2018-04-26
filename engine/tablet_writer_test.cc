// Copyright (C) 2018, Baidu Inc.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "engine/tablet_writer.h"

#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/format.h"
#include "toft/base/string/number.h"
#include "toft/system/threading/event.h"
#include "toft/system/threading/this_thread.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"


#include "proto/tablet.pb.h"
#include "proto/status_code.pb.h"

namespace xsheet {

const std::string working_dir = "tablet_writer_testdata/";

class TabletWriterTest : public ::testing::Test {
public:
    TabletWriterTest() {
        std::string cmd = std::string("mkdir -p ") + working_dir;
        system(cmd.c_str());
    }
    ~TabletWriterTest() {
        std::string cmd = std::string("rm -rf ") + working_dir;
        system(cmd.c_str());
    }

    void WriteHandler(std::vector<const RowMutationSequence*>* row_mutation_vec,
                      std::vector<StatusCode>* status_vec) {
        LOG(INFO) << "WriteHandler()";
        m_callback_count++;
    }

    void CreateTestData(const std::string& table_name,
                        int32_t key_start, int32_t key_end,
                        std::vector<TabletWriter::WriteTask>* task_list) {
        for (int32_t i = key_start; i < key_end; ++i) {
            TabletWriter::WriteTask task;
            std::string str = toft::StringPrint("%08llu", i);
            RowMutationSequence* mu_seq = new RowMutationSequence;
            mu_seq->set_row_key(str);
            Mutation* mutation = mu_seq->add_mutation_list();
            mutation->set_type(kPut);
            mutation->set_value(str);

            std::vector<const RowMutationSequence*>* row_mutation_vec =
                new std::vector<const RowMutationSequence*>;
            row_mutation_vec->push_back(mu_seq);
            std::vector<StatusCode>* status_vec = new std::vector<StatusCode>;

            task.row_mutation_vec = row_mutation_vec;
            task.status_vec = status_vec;
            task.callback = std::bind(&TabletWriterTest::WriteHandler, this,
                                      std::placeholders::_1, std::placeholders:: _2);
            task_list->push_back(task);
        }
    }

    void CleanTestData(std::vector<TabletWriter::WriteTask> task_list) {
        for (uint32_t i = 0; i < task_list.size(); ++i) {
            delete task_list[i].row_mutation_vec->at(0);
            delete task_list[i].row_mutation_vec;
            delete task_list[i].status_vec;
        }
    }

    void CreateTestTable(const std::string& table_name,
                         const std::vector<TabletWriter::WriteTask>& task_list) {
        std::string tablet_path = working_dir + table_name;
        TabletSchema tablet_schema;
        tablet_schema.set_name(table_name);


        kvbase_.reset(KvBase::Open(tablet_path, BaseOptions()));
        CHECK(kvbase_.get());
        TabletWriter tablet_writer(tablet_schema, kvbase_.get());

        for (uint32_t i = 0; i < task_list.size(); ++i) {
            EXPECT_EQ(kTabletOk, tablet_writer.Write(
                    task_list[i].row_mutation_vec,
                    task_list[i].status_vec,
                    task_list[i].callback));
        }
    }

    void VerifyOperation(const std::string& table_name,
                         int32_t key_start, int32_t key_end) {
    }

protected:
    toft::scoped_ptr<KvBase> kvbase_;
    int32_t m_callback_count;
};

TEST_F(TabletWriterTest, General) {
    int32_t start = 0;
    int32_t end = 1000;
    std::string table_name = "gneral";
    std::vector<TabletWriter::WriteTask> task_list;

    CreateTestData(table_name, start, end, &task_list);
    EXPECT_TRUE(task_list.size() > 0);
    CreateTestTable(table_name, task_list);
    VerifyOperation(table_name, start, end);
    CleanTestData(task_list);
}


} // namespace xsheet

