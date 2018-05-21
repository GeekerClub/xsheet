// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <string>
#include <iostream>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/storage/path/path_ext.h"

// #include "sdk/table_impl.h"
#include "sdk/schema_utils.h"
#include "engine/tablet.h"
#include "engine/tablet_schema.pb.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);


DEFINE_string(xsheet_test_table, "xsheet_db", "test database for xsheet benchmark");
DEFINE_string(xsheet_test_dir, "xsheet_test", "test path for xsheet benchmark");

namespace xsheet {

const std::string db_prefix = "/leveldb/";

int32_t CreateOp(int argc, char* argv[]) {
    std::string table_name;
    if (argc < 3) {
        LOG(WARNING) << "miss table name. use default one: "
            << FLAGS_xsheet_test_table;
        table_name = FLAGS_xsheet_test_table;
    } else {
        table_name = argv[2];
    }

    if (!toft::IsExist(FLAGS_xsheet_test_dir)
        && !toft::CreateDirWithRetry(FLAGS_xsheet_test_dir)) {
        LOG(ERROR) << "fail to create work dir: " << FLAGS_xsheet_test_dir;
        return -1;
    }
    std::string db_path = db_prefix + FLAGS_xsheet_test_dir + "/" + table_name;

    TabletSchema schema;
    if (!ParseTableSchemaFile(argv[3], &schema)) {
        LOG(ERROR) << "fail to parse schema file: " << argv[3];
        return -1;
    }

    Tablet tablet_engine(db_path, schema);
    return 0;
}

int32_t WriteOp() {
    return -1;
}

int32_t ReadOp() {
    return -1;
}

} // namespace xsheet

void Usage(const std::string& prg_name) {
    std::cout << prg_name << " [cmd] [options] " << std::endl;
    std::cout << "   create [db name] [schema file]" << std::endl;
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc < 2) {
        Usage(argv[0]);
        return -1;
    }

    int32_t ret = 0;
    std::string cmd = argv[1];
    if (cmd == "create") {
        ret = xsheet::CreateOp(argc, argv);
    }
    return ret;
}
