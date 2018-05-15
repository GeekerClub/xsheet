// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <string>
#include <iostream>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/storage/path/path_ext.h"

#include "sdk/table_impl.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);


DEFINE_string(xsheet_test_table, "xsheet_db", "test database for xsheet benchmark");
DEFINE_string(xsheet_test_dir, "xsheet_test", "test path for xsheet benchmark");

int32_t CreateOp(int argc, char* argv[]) {
    std::string table_name;
    if (argc < 3) {
        LOG(WARNING) << "miss table name. use default one: "
            << FLAGS_xsheet_test_table;
        table_name = FLAGS_xsheet_test_table
    } else {
        table_name = argv[2];
    }
}

int32_t WriteOp() {

}

int32_t ReadOp() {

}

void Usage(const std::string& prg_name) {
    std::cout << prg_name << " [cmd] [options] " << std::endl;
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
        ret = CreateOp(argc, argv);
    }
    return ret;
}
