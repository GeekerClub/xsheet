// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <string>
#include <iostream>

#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>

#include "toft/base/scoped_ptr.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/storage/path/path_ext.h"

// #include "sdk/table_impl.h"
#include "sdk/schema_utils.h"
#include "sdk/meta_base.h"
#include "engine/tablet.h"
#include "engine/tablet_schema.pb.h"
#include "engine/codec/string_utils.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);


DEFINE_string(xsheet_default_table, "xsheet_db", "test database for xsheet benchmark");
DEFINE_string(xsheet_workspace_dir, "xsheet_test", "test path for xsheet benchmark");
DEFINE_string(xsheet_kvbase_prefix, "/leveldb/", "the default engine to be activated");
DEFINE_string(xsheet_cli_metabase, "metabase.db", "");

namespace xsheet {


const char* builtin_cmd_list[] = {
    "create",
    "createbyfile <schema_file>",

    "put",
    "put <tablename> <rowkey> [<columnfamily:qualifier>] <value>",

    "get",
    "get <tablename> <rowkey> [<columnfamily:qualifier>]",

    "help",
    "help [cmd]                                                       \n\
         show manual for a or all cmd(s)",

};

static void PrintCmdHelpInfo(const char* msg) {
    if (msg == NULL) {
        return;
    }
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if(strncmp(msg, builtin_cmd_list[i], 32) == 0) {
            std::cout << builtin_cmd_list[i + 1] << std::endl;
            return;
        }
    }
}

static void PrintCmdHelpInfo(const std::string& msg) {
    PrintCmdHelpInfo(msg.c_str());
}

static void PrintAllCmd() {
    std::cout << "there is cmd list:" << std::endl;
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    bool newline = false;
    for (int i = 0; i < count; i+=2) {
        std::cout << std::setiosflags(std::ios::left) << std::setw(20) << builtin_cmd_list[i];
        if (newline) {
            std::cout << std::endl;
            newline = false;
        } else {
            newline = true;
        }
    }

    std::cout << std::endl << "help [cmd] for details." << std::endl;
}

static bool PromptSimilarCmd(const char* msg) {
    if (msg == NULL) {
        return false;
    }
    bool found = false;
    int64_t len = strlen(msg);
    int64_t threshold = int64_t((len * 0.3 < 3) ? 3 : len * 0.3);
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if (EditDistance(msg, builtin_cmd_list[i]) <= threshold) {
            if (!found) {
                std::cout << "Did you mean:" << std::endl;
                found = true;
            }
            std::cout << "    " << builtin_cmd_list[i] << std::endl;
        }
    }
    return found;
}

static void PrintUnknownCmdHelpInfo(const char* msg) {
    if (msg != NULL) {
        std::cout << "'" << msg << "' is not a valid command." << std::endl << std::endl;
    }
    if ((msg != NULL)
        && PromptSimilarCmd(msg)) {
        return;
    }
    PrintAllCmd();
}


int32_t HelpOp(int32_t argc, char* argv[]) {
    if (argc == 2) {
        PrintAllCmd();
    } else if (argc == 3) {
        PrintCmdHelpInfo(argv[2]);
    } else {
        PrintCmdHelpInfo("help");
    }
    return 0;
}

void ParseCfQualifier(const std::string& input, std::string* columnfamily,
                      std::string* qualifier, bool *has_qualifier = NULL) {
    std::string::size_type pos = input.find(":", 0);
    if (pos != std::string::npos) {
        *columnfamily = input.substr(0, pos);
        *qualifier = input.substr(pos + 1);
        if (has_qualifier) {
            *has_qualifier = true;
        }
    } else {
        *columnfamily = input;
        if (has_qualifier) {
            *has_qualifier = false;
        }
    }
}

int32_t CreateOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    if (!toft::IsExist(FLAGS_xsheet_workspace_dir)
        && !toft::CreateDirWithRetry(FLAGS_xsheet_workspace_dir)) {
        LOG(ERROR) << "fail to create work dir: " << FLAGS_xsheet_workspace_dir;
        return -1;
    }

    TabletSchema schema;
    if (!ParseTableSchemaFile(argv[2], &schema)) {
        LOG(ERROR) << "fail to parse schema file: " << argv[2];
        return -1;
    }
//     std::string db_path = FLAGS_xsheet_kvbase_prefix + FLAGS_xsheet_workspace_dir
//         + "/" + schema.name();

    return meta_base->Put(schema.name(), schema)?0:-1;
}

int32_t WriteOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc != 5 && argc != 6) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TabletSchema tablet_schema;
    if (!meta_base->Get(tablename, &tablet_schema)) {
        LOG(ERROR) << "tablet not exist: " << tablename;
        return -1;
    }
    std::string db_path = FLAGS_xsheet_kvbase_prefix + FLAGS_xsheet_workspace_dir
        + "/" + tablename;
    Tablet table(db_path, tablet_schema);

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }

    StatusCode status = table.Put(rowkey, columnfamily, qualifier, value);
    if (status != kBaseOk) {
        LOG(ERROR) << "fail to put data, status: " << StatusCode_Name(status);
        return -1;
    }
    return 0;
}

int32_t ReadOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc != 4 && argc != 5) {
        LOG(ERROR) << "args number error: " << argc << ", need 5 | 6.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    TabletSchema tablet_schema;
    if (!meta_base->Get(tablename, &tablet_schema)) {
        LOG(ERROR) << "tablet not exist: " << tablename;
        return -1;
    }
    std::string db_path = FLAGS_xsheet_kvbase_prefix + FLAGS_xsheet_workspace_dir
        + "/" + tablename;
    Tablet table(db_path, tablet_schema);

    std::string rowkey = argv[3];
    std::string columnfamily = "";
    std::string qualifier = "";
    std::string value;
    if (argc == 5) {
        // use table as kv
        value = argv[4];
    } else if (argc == 6) {
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
        value = argv[5];
    }

    StatusCode status = table.Get(rowkey, columnfamily, qualifier, &value);
    if (status != kBaseOk) {
        LOG(ERROR) << "fail to get data";
        return -1;
    }

    std::cout << rowkey << ":" << columnfamily << ":" << qualifier
        << "\t" << value << std::endl;
    return 0;
}


MetaBase* PrepareMetaBase() {
    if (!toft::IsExist(FLAGS_xsheet_workspace_dir)
        && !toft::CreateDirWithRetry(FLAGS_xsheet_workspace_dir)) {
        LOG(ERROR) << "fail to create work dir: " << FLAGS_xsheet_workspace_dir;
        return NULL;
    }
    std::string db_path = FLAGS_xsheet_kvbase_prefix + FLAGS_xsheet_workspace_dir + "/"
        + FLAGS_xsheet_cli_metabase;

    return new MetaBase(db_path, BaseOptions());
}

} // namespace xsheet

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc > 1 && std::string(argv[1]) == "help") {
        xsheet::HelpOp(argc, argv);
        return 0;
    }

    toft::scoped_ptr<xsheet::MetaBase> meta_base(xsheet::PrepareMetaBase());

    int32_t ret = 0;
    std::string cmd = argv[1];
    if (cmd == "createbyfile") {
        ret = xsheet::CreateOp(meta_base.get(), argc, argv);
    } else if (cmd == "put") {
        ret = xsheet::WriteOp(meta_base.get(), argc, argv);
    } else if (cmd == "get") {
        ret = xsheet::ReadOp(meta_base.get(), argc, argv);
    }
    return ret;
}
