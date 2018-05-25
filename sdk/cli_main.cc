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

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/readline/history.h"
#include "thirdparty/readline/readline.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"
#include "toft/base/string/number.h"
#include "toft/storage/path/path_ext.h"

// #include "sdk/table_impl.h"
#include "sdk/schema_utils.h"
#include "sdk/meta_base.h"
#include "engine/tablet.h"
#include "engine/tablet_schema.pb.h"
#include "engine/codec/string_utils.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);
DECLARE_int32(v);

DEFINE_string(xsheet_default_table, "xsheet_db", "test database for xsheet benchmark");
DEFINE_string(xsheet_workspace_dir, "xsheet_test", "test path for xsheet benchmark");
DEFINE_string(xsheet_kvbase_prefix, "/leveldb/", "the default engine to be activated");
DEFINE_string(xsheet_cli_metabase, "metabase.db", "");


typedef std::map<std::string, int32_t(*)(xsheet::MetaBase*, int32_t, char* argv[])> CommandTable;

namespace xsheet {


const char* builtin_cmd_list[] = {
    "create",
    "createbyfile <schema_file>",

    "put",
    "put <tablename> <rowkey> [<columnfamily:qualifier>] <value>",

    "get",
    "get <tablename> <rowkey> [<columnfamily:qualifier>]",

    "drop",
    "drop <tablename>",

    "config",
    "config [<key=value]]",

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


int32_t HelpOp(MetaBase* meta_base, int32_t argc, char* argv[]) {
    if (argc == 2) {
        PrintAllCmd();
    } else if (argc == 3) {
        PrintCmdHelpInfo(argv[2]);
    } else {
        PrintCmdHelpInfo("help");
    }
    return 0;
}

int32_t HelpOp(int32_t argc, char* argv[]) {
    return HelpOp(NULL, argc, argv);
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

int32_t CreateByFileOp(MetaBase* meta_base, int argc, char* argv[]) {
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
    VLOG(10) << "parse from file: " << schema.DebugString();
//     std::string db_path = FLAGS_xsheet_kvbase_prefix + FLAGS_xsheet_workspace_dir
//         + "/" + schema.name();

    return meta_base->Put(schema.name(), schema)?0:-1;
}

int32_t DropOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string tablename = argv[2];
    if (!meta_base->Delete(tablename)) {
        LOG(ERROR) << "fail to drop table: " << tablename;
        return -1;
    }
    return 0;
}

int32_t ShowSingleTable(MetaBase* meta_base, const std::string& table_name,
                        bool is_x) {
    return -1;
}

int32_t ShowSingleTable(MetaBase* meta_base, bool is_x) {
    std::vector<TabletSchema> schema_list;
    if (!meta_base->Get(&schema_list)) {
        LOG(ERROR) << "fail to get schema list";
        return -1;
    }
    for (uint32_t i = 0; i < schema_list.size(); ++i) {
        std::cout << schema_list[i].ShortDebugString() << std::endl;
    }
    return 0;
}

int32_t ShowOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc < 2) {
        LOG(ERROR) << "args number error: " << argc << ", need >2.";
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    int32_t ret_val;
    std::string cmd = argv[1];

    return ShowSingleTable(meta_base, true);
//     if (argc == 3) {
//         ret_val = ShowSingleTable(meta_base, argv[2], cmd == "showx");
//     } else if (argc == 2 && (cmd == "show" || cmd == "showx" || cmd == "showall")) {
//         ret_val = ShowAllTables(meta_base, cmd == "showx");
//     } else {
//         ret_val = -1;
//         LOG(ERROR) << "error: arg num: " << argc;
//     }
//     return ret_val;
}

int32_t PutOp(MetaBase* meta_base, int argc, char* argv[]) {
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
    LOG(INFO) << "get db schema: " << tablet_schema.DebugString();
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

int32_t GetOp(MetaBase* meta_base, int argc, char* argv[]) {
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
        ParseCfQualifier(argv[4], &columnfamily, &qualifier);
    }

    StatusCode status = table.Get(rowkey, columnfamily, qualifier, &value);
    if (status != kBaseOk) {
        LOG(ERROR) << "fail to get data, status: " << StatusCode_Name(status);
        return -1;
    }

    std::cout << rowkey << ":" << columnfamily << ":" << qualifier
        << "\t" << value << std::endl;
    return 0;
}

void UpdateSysConfig(const std::string& k, const std::string& v) {
    if (k == "v") {
        int32_t value = FLAGS_minloglevel;
        if (toft::StringToNumber(v, &value)) {
            FLAGS_v = value;
            std::cout << "set: " << k << " <-- " << value << std::endl;
        }
    }
}

int32_t ConfigOp(MetaBase* meta_base, int argc, char* argv[]) {
    if (argc < 3) {
        LOG(ERROR) << "error argument, need 3";
        return -1;
    }
    std::string config_str = argv[2];
    std::vector<std::string> params;
    toft::SplitString(config_str, ",", &params);
    for (uint32_t i = 0; i < params.size(); ++i) {
        std::vector<std::string> items;
        toft::SplitString(params[i], "=", &items);
        if (items.size() > 1) {
            UpdateSysConfig(items[0], items[1]);
        }
    }
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

static CommandTable& GetCommandTable(){
    static CommandTable command_table;
    return command_table;
}

static void InitializeCommandTable(){
    CommandTable& command_table = GetCommandTable();
    command_table["createbyfile"] = xsheet::CreateByFileOp;
    command_table["drop"] = xsheet::DropOp;
    command_table["show"] = xsheet::ShowOp;
    command_table["put"] = xsheet::PutOp;
    command_table["get"] = xsheet::GetOp;
    command_table["show"] = xsheet::ShowOp;
    command_table["drop"] = xsheet::DropOp;
    command_table["config"] = xsheet::ConfigOp;
    command_table["help"] = xsheet::HelpOp;
}

void PrintSystemVersion() {

}

int32_t ExecuteCommand(xsheet::MetaBase* meta_base, int argc, char* argv[]) {
    int32_t ret = 0;

    CommandTable& command_table = GetCommandTable();
    std::string cmd = argv[1];
    if (cmd == "version") {
        PrintSystemVersion();
    } else if (command_table.find(cmd) != command_table.end()) {
        ret = command_table[cmd](meta_base, argc, argv);
    } else {
        xsheet::PrintUnknownCmdHelpInfo(argv[1]);
        ret = -1;
    }

    return ret;
}

int main(int argc, char* argv[]) {
    FLAGS_minloglevel = 2;
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc > 1 && std::string(argv[1]) == "help") {
        xsheet::HelpOp(argc, argv);
        return 0;
    }

    toft::scoped_ptr<xsheet::MetaBase> meta_base(xsheet::PrepareMetaBase());

//     int32_t ret = 0;
//     std::string cmd = argv[1];
//     if (cmd == "createbyfile") {
//         ret = xsheet::CreateOp(meta_base.get(), argc, argv);
//     } else if (cmd == "put") {
//         ret = xsheet::WriteOp(meta_base.get(), argc, argv);
//     } else if (cmd == "get") {
//         ret = xsheet::ReadOp(meta_base.get(), argc, argv);
//     }


    InitializeCommandTable();

    int32_t ret  = 0;
    if (argc == 1) {
        char* line = NULL;
        while ((line = readline("xsheet> ")) != NULL) {
            char* line_copy = strdup(line);
            std::vector<char*> arg_list;
            arg_list.push_back(argv[0]);
            char* tmp = NULL;
            char* token = strtok_r(line, " \t", &tmp);
            while (token != NULL) {
                arg_list.push_back(token);
                token = strtok_r(NULL, " \t", &tmp);
            }
            if (arg_list.size() == 2 &&
                (strcmp(arg_list[1], "quit") == 0 || strcmp(arg_list[1], "exit") == 0)) {
                free(line_copy);
                free(line);
                break;
            }
            if (arg_list.size() > 1) {
                add_history(line_copy);
                ret = ExecuteCommand(meta_base.get(), arg_list.size(), &arg_list[0]);
            }
            free(line_copy);
            free(line);
        }
    } else {
        ret = ExecuteCommand(meta_base.get(), argc, argv);
    }

    return ret;
}
