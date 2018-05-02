// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/client_impl.h"

#include "toft/system/threading/mutex.h"
#include "toft/storage/path/path_ext.h"

DECLARE_string(xsheet_sdk_conf_file);
DECLARE_string(xsheet_user_identity);
DECLARE_string(xsheet_user_passcode);

namespace xsheet {

ClientImpl::ClientImpl() {}

ClientImpl::~ClientImpl() {}

Table* ClientImpl::OpenTable(const std::string& table_name, ErrorCode* err) {

}

bool ClientImpl::CreateTable(const TabletSchema& schema, ErrorCode* err) {
    std::string internal_table_name =
        schema.name() + "@" + toft::NumberToString(toft::GetTimestampInMs());
    schema.set_alias(schema.name());
    schema.set_name(internal_table_name);

}

bool ClientImpl::UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err) {

}

bool ClientImpl::UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) {

}

bool ClientImpl::DisableTable(const std::string& name, ErrorCode* err) {

}

bool ClientImpl::DropTable(const std::string& name, ErrorCode* err) {

}

bool ClientImpl::EnableTable(const std::string& name, ErrorCode* err) {

}


bool ClientImpl::List(std::vector<TableInfo>* table_list, ErrorCode* err) {

}

bool ClientImpl::List(const std::string& table_name, TableInfo* table_info,
                  std::vector<TabletInfo>* tablet_list, ErrorCode* err) {

}

bool ClientImpl::IsTableExist(const std::string& table_name, ErrorCode* err) {

}

bool ClientImpl::IsTableEnabled(const std::string& table_name, ErrorCode* err) {

}

bool ClientImpl::IsTableEmpty(const std::string& table_name, ErrorCode* err) {

}

bool ClientImpl::Rename(const std::string& old_table_name, const std::string& new_table_name,
                    ErrorCode* err) {

}


static toft::Mutex g_mutex;
static bool g_is_glog_init = false;

static int SpecifiedFlagfileCount(const std::string& confpath) {
    int count = 0;
    if (!confpath.empty()) {
        count++;
    }
    if (!FLAGS_xsheet_sdk_conf_file.empty()) {
        count++;
    }
    return count;
}

static int InitFlags(const std::string& confpath, const std::string& log_prefix) {
    // search conf file, priority:
    //   user-specified > ./xsheet.flag > ../conf/xsheet.flag
    std::string flagfile;
    if (SpecifiedFlagfileCount(confpath) > 1) {
        LOG(ERROR) << "should specify no more than one config file";
        return -1;
    }

    if (!confpath.empty() && toft::IsExist(confpath)){
        flagfile = confpath;
    } else if(!confpath.empty() && !toft::IsExist(confpath)){
        LOG(ERROR) << "specified config file(function argument) not found: "
            << confpath;
        return -1;
    } else if (!FLAGS_xsheet_sdk_conf_file.empty() && toft::IsExist(confpath)) {
        flagfile = FLAGS_xsheet_sdk_conf_file;
    } else if (!FLAGS_xsheet_sdk_conf_file.empty() && !toft::IsExist(confpath)) {
        LOG(ERROR) << "specified config file(FLAGS_xsheet_sdk_conf_file) not found";
        return -1;
    } else if (toft::IsExist("./xsheet.flag")) {
        flagfile = "./xsheet.flag";
    } else if (toft::IsExist("../conf/xsheet.flag")) {
        flagfile = "../conf/xsheet.flag";
    } else if (toft::IsExist(utils::GetValueFromEnv("XSHEET_CONF"))) {
        flagfile = utils::GetValueFromEnv("XSHEET_CONF");
    } else {
        LOG(ERROR) << "hasn't specify the flagfile, but default config file not found";
        return -1;
    }

    utils::LoadFlagFile(flagfile);

    if (!g_is_glog_init) {
        ::google::InitGoogleLogging(log_prefix.c_str());
        utils::SetupLog(log_prefix);
        g_is_glog_init = true;
    }

    LOG(INFO) << "USER = " << FLAGS_xsheet_user_identity;
    LOG(INFO) << "Load config file: " << flagfile;
    return 0;
}

Client* Client::NewClient(const string& confpath, const string& log_prefix, ErrorCode* err) {
    // Protect the section from [load flagfile] to [new a client instance],
    // because the client constructor will use flagfile options to initial its private options
    toft::MutexLocker locker(&g_mutex);
    if (InitFlags(confpath, log_prefix) != 0) {
        if (err != NULL) {
            std::string reason = "init xsheet flag failed";
            err->SetFailed(ErrorCode::kBadParam, reason);
        }
        return NULL;
    }
    return new ClientImpl(FLAGS_xsheet_user_identity,
                          FLAGS_xsheet_user_passcode);
}

Client* Client::NewClient(const string& confpath, ErrorCode* err) {
    return NewClient(confpath, "xsheetcli", err);
}

Client* Client::NewClient() {
    return NewClient("", "xsheetcli", NULL);
}

void Client::SetGlogIsInitialized() {
    MutexLock locker(&g_mutex);
    g_is_glog_init = true;
}

} // namespace xsheet
