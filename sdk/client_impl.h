// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_CLIENT_IMPL_H
#define  XSHEET_CLIENT_IMPL_H

#include <stdint.h>
#include <string>
#include <vector>

#include "xsheet/error_code.h"
#include "xsheet/error_code.h"
#include "xsheet/table.h"

namespace xsheet {

class ClientImpl : public Client {
public:
    static Client* NewClient(const std::string& confpath,
                             const std::string& log_prefix,
                             ErrorCode* err = NULL);
    static Client* NewClient(const std::string& confpath,
                             ErrorCode* err = NULL);
    static Client* NewClient();

    virtual Table* OpenTable(const std::string& table_name, ErrorCode* err);

    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err);
    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<std::string>& tablet_delim,
                             ErrorCode* err);

    virtual bool UpdateTableSchema(const TableDescriptor& desc, ErrorCode* err);
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err);
    virtual bool DisableTable(const std::string& name, ErrorCode* err);
    virtual bool DropTable(const std::string& name, ErrorCode* err);
    virtual bool EnableTable(const std::string& name, ErrorCode* err);

    virtual bool List(std::vector<TableInfo>* table_list, ErrorCode* err);
    virtual bool List(const std::string& table_name, TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list, ErrorCode* err);

    virtual bool IsTableExist(const std::string& table_name, ErrorCode* err);
    virtual bool IsTableEnabled(const std::string& table_name, ErrorCode* err);
    virtual bool IsTableEmpty(const std::string& table_name, ErrorCode* err);

    virtual bool Rename(const std::string& old_table_name, const std::string& new_table_name,
                        ErrorCode* err);
    ClientImpl();
    virtual ~ClientImpl();

private:
    Client(const Client&);
    void operator=(const Client&);
};

} // namespace xsheet

#endif  // XSHEET_CLIENT_IMPL_H
