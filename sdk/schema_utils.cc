// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "sdk/schema_utils.h"

#include <map>
#include <fstream>
#include <sstream>
#include <iostream>

#include "toft/base/string/number.h"

DECLARE_int64(engine_write_block_size);

namespace xsheet {

std::string LgProp2Str(bool type) {
    if (type) {
        return "snappy";
    } else {
        return "none";
    }
}

std::string LgProp2Str(StoreType type) {
    if (type == SATA_TYPE) {
        return "disk";
    } else if (type == SSD_TYPE) {
        return "flash";
    } else if (type == MEMORY_TYPE) {
        return "memory";
    } else {
        return "";
    }
}

std::string TableProp2Str(KeyType type) {
    if (type == Readable) {
        return "readable";
    } else if (type == Binary) {
        return "binary";
    } else if (type == TTLKv) {
        return "ttlkv";
    } else if (type == GeneralKv) {
        return "kv";
    } else {
        return "";
    }
}

std::string Switch2Str(bool enabled) {
    if (enabled) {
        return "on";
    } else {
        return "off";
    }
}

void ReplaceStringInPlace(std::string& subject,
                          const std::string& search,
                          const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
}

void ShowTableSchema(const TabletSchema& s, bool is_x) {
    TabletSchema schema = s;
    std::stringstream ss;
    std::string str;
    std::string table_alias = schema.name();
    if (!schema.alias().empty()) {
        table_alias = schema.alias();
    }
    if (schema.raw_key_type() != GeneralKv) {
        std::cerr << "caution: old style schema, do not update it if necessary." << std::endl;
        schema.set_raw_key_type(GeneralKv);
    }

    if (schema.raw_key_type() == TTLKv || schema.raw_key_type() == GeneralKv) {
        const LocalityGroupSchema& lg_schema = schema.locality_groups(0);
        ss << "\n  " << table_alias << " <";
        if (is_x) {
            ss << "rawkey=" << TableProp2Str(schema.raw_key_type()) << ",";
        }
        if (is_x || lg_schema.store_type() != SATA_TYPE) {
            ss << "storage=" << LgProp2Str(lg_schema.store_type()) << ",";
        }
        if (is_x || lg_schema.block_size() != FLAGS_engine_write_block_size) {
            ss << "blocksize=" << lg_schema.block_size() << ",";
        }
        ss << "\b>\n" << "  (kv mode)\n";
        str = ss.str();
        ReplaceStringInPlace(str, ",\b", "");
        std::cout << str << std::endl;
        return;
    }

    ss << "\n  " << table_alias << " <";
    if (is_x) {
        ss << "rawkey=" << TableProp2Str(schema.raw_key_type()) << ",";
    }
    ss << "\b> {" << std::endl;

    size_t lg_num = schema.locality_groups_size();
    size_t cf_num = schema.column_families_size();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const LocalityGroupSchema& lg_schema = schema.locality_groups(lg_no);
        ss << "      " << lg_schema.name() << " <";
        ss << "storage=" << LgProp2Str(lg_schema.store_type()) << ",";
        if (is_x || lg_schema.block_size() != FLAGS_engine_write_block_size) {
            ss << "blocksize=" << lg_schema.block_size() << ",";
        }
        ss << "\b> {" << std::endl;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ColumnFamilySchema& cf_schema = schema.column_families(cf_no);
            if (cf_schema.locality_group() != lg_schema.name()) {
                continue;
            }
            ss << "          " << cf_schema.name();
            std::stringstream cf_ss;
            cf_ss << " <";
            if (is_x || cf_schema.max_versions() != 1) {
                cf_ss << "maxversions=" << cf_schema.max_versions() << ",";
            }
            if (is_x || cf_schema.min_versions() != 1) {
                cf_ss << "minversions=" << cf_schema.min_versions() << ",";
            }
            if (is_x || cf_schema.time_to_live() != 0) {
                cf_ss << "ttl=" << cf_schema.time_to_live() << ",";
            }
            if (is_x || (cf_schema.type() != "bytes" && cf_schema.type() != "")) {
                if (cf_schema.type() != "") {
                    cf_ss << "type=" << cf_schema.type() << ",";
                } else {
                    cf_ss << "type=bytes" << ",";
                }
            }
            cf_ss << "\b>";
            if (cf_ss.str().size() > 5) {
                ss << cf_ss.str();
            }
            ss << "," << std::endl;
        }
        ss << "      }," << std::endl;
    }
    ss << "  }" << std::endl;
    str = ss.str();
    ReplaceStringInPlace(str, ",\b", "");
    std::cout << str << std::endl;
}

bool SetCfProperties(const std::string& name, const std::string& value,
                     ColumnFamilySchema* cf_schema) {
    if (cf_schema == NULL) {
        return false;
    }
    if (name == "ttl") {
        int32_t ttl;
        if (!toft::StringToNumber(value, &ttl) || (ttl < 0)) {
            return false;
        }
        cf_schema->set_time_to_live(ttl);
    } else if (name == "maxversions") {
        int32_t versions;
        if (!toft::StringToNumber(value, &versions) || (versions <= 0)) {
            return false;
        }
        cf_schema->set_max_versions(versions);
    } else if (name == "minversions") {
        int32_t versions;
        if (!toft::StringToNumber(value, &versions) || (versions <= 0)) {
            return false;
        }
        cf_schema->set_min_versions(versions);
    } else if (name == "diskquota") {
        int64_t quota;
        if (!toft::StringToNumber(value, &quota) || (quota <= 0)) {
            return false;
        }
        cf_schema->set_disk_quota(quota);
    } else if (name == "type") {
        if (value != "bytes") {
            return false;
        }
        cf_schema->set_type(value);
    }else {
        return false;
    }
    return true;
}

bool SetLgProperties(const std::string& name, const std::string& value,
                     LocalityGroupSchema* lg_schema) {
    if (lg_schema == NULL) {
        return false;
    }
    if (name == "compress") {
        if (value == "none") {
            lg_schema->set_compress_type(NONE);
        } else if (value == "snappy") {
            lg_schema->set_compress_type(SNAPPY);
        } else {
            return false;
        }
    } else if (name == "storage") {
        if (value == "disk") {
            lg_schema->set_store_type(SATA_TYPE);
        } else if (value == "flash") {
            lg_schema->set_store_type(SSD_TYPE);
        } else if (value == "memory") {
            lg_schema->set_store_type(MEMORY_TYPE);
        } else {
            return false;
        }
    } else if (name == "blocksize") {
        int blocksize;
        if (!toft::StringToNumber(value, &blocksize) || (blocksize <= 0)){
            return false;
        }
        lg_schema->set_block_size(blocksize);
    } else {
        return false;
    }
    return true;
}

bool SetTableProperties(const std::string& name, const std::string& value,
                        TabletSchema* schema) {
    if (schema == NULL) {
        return false;
    }
    if (name == "rawkey") {
        if (value == "readable") {
            schema->set_raw_key_type(Readable);
        } else if (value == "binary") {
            schema->set_raw_key_type(Binary);
        } else if (value == "ttlkv") {
            schema->set_raw_key_type(TTLKv);
        } else if (value == "kv") {
            schema->set_raw_key_type(GeneralKv);
        } else {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool FillTabletSchema(PropTree& schema_tree, TabletSchema* tablet_schema) {
    PropTree::Node* table_node = schema_tree.GetRootNode();
    tablet_schema->set_name(table_node->name_);

    if (schema_tree.MaxDepth() == 1) {
        // kv mode
        tablet_schema->set_raw_key_type(TTLKv);
        LocalityGroupSchema* lg_schema = tablet_schema->add_locality_groups();

        for (std::map<std::string, std::string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, tablet_schema) &&
                !SetLgProperties(i->first, i->second, lg_schema)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }


    } else if (schema_tree.MaxDepth() == 2) {
        // simple table mode, have 1 default lg
        // e.g. table1{cf1, cf2, cf3}

        ColumnFamilySchema* cf_schema = tablet_schema->add_column_families();
        for (size_t i = 0; i < table_node->children_.size(); ++i) {
            PropTree::Node* cf_node = table_node->children_[i];

            for (std::map<std::string, std::string>::iterator it = cf_node->properties_.begin();
                 it != cf_node->properties_.end(); ++it) {
                if (!SetCfProperties(it->first, it->second, cf_schema)) {
                    LOG(ERROR) << "illegal value: " << it->second
                        << " for cf property: " << it->first;
                    return false;
                }
            }
        }

        for (std::map<std::string, std::string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, tablet_schema)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }
    } else if (schema_tree.MaxDepth() == 3) {
        // full mode, all elements are user-defined
        // e.g. table1<mergesize=100>{
        //          lg0<storage=memory>{
        //              cf1<maxversions=3>,
        //              cf2<ttl=100>
        //          },
        //          lg1{cf3}
        //      }
        for (size_t i = 0; i < table_node->children_.size(); ++i) {
            PropTree::Node* lg_node = table_node->children_[i];
            LocalityGroupSchema* lg_schema = tablet_schema->add_locality_groups();
            lg_schema->set_name(lg_node->name_);

            // add all column families and properties
            for (size_t j = 0; j < lg_node->children_.size(); ++j) {
                PropTree::Node* cf_node = lg_node->children_[j];
                ColumnFamilySchema* cf_schema = tablet_schema->add_column_families();
                cf_schema->set_name(cf_node->name_);
                for (std::map<std::string, std::string>::iterator it = cf_node->properties_.begin();
                     it != cf_node->properties_.end(); ++it) {
                    if (!SetCfProperties(it->first, it->second, cf_schema)) {
                        LOG(ERROR) << "illegal value: " << it->second
                            << " for cf property: " << it->first;
                        return false;
                    }
                }
            }
            // set locality group properties
            for (std::map<std::string, std::string>::iterator it_lg = lg_node->properties_.begin();
                 it_lg != lg_node->properties_.end(); ++it_lg) {
                if (!SetLgProperties(it_lg->first, it_lg->second, lg_schema)) {
                    LOG(ERROR) << "illegal value: " << it_lg->second
                        << " for lg property: " << it_lg->first;
                    return false;
                }
            }
        }
        // set table properties
        for (std::map<std::string, std::string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, tablet_schema)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }

    } else {
        LOG(FATAL) << "somehing wrong";
    }
    return true;
}

bool ParseTableSchemaFile(const std::string& file, TabletSchema* tablet_schema) {
    PropTree schema_tree;
    if (!schema_tree.ParseFromFile(file)) {
        LOG(ERROR) << schema_tree.State();
        LOG(ERROR) << file;
        return false;
    }

    VLOG(10) << "table to create: " << schema_tree.FormatString();
    if (FillTabletSchema(schema_tree, tablet_schema)) {
        return true;
    }
    return false;
}

} // namespace xsheet
