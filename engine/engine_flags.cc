// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "thirdparty/gflags/gflags.h"


// engine

DEFINE_string(engine_leveldb_env_type, "local", "base env for leveldb");

DEFINE_int32(engine_writer_pending_limit, 10000, "the max pending data size (KB) in async writer");
DEFINE_int32(engine_writer_sync_interval, 100, "the interval (in ms) to sync write buffer to disk");
DEFINE_int32(engine_writer_sync_size_threshold, 1024, "force sync per X KB");
DEFINE_int32(xsheet_asyncwriter_batch_size, 1024, "write batch to leveldb per X KB");




