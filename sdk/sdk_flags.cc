// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "thirdparty/gflags/gflags.h"

DEFINE_int64(xsheet_sdk_scan_buffer_size, 65536, "default buffer limit for scan");
DEFINE_int64(xsheet_sdk_scan_number_limit, 1000000000, "default number limit for scan");
DEFINE_int32(xsheet_sdk_max_batch_scan_req, 30, "the max number of concurrent scan req");
DEFINE_int32(xsheet_sdk_batch_scan_max_retry, 60, "the max retry times for session scan");
DEFINE_int64(xsheet_sdk_scan_timeout, 30000, "scan timeout");
DEFINE_int64(batch_scan_delay_retry_in_us, 1000000, "timewait in us before retry batch scan");
