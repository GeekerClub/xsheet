// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/tablet_writer.h"

#include <set>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "toft/system/threading/this_thread.h"
#include "toft/system/time/timestamp.h"

#include "engine/codec/string_utils.h"
#include "engine/kvbase/base_options.h"
#include "engine/tablet_utils.h"
#include "engine/types.h"

DECLARE_int32(engine_writer_pending_limit);
DECLARE_int32(engine_writer_sync_interval);
DECLARE_int32(engine_writer_sync_size_threshold);
DECLARE_int32(xsheet_asyncwriter_batch_size);

namespace xsheet {

TabletWriter::TabletWriter(const TabletSchema& schema, KvBase* kvbase)
    : tablet_schema_(schema), kvbase_(kvbase),
      key_operator_(utils::GetRawKeyOperatorFromSchema(tablet_schema_)),
      stopped_(true),
      sync_timestamp_(0),
      active_buffer_instant_(false),
      active_buffer_size_(0),
      tablet_busy_(false) {
    active_buffer_ = new WriteTaskBuffer;
    sealed_buffer_ = new WriteTaskBuffer;
}

TabletWriter::~TabletWriter() {
    Stop();
    delete active_buffer_;
    delete sealed_buffer_;
}

void TabletWriter::Start() {
    {
        toft::MutexLocker lock(&status_mutex_);
        if (!stopped_) {
            LOG(WARNING) << "tablet writer has been started";
            return;
        }
        stopped_ = false;
    }
    LOG(INFO) << "start tablet writer ...";
    thread_.Start(std::bind(&TabletWriter::DoWork, this));
    toft::ThisThread::Yield();
}

void TabletWriter::Stop() {
    {
        toft::MutexLocker lock(&status_mutex_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
    }

    worker_done_event_.Wait();

    FlushToDiskBatch(sealed_buffer_);
    FlushToDiskBatch(active_buffer_);

    LOG(INFO) << "tablet writer is stopped";
}

uint64_t TabletWriter::CountRequestSize(std::vector<const RowMutationSequence*>& row_mutation_vec,
                                        bool kv_only) {
    uint64_t data_size = 0;
    for (uint32_t i = 0; i < row_mutation_vec.size(); i++) {
        const RowMutationSequence& mu_seq = *row_mutation_vec[i];
        int32_t mu_num = mu_seq.mutation_list_size();
        for (int32_t j = 0; j < mu_num; j++) {
            const Mutation& mu = mu_seq.mutation_list(j);
            data_size += mu_seq.row_key().size() + mu.value().size();
            if (!kv_only) {
                data_size += mu.family().size()
                    + mu.qualifier().size()
                    + sizeof(mu.timestamp());
            }
        }
    }
    return data_size;
}

StatusCode TabletWriter::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                         std::vector<StatusCode>* status_vec, WriteCallback callback) {
    static uint32_t last_print = time(NULL);
    const uint64_t MAX_PENDING_SIZE = FLAGS_engine_writer_pending_limit * 1024UL;

    toft::MutexLocker lock(&task_mutex_);
    if (stopped_) {
        LOG(ERROR) << "tablet writer is stopped";
        return kTabletAsyncNotRunning;
    }
    if (active_buffer_size_ >= MAX_PENDING_SIZE || tablet_busy_) {
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "[" << kvbase_->GetPath()
                << "] is too busy, active_buffer_size_: "
                << (active_buffer_size_>>10) << "KB, tablet_busy_: "
                << tablet_busy_;
            last_print = now_time;
        }
        return kTabletAsyncTooBusy;
    }

    uint64_t request_size = CountRequestSize(*row_mutation_vec,
                    (tablet_schema_.raw_key_type() == TTLKv || tablet_schema_.raw_key_type() == GeneralKv));
    WriteTask task;
    task.row_mutation_vec = row_mutation_vec;
    task.status_vec = status_vec;
    task.callback = callback;

    active_buffer_->push_back(task);
    active_buffer_size_ += request_size;
//     active_buffer_instant_ |= is_instant;
    if (active_buffer_size_ >= FLAGS_engine_writer_sync_size_threshold * 1024UL ||
        active_buffer_instant_) {
        write_event_.Set();
    }
    return kTabletOk;
}

void TabletWriter::DoWork() {
    sync_timestamp_ = toft::GetTimestampInMs();
    int32_t sync_interval = FLAGS_engine_writer_sync_interval;
    if (sync_interval == 0) {
        sync_interval = 1;
    }

    while (!stopped_) {
        int64_t sleep_duration =
            sync_timestamp_ + sync_interval - toft::GetTimestampInMs();
        if (!SwapActiveBuffer(sleep_duration <= 0)) {
            if (sleep_duration <= 0) {
                sync_timestamp_ = toft::GetTimestampInMs();
            } else {
                write_event_.TimedWait(sleep_duration);
            }
            continue;
        }
        VLOG(7) << "write data, sleep_duration: " << sleep_duration;

        FlushToDiskBatch(sealed_buffer_);
        sealed_buffer_->clear();
        sync_timestamp_ = toft::GetTimestampInMs();
    }
    LOG(INFO) << "AsyncWriter::DoWork done";
    worker_done_event_.Set();
}

bool TabletWriter::SwapActiveBuffer(bool force) {
    const uint64_t SYNC_SIZE = FLAGS_engine_writer_sync_size_threshold * 1024UL;
//     if (FLAGS_xsheet_enable_level0_limit == true) {
//         tablet_busy_ = tablet_->IsBusy();
//     }

    toft::MutexLocker lock(&task_mutex_);
    if (active_buffer_->size() <= 0) {
        return false;
    }
    if (!force && !active_buffer_instant_ && active_buffer_size_ < SYNC_SIZE) {
        return false;
    }
    VLOG(7) << "SwapActiveBuffer, buffer:" << active_buffer_size_
        << ":" <<active_buffer_->size() << ", force:" << force
        << ", instant:" << active_buffer_instant_;
    WriteTaskBuffer* temp = active_buffer_;
    active_buffer_ = sealed_buffer_;
    sealed_buffer_ = temp;
    CHECK_EQ(0U, active_buffer_->size());

    active_buffer_size_ = 0;
    active_buffer_instant_ = false;

    return true;
}

void TabletWriter::BatchRequest(WriteTaskBuffer* task_buffer, WriteBatch* batch) {
    int64_t timestamp_old = 0;
    for (uint32_t task_idx = 0; task_idx < task_buffer->size(); ++task_idx) {
        WriteTask& task = (*task_buffer)[task_idx];
        const std::vector<const RowMutationSequence*>& row_mutation_vec = *(task.row_mutation_vec);
        std::vector<StatusCode>* status_vec = task.status_vec;

        for (uint32_t i = 0; i < row_mutation_vec.size(); ++i) {
            StatusCode status = (*status_vec)[i];
            const RowMutationSequence& row_mu = *row_mutation_vec[i];
            const std::string& row_key = row_mu.row_key();
            int32_t mu_num = row_mu.mutation_list().size();
            if (status != kTabletOk) {
                VLOG(11) << "batch write fail, row " << DebugString(row_key)
                    << ", status " << StatusCode_Name(status);
                continue;
            }
            if (mu_num == 0) {
                continue;
            }
            if (tablet_schema_.raw_key_type() == TTLKv || tablet_schema_.raw_key_type() == GeneralKv) {
                // only the last mutation take effect for kv
                const Mutation& mu = row_mu.mutation_list().Get(mu_num - 1);
                std::string raw_key;
                if (tablet_schema_.raw_key_type() == TTLKv) { // TTL-KV
                    if (mu.ttl() == -1) { // never expires
                        key_operator_->EncodeRawKey(row_key, "", "",
                                kLatestTimestamp, TKT_FORSEEK, &raw_key);
                    } else { // no check of overflow risk ...
                        key_operator_->EncodeRawKey(row_key, "", "",
                                toft::GetTimestampInMs() / 1000000 + mu.ttl(), TKT_FORSEEK, &raw_key);
                    }
                } else { // Readable-KV
                    raw_key.assign(row_key);
                }
                if (mu.type() == kPut) {
                    batch->Put(raw_key, mu.value());
                } else {
                    batch->Delete(raw_key);
                }
            } else {
                for (int32_t t = 0; t < mu_num; ++t) {
                    const Mutation& mu = row_mu.mutation_list().Get(t);
                    std::string raw_key;
                    RawKeyType type = TKT_VALUE;
                    switch (mu.type()) {
                        case kDeleteRow:
                            type = TKT_DEL;
                            break;
                        case kDeleteFamily:
                            type = TKT_DEL_COLUMN;
                            break;
                        case kDeleteColumn:
                            type = TKT_DEL_QUALIFIER;
                            break;
                        case kDeleteColumns:
                            type = TKT_DEL_QUALIFIERS;
                            break;
                        case kAdd:
                            type = TKT_ADD;
                            break;
                        case kAddInt64:
                            type = TKT_ADDINT64;
                            break;
                        case kPutIfAbsent:
                            type = TKT_PUT_IFABSENT;
                            break;
                        case kAppend:
                            type = TKT_APPEND;
                            break;
                        default:
                            break;
                    }
                    int64_t timestamp = toft::GetTimestampInMs();
                    CHECK(timestamp != timestamp_old);
                    timestamp_old = timestamp;
                    if (RawKey::IsTypeAllowUserSetTimestamp(type) &&
                        mu.has_timestamp() && mu.timestamp() < timestamp) {
                        timestamp = mu.timestamp();
                    }
                    key_operator_->EncodeRawKey(row_key, mu.family(), mu.qualifier(),
                            timestamp, type, &raw_key);
                    VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                        << " family: " << mu.family() << ", qualifier " << mu.qualifier()
                        << ", ts " << timestamp << ", type " << type;
                    batch->Put(raw_key, mu.value());
                }
            }
        }
    }
    return;
}

void TabletWriter::FinishTask(WriteTaskBuffer* task_buffer, StatusCode status) {
    for (uint32_t task_idx = 0; task_idx < task_buffer->size(); ++task_idx) {
        WriteTask& task = (*task_buffer)[task_idx];
        for (uint32_t i = 0; i < task.row_mutation_vec->size(); i++) {
            if ((*task.status_vec)[i] == kTabletOk) {
                (*task.status_vec)[i] = status;
            }
        }
        task.callback(task.row_mutation_vec, task.status_vec);
    }
    return;
}

// set status to kTxnFail, if transaction conflicts.
bool TabletWriter::CheckSingleRowTxnConflict(const RowMutationSequence& row_mu,
                                             std::set<std::string>* commit_row_key_set,
                                             StatusCode* status) {
    const std::string& row_key = row_mu.row_key();
//     if (row_mu.txn_read_info().has_read()) {
//         if (!tablet_->GetSchema().enable_txn()) {
//             VLOG(10) << "txn of row " << DebugString(row_key)
//                      << " is interrupted: txn not enabled";
//             SetStatusCode(kTxnFail, status);
//             return true;
//         }
//         if (commit_row_key_set->find(row_key) != commit_row_key_set->end()) {
//             VLOG(10) << "txn of row " << DebugString(row_key)
//                      << " is interrupted: found same row in one batch";
//             SetStatusCode(kTxnFail, status);
//             return true;
//         }
//         if (!tablet_->SingleRowTxnCheck(row_key, row_mu.txn_read_info(), status)) {
//             VLOG(10) << "txn of row " << DebugString(row_key)
//                      << " is interrupted: check fail, status: "
//                      << StatusCodeToString(*status);
//             return true;
//         }
//         VLOG(10) << "txn of row " << DebugString(row_key) << " check pass";
//     }
    commit_row_key_set->insert(row_key);
    return false;
}

StatusCode TabletWriter::CheckIllegalRowArg(const RowMutationSequence& row_mu,
                                      const std::set<std::string>& cf_set) {
    // check arguments
    if (row_mu.row_key().size() >= 64 * 1024) {
        return kTabletInvalidArg;
    }
    for (int32_t i = 0; i < row_mu.mutation_list().size(); ++i) {
        const Mutation& mu = row_mu.mutation_list(i);
        if (mu.value().size() >= 32 * 1024 * 1024) {
            return kTabletInvalidArg;
        }

        if (tablet_schema_.raw_key_type() != TTLKv && tablet_schema_.raw_key_type() != GeneralKv) {
            if (mu.qualifier().size() >= 64 * 1024) {     // 64KB
                return kTabletInvalidArg;
            }
            if (mu.type() != kDeleteRow &&
               (cf_set.find(mu.family()) == cf_set.end())) {
                VLOG(11) << "batch write check, illegal cf, row " << DebugString(row_mu.row_key())
                    << ", cf " << mu.family() << ", qu " << mu.qualifier()
                    << ", ts " << mu.timestamp() << ", type " << mu.type()
                    << ", cf_set.size " << cf_set.size();
                return kTabletInvalidArg;
            }
        }
    }
    return kTabletOk;
}

void TabletWriter::CheckRows(WriteTaskBuffer* task_buffer) {
    std::set<std::string> cf_set;
    for (int32_t cf_idx = 0; cf_idx < tablet_schema_.column_families_size(); ++cf_idx) {
        cf_set.insert(tablet_schema_.column_families(cf_idx).name());
    }

    std::set<std::string> commit_row_key_set;
    for (uint32_t task_idx = 0; task_idx < task_buffer->size(); ++task_idx) {
        WriteTask& task = (*task_buffer)[task_idx];
        std::vector<const RowMutationSequence*>& row_mutation_vec = *task.row_mutation_vec;
        std::vector<StatusCode>& status_vec = *task.status_vec;

        for (uint32_t row_idx = 0; row_idx < row_mutation_vec.size(); ++row_idx) {
            const RowMutationSequence* row_mu = row_mutation_vec[row_idx];
//             if(CheckSingleRowTxnConflict(*row_mu, &commit_row_key_set, &status_vec[row_idx])) {
//                 continue;
//             }
            status_vec[row_idx] = CheckIllegalRowArg(*row_mu, cf_set);
            if (status_vec[row_idx] != kTabletOk) {
                continue;
            }
        }
    }
    return;
}

StatusCode TabletWriter::FlushToDiskBatch(WriteTaskBuffer* task_buffer) {
    int64_t ts = toft::GetTimestampInMs();
    CheckRows(task_buffer);

    WriteBatch batch;
    BatchRequest(task_buffer, &batch);
    StatusCode status = kTabletOk;
    const bool disable_wal = false;
    status = kvbase_->Write(WriteOptions(), &batch);
    batch.Clear();

    FinishTask(task_buffer, status);
    VLOG(7) << "finish a batch: " << task_buffer->size() << ", use " << toft::GetTimestampInMs() - ts;
    return status;
}

} // namespace xsheet
