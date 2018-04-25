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

#include "engine/tablet_utils.h"
#include "engine/base_options.h"

DECLARE_int32(xsheet_asyncwriter_pending_limit);
// DECLARE_bool(xsheet_enable_level0_limit);
DECLARE_int32(xsheet_asyncwriter_sync_interval);
DECLARE_int32(xsheet_asyncwriter_sync_size_threshold);
DECLARE_int32(xsheet_asyncwriter_batch_size);
// DECLARE_bool(xsheet_sync_log);

namespace xsheet {

TabletWriter::TabletWriter(const TabletSchema& schema, KvBase* kvbase)
    : tablet_schema_(schema), kvbase_(kvbase), stopped_(true),
      sync_timestamp_(0),
      active_buffer_instant_(false),
      active_buffer_size_(0),
      tablet_busy_(false) {
    active_buffer_ = new WriteTaskBuffer;
    sealed_buffer_ = new WriteTaskBuffer;
    key_operator_ = utils::GetRawKeyOperatorFromSchema(tablet_schema_);
}

TabletWriter::~TabletWriter() {
    Stop();
    delete active_buffer_;
    delete sealed_buffer_;
}

void TabletWriter::Start() {
    {
        toft::MutexLock lock(&status_mutex_);
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
        toft::MutexLock lock(&status_mutex_);
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
        int32_t mu_num = mu_seq.mutation_sequence_size();
        for (int32_t j = 0; j < mu_num; j++) {
            const Mutation& mu = mu_seq.mutation_sequence(j);
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
    const uint64_t MAX_PENDING_SIZE = FLAGS_xsheet_asyncwriter_pending_limit * 1024UL;

    toft::MutexLock lock(&task_mutex_);
    if (stopped_) {
        LOG(ERROR) << "tablet writer is stopped";
        return kTabletAsyncNotRunning;
    }
    if (active_buffer_size_ >= MAX_PENDING_SIZE || tablet_busy_) {
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "[" << tablet_->GetTablePath()
                << "] is too busy, active_buffer_size_: "
                << (active_buffer_size_>>10) << "KB, tablet_busy_: "
                << tablet_busy_;
            last_print = now_time;
        }
        return kTabletAsyncTooBusy;
    }

    uint64_t request_size = CountRequestSize(*row_mutation_vec, tablet_->KvOnly());
    WriteTask task;
    task.row_mutation_vec = row_mutation_vec;
    task.status_vec = status_vec;
    task.callback = callback;

    active_buffer_->push_back(task);
    active_buffer_size_ += request_size;
    active_buffer_instant_ |= is_instant;
    if (active_buffer_size_ >= FLAGS_xsheet_asyncwriter_sync_size_threshold * 1024UL ||
        active_buffer_instant_) {
        write_event_.Set();
    }
    return kTabletOk;
}

void TabletWriter::DoWork() {
    sync_timestamp_ = toft::GetTimeStampInMs();
    int32_t sync_interval = FLAGS_xsheet_asyncwriter_sync_interval;
    if (sync_interval == 0) {
        sync_interval = 1;
    }

    while (!stopped_) {
        int64_t sleep_duration =
            sync_timestamp_ + sync_interval - toft::GetTimeStampInMs();
        if (!SwapActiveBuffer(sleep_duration <= 0)) {
            if (sleep_duration <= 0) {
                sync_timestamp_ = toft::GetTimeStampInMs();
            } else {
                write_event_.TimeWait(sleep_duration);
            }
            continue;
        }
        VLOG(7) << "write data, sleep_duration: " << sleep_duration;

        FlushToDiskBatch(sealed_buffer_);
        sealed_buffer_->clear();
        sync_timestamp_ = toft::GetTimeStampInMs();
    }
    LOG(INFO) << "AsyncWriter::DoWork done";
    worker_done_event_.Set();
}

bool TabletWriter::SwapActiveBuffer(bool force) {
    const uint64_t SYNC_SIZE = FLAGS_xsheet_asyncwriter_sync_size_threshold * 1024UL;
//     if (FLAGS_xsheet_enable_level0_limit == true) {
//         tablet_busy_ = tablet_->IsBusy();
//     }

    toft::MutexLock lock(&task_mutex_);
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
            StatusCode* status = &((*status_vec)[i]);
            const RowMutationSequence& row_mu = *row_mutation_vec[i];
            const std::string& row_key = row_mu.row_key();
            int32_t mu_num = row_mu.mutation_sequence().size();
            if (*status != kTabletNodeOk) {
                VLOG(11) << "batch write fail, row " << DebugString(row_key)
                    << ", status " << StatusCodeToString(*status);
                continue;
            }
            if (mu_num == 0) {
                continue;
            }
            if (tablet_schema_.raw_key() == TTLKv || tablet_schema_.raw_key() == GeneralKv) {
                // only the last mutation take effect for kv
                const Mutation& mu = row_mu.mutation_sequence().Get(mu_num - 1);
                std::string raw_key;
                if (tablet_schema_.raw_key() == TTLKv) { // TTL-KV
                    if (mu.ttl() == -1) { // never expires
                        key_opeartor_->EncodeTeraKey(row_key, "", "",
                                kLatestTs, leveldb::TKT_FORSEEK, &raw_key);
                    } else { // no check of overflow risk ...
                        key_opeartor_->EncodeTeraKey(row_key, "", "",
                                toft::GetTimeStampInMs() / 1000000 + mu.ttl(), leveldb::TKT_FORSEEK, &raw_key);
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
                    const Mutation& mu = row_mu.mutation_sequence().Get(t);
                    std::string raw_key;
                    leveldb::TeraKeyType type = leveldb::TKT_VALUE;
                    switch (mu.type()) {
                        case kDeleteRow:
                            type = leveldb::TKT_DEL;
                            break;
                        case kDeleteFamily:
                            type = leveldb::TKT_DEL_COLUMN;
                            break;
                        case kDeleteColumn:
                            type = leveldb::TKT_DEL_QUALIFIER;
                            break;
                        case kDeleteColumns:
                            type = leveldb::TKT_DEL_QUALIFIERS;
                            break;
                        case kAdd:
                            type = leveldb::TKT_ADD;
                            break;
                        case kAddInt64:
                            type = leveldb::TKT_ADDINT64;
                            break;
                        case kPutIfAbsent:
                            type = leveldb::TKT_PUT_IFABSENT;
                            break;
                        case kAppend:
                            type = leveldb::TKT_APPEND;
                            break;
                        default:
                            break;
                    }
                    int64_t timestamp = get_unique_micros(timestamp_old);
                    timestamp_old = timestamp;
                    if (tablet_->GetSchema().enable_txn()) {
                        if (mu.has_timestamp() && (mu.timestamp() != kLatestTimestamp)) {
                            timestamp = mu.timestamp();
                        }
                    } else {
                        if (leveldb::TeraKey::IsTypeAllowUserSetTimestamp(type) &&
                            mu.has_timestamp() && mu.timestamp() < timestamp) {
                            timestamp = mu.timestamp();
                        }
                    }
                    key_opeartor_->EncodeTeraKey(row_key, mu.family(), mu.qualifier(),
                            timestamp, type, &raw_key);
                    uint32_t lg_id = 0;
                    size_t lg_num = tablet_->ldb_options_.exist_lg_list->size();
                    if (lg_num > 1) {
                        if (type != leveldb::TKT_DEL) {
                            lg_id = tablet_->GetLGidByCFName(mu.family());
                            leveldb::PutFixed32LGId(&raw_key, lg_id);
                            VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                                << " family: " << mu.family() << ", lg_id: " << lg_id;
                            batch->Put(raw_key, mu.value());
                        } else {
                            // put row_del mark to all LGs
                            for (lg_id = 0; lg_id < lg_num; ++lg_id) {
                                std::string raw_key_tmp = raw_key;
                                leveldb::PutFixed32LGId(&raw_key_tmp, lg_id);
                                VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                                    << " family: " << mu.family() << ", lg_id: " << lg_id;
                                batch->Put(raw_key_tmp, mu.value());
                            }
                        }
                    } else {
                        VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                            << " family: " << mu.family() << ", qualifier " << mu.qualifier()
                            << ", ts " << timestamp << ", type " << type << ", lg_id: " << lg_id;
                        batch->Put(raw_key, mu.value());
                    }
                }
            }
        }
    }
    return;
}

void TabletWriter::FinishTask(WriteTaskBuffer* task_buffer, StatusCode status) {
    for (uint32_t task_idx = 0; task_idx < task_buffer->size(); ++task_idx) {
        WriteTask& task = (*task_buffer)[task_idx];
        tablet_->GetCounter().write_rows.Add(task.row_mutation_vec->size());
        for (uint32_t i = 0; i < task.row_mutation_vec->size(); i++) {
            tablet_->GetCounter().write_kvs.Add((*task.row_mutation_vec)[i]->mutation_sequence_size());
            // set batch_write status for row_mu
            if ((*task.status_vec)[i] == kTabletNodeOk) {
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
    if (row_mu.txn_read_info().has_read()) {
        if (!tablet_->GetSchema().enable_txn()) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: txn not enabled";
            SetStatusCode(kTxnFail, status);
            return true;
        }
        if (commit_row_key_set->find(row_key) != commit_row_key_set->end()) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: found same row in one batch";
            SetStatusCode(kTxnFail, status);
            return true;
        }
        if (!tablet_->SingleRowTxnCheck(row_key, row_mu.txn_read_info(), status)) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: check fail, status: "
                     << StatusCodeToString(*status);
            return true;
        }
        VLOG(10) << "txn of row " << DebugString(row_key) << " check pass";
    }
    commit_row_key_set->insert(row_key);
    return false;
}

bool TabletWriter::CheckIllegalRowArg(const RowMutationSequence& row_mu,
                                      const std::set<std::string>& cf_set,
                                      StatusCode* status) {
    // check arguments
    if (row_mu.row_key().size() >= 64 * 1024) {
        SetStatusCode(kTableInvalidArg, status);
        return true;
    }
    for (int32_t i = 0; i < row_mu.mutation_sequence().size(); ++i) {
        const Mutation& mu = row_mu.mutation_sequence(i);
        if (mu.value().size() >= 32 * 1024 * 1024) {
            SetStatusCode(kTableInvalidArg, status);
            return true;
        }
        if (!tablet_->KvOnly()) {
            if (mu.qualifier().size() >= 64 * 1024) {     // 64KB
                SetStatusCode(kTableInvalidArg, status);
                return true;
            }
            if (mu.type() != kDeleteRow &&
               (cf_set.find(mu.family()) == cf_set.end())) {
                SetStatusCode(kTableInvalidArg, status);
                VLOG(11) << "batch write check, illegal cf, row " << DebugString(row_mu.row_key())
                    << ", cf " << mu.family() << ", qu " << mu.qualifier()
                    << ", ts " << mu.timestamp() << ", type " << mu.type()
                    << ", cf_set.size " << cf_set.size()
                    << ", status " << StatusCodeToString(*status);
                return true;
            }
        }
    }
    return false;
}

void TabletWriter::CheckRows(WriteTaskBuffer* task_buffer) {
    std::set<std::string> cf_set;
    TableSchema schema = tablet_->GetSchema();
    for (int32_t cf_idx = 0; cf_idx < schema.column_families_size(); ++cf_idx) {
        cf_set.insert(schema.column_families(cf_idx).name());
    }

    std::set<std::string> commit_row_key_set;
    for (uint32_t task_idx = 0; task_idx < task_buffer->size(); ++task_idx) {
        WriteTask& task = (*task_buffer)[task_idx];
        std::vector<const RowMutationSequence*>& row_mutation_vec = *task.row_mutation_vec;
        std::vector<StatusCode>& status_vec = *task.status_vec;

        for (uint32_t row_idx = 0; row_idx < row_mutation_vec.size(); ++row_idx) {
            const RowMutationSequence* row_mu = row_mutation_vec[row_idx];
            if(CheckSingleRowTxnConflict(*row_mu, &commit_row_key_set, &status_vec[row_idx])) {
                continue;
            }
            if (CheckIllegalRowArg(*row_mu, cf_set, &status_vec[row_idx])) {
                continue;
            }
            status_vec[row_idx] = kTabletNodeOk;
        }
    }
    return;
}

StatusCode TabletWriter::FlushToDiskBatch(WriteTaskBuffer* task_buffer) {
    int64_t ts = toft::GetTimeStampInMs();
    CheckRows(task_buffer);

    WriteBatch batch;
    BatchRequest(task_buffer, &batch);
    StatusCode status = kTabletOk;
    const bool disable_wal = false;
    tablet_->WriteBatch(&batch, disable_wal, FLAGS_xsheet_sync_log, &status);
    batch.Clear();

    FinishTask(task_buffer, status);
    VLOG(7) << "finish a batch: " << task_buffer->size() << ", use " << toft::GetTimeStampInMs() - ts;
    return status;
}

} // namespace xsheet
