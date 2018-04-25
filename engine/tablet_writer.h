// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_KVBASE_ASYNC_WRITER_H
#define XSHEET_ENGINE_KVBASE_ASYNC_WRITER_H

#include <functional>

#include "toft/system/threading/event.h"
#include "toft/system/threading/mutex.h"
#include "toft/system/threading/thread.h"

#include "engine/kvbase/kv_base.h"
#include "engine/tablet_schema.pb.h"
#include "proto/status_code.pb.h"
#include "proto/tablet.pb.h"

namespace xsheet {


class TabletWriter {
public:
    typedef std::function<void (std::vector<const RowMutationSequence*>*, \
                                std::vector<StatusCode>*)> WriteCallback;

    struct WriteTask {
        std::vector<const RowMutationSequence*>* row_mutation_vec;
        std::vector<StatusCode>* status_vec;
        WriteCallback callback;
    };

    typedef std::vector<WriteTask> WriteTaskBuffer;

public:
    TabletWriter(const TabletSchema& schema, KvBase* kvbase);
    ~TabletWriter();
    StatusCode Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                     std::vector<StatusCode>* status_vec, WriteCallback callback);
    static uint64_t CountRequestSize(std::vector<const RowMutationSequence*>& row_mutation_vec,
                                     bool kv_only);
    void Start();
    void Stop();

private:
    void DoWork();
    bool SwapActiveBuffer(bool force);
    void BatchRequest(WriteTaskBuffer* task_buffer, WriteBatch* batch);
    bool CheckSingleRowTxnConflict(const RowMutationSequence& row_mu,
                                   std::set<std::string>* commit_row_key_set,
                                   StatusCode* status);
    bool CheckIllegalRowArg(const RowMutationSequence& row_mu,
                            const std::set<std::string>& cf_set,
                            StatusCode* status);
    void CheckRows(WriteTaskBuffer* task_buffer);
    void FinishTask(WriteTaskBuffer* task_buffer, StatusCode status);
    StatusCode FlushToDiskBatch(WriteTaskBuffer* task_buffer);

private:
    TabletSchema tablet_schema_;
    KvBase* kvbase_;
    RawKeyOperator* key_operator_;

    mutable toft::Mutex task_mutex_;
    mutable toft::Mutex status_mutex_;
    toft::AutoResetEvent write_event_;
    toft::AutoResetEvent worker_done_event_;

    bool stopped_;
    toft::Thread thread_;

    WriteTaskBuffer* active_buffer_;
    WriteTaskBuffer* sealed_buffer_;
    int64_t sync_timestamp_;

    bool active_buffer_instant_;
    uint64_t active_buffer_size_;
    bool tablet_busy_;
};

} // namespace xsheet

#endif // XSHEET_ENGINE_KVBASE_ASYNC_WRITER_H
