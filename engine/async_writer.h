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

#include "proto/status_code.pb.h"
#include "proto/tablet.pb.h"

namespace leveldb {
class WriteBatch;
}

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
    TabletWriter(leveldb::DB* ldb);
    ~TabletWriter();
    bool Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
               std::vector<StatusCode>* status_vec, bool is_instant,
               WriteCallback callback, StatusCode* status = NULL);
    static uint64_t CountRequestSize(std::vector<const RowMutationSequence*>& row_mutation_vec,
                                     bool kv_only);
    void Start();
    void Stop();

private:
    void DoWork();
    bool SwapActiveBuffer(bool force);
    void BatchRequest(WriteTaskBuffer* task_buffer,
                      leveldb::WriteBatch* batch);
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
    leveldb::DB* ldb_;

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
