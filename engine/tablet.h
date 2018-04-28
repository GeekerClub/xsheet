// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_TABLET_H
#define XSHEET_ENGINE_TABLET_H

#include "toft/base/closure.h"

#include "proto/tablet.pb.h"

namespace xsheet {

class Tablet {
public:

    typedef toft::Closure<void (const RowMutationSequence*, StatusCode)> WriteCallback;


    Tablet();
    ~Tablet();

    StatusCode  Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                      std::vector<StatusCode>* status_vec, WriteCallback callback);
    StatusCode  Write(const RowMutationSequence* row_mutation, WriteCallback callback);
};

} // namespace xsheet

#endif // XSHEET_ENGINE_TABLET_H
