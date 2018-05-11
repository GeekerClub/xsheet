// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  XSHEET_ERROR_CODE_H
#define  XSHEET_ERROR_CODE_H

#include <string>

namespace xsheet {

class ErrorCode {
public:
    enum ErrorCodeType {
        kOK        = 0,
        kNotFound  = 1,
        kBadParam  = 2,
        kSystem    = 3,
        kTimeout   = 4,
        kBusy      = 5,
        kNoQuota   = 6,
        kNoAuth    = 7,
        kUnknown   = 8,
        kNotImpl   = 9,
        kTxnFail   = 10
    };

public:
    // Returns a string includes type&reason
    // Format: "type [kOK], reason [success]"
    std::string ToString() const;

    ErrorCodeType GetType() const;
    std::string GetReason() const;

    // Internal funcion, do not use
    ErrorCode();
    void SetFailed(ErrorCodeType err, const std::string& reason = "");

private:
    ErrorCodeType err_;
    std::string reason_;
};

// DEPRECATED. Use error_code.ToString() instead.
const char* strerr(ErrorCode error_code);

} // namespace xsheet

#endif  // XSHEET_ERROR_CODE_H
