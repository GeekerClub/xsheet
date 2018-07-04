// Copyright (C) 2018, For GeekerClub authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_TXN_TXN_H
#define XSHEET_TXN_TXN_H

class Txn {
public:
    Txn();
    ~Txn();

    void StartTransaction();
    void PreCommit();
    void Commit();
    void Rollback();

private:

};

#endif // XSHEET_TXN_TXN_H
