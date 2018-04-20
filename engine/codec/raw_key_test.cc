// Copyright (C) 2018, For authors
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <sys/time.h>
#include <iostream>

#include "thirdparty/gtest/gtest.h"

#include "engine/codec//raw_key.h"
#include "engine/codec//raw_key_operator.h"


namespace xsheet {


void TestFunc(const RawKeyOperator* op) {
    RawKey tk(op);
    ASSERT_TRUE(tk.empty());
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x0001020304050607;
    RawKeyType type = TKT_VALUE;

    ASSERT_TRUE(tk.Encode(key, column, qualifier, timestamp, type));
    ASSERT_TRUE(!tk.empty());
    ASSERT_EQ(tk.key().as_string(), key);
    ASSERT_EQ(tk.column().as_string(), column);
    ASSERT_EQ(tk.qualifier().as_string(), qualifier);
    ASSERT_EQ(tk.timestamp(), timestamp);
    ASSERT_EQ(tk.type(), type);
    std::cout << tk.DebugString() << std::endl;

    std::string temp_key = tk.raw_key().as_string();
    RawKey tk2(op);
    ASSERT_TRUE(tk2.Decode(temp_key));
    ASSERT_EQ(tk2.Compare(tk), 0);
    ASSERT_TRUE(!tk2.empty());
    ASSERT_EQ(tk2.key().as_string(), key);
    ASSERT_EQ(tk2.column().as_string(), column);
    ASSERT_EQ(tk2.qualifier().as_string(), qualifier);
    ASSERT_EQ(tk2.timestamp(), timestamp);
    ASSERT_EQ(tk2.type(), type);
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(tk.SameColumn(tk2));
    ASSERT_TRUE(tk.SameQualifier(tk2));

    ASSERT_TRUE(tk2.Encode("haha", column, qualifier, 0, TKT_VALUE));
    ASSERT_LT(tk2.Compare(tk), 0);
    ASSERT_TRUE(!tk.SameRow(tk2));
    ASSERT_TRUE(!tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk2.Encode(key, "hello", "world", 0, TKT_VALUE));
    ASSERT_GT(tk2.Compare(tk), 0);
    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(!tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    ASSERT_TRUE(tk2.Encode(key, column, "world", 0, TKT_VALUE));
    ASSERT_GT(tk2.Compare(tk), 0);
    ASSERT_TRUE(tk.SameRow(tk2));
    ASSERT_TRUE(tk.SameColumn(tk2));
    ASSERT_TRUE(!tk.SameQualifier(tk2));
    std::cout << tk2.DebugString() << std::endl;

    RawKey tk3(tk);
    ASSERT_TRUE(tk.Encode("haha", "hello", "world", 0, TKT_VALUE));
    ASSERT_GT(tk3.Compare(tk), 0);
    ASSERT_TRUE(!tk3.empty());
    ASSERT_EQ(tk3.key().as_string(), key);
    ASSERT_EQ(tk3.column().as_string(), column);
    ASSERT_EQ(tk3.qualifier().as_string(), qualifier);
    ASSERT_EQ(tk3.timestamp(), timestamp);
    ASSERT_EQ(tk3.type(), type);
    std::cout << tk3.DebugString() << std::endl;
}

TEST(RawKeyTest, Readable) {
    TestFunc(ReadableRawKeyOperator());
    TestFunc(BinaryRawKeyOperator());
}
}  // namespace xsheet
