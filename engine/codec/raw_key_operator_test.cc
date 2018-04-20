// Copyright (C) 2018, For authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/codec/raw_key_operator.h"

#include <sys/time.h>
#include <iostream>

#include "thirdparty/gtest/gtest.h"

namespace xsheet {

void print_bytes(const char* str, int len) {
    for (int i = 0; i < len; ++i) {
        printf("%x ", str[i]);
    }
    printf("\n");
}

int64_t get_micros() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
}

class RawKeyOperatorTest {};

TEST(RawKeyOperatorTest, ReadableEncodeRawKey) {
    const RawKeyOperator* key_operator = ReadableRawKeyOperator();
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x0001020304050607;

    std::string temp_key1;
    std::string temp_key2;

    key_operator->EncodeRawKey(key, column, qualifier, timestamp,
                                TKT_VALUE, &temp_key1);
    key_operator->EncodeRawKey(key, column, qualifier, timestamp,
                                TKT_DEL, &temp_key2);

    size_t len = key.size() + column.size() + qualifier.size() + sizeof(timestamp) + 3;
    ASSERT_EQ(temp_key1.size(), len);

    std::string raw1("row_key\0column\0qualifier\0\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x05", len);
    ASSERT_TRUE(temp_key1 == raw1);
//    print_bytes(temp_key1.data(), temp_key1.size());
//    print_bytes(raw1.data(), raw1.size());

    std::string raw2("row_key\0column\0qualifier\0\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x01", len);
    ASSERT_TRUE(temp_key2 == raw2);

    ASSERT_TRUE(temp_key1.compare(temp_key2) > 0);
}

TEST(RawKeyOperatorTest, ReadableExtractRawKey) {
    const RawKeyOperator* key_operator = ReadableRawKeyOperator();
    std::string temp_key1;
    std::string row_key1 = "row";
    std::string column1 = "column";
    std::string qualifier1 = "qualifier";
    int64_t timestamp1 = time(NULL);
    key_operator->EncodeRawKey(row_key1, column1,qualifier1,
                              timestamp1, TKT_VALUE, &temp_key1);

    toft::StringPiece row_key2;
    toft::StringPiece column2;
    toft::StringPiece qualifier2;
    int64_t timestamp2;
    RawKeyType type2;
    ASSERT_TRUE(key_operator->ExtractRawKey(temp_key1, &row_key2, &column2,
                                           &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.as_string());
    ASSERT_EQ(column1, column2.as_string());
    ASSERT_EQ(qualifier1, qualifier2.as_string());
    ASSERT_EQ(timestamp1, timestamp2);
    ASSERT_EQ(type2, TKT_VALUE);
}

TEST(RawKeyOperatorTest, BinaryEncodeRawKey) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string key("row_key");
    std::string column("column");
    std::string qualifier("qualifier");
    int64_t timestamp = 0x01020304050607;

    std::string temp_key1;
    std::string temp_key2;

    key_operator->EncodeRawKey(key, column, qualifier, timestamp,
                                TKT_VALUE, &temp_key1);
    key_operator->EncodeRawKey(key, column, qualifier, timestamp,
                                TKT_DEL, &temp_key2);

    size_t len = key.size() + column.size() + qualifier.size() + sizeof(timestamp) + 5;
    ASSERT_EQ(temp_key1.size(), len);

    std::string raw1("row_keycolumn\0qualifier\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x5\x0\x7\x0\x9", len);
    ASSERT_TRUE(temp_key1 == raw1);
//    print_bytes(temp_key1.data(), temp_key1.size());
//    print_bytes(raw1.data(), raw1.size());

    std::string raw2("row_keycolumn\0qualifier\xFE\xFD\xFC\xFB\xFA\xF9\xF8\x01\x0\x7\x0\x9", len);
    ASSERT_TRUE(temp_key2 == raw2);

    ASSERT_TRUE(temp_key1.compare(temp_key2) > 0);

}

TEST(RawKeyOperatorTest, BinaryExtractRawKey) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string temp_key1;
    std::string row_key1 = "row";
    std::string column1 = "column";
    std::string qualifier1 = "qualifier";
    key_operator->EncodeRawKey(row_key1, column1,qualifier1,
                              0, TKT_VALUE, &temp_key1);

    toft::StringPiece row_key2;
    toft::StringPiece column2;
    toft::StringPiece qualifier2;
    int64_t timestamp2;
    RawKeyType type2;
    ASSERT_TRUE(key_operator->ExtractRawKey(temp_key1, &row_key2, &column2,
                                           &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.as_string());
    ASSERT_EQ(column1, column2.as_string());
    ASSERT_EQ(qualifier1, qualifier2.as_string());
    ASSERT_EQ(timestamp2, 0);
    ASSERT_EQ(type2, TKT_VALUE);
}

void GenTestString(int64_t len, std::string* output) {
    for (int i = 0; i < len; ++i) {
        output->append("a");
    }
}

TEST(RawKeyOperatorTest, TestBigRow) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string test_str_60K;
    GenTestString(60000, &test_str_60K);

    std::string temp_key1;
    std::string row_key1 = test_str_60K;
    std::string column1 = test_str_60K;
    std::string qualifier1 = test_str_60K;
    key_operator->EncodeRawKey(row_key1, column1,qualifier1,
                                0, TKT_VALUE, &temp_key1);
    ASSERT_EQ(temp_key1.size(), 180013u);

    toft::StringPiece row_key2;
    toft::StringPiece column2;
    toft::StringPiece qualifier2;
    int64_t timestamp2;
    RawKeyType type2;
    ASSERT_TRUE(key_operator->ExtractRawKey(temp_key1, &row_key2, &column2,
                                             &qualifier2, &timestamp2, &type2));

    ASSERT_EQ(row_key1, row_key2.as_string());
    ASSERT_EQ(column1, column2.as_string());
    ASSERT_EQ(qualifier1, qualifier2.as_string());
    ASSERT_EQ(timestamp2, 0);
    ASSERT_EQ(type2, TKT_VALUE);
}

TEST(RawKeyOperatorTest, Compare) {
    const RawKeyOperator* key_operator = BinaryRawKeyOperator();
    std::string temp_key1, temp_key2;
    std::string key1, key2;
    std::string column1, column2;
    std::string qualifier1, qualifier2;
    int64_t ts1, ts2;
    RawKeyType type1, type2;

    key1 = "row";
    column1 = "column";
    qualifier1 = "qualifier";
    ts1 = 0;
    type1 = TKT_VALUE;
    key_operator->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_EQ(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row1";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_LT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "ro";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_GT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row";
    column2 = "columny";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_LT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifierr";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_LT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 1;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_GT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_DEL;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_GT(key_operator->Compare(temp_key1, temp_key2), 0);

    //
    type1 = TKT_DEL_COLUMN;
    key_operator->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 0;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_LT(key_operator->Compare(temp_key1, temp_key2), 0);

    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 1;
    type2 = TKT_VALUE;
    key_operator->EncodeRawKey(key2, column2, qualifier2, ts2, type2, &temp_key2);
    ASSERT_GT(key_operator->Compare(temp_key1, temp_key2), 0);
}

void EncodeRawKeyPerformanceTest(const RawKeyOperator* key_operator,
                                   const std::string& row,
                                   const std::string& col,
                                   const std::string& qual,
                                   int64_t ts,
                                   RawKeyType type,
                                   const std::string& desc) {
    std::string temp_key;
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->EncodeRawKey(row, col, qual, ts, type, &temp_key);
    }
    int64_t end = get_micros();
    std::cout << "[Encode RawKey Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, EncodeRawKeyPerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    std::string temp_key, row, col, qual;
    int64_t ts;
    RawKeyType type;
    row = "row";
    col = "col";
    qual = "qual";
    ts = 123456789;
    type = TKT_VALUE;

    EncodeRawKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary short");

    row = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    col = "colcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcol";
    qual = "qualqualqualqualqualqualqualqualqualqualqualqualqualqualqualqual";
    EncodeRawKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary long");
    EncodeRawKeyPerformanceTest(keyop_bin, row, col, qual, ts, type, "binary long qualnull");
}

void ExtractRawKeyPerformanceTest(const RawKeyOperator* key_operator,
                                   const std::string& key,
                                   const std::string& desc) {
    toft::StringPiece row, col, qual;
    int64_t ts;
    RawKeyType type;
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->ExtractRawKey(key, &row, &col, &qual, &ts, &type);
    }
    int64_t end = get_micros();
    std::cout << "[Extract RawKey Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, ExtractRawKeyPerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    std::string temp_key, row, col, qual;
    row = "row";
    col = "col";
    qual = "qual";
    keyop_bin->EncodeRawKey(row, col, qual, 0, TKT_VALUE, &temp_key);
    ExtractRawKeyPerformanceTest(keyop_bin, temp_key, "binary short");

    row = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    col = "colcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcolcol";
    qual = "qualqualqualqualqualqualqualqualqualqualqualqualqualqualqualqual";
    keyop_bin->EncodeRawKey(row, col, qual, 0, TKT_VALUE, &temp_key);
    ExtractRawKeyPerformanceTest(keyop_bin, temp_key, "binary long");

    keyop_bin->EncodeRawKey(row, col, "", 0, TKT_VALUE, &temp_key);
    ExtractRawKeyPerformanceTest(keyop_bin, temp_key, "binary long qualnull");
}

void ComparePerformanceTest(const RawKeyOperator* key_operator,
                     const std::string& key1,
                     const std::string& key2,
                     const std::string& desc) {
    int64_t start = get_micros();
    for (int i = 0; i < 10000000; ++i) {
        key_operator->Compare(key1, key2);
    }
    int64_t end = get_micros();
    std::cout << "[Compare Performance ("
        << desc << ")] cost: " << (end - start) / 1000 << "ms\n";
}

TEST(RawKeyOperatorTest, ComparePerformace) {
    const RawKeyOperator* keyop_bin = BinaryRawKeyOperator();
    const RawKeyOperator* keyop_read = ReadableRawKeyOperator();
    std::string temp_key1, temp_key2;
    std::string key1, key2;
    std::string column1, column2;
    std::string qualifier1, qualifier2;
    int64_t ts1, ts2;
    RawKeyType type1;

    key1 = "rowrowrowrowrowrowrowrowrowrowrowrowrowrowrowrow";
    column1 = "columncolumncolumncolumn";
    qualifier1 = "qualifierqualifierqualifier";
    ts1 = 123456789;
    type1 = TKT_VALUE;
    key2 = "row";
    column2 = "column";
    qualifier2 = "qualifier";
    ts2 = 987654321;

    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key2, column2, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same none");

    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key1, column2, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same row");

    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key1, column1, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same row/col");

    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same row/col/qu");

    keyop_bin->EncodeRawKey(key1, column1, "", ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key1, column1, "", ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same row/col/null");

    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary long same all");

    keyop_bin->EncodeRawKey(key2, column2, qualifier2, ts1, type1, &temp_key1);
    keyop_bin->EncodeRawKey(key2, column2, qualifier2, ts1, type1, &temp_key2);
    ComparePerformanceTest(keyop_bin, temp_key1, temp_key2, "binary short");

    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key2, column2, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable long same none");

    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key1, column2, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable long same row");

    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key1, column1, qualifier2, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable long same row/col");

    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts2, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable long same row/col/qu");

    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key1, column1, qualifier1, ts1, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable long same all");

    keyop_read->EncodeRawKey(key2, column2, qualifier2, ts1, type1, &temp_key1);
    keyop_read->EncodeRawKey(key2, column2, qualifier2, ts1, type1, &temp_key2);
    ComparePerformanceTest(keyop_read, temp_key1, temp_key2, "readable short");
}
}  // namespace xsheet

