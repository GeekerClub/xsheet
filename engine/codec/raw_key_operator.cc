#include "engine/codec/raw_key_operator.h"

#include <pthread.h>

#include "engine/codec//key_coding.h"

namespace xsheet {


static inline void AppendTsAndType(std::string* raw_key,
                                   int64_t timestamp,
                                   RawKeyType type) {
    timestamp &= 0x00FFFFFFFFFFFFFF;
    uint64_t n = ((1UL << 56) - 1 - timestamp) << 8 | (type & 0xFF);
    char str[8];
    EncodeBigEndian(str, n);
    raw_key->append(str, 8);
}

static inline void ExtractTsAndType(const toft::StringPiece& raw_key,
                                    int64_t* timestamp,
                                    RawKeyType* type) {
    uint64_t n = DecodeBigEndain(raw_key.data() + raw_key.size() - sizeof(uint64_t));
    if (type) {
        *type = static_cast<RawKeyType>((n << 56) >> 56);
    }
    if (timestamp) {
        *timestamp = (1L << 56) - 1 - (n >> 8);
    }
}

static inline void AppendRowQualifierLength(std::string* raw_key,
                                            const std::string& row_key,
                                            const std::string& qualifier) {
    uint32_t rlen = row_key.size();
    uint32_t qlen = qualifier.size();
    uint32_t n = (rlen<<16) | qlen;
    char str[4];
    EncodeBigEndian32(str, n);
    raw_key->append(str, 4);
}


/**
 *  readable encoding format:
 *  [rowkey\0|column\0|qualifier\0|type|timestamp]
 *  [ rlen+1B| clen+1B| qlen+1B   | 1B | 7B      ]
 **/
class ReadableRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeRawKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               RawKeyType type,
                               std::string* raw_key) const {
        *raw_key = row_key;
        raw_key->push_back('\0');
        raw_key->append(family);
        raw_key->push_back('\0');
        raw_key->append(qualifier);
        raw_key->push_back('\0');
        AppendTsAndType(raw_key, timestamp, type);
    }

    virtual bool ExtractRawKey(const toft::StringPiece& raw_key,
                                toft::StringPiece* row_key,
                                toft::StringPiece* family,
                                toft::StringPiece* qualifier,
                                int64_t* timestamp,
                                RawKeyType* type) const {
        int key_len = strlen(raw_key.data());
        if (row_key) {
            *row_key = toft::StringPiece(raw_key.data(), key_len);
        }

        int family_len = strlen(raw_key.data() + key_len + 1);
        toft::StringPiece family_data(raw_key.data() + key_len + 1, family_len);
        if (family) {
            *family = family_data;
        }

        int qualifier_len = strlen(family_data.data() + family_len + 1);
        if (qualifier) {
            *qualifier = toft::StringPiece(family_data.data() + family_len + 1, qualifier_len);
        }

        if (key_len + family_len + qualifier_len + 3 + sizeof(uint64_t) != raw_key.size()) {
            return false;
        }
        ExtractTsAndType(raw_key, timestamp, type);
        return true;
    }

    virtual int Compare(const toft::StringPiece& key1, const toft::StringPiece& key2) const {
        return key1.compare(key2);
    }

    const char* Name() const {
        return "raw.RawKeyOperator.readable";
    }
};

/**
 *  binary encoding format:
 *  [rowkey|column\0|qualifier|type|timestamp|rlen|qlen]
 *  [ rlen | clen+1B| qlen    | 1B |   7B    | 2B | 2B ]
 **/
class BinaryRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeRawKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               RawKeyType type,
                               std::string* raw_key) const {
        uint32_t rlen = row_key.size();
        uint32_t flen = family.size();
        uint32_t qlen = qualifier.size();

        raw_key->resize(rlen + flen + qlen + 13);
        char* key = (char*)(raw_key->data());

        // fill rowkey segment
        memcpy(key, row_key.data(), rlen);
        int pos = rlen;
        // fill column family segment
        memcpy(key + pos, family.data(), flen);
        pos += flen;
        key[pos] = '\0';
        pos++;

        // fill qualifier segment
        memcpy(key + pos, qualifier.data(), qlen);
        pos += qlen;

        // fill timestamp&type segment
        uint64_t n = ((1UL << 56) - 1 - timestamp) << 8 | (type & 0xFF);
        EncodeBigEndian(key + pos, n);
        pos += 8;

        // fill row len and qualifier len segment
        uint32_t m = (rlen << 16) | (qlen & 0xFFFF);
        EncodeBigEndian32(key + pos, m);
    }

    virtual bool ExtractRawKey(const toft::StringPiece& raw_key,
                                toft::StringPiece* row_key,
                                toft::StringPiece* family,
                                toft::StringPiece* qualifier,
                                int64_t* timestamp,
                                RawKeyType* type) const {
        uint32_t len = DecodeBigEndain32(raw_key.data() + raw_key.size() - sizeof(uint32_t));
        int key_len = static_cast<int>(len >> 16);
        int family_len = strlen(raw_key.data() + key_len);
        int qualifier_len = static_cast<int>(len & 0xFFFF);

        if (key_len + family_len + qualifier_len + 1 +
            sizeof(uint64_t) + sizeof(uint32_t) != raw_key.size()) {
            return false;
        }

        if (row_key) {
            *row_key = toft::StringPiece(raw_key.data(), key_len);
        }
        toft::StringPiece family_data(raw_key.data() + key_len, family_len);
        if (family) {
            *family = family_data;
        }
        if (qualifier) {
            *qualifier = toft::StringPiece(family_data.data() + family_len + 1, qualifier_len);
        }

        toft::StringPiece internal_raw_key = toft::StringPiece(raw_key.data(), raw_key.size() - sizeof(uint32_t));
        ExtractTsAndType(internal_raw_key, timestamp, type);
        return true;
    }

    virtual int Compare(const toft::StringPiece& key1, const toft::StringPiece& key2) const {
        // for performance optimiztion
        // rawkey_compare_counter.Inc();
        uint32_t len1, len2, rlen1, rlen2, clen1, clen2, qlen1, qlen2;
        int ret;
        const char* data1 = key1.data();
        const char* data2 = key2.data();
        int size1 = key1.size();
        int size2 = key2.size();

        // decode rowlen and qualifierlen from raw key
        len1 = DecodeBigEndain32(data1 + size1 - 4);
        len2 = DecodeBigEndain32(data2 + size2 - 4);

        // rowkey compare, if ne, return
        rlen1 = static_cast<int>(len1 >> 16);
        rlen2 = static_cast<int>(len2 >> 16);
        toft::StringPiece row1(data1, rlen1);
        toft::StringPiece row2(data2, rlen2);
        ret = row1.compare(row2);
        if (ret != 0) {
            return ret;
        }

        // column family compare, if ne, return
        qlen1 = static_cast<int>(len1 & 0x00FF);
        qlen2 = static_cast<int>(len2 & 0x00FF);
        clen1 = size1 - rlen1 - qlen1 - 13;
        clen2 = size2 - rlen2 - qlen2 - 13;
        toft::StringPiece col1(data1 + rlen1, clen1);
        toft::StringPiece col2(data2 + rlen2, clen2);
        ret = col1.compare(col2);
        if (ret != 0) {
            return ret;
        }

        // qualifier compare, if ne, return
        toft::StringPiece qual1(data1 + size1 - qlen1 - 12, qlen1);
        toft::StringPiece qual2(data2 + size2 - qlen2 - 12, qlen2);
        ret = qual1.compare(qual2);
        if (ret != 0) {
            return ret;
        }

        // timestamp&type compared together
        toft::StringPiece ts_type1(data1 + size1 - 12, 8);
        toft::StringPiece ts_type2(data2 + size2 - 12, 8);
        return ts_type1.compare(ts_type2);
    }

    const char* Name() const {
        return "raw.RawKeyOperator.binary";
    }
};

// support KV-pair with TTL, Key's format :
// [row_key|expire_timestamp]
// [rlen|4B]
class KvRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeRawKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp, // must >= 0
                               RawKeyType type,
                               std::string* raw_key) const {
        char expire_timestamp[8];
        EncodeBigEndian(expire_timestamp, timestamp);
        raw_key->assign(row_key).append(expire_timestamp, 8);
    }

    virtual bool ExtractRawKey(const toft::StringPiece& raw_key,
                                toft::StringPiece* row_key,
                                toft::StringPiece* family,
                                toft::StringPiece* qualifier,
                                int64_t* timestamp,
                                RawKeyType* type) const {
        if (row_key) {
            *row_key = toft::StringPiece(raw_key.data(), raw_key.size() - sizeof(int64_t));
        }
        if (timestamp) {
            *timestamp = DecodeBigEndain(raw_key.data() + raw_key.size() - sizeof(int64_t));
        }
        return true;
    }

    // only compare row_key
    virtual int Compare(const toft::StringPiece& key1, const toft::StringPiece& key2) const {
        toft::StringPiece key1_rowkey(key1.data(), key1.size() - sizeof(int64_t));
        toft::StringPiece key2_rowkey(key2.data(), key2.size() - sizeof(int64_t));
        return key1_rowkey.compare(key2_rowkey);
    }

    const char* Name() const {
        return "raw.RawKeyOperator.kv";
    }
};

static pthread_once_t once = PTHREAD_ONCE_INIT;
static const RawKeyOperator* readable_key;
static const RawKeyOperator* binary_key;
static const KvRawKeyOperatorImpl* kv_key;

static void InitModule() {
    readable_key = new ReadableRawKeyOperatorImpl;
    binary_key = new BinaryRawKeyOperatorImpl;
    kv_key = new KvRawKeyOperatorImpl;
}

const RawKeyOperator* ReadableRawKeyOperator() {
    pthread_once(&once, InitModule);
    return readable_key;
}

const RawKeyOperator* BinaryRawKeyOperator() {
    pthread_once(&once, InitModule);
    return binary_key;
}

const RawKeyOperator* KvRawKeyOperator() {
    pthread_once(&once, InitModule);
    return kv_key;
}

} // namespace xsheet
