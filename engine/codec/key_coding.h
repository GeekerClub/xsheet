// Copyright (C) 2018, For authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CODEC_KEY_CODING_H
#define XSHEET_ENGINE_CODEC_KEY_CODING_H


#include <endian.h>
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)


#include <stdint.h>
#include <string.h>
#include <string>

#include "toft/base/string/string_piece.h"

namespace xsheet {

extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint64(std::string* dst, uint64_t value);
extern void PutLengthPrefixedSlice(std::string* dst, const toft::StringPiece& value);

extern bool GetVarint32(toft::StringPiece* input, uint32_t* value);
extern bool GetVarint64(toft::StringPiece* input, uint64_t* value);
extern bool GetLengthPrefixedSlice(toft::StringPiece* input, toft::StringPiece* result);

extern const char* GetVarint32Ptr(const char* p,const char* limit, uint32_t* v);
extern const char* GetVarint64Ptr(const char* p,const char* limit, uint64_t* v);

extern int VarintLength(uint64_t v);

extern void EncodeFixed32(char* dst, uint32_t value);
extern void EncodeFixed64(char* dst, uint64_t value);

extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);


inline uint32_t DecodeFixed32(const char* ptr) {
  if (PLATFORM_IS_LITTLE_ENDIAN) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (PLATFORM_IS_LITTLE_ENDIAN) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p,
                                          const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

inline void EncodeBigEndian32(char* buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

inline uint32_t DecodeBigEndain32(const char* ptr) {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])) << 24));
}

inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

inline uint64_t DecodeBigEndain(const char* ptr) {
    uint64_t lo = DecodeBigEndain32(ptr + 4);
    uint64_t hi = DecodeBigEndain32(ptr);
    return (hi << 32) | lo;
}

}  // namespace xsheet

#endif  // XSHEET_ENGINE_CODEC_KEY_CODING_H
