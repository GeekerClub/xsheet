// Copyright (C) 2018, For authors
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef XSHEET_ENGINE_CODEC_STRING_UTILS_H
#define XSHEET_ENGINE_CODEC_STRING_UTILS_H

#include <stdio.h>
#include <stdint.h>
#include <string>

#include "toft/base/string/string_piece.h"

namespace xsheet {

// Append a human-readable printout of "num" to *str
extern void AppendNumberTo(std::string* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
extern void AppendEscapedStringTo(std::string* str, const toft::StringPiece& value);

// Return a human-readable printout of "num"
extern std::string NumberToString(uint64_t num);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
extern std::string EscapeString(const toft::StringPiece& value);

// If *in starts with "c", advances *in past the first character and
// returns true.  Otherwise, returns false.
extern bool ConsumeChar(toft::StringPiece* in, char c);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
extern bool ConsumeDecimalNumber(toft::StringPiece* in, uint64_t* val);

extern bool ConsumeHexDecimalNumber(toft::StringPiece* in, uint64_t* val);

void SplitStringEnd(const std::string& full, std::string* begin_part,
                    std::string* end_part, std::string delim = ".");

void SplitStringStart(const std::string& full, std::string* begin_part,
                      std::string* end_part, std::string delim = ".");

std::string ReplaceString(const std::string& str, const std::string& src,
                          const std::string& dest);


std::string TrimString(const std::string& str, const std::string& trim = " ");

bool StringEndsWith(const std::string& str, const std::string& sub_str);

bool StringStartWith(const std::string& str, const std::string& sub_str);

char* StringAsArray(std::string* str);

std::string Uint64ToString(uint64_t i, int base = 10);

uint64_t StringToUint64(const std::string& int_str, int base = 10);


std::string DebugString(const std::string& src);
bool ParseDebugString(const std::string& src, std::string* dst);

int EditDistance(const std::string& a, const std::string& b);

}  // namespace xsheet

#endif  // XSHEET_ENGINE_CODEC_STRING_UTILS_H
