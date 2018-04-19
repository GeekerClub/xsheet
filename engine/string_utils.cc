// Copyright (C) 2018, For authors
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "engine/string_utils.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iomanip>
#include <sstream>

#include "toft/base/string/string_piece.h"


namespace xsheet {

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const toft::StringPiece& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const toft::StringPiece& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeChar(toft::StringPiece* in, char c) {
  if (!in->empty() && (*in)[0] == c) {
    in->remove_prefix(1);
    return true;
  } else {
    return false;
  }
}

bool ConsumeDecimalNumber(toft::StringPiece* in, uint64_t* val) {
  if (in->size() > 1 && (*in)[0] == 'H') {
      return ConsumeHexDecimalNumber(in, val);
  }
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && static_cast<uint64_t>(delta) > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

bool ConsumeHexDecimalNumber(toft::StringPiece* in, uint64_t* val) {
    char c = (*in)[0];
    if (c != 'H') {
        return false;
    }
    in->remove_prefix(1);
    std::string hex_str = in->as_string();
    std::string log_num_str;
    SplitStringStart(hex_str, &log_num_str, NULL);
    if (log_num_str.empty()) {
        return false;
    }
    *val = StringToUint64(log_num_str, 16);
    in->remove_prefix(log_num_str.length());
    return true;
}

void SplitStringEnd(const std::string& full, std::string* begin_part,
                    std::string* end_part, std::string delim) {
    std::string::size_type pos = full.find_last_of(delim);
    if (pos != std::string::npos && pos != 0) {
        if (end_part) {
            *end_part = full.substr(pos + 1);
        }
        if (begin_part) {
            *begin_part = full.substr(0, pos);
        }
    } else {
        if (end_part) {
            *end_part = full;
        }
    }
}

void SplitStringStart(const std::string& full, std::string* begin_part,
                      std::string* end_part, std::string delim) {
    std::string::size_type pos = full.find_first_of(delim);
    if (pos == std::string::npos || (pos < full.size() - 1)) {
        if (end_part) {
            *end_part = full.substr(pos + 1);
        }
    }
    if (pos != std::string::npos && pos >= 1) {
        if (begin_part) {
            *begin_part = full.substr(0, pos);
        }
    }
}

std::string ReplaceString(const std::string& str, const std::string& src,
                          const std::string& dest) {
    std::string ret;

    std::string::size_type pos_begin = 0;
    std::string::size_type pos = str.find(src);
    while (pos != std::string::npos) {
        // cout <<"replacexxx:" << pos_begin <<" " << pos <<"\n";
        ret.append(str.data() + pos_begin, pos - pos_begin);
        ret += dest;
        pos_begin = pos + src.length();
        pos = str.find(src, pos_begin);
    }
    if (pos_begin < str.length()) {
        ret.append(str.begin() + pos_begin, str.end());
    }
    return ret;
}

std::string TrimString(const std::string& str, const std::string& trim) {
    std::string::size_type pos = str.find_first_not_of(trim);
    if (pos == std::string::npos) {
        return str;
    }
    std::string::size_type pos2 = str.find_last_not_of(trim);
    if (pos2 != std::string::npos) {
        return str.substr(pos, pos2 - pos + 1);
    }
    return str.substr(pos);
}

bool StringEndsWith(const std::string& str, const std::string& sub_str) {
    if (str.length() < sub_str.length()) {
        return false;
    }
    if (str.substr(str.length() - sub_str.length()) != sub_str) {
        return false;
    }
    return true;
}

bool StringStartWith(const std::string& str, const std::string& sub_str) {
    if (str.length() < sub_str.length()) {
        return false;
    }
    if (str.substr(0, sub_str.length()) != sub_str) {
        return false;
    }
    return true;
}

char* StringAsArray(std::string* str) {
    return str->empty() ? NULL : &*str->begin();
}

std::string Uint64ToString(uint64_t i, int base) {
    std::stringstream ss;
    if (base == 16) {
        ss << std::hex << std::setfill('0') << std::setw(16) << i;
    } else if (base == 8) {
        ss << std::oct << std::setfill('0') << std::setw(8) << i;
    } else {
        ss << i;
    }
    return ss.str();
}

uint64_t StringToUint64(const std::string& int_str, int base) {
    uint64_t value;
    std::istringstream buffer(int_str);
    if (base == 16) {
        buffer >> std::hex >> value;
    } else if (base == 8) {
        buffer >> std::oct >> value;
    } else {
        buffer >> value;
    }
    return value;
}
}  // namespace xsheet
