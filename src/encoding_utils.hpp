#ifndef ENCODING_UTILS_HPP
#define ENCODING_UTILS_HPP
#include "../include/sas7bdat.hpp"

static bool is_ascii(uint8_t c) {
  return !(c & 0x80);
}

static bool is_ascii_8(uint64_t cs) {
  return !(cs & 0x8080808080808080ull);
}

static bool maybe_iso_8859_15_8(uint64_t cs) {
  return cs & 0xa0a0a0a0a0a0a0a0ull;
}

static bool is_iso_8859_15(uint8_t c) {
  switch (c) {
  case 0xa4:
  case 0xa6:
  case 0xa8:
  case 0xb4:
  case 0xb8:
  case 0xbc:
  case 0xbd:
  case 0xbe:
    return true;
  }
  return false;
}

static bool is_windows_1252(uint8_t c) {
  return c >= 0x80 && c <= 0x9f;
}

enum Encoding detect_iso_8859_15_variant(const uint8_t *s, size_t len) {
  const uint8_t *end = &s[len];

  if (len >= 8) {
    const uint8_t *s_aligned = (uint8_t *)((size_t)s & -8);
    for (; s < s_aligned; ++s) {
      if (!is_ascii(*s)) {
        for (; s < s_aligned; ++s) {
          if (is_iso_8859_15(*s)) {
            return encoding_iso_8859_15;
          }
        }
        return encoding_iso_8859_1;
      }
    }

    for (; s + 8 < end; s += 8) {
      if (!is_ascii_8(*(uint64_t *)s)) {
        for (; s + 8 < end; s += 8) {
          if (maybe_iso_8859_15_8(*(uint64_t *)s)) {
            for (size_t i = 0; i < 8; ++i) {
              if (is_iso_8859_15(s[i])) {
                return encoding_iso_8859_15;
              }
            }
          }
        }
        return encoding_iso_8859_1;
      }
    }
  }

  for (; s < end; ++s) {
    if (!is_ascii(*s)) {
      for (; s < end; ++s) {
        if (is_iso_8859_15(*s)) {
          return encoding_iso_8859_15;
        }
      }
      return encoding_iso_8859_1;
    }
  }

  return encoding_ascii;
}

enum Encoding detect_windows_1252_variant(const uint8_t *s, size_t len) {
  // todo vectorize
  const uint8_t *end = &s[len];
  enum Encoding res = encoding_ascii;
  for (; s < end; ++s) {
    if (is_windows_1252(*s)) {
      return encoding_windows_1252;
    } else if (!is_ascii(*s)) {
      res = encoding_iso_8859_1;
    }
  }
  return res;
}

#endif
