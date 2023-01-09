#ifndef ENCODING_UTILS_HPP
#define ENCODING_UTILS_HPP
#include "../include/sas7bdat.hpp"
#include <string.h>

// TODO i + 8 overflow! also in cython

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

static enum Encoding detect_iso_8859_15_variant(const uint8_t *s, size_t len) {
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

static enum Encoding detect_windows_1252_variant(const uint8_t *s, size_t len) {
  // TODO(perf) vectorize
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

#if 0
#define _utf_8_converter_check_left(n)                                                             \
  if (left < n) {                                                                                  \
    return SIZE_MAX;                                                                               \
  }
#else
// unsafe but faster
#define _utf_8_converter_check_left(n)
#endif

#define _utf_8_converter_handle_single_char(cc, non_ascii_code)                                    \
  uint8_t c = (cc);                                                                                \
  if (is_ascii(c)) {                                                                               \
    _utf_8_converter_check_left(1);                                                                \
    out[0] = c;                                                                                    \
    left -= 1;                                                                                     \
    out += 1;                                                                                      \
  } else {                                                                                         \
    non_ascii_code                                                                                 \
  }

#define DEFINE_UTF_8_CONVERTER(name, non_ascii_code)                                               \
  static size_t name##_to_utf_8(const uint8_t *s, size_t len, uint8_t *out, size_t out_len) {      \
    const uint8_t *out1 = out;                                                                     \
    size_t left = out_len;                                                                         \
    size_t ileft = len;                                                                            \
    while (ileft >= 8) {                                                                           \
      uint64_t c8;                                                                                 \
      _utf_8_converter_check_left(8);                                                              \
      if (is_ascii(c8 = *(uint64_t *)s)) {                                                         \
        *(uint64_t *)out = c8;                                                                     \
        out += 8;                                                                                  \
        left -= 8;                                                                                 \
      } else {                                                                                     \
        for (size_t ci = 0; ci < 8; ++ci) {                                                        \
          _utf_8_converter_handle_single_char(s[ci], non_ascii_code);                              \
        }                                                                                          \
      }                                                                                            \
      ileft -= 8;                                                                                  \
      s += 8;                                                                                      \
    }                                                                                              \
    while (ileft > 0) {                                                                            \
      _utf_8_converter_handle_single_char(*s, non_ascii_code);                                     \
      --ileft;                                                                                     \
      ++s;                                                                                         \
    }                                                                                              \
    return out - out1;                                                                             \
  }

// clang-format off
DEFINE_UTF_8_CONVERTER(iso_8859_1,
  _utf_8_converter_check_left(2);
  out[0] = 0xc0 | (c >> 6);
  out[1] = 0x80 | (c & 0x3f);
  left -= 2;
  out += 2;
);

static const uint16_t iso_8859_15_lookup[] = {
  0x00a0, 0x00a1, 0x00a2, 0x00a3, 0x20ac, 0x00a5, 0x0160, 0x00a7, 0x0161, 0x00a9, 0x00aa, 0x00ab, 0x00ac, 0x00ad, 0x00ae, 0x00af,
  0x00b0, 0x00b1, 0x00b2, 0x00b3, 0x017d, 0x00b5, 0x00b6, 0x00b7, 0x017e, 0x00b9, 0x00ba, 0x00bb, 0x0152, 0x0153, 0x0178, 0x00bf,
};
DEFINE_UTF_8_CONVERTER(iso_8859_15,
  if (c == 0xa4) {
    _utf_8_converter_check_left(3);
    out[0] = 0xe2;
    out[1] = 0x82;
    out[2] = 0xac;
    left -= 3;
    out += 3;
  } else {
    _utf_8_converter_check_left(2);
    uint16_t c2 = (c < 0xa6 || c >= 0xbf) ? c : iso_8859_15_lookup[c-0xa0];
    out[0] = 0xc0 | (c2 >> 6);
    out[1] = 0x80 | (c2 & 0x3f);
    left -= 2;
    out += 2;
  }
);

static const uint16_t windows_1252_lookup[] = {
  0x20ac, 0xffff, 0x201a, 0x0192, 0x201e, 0x2026, 0x2020, 0x2021, 0x02c6, 0x2030, 0x0160, 0x2039, 0x0152, 0xffff, 0x017d, 0xffff,
  0xffff, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0x02dc, 0x2122, 0x0161, 0x203a, 0x0153, 0xffff, 0x017e, 0x0178,
};
DEFINE_UTF_8_CONVERTER(windows_1252,
  if (is_windows_1252(c)) {
    uint16_t c2 = windows_1252_lookup[c - 0x80];
    if (c2 == 0xffff) { return SIZE_MAX; }
    if (c2 < 0x800) {
      _utf_8_converter_check_left(2);
      out[0] = 0xc0 | (c2 >> 6);
      out[1] = 0x80 | (c2 & 0x3f);
      left -= 2;
      out += 2;
    } else {
      _utf_8_converter_check_left(3);
      out[0] = 0xe0 | (c2 >> 12);
      out[1] = 0x80 | ((c2 >> 6) & 0x3f);
      out[2] = 0x80 | (c2 & 0x3f);
      left -= 3;
      out += 3;
    }
  } else {
    _utf_8_converter_check_left(2);
    out[0] = 0xc0 | (c >> 6);
    out[1] = 0x80 | (c & 0x3f);
    left -= 2;
    out += 2;
  }
);
// clang-format on

static size_t to_utf_8(const uint8_t *s, size_t len, uint8_t *out, size_t out_len,
                       enum Encoding encoding) {
  if (out_len < len) {
    return SIZE_MAX;
  }
  switch (encoding) {
  case encoding_ascii:
  case encoding_utf_8:
    memcpy(out, s, len);
    return len;
  case encoding_iso_8859_1:
    return iso_8859_1_to_utf_8(s, len, out, out_len);
  case encoding_iso_8859_15:
    return iso_8859_15_to_utf_8(s, len, out, out_len);
  case encoding_windows_1252:
    return windows_1252_to_utf_8(s, len, out, out_len);
  default:
    return SIZE_MAX;
  }
}

#endif
