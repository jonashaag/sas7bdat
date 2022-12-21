#ifndef SAS7BDAT_HPP
#define SAS7BDAT_HPP
#include <stdbool.h>
#include <stdexcept>
#include <stdlib.h>

// From pyreadstat/src/sas/readstat_sas.c
enum Encoding {
  encoding_infer = -2,
  encoding_raw = -1,

  encoding_default = 0,
  encoding_utf_8 = 20,
  encoding_ascii = 28,
  encoding_iso_8859_1 = 29,
  encoding_iso_8859_2 = 30,
  encoding_iso_8859_3 = 31,
  encoding_iso_8859_4 = 32,
  encoding_iso_8859_5 = 33,
  encoding_iso_8859_6 = 34,
  encoding_iso_8859_7 = 35,
  encoding_iso_8859_8 = 36,
  encoding_iso_8859_9 = 37,
  encoding_iso_8859_11 = 39,
  encoding_iso_8859_15 = 40,
  encoding_cp437 = 41,
  encoding_cp850 = 42,
  encoding_cp852 = 43,
  encoding_cp857 = 44,
  encoding_cp858 = 45,
  encoding_cp862 = 46,
  encoding_cp864 = 47,
  encoding_cp865 = 48,
  encoding_cp866 = 49,
  encoding_cp869 = 50,
  encoding_cp874 = 51,
  encoding_cp921 = 52,
  encoding_cp922 = 53,
  encoding_cp1129 = 54,
  encoding_cp720 = 55,
  encoding_cp737 = 56,
  encoding_cp775 = 57,
  encoding_cp860 = 58,
  encoding_cp863 = 59,
  encoding_windows_1250 = 60,
  encoding_windows_1251 = 61,
  encoding_windows_1252 = 62,
  encoding_windows_1253 = 63,
  encoding_windows_1254 = 64,
  encoding_windows_1255 = 65,
  encoding_windows_1256 = 66,
  encoding_windows_1257 = 67,
  encoding_windows_1258 = 68,
  encoding_macroman = 69,
  encoding_macarabic = 70,
  encoding_machebrew = 71,
  encoding_macgreek = 72,
  encoding_macthai = 73,
  encoding_macturkish = 75,
  encoding_macukraine = 76,
  encoding_cp950 = 118,
  encoding_euc_tw = 119,
  encoding_big_5 = 123,
  encoding_euc_cn = 125,
  encoding_zwin = 126,
  encoding_shift_jis = 128,
  encoding_euc_jp = 134,
  encoding_cp949 = 136,
  encoding_cp942 = 137,
  encoding_cp932 = 138,
  encoding_euc_kr = 140,
  encoding_kpce = 141,
  encoding_kwin = 142,
  encoding_maciceland = 163,
  encoding_iso_2022_jp = 167,
  encoding_iso_2022_kr = 168,
  encoding_iso_2022_cn = 169,
  encoding_iso_2022_cn_ext = 172,
  encoding_any = 204,
  encoding_gb18030 = 205,
  encoding_iso_8859_14 = 227,
  encoding_iso_8859_13 = 242,
  encoding_maccroatian = 245,
  encoding_maccyrillic = 246,
  encoding_macromania = 247,
  encoding_shift_jisx0213 = 248,
};

enum ColumnFormat {
  column_format_none,

  // Formats derived from SAS strings
  column_format_raw,    // Fixed-length raw bytes
  column_format_string, // Variable-length bytes/str

  // Formats derived from SAS decimals
  column_format_double,
  column_format_float,
  column_format_bool,
  column_format_int8,
  column_format_uint8,
  column_format_int16,
  column_format_uint16,
  column_format_int32,
  column_format_uint32,
  column_format_int64,
  column_format_uint64,
  column_format_date,
  column_format_datetime,
};

static bool is_sas_space(uint8_t value) {
  return value == '\0' || value == ' ';
}

static bool column_format_is_floating(enum ColumnFormat fmt) {
  return fmt == column_format_double || fmt == column_format_float;
}

static bool column_format_is_date_time(enum ColumnFormat fmt) {
  return fmt == column_format_date || fmt == column_format_datetime;
}

struct ColumnInfo {
  size_t offset;
  uint32_t len;
  char *name;
  enum ColumnFormat format;
};

struct FileInfo {
  bool is_32_bit;
  bool is_little_endian;
  bool need_byteswap;
  enum Encoding encoding;
  size_t page_count;
  size_t row_count, column_count;
  struct ColumnInfo *columns;
};

struct FormatOverride {
  const char *column_name;
  enum ColumnFormat format;
  size_t len;
};

struct ParserConfig {
  void *userdata;
  size_t filesize_override;
  const struct FormatOverride *column_format_overrides;
  size_t max_rows;
  size_t max_pages;
  void (*on_pagefault)(void *userdata, size_t requested_data_start, size_t requested_data_len,
                       size_t *new_data_offset, const uint8_t **new_data, size_t *new_data_len);
  void (*on_metadata)(void *userdata, const struct FileInfo *);
  bool (*on_row)(void *userdata, const uint8_t *buf, bool have_fast_space_offsets);
};

struct Parser;
size_t parser_struct_size();
void parser_init(struct Parser *Parser, const struct ParserConfig *config);
bool parse(struct Parser *parser);
void parser_deinit(struct Parser *parser); // Not thread safe if children exist

// ????????????????sssssssssssssssssssssssss
//                 ^
size_t column_last_known_space_offset(const struct Parser *, const struct ColumnInfo *);

static bool machine_is_little_endian() {
  int x = 1;
  return *(char *)&x;
}

#define decimal2double_inner                                                                       \
  do {                                                                                             \
    const int8_t out_sign = machine_is_little_endian() ? -1 : 1;                                   \
    const uint8_t out_off = machine_is_little_endian() ? 7 : 0;                                    \
    const int8_t buf_sign = file_is_little_endian ? -1 : 1;                                        \
    const uint8_t buf_off = file_is_little_endian ? (len - 1) : 0;                                 \
    for (uint8_t i = 0; i < len; ++i) {                                                            \
      ((uint8_t *)out)[out_off + out_sign * i] = buf[buf_off + buf_sign * i];                      \
    }                                                                                              \
  } while (0)

static void decimal2double(const uint8_t *buf, size_t len, bool file_is_little_endian,
                           double *out) {
  *out = 0.;
  switch (__builtin_expect(len, 8)) {
  case 3:
    decimal2double_inner;
    break;
  case 4:
    decimal2double_inner;
    break;
  case 5:
    decimal2double_inner;
    break;
  case 6:
    decimal2double_inner;
    break;
  case 7:
    decimal2double_inner;
    break;
  case 8:
    decimal2double_inner;
    break;
  default:
    __builtin_unreachable();
    break;
  }
}

#endif
