#include <new>
#include <stdexcept>

#include "../include/sas7bdat.hpp"
#include "bitmap.hpp"
#include "sas_compression.cpp"

// TODO(corr): amd pages

#define SAS_MAGIC                                                                                  \
  "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2\xea\x81\x60\xb3\x14"                       \
  "\x11\xcf\xbd\x92\x08\x00\x09\xc7\x31\x8c\x18\x1f\x10\x11"
#define SAS_COMPRESSION_SIGNATURE_RLE "SASYZCRL"
#define SAS_COMPRESSION_SIGNATURE_RDC "SASYZCR2"
#define SAS_DATE_FORMATS                                                                           \
  " DATE DAY DDMMYY DOWNAME JULDAY JULIAN MMDDYY MMYY MMYYC MMYYD MMYYP "                          \
  "MMYYS MMYYN MONNAME MONTH MONYY QTR QTRR NENGO WEEKDATE WEEKDATX WEEKDAY "                      \
  "WEEKV WORDDATE WORDDATX YEAR YYMM YYMMC YYMMD YYMMP YYMMS YYMMN YYMON "                         \
  "YYMMDD YYQ YYQC YYQD YYQP YYQS YYQN YYQR YYQRC YYQRD YYQRP YYQRS YYQRN "                        \
  "YYMMDDP YYMMDDC E8601DA YYMMDDN MMDDYYC MMDDYYS MMDDYYD YYMMDDS B8601DA "                       \
  "DDMMYYN YYMMDDD DDMMYYB DDMMYYP MMDDYYP YYMMDDB MMDDYYN DDMMYYC DDMMYYD "                       \
  "DDMMYYS MINGUO "
#define SAS_DATETIME_FORMATS                                                                       \
  " DATETIME DTWKDATX B8601DN B8601DT B8601DX B8601DZ B8601LX E8601DN "                            \
  "E8601DT E8601DX E8601DZ E8601LX DATEAMPM DTDATE DTMONYY DTMONYY DTWKDATX "                      \
  "DTYEAR TOD MDYAMPM "

#define PAGE_TYPE_MASK 0x0F00
// Keep "page_comp_type" bits
#define PAGE_TYPE_MASK2 (0xF000 | PAGE_TYPE_MASK)

enum PageType {
  page_type_meta = 0x0000,
  page_type_data = 0x0100,
  page_type_mix = 0x0200,
  // Unused
  // page_type_amd   = 0x0400,
  page_type_meta2 = 0x4000,
  // Unused
  // page_type_comp  = 0x9000,
};

enum SubheaderSignature {
  sh_signature_none = 0x00000000,
  sh_signature_row_size = 0xf7f7f7f7,
  sh_signature_column_size = 0xf6f6f6f6,
  sh_signature_column_text = 0xfffffffd,
  sh_signature_column_name = 0xffffffff,
  sh_signature_column_attributes = 0xfffffffc,
  sh_signature_column_format_and_label = 0xfffffbfe,
  sh_signature_unknown1 = 0xfffffc00,
  sh_signature_unknown2 = 0xfffffffe,
};

struct ParserFreeList {
  void *ptr;
  struct ParserFreeList *next;
};

struct Bytestring {
  uint8_t *data;
  size_t len;
};

struct ConstBytestring {
  const uint8_t *data;
  size_t len;
};

struct Parser {
  struct FileInfo fileinfo;
  const struct ParserConfig *config;
  struct ParserFreeList *freelist;
  struct ConstBytestring data;
  size_t data_min_offset;
  size_t header_count;
  size_t current_page;
  size_t current_subheader;
  size_t current_packed_row; // for mix and data pages
  size_t header_len, page_len;
  size_t row_length;
  size_t row_length8;
  struct bitmap *is_string_column_byte;
  struct bitmap *is_space_byte;
  // size_t mix_page_row_count;
  Bytestring *column_texts;
  size_t next_column_text_idx;
  size_t next_column_name_idx;
  size_t next_column_attributes_idx;
  size_t next_column_format_idx;
  struct {
    bool row_size : 1, column_size : 1;
  } seen_subheaders;
  bool row_compression_rle; // Otherwise RDC or no compression
  uint8_t *decompression_buf;
  size_t rows_processed;
};

struct Page {
  size_t offset;
  uint16_t type, block_count, subheader_count;
};

struct Subheader {
  size_t offset, len;
  uint32_t signature;
};

typedef void subheader_handler(struct Parser *ctx, const struct Subheader *sh, size_t page_offset);

#define L(msg, ...) printf(msg "\n", ##__VA_ARGS__)
#define if32bit(parser, v32, v64) (parser->fileinfo.is_32_bit ? (v32) : (v64))
#define is_valid_range(needle_offset, needle_len, haystack_len)                                    \
  ((needle_offset) < (haystack_len) && (needle_len) <= (haystack_len) &&                           \
   (needle_offset) <= (haystack_len) - (needle_len))

#if 0
static void print_bytes(uint8_t *b, size_t l) {
  if (l == 0) {
    printf("<empty bytes>\n");
    return;
  }
  size_t l2 = l;
  while (l2 >= 2 && b[l2 - 2] == b[l - 1])
    --l2;
  for (size_t i = 0; i < l2; ++i) {
    printf("%03d ", b[i]);
  }
  if (l2 != l) {
    printf(" (%lu trailing %03d)", l - l2, b[l - 1]);
  }
  printf("\t");
  for (size_t i = 0; i < l2; ++i) {
    printf("%c ", (b[i] < 128 && isprint(b[i])) ? b[i] : '*');
  }
  printf("\n");
}
#endif

static size_t next_multiple(size_t x, uint8_t n) {
  return ((x + n - 1) / n) * n;
}

// Memory allocation

static void *parserzalloc(struct Parser *parser, size_t n) {
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
  assume(n <= 1 * 1024 * 1024, std::bad_alloc());
#endif
  void *ptr = calloc(n, 1);
  assume(ptr != NULL, std::bad_alloc());
  struct ParserFreeList *tail = parser->freelist;
  parser->freelist = (struct ParserFreeList *)malloc(sizeof(struct ParserFreeList));
  assume(parser->freelist != NULL, std::bad_alloc());
  parser->freelist->ptr = ptr;
  parser->freelist->next = tail;
  return ptr;
}

// Reading from SAS7BDAT bytes

static void check_parser_data_read(const struct Parser *parser, size_t offset, size_t len) {
  check_read((offset) >= (parser)->data_min_offset && (len) <= (parser)->data.len &&
             (offset) <= (parser)->data.len - (len));
}

static void read_bytes(const struct Parser *parser, size_t offset, uint8_t *dst, size_t len) {
  check_parser_data_read(parser, offset, len);
  memcpy(dst, &parser->data.data[offset], len);
}

static uint8_t read_uint8(const struct Parser *parser, size_t offset) {
  check_parser_data_read(parser, offset, 1);
  return parser->data.data[offset];
}

#define _return_read_byteswap(parser, offset, intsize)                                             \
  do {                                                                                             \
    check_parser_data_read(parser, offset, sizeof(uint##intsize##_t));                             \
    uint##intsize##_t _val;                                                                        \
    memcpy(&_val, (void *)&parser->data.data[offset], sizeof(uint##intsize##_t));                  \
    if (parser->fileinfo.need_byteswap) {                                                          \
      _val = __builtin_bswap##intsize(_val);                                                       \
    }                                                                                              \
    return _val;                                                                                   \
  } while (0)

static uint16_t read_uint16(const struct Parser *parser, size_t offset) {
  _return_read_byteswap(parser, offset, 16);
}

static uint32_t read_uint32(const struct Parser *parser, size_t offset) {
  _return_read_byteswap(parser, offset, 32);
}

static uint64_t read_uint64(const struct Parser *parser, size_t offset) {
  _return_read_byteswap(parser, offset, 64);
}

static uint64_t read_word(const struct Parser *parser, size_t offset) {
  return if32bit(parser, read_uint32(parser, offset), read_uint64(parser, offset));
}

static size_t read_size_t(const struct Parser *parser, size_t offset) {
  return read_word(parser, offset);
}

static uint8_t wordsize(const struct Parser *parser) {
  return if32bit(parser, 4, 8);
}

static uint8_t bit_offset(const struct Parser *parser) {
  return if32bit(parser, 16, 32);
}

// Subheader parsing

static size_t subheader_contents_offset(const struct Parser *parser, size_t subheader_offset,
                                        size_t page_offset) {
  return page_offset + subheader_offset + wordsize(parser);
}

static void handle_row_size_subheader(struct Parser *parser, const struct Subheader *sh,
                                      size_t page_offset) {
  assume(!parser->seen_subheaders.row_size, sas7bdat_error("Unexpected row size subheader"));
  size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
  parser->row_length = read_size_t(parser, contents_offset + 4 * wordsize(parser));
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
  assume(parser->row_length <= 100 * 1024, sas7bdat_error("bad alloc"));
#endif
  assume(parser->row_length > 0, sas7bdat_error("No rows"));
  parser->decompression_buf = (uint8_t *)parserzalloc(parser, parser->row_length);
  size_t bitmap_size = next_multiple(parser->row_length, 8);
  parser->is_string_column_byte =
      (struct bitmap *)parserzalloc(parser, bitmap_compute_size(bitmap_size));
  parser->is_space_byte = (struct bitmap *)parserzalloc(parser, bitmap_compute_size(bitmap_size));
  bitmap_init(parser->is_space_byte, bitmap_size, false);
  bitmap_init(parser->is_string_column_byte, bitmap_size, false);
  parser->fileinfo.row_count = read_size_t(parser, contents_offset + 5 * wordsize(parser));
  // parser->mix_page_row_count = read_size_t(parser, contents_offset + 14 * wordsize(parser));
  parser->seen_subheaders.row_size = true;
}

static void handle_column_size_subheader(struct Parser *parser, const struct Subheader *sh,
                                         size_t page_offset) {
  assume(!parser->seen_subheaders.column_size, sas7bdat_error("Unexpected column size subheader"));
  size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
  parser->fileinfo.column_count = read_size_t(parser, contents_offset);
  assume(parser->fileinfo.column_count > 0, sas7bdat_error("No columns in column_size subheader"));
  parser->fileinfo.columns = (struct ColumnInfo *)parserzalloc(
      parser, sizeof(struct ColumnInfo) * parser->fileinfo.column_count);
  parser->column_texts =
      (Bytestring *)parserzalloc(parser, sizeof(Bytestring) * parser->fileinfo.column_count);
  parser->seen_subheaders.column_size = true;
}

static void handle_column_text_subheader(struct Parser *parser, const struct Subheader *sh,
                                         size_t page_offset) {
  size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
  uint16_t text_length = read_uint16(parser, contents_offset);
  struct Bytestring text = {.data = (uint8_t *)parserzalloc(parser, text_length + 1),
                            .len = text_length};
  read_bytes(parser, contents_offset, text.data, text.len);
  text.data[text.len] = '\0';
  if (parser->next_column_text_idx == 0) {
    parser->row_compression_rle = memmem(text.data, text.len, SAS_COMPRESSION_SIGNATURE_RLE,
                                         strlen(SAS_COMPRESSION_SIGNATURE_RLE));
  }
  parser->column_texts[parser->next_column_text_idx] = text;
  ++parser->next_column_text_idx;
}

static void handle_column_name_subheader(struct Parser *parser, const struct Subheader *sh,
                                         size_t page_offset) {
  size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
  size_t n = (sh->len - 2 * wordsize(parser) - 12) / 8;
  assume(parser->next_column_name_idx + n <= parser->fileinfo.column_count,
         sas7bdat_error("Unexpected column name subheader"));
  for (size_t i = 0; i < n; ++i) {
    struct ColumnInfo *colinfo = &parser->fileinfo.columns[parser->next_column_name_idx];
    size_t col_offset = contents_offset + 8 * (i + 1);
    uint16_t column_text_idx = read_uint16(parser, col_offset);
    uint16_t name_offset = read_uint16(parser, col_offset + 2);
    uint16_t name_len = read_uint16(parser, col_offset + 4);
    assume(column_text_idx < parser->next_column_text_idx,
           sas7bdat_error("Unexpected column text index"));
    assume(is_valid_range(name_offset, name_len, parser->column_texts[column_text_idx].len),
           sas7bdat_error("Unexpected column name offset or length"));
    char *name_buf = (char *)parserzalloc(parser, name_len + 1);
    snprintf(name_buf, name_len + 1, "%s",
             &parser->column_texts[column_text_idx].data[name_offset]);
    colinfo->name = name_buf;
    ++parser->next_column_name_idx;
  }
}

static void handle_column_attributes_subheader(struct Parser *parser, const struct Subheader *sh,
                                               size_t page_offset) {
  size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
  size_t n = (sh->len - if32bit(parser, 20, 28)) / if32bit(parser, 12, 16);
  assume(parser->next_column_attributes_idx + n <= parser->fileinfo.column_count,
         sas7bdat_error("Unexpected column attributes subheader"));
  for (size_t i = 0; i < n; ++i) {
    struct ColumnInfo *col = &parser->fileinfo.columns[parser->next_column_attributes_idx];
    size_t col_offset = contents_offset + 8 + i * if32bit(parser, 12, 16);
    col->offset = read_size_t(parser, col_offset);
    col->len = read_uint32(parser, col_offset + wordsize(parser));
    assume(is_valid_range(col->offset, col->len, parser->row_length),
           sas7bdat_error("Unexpected column attribute offset or length"));
    col->format = read_uint8(parser, col_offset + wordsize(parser) + 6) == 1 ? column_format_double
                                                                             : column_format_string;
    assume(col->len > 0, sas7bdat_error("Found column of length 0"));
    assume(col->format != column_format_none, sas7bdat_error("Found column with unknown format"));
    if (col->format == column_format_double) {
      assume(col->len >= 3 && col->len <= 8, sas7bdat_error("Found invalid numeric column length"));
    }
    ++parser->next_column_attributes_idx;
  }
}

static void handle_column_format_and_label_subheader(struct Parser *parser,
                                                     const struct Subheader *sh,
                                                     size_t page_offset) {
  assume(parser->next_column_format_idx < parser->fileinfo.column_count,
         sas7bdat_error("Unexpected column format subheader"));

  struct ColumnInfo *colinfo = &parser->fileinfo.columns[parser->next_column_format_idx];

  bool found_override = false;
  const struct FormatOverride *column_format_overrides = parser->config->column_format_overrides;
  if (column_format_overrides != NULL) {
    while (column_format_overrides->column_name != NULL) {
      if (strstr(column_format_overrides->column_name, colinfo->name)) {
        if (column_format_overrides->format >= column_format_double) {
          assume(colinfo->format == column_format_double,
                 std::invalid_argument("Invalid column format override"));
        }
        colinfo->format = column_format_overrides->format;
        if (column_format_overrides->len > 0) {
          colinfo->len = column_format_overrides->len;
        }
        found_override = true;
        break;
      }
      ++column_format_overrides;
    }
  }

  if (!found_override && colinfo->format == column_format_double) {
    size_t contents_offset = subheader_contents_offset(parser, sh->offset, page_offset);
    size_t column_format_contents_offset = contents_offset + 22 + 2 * wordsize(parser);
    uint16_t column_format_index = read_uint16(parser, column_format_contents_offset);
    uint16_t column_format_offset = read_uint16(parser, column_format_contents_offset + 2);
    uint16_t column_format_len = read_uint16(parser, column_format_contents_offset + 4);
    if (column_format_index >= parser->next_column_text_idx) {
      // TODO(corr) in readstat this is an error (?)
      // probably related to amd pages
      column_format_index = parser->next_column_text_idx - 1;
    }
    // Shortest date/datetime format string has length 3
    if (column_format_len >= 3) {
      char column_format_buf[column_format_len + 3];
      assume(is_valid_range(column_format_offset, column_format_len,
                            parser->column_texts[column_format_index].len),
             sas7bdat_error("Unexpected column name offset or length"));
      snprintf(column_format_buf, sizeof(column_format_buf) - 1, " %s ",
               &parser->column_texts[column_format_index].data[column_format_offset]);
      if (strstr(SAS_DATE_FORMATS, column_format_buf)) {
        colinfo->format = column_format_date;
      } else if (strstr(SAS_DATETIME_FORMATS, column_format_buf)) {
        colinfo->format = column_format_datetime;
      }
    }
  }

  ++parser->next_column_format_idx;
}

static void handle_unknown(struct Parser *, const struct Subheader *sh, size_t) {
}

static subheader_handler *get_subheader_handler(uint32_t signature) {
  switch (signature) {
  case sh_signature_row_size:
    return handle_row_size_subheader;
  case sh_signature_column_size:
    return handle_column_size_subheader;
  case sh_signature_column_text:
    return handle_column_text_subheader;
  case sh_signature_column_name:
    return handle_column_name_subheader;
  case sh_signature_column_attributes:
    return handle_column_attributes_subheader;
  case sh_signature_column_format_and_label:
    return handle_column_format_and_label_subheader;
  case sh_signature_unknown1:
    return handle_unknown;
  case sh_signature_unknown2:
    return handle_unknown;
  default:
    // if ((signature & 0xfffffff8) == 0xfffffff8) { }
    return NULL;
  }
}

static bool parse_subheader(const struct Parser *parser, size_t page_offset, size_t subheader_idx,
                            struct Subheader *sh) {
  // TODO(corr) check ptr size_t size
  size_t start = page_offset + bit_offset(parser) + 8 + subheader_idx * if32bit(parser, 12, 24);
  uint8_t compression = read_uint8(parser, start + 2 * wordsize(parser));
  sh->offset = read_size_t(parser, start);
  sh->len = read_size_t(parser, start + wordsize(parser));
  if (compression == 1 || sh->len == 0) {
    // Truncated or empty subheader
    return true;
  }
  if (compression == 4) {
    sh->signature = sh_signature_none;
  } else {
    sh->signature = read_uint32(parser, page_offset + sh->offset);
    if (sh->signature == 0xffffffff && !parser->fileinfo.is_little_endian &&
        !parser->fileinfo.is_32_bit) {
      sh->signature = read_uint32(parser, page_offset + sh->offset + 4);
      // print_bytes(&sh->signature, 4);
    }
    if (get_subheader_handler(sh->signature) == NULL) {
      sh->signature = sh_signature_none;
    }
  }
  return false;
}

// Page parsing

static bool page_is_meta_type(size_t type) {
  return type == page_type_meta || type == page_type_meta2;
}

// Check if a page may contain (compressed) data in its subheaders.
static bool page_may_have_data_subheaders(size_t type) {
  return page_is_meta_type(type);
}

// Check if a page may contain data outside its subheaders ("packed data").
static bool page_may_have_packed_data(size_t type) {
  return type == page_type_data || type == page_type_mix;
}

static size_t page_packed_row_count(const struct Page *page) {
  assert2(page_may_have_packed_data(page->type));
  return page->block_count - page->subheader_count;
}

static int _cmp_columninfo_offset(const void *a, const void *b) {
  const struct ColumnInfo *ca = (struct ColumnInfo *)a, *cb = (struct ColumnInfo *)b;
  return ca->offset > cb->offset ? 1 : (ca->offset == cb->offset ? 0 : -1);
}

static bool parse_meta_page_data(struct Parser *parser, const struct Page *page) {
  // TODO(corr) parser->rows_processed < row_count
  // TODO(corr) wrong on some files (not truncated?)
  assume(parser->current_subheader < page->subheader_count - 1,
         sas7bdat_error("Unexpected subheader"));

  // TODO(corr) if we abort here due to rows_processed >= row_count, will we
  // come back here and fall into an infinite loop or are exiting somwhere
  // else?
  bool yield = false;
  size_t i;
  for (i = parser->current_subheader; !yield && i < page->subheader_count; ++i) {
    // TODO(corr) previous version first parsed all subheaders, why?
    struct Subheader sh;
    if (parse_subheader(parser, page->offset, i, &sh)) {
      continue;
    }
    assert2(sh.signature == sh_signature_none);
    // Data subheaders don't have a signature, thus start wordsize() earlier
    // than other subheaders
    size_t contents_offset =
        subheader_contents_offset(parser, sh.offset, page->offset) - wordsize(parser);
    check_parser_data_read(parser, contents_offset, sh.len);
    const uint8_t *row_source = &parser->data.data[contents_offset];
    if (sh.len < parser->row_length) {
      sas_decompressor *decompressor;
      if (parser->row_compression_rle) {
        decompressor = rle;
        bitmap_set(parser->is_space_byte, 0, bitmap_data_size(parser->is_space_byte), false);
      } else {
        decompressor = rdc;
      }
      size_t len_decompressed =
          decompressor(row_source, sh.len, parser->decompression_buf, parser->row_length,
                       parser->is_string_column_byte, parser->is_space_byte);
      assume(len_decompressed == parser->row_length,
             sas7bdat_error("Too few decompressed bytes in row"));
      yield = parser->config->on_row(parser->config->userdata, parser->decompression_buf,
                                     decompressor == rle);
    } else {
      yield = parser->config->on_row(parser->config->userdata, row_source, false);
    }
  }
  assert2(i > 0);
  if (i >= page->subheader_count - 1 /* truncated */) {
    ++parser->current_page;
    parser->current_subheader = 0;
  } else {
    parser->current_subheader = i;
  }
  return yield;
}

static bool parse_mix_or_data_page_data(struct Parser *parser, const struct Page *page) {
  assume(parser->current_packed_row < page->block_count, sas7bdat_error("Unexpected packaged row"));
  // TODO(corr) parser->rows_processed < row_count

  size_t data_start = page->offset + bit_offset(parser) + 8;
  if (page->type == page_type_mix) {
    // Skip leading subheaders
    data_start += page->subheader_count * if32bit(parser, 12, 24);
    data_start += data_start % 8;
  }
  bool yield = false;
  size_t i;
  assert2(page_packed_row_count(page) > 0);
  for (i = parser->current_packed_row; !yield && i < page_packed_row_count(page); ++i) {
    yield = parser->config->on_row(parser->config->userdata,
                                   &parser->data.data[data_start + i * parser->row_length], false);
  }
  assert2(i > 0);
  if (i >= page_packed_row_count(page)) {
    ++parser->current_page;
    parser->current_packed_row = 0;
  } else {
    parser->current_packed_row = i;
  }
  return yield;
}

static size_t page_offset(const struct Parser *parser, size_t page_idx) {
  return parser->header_len + page_idx * parser->page_len;
}

static void handle_pagefault(struct Parser *parser, size_t requested_data_start,
                             size_t requested_data_len) {
  // TODO(ref) All of this is too complicated. Replace data_min_offset.
  size_t new_data_offset, new_data_len;
  const uint8_t *new_data = NULL;
  parser->config->on_pagefault(parser->config->userdata, requested_data_start, requested_data_len,
                               &new_data_offset, &new_data, &new_data_len);
  assume(new_data != NULL, std::runtime_error("Error loading more data from input buffer"));
  assume((size_t)new_data <= SIZE_MAX - new_data_offset &&
             (size_t)new_data + new_data_offset >= requested_data_start

             && new_data_len <= SIZE_MAX - requested_data_start,
         std::domain_error("Invalid allocation"));
  parser->data_min_offset = requested_data_start;
  parser->data.data = new_data + new_data_offset - requested_data_start;
  parser->data.len = new_data_len + requested_data_start;
}

// TODO(ref) name
static void ensure_current_page(struct Parser *parser) {
  size_t current_page_offset = page_offset(parser, parser->current_page);
  size_t next_page_offset = page_offset(parser, parser->current_page + 1);
  if (current_page_offset < parser->data_min_offset || next_page_offset > parser->data.len) {
    handle_pagefault(parser, current_page_offset, parser->page_len);
  }
}

static struct Page read_current_page(const struct Parser *parser) {
  size_t current_page_offset = page_offset(parser, parser->current_page);
  struct Page page = {
      .offset = current_page_offset,
      .type = (uint16_t)(read_uint16(parser, current_page_offset + bit_offset(parser)) &
                         PAGE_TYPE_MASK2),
      .block_count = read_uint16(parser, current_page_offset + bit_offset(parser) + 2),
      .subheader_count = read_uint16(parser, current_page_offset + bit_offset(parser) + 4),
  };
  switch (page.type) {
  case page_type_meta:
  case page_type_meta2:
    assume(page.subheader_count > 0, sas7bdat_error("Unexpected empty meta page"));
    break;
  case page_type_mix:
  case page_type_data:
    break;
  default:
    goto unknown_page_type;
  }
  assert2((page_may_have_packed_data(page.type) ^ page_may_have_data_subheaders(page.type)));
unknown_page_type:
  assume(page.subheader_count < SIZE_MAX, std::domain_error("Too many subheaders in page"));
  return page;
}

static size_t process_metadata_subheaders(struct Parser *parser, const struct Page *page) {
  size_t i;
  for (i = 0; i < page->subheader_count; ++i) {
    struct Subheader sh;
    if (parse_subheader(parser, page->offset, i, &sh)) {
      continue;
    } else if (sh.signature == sh_signature_none) {
      assume(page_may_have_data_subheaders(page->type),
             sas7bdat_error("Expeced subheader with signature"));
      assert2(i > 0);
      break;
    } else {
      subheader_handler *handler = get_subheader_handler(sh.signature);
      assume(handler != NULL, sas7bdat_error("Unknown subheader signature"));
      handler(parser, &sh, page->offset);
    }
  }
  return i;
}

// Parser object lifecycle

size_t parser_struct_size() {
  return sizeof(struct Parser);
}

void parser_init(struct Parser *parser, const struct ParserConfig *config) {
  assume(parser != NULL, std::invalid_argument("parser must not be NULL"));
  assume(config->on_pagefault != NULL, std::invalid_argument("on_pagefault must not be NULL"));
  assume(config->on_metadata != NULL, std::invalid_argument("on_metadata must not be NULL"));
  assume(config->on_row != NULL, std::invalid_argument("on_row must not be NULL"));

  memset(parser, 0, parser_struct_size());
  parser->config = config;

  handle_pagefault(parser, 0, 1000);
  assume(!strcmp((char *)parser->data.data, (char *)SAS_MAGIC),
         sas7bdat_error("Invalid SAS magic, not a SAS7BDAT file?"));

  parser->fileinfo.is_32_bit = read_uint8(parser, 32) != '3';
  parser->fileinfo.is_little_endian = read_uint8(parser, 37);
  parser->fileinfo.need_byteswap = parser->fileinfo.is_little_endian != machine_is_little_endian();
  parser->fileinfo.encoding = (enum Encoding)read_uint8(parser, 70);
  uint8_t align = read_uint8(parser, 35) == '3' ? 4 : 0;
  parser->header_len = read_uint32(parser, 196 + align);
  parser->page_len = read_uint32(parser, 200 + align);
  parser->fileinfo.page_count = read_size_t(parser, 204 + align);
  assume(parser->page_len > 0 && parser->fileinfo.page_count > 0 && parser->header_len > 0,
         sas7bdat_error("Invalid page length, page count, or header length"));
  // header_count = actual headers. == page_count for valid files.
  if (config->filesize_override) {
    assume(config->filesize_override >= parser->header_len,
           std::invalid_argument("filesize_override must be >= header_len"));
    parser->header_count = (config->filesize_override - parser->header_len) / parser->page_len;
  } else {
    parser->header_count = parser->fileinfo.page_count;
  }

  for (; parser->current_page < parser->header_count; ++parser->current_page) {
    ensure_current_page(parser);
    struct Page page = read_current_page(parser);
    if (page_is_meta_type(page.type)) {
      assume(page.block_count == page.subheader_count, "Invalid subheader count in page");
      size_t n_subheaders_processed = process_metadata_subheaders(parser, &page);
      if (n_subheaders_processed < page.subheader_count) {
        // Set up page + subheader pointers for call to parse()
        parser->current_subheader = n_subheaders_processed;
        goto done;
      }
    } else if (page_may_have_packed_data(page.type)) {
      assume(page_packed_row_count(&page) > 0, sas7bdat_error("Empty packed data page"));
      process_metadata_subheaders(parser, &page);
      // Set up page + subheader pointers for call to parse()
      parser->current_packed_row = 0;
      goto done;
    }
  }

  assume(false, sas7bdat_error("No rows 2"));

done:

  assume(parser->seen_subheaders.column_size, sas7bdat_error("Missing column size subheader"));
  assume(parser->seen_subheaders.row_size, sas7bdat_error("Missing row size subheader"));
  assume(parser->next_column_name_idx == parser->fileinfo.column_count &&
             parser->next_column_attributes_idx == parser->fileinfo.column_count &&
             parser->next_column_format_idx == parser->fileinfo.column_count,
         sas7bdat_error("Incomplete column metadata"));

#if 0
  // Sort columns by offset
  // TODO(perf) Sometimes decreases performance but never increases it.
  qsort(parser->fileinfo.columns, parser->fileinfo.column_count, sizeof(*parser->fileinfo.columns), _cmp_columninfo_offset);
#endif

  for (size_t i = 0; i < parser->fileinfo.column_count; ++i) {
    bool is_string_column = parser->fileinfo.columns[i].format == column_format_string;
    bitmap_set(parser->is_string_column_byte, parser->fileinfo.columns[i].offset,
               parser->fileinfo.columns[i].offset + parser->fileinfo.columns[i].len,
               is_string_column);
  }

  parser->config->on_metadata(parser->config->userdata, &parser->fileinfo);
}

bool parse(struct Parser *parser) {
  size_t stop =
      parser->config->max_pages > 0 ? parser->current_page + parser->config->max_pages : SIZE_MAX;
  if (stop > parser->header_count) {
    stop = parser->header_count;
  }
  bool yield = false;
  for (; !yield && parser->current_page < stop;) {
    ensure_current_page(parser);
    struct Page page = read_current_page(parser);
    if (page_may_have_data_subheaders(page.type)) {
      yield = parse_meta_page_data(parser, &page);
    } else if (page_may_have_packed_data(page.type)) {
      yield = parse_mix_or_data_page_data(parser, &page);
    } else {
      ++parser->current_page;
      continue;
    }
  }
  // TODO(corr) what to return if actual pages read < expected pages read?
  // (truncated buffer)
  return yield; // have more data?
}

void parser_deinit(struct Parser *parser) {
  for (struct ParserFreeList *item = parser->freelist; item != NULL;) {
    free(item->ptr);
    struct ParserFreeList *next = item->next;
    free(item);
    item = next;
  }
}

size_t column_fast_max_length(const struct Parser *parser, const struct ColumnInfo *colinfo) {
  // TODO(perf) optimize: 50% of cases within single cell max 4% faster
  assert2(colinfo->len < SIZE_MAX);
  assert2(colinfo->offset < SIZE_MAX - colinfo->len - 1);
  size_t first_unknown_byte = bitmap_get_last_bit(parser->is_space_byte, colinfo->offset,
                                                  colinfo->offset + colinfo->len, 0);
  if (first_unknown_byte == SIZE_MAX || first_unknown_byte < colinfo->offset) {
    return 0;
  } else {
    return first_unknown_byte - colinfo->offset + 1;
  }
}
