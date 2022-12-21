#include "src/sas7bdat.cpp"
#include <assert.h>

static const uint8_t *data2;
static size_t size2;
static unsigned int n, m;

void on_pagefault(void *userdata, size_t requested_data_start, size_t requested_data_len,
                  size_t *new_data_offset, const uint8_t **new_data, size_t *new_data_len) {
  *new_data_offset = 0;
  *new_data = data2;
  *new_data_len = size2;
}

void on_metadata(void *userdata, const struct FileInfo *) {
}
bool on_row(void *userdata, const uint8_t *buf, bool have_fast_space_offsets) {
  if (n++ > m) {
    n = 0;
    return true;
  }
  return false;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  srand(size > 0 ? data[0] : 0);

  data2 = data;
  size2 = size;

  struct ParserConfig cfg = {
      .on_pagefault = on_pagefault, .on_metadata = on_metadata, .on_row = on_row};
  assert(parser_struct_size() < 500);
  uint8_t parser[500];
  n = 0;
  m = rand() / ((RAND_MAX + 1u) / 1000);
  try {
    parser_init((struct Parser *)&parser, &cfg);
    for (int i = 0; i < 3; ++i) {
      if (!parse((struct Parser *)parser)) {
        break;
      }
    }
  } catch (...) {
  }
  parser_deinit((struct Parser *)&parser);
  return 0;
}
