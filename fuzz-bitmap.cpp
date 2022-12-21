#include "src/bitmap.hpp"

enum Op {
  first_op,
  // get1,
  get_first_bit,
  get_last_bit,
  set,
  xor_,
  // or_,
  and_,
  last_op,
};

#define nodata

struct FuzzRun {
  size_t data_size1, data_size2;
  size_t op;
  size_t arg1, arg2, arg3;
#ifndef nodata
  uint8_t initial_data_start[1];
#endif
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  struct FuzzRun *fr = (struct FuzzRun *)data;
  if (size < sizeof(FuzzRun)) {
    return -1;
  }
  if (fr->op <= first_op || fr->op >= last_op) {
    return -1;
  }
  size_t data_size1_8 = fr->data_size1 / 8 + 1;
  size_t data_size2_8 = fr->data_size2 / 8 + 1;
  if (data_size1_8 >= 140 || data_size2_8 >= 140) {
    return -1;
  }
#ifdef nodata
  if (size != sizeof(FuzzRun)) {
    return -1;
  }
#else
  if (size < data_size1_8 + data_size2_8 + sizeof(FuzzRun)) {
    return -1;
  }
#endif
  uint8_t bitmap1_data[sizeof(struct bitmap) + data_size1_8];
  struct bitmap *b1 = (struct bitmap *)&bitmap1_data;
  b1->data_size = fr->data_size1;
#ifdef nodata
  for (size_t i = 0; i < data_size1_8; ++i)
    ((uint8_t *)&b1->data_start[0])[i] = rand() % 256;
#else
  memcpy(&b1->data_start[0], fr->initial_data_start, data_size1_8);
#endif
  try {
    switch (fr->op) {
    // case get1:
    //     bitmap_get1(b1, fr->arg1);
    //     break;
    case get_first_bit:
      if (fr->data_size2 > 0 || fr->arg3 > 1)
        return -1;
      bitmap_get_first_bit(b1, fr->arg1, fr->arg2, fr->arg3);
      break;
    case get_last_bit:
      if (fr->data_size2 > 0 || fr->arg3 > 1)
        return -1;
      bitmap_get_last_bit(b1, fr->arg1, fr->arg2, fr->arg3);
      break;
    case set:
      if (fr->data_size2 > 0 || fr->arg3 > 1)
        return -1;
      bitmap_set(b1, fr->arg1, fr->arg2, fr->arg3);
      break;
    case xor_:
    // case or_:
    case and_: {
      if (fr->arg3 != 0)
        return -1;
      uint8_t bitmap2_data[sizeof(struct bitmap) + data_size2_8];
      struct bitmap *b2 = (struct bitmap *)&bitmap2_data;
      b2->data_size = fr->data_size2;
#ifdef nodata
      for (size_t i = 0; i < data_size2_8; ++i)
        ((uint8_t *)&b2->data_start[0])[i] = rand() % 256;
#else
      memcpy(&b2->data_start[0], &fr->initial_data_start[data_size1_8], data_size2_8);
#endif
      switch (fr->op) {
      case xor_:
        bitmap_xor(b1, b2, fr->arg1, fr->arg2);
        break;
      // case or_:  bitmap_or(b1, b2, fr->arg1, fr->arg2); break;
      case and_:
        bitmap_and(b1, b2, fr->arg1, fr->arg2);
        break;
      }
    } break;
    default:
      return -1;
    }
  } catch (...) {
  }
  return 0;
}
