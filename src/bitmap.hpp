#ifndef BITMAP_HPP
#define BITMAP_HPP
#include "assume.hpp"
#include <string.h>

#define align64(x) ((x) & -64)
#define aligned64(x) !((x) % 64)

#define restrict __restrict__

struct bitmap {
  size_t data_size;
  uint64_t data_start[1];
};

#define _bitmap_data(b) (&(b)->data_start[0])
#define _bitmap_elem(b, pos) (_bitmap_data(b))[(pos) / 64]

static size_t bitmap_compute_size(size_t data_size) {
  return sizeof(struct bitmap) + data_size / 8;
}

static size_t bitmap_data_size(const struct bitmap *b) {
  return b->data_size;
}

static size_t bitmap_size(const struct bitmap *b) {
  return bitmap_compute_size(b->data_size);
}

static bool bitmap_get1(const struct bitmap *b, size_t pos) {
  assert2(pos < b->data_size);
  return (_bitmap_elem(b, pos) >> (pos % 64)) & 0b1;
}

static void bitmap_to_bytes(const struct bitmap *b, uint8_t *out) {
  for (size_t i = 0; i < b->data_size; ++i) {
    out[i] = bitmap_get1(b, i);
  }
}

#define _safe_shift_left(x, s) ((s) >= 64 ? 0ull : x << (s))
#define _rangemask(start, stop)                                                                    \
  ((~0ull << (start % 64)) & ~_safe_shift_left(~0ull, stop - align64(start)))

#define _bitmap_op_loop(b1, b2, start, stop, body)                                                 \
  do {                                                                                             \
    assert2(b1->data_size == b2->data_size);                                                       \
    assert2(stop <= b1->data_size);                                                                \
    for (size_t pos = start; pos < stop; pos = align64(pos) + 64) {                                \
      body;                                                                                        \
    }                                                                                              \
  } while (0)
#define _bitmap_op_loop_reverse(b1, b2, start, stop, body)                                         \
  do {                                                                                             \
    assert2(b1->data_size == b2->data_size);                                                       \
    assert2(stop <= b1->data_size);                                                                \
    size_t aligned_pos = stop + 64 - (stop % 64);                                                  \
    do {                                                                                           \
      aligned_pos -= 64;                                                                           \
      size_t pos = aligned_pos < start ? start : aligned_pos;                                      \
      body;                                                                                        \
    } while (aligned_pos >= 64 && aligned_pos > start);                                            \
  } while (0)
#define _define_range_op(name, op, maskop)                                                         \
  static void bitmap_##name(struct bitmap *restrict b1, const struct bitmap *restrict b2,          \
                            size_t start, size_t stop) {                                           \
    _bitmap_op_loop(b1, b2, start, stop,                                                           \
                    _bitmap_elem(b1, pos) op## =                                                   \
                        _bitmap_elem(b2, pos) maskop _rangemask(pos, stop));                       \
  }

static size_t bitmap_get_first_bit(const struct bitmap *b, size_t start, size_t stop, bool v) {
  assert2(stop <= b->data_size);
  assert2(start <= stop);
  uint64_t elem;
  if (v) {
    _bitmap_op_loop(b, b, start, stop,
                    if ((elem = _bitmap_elem(b, pos) & _rangemask(pos, stop))) return align64(pos) +
                        __builtin_ctzll(elem));
  } else {
    _bitmap_op_loop(
        b, b, start, stop,
        if ((elem = ~(_bitmap_elem(b, pos) | ~_rangemask(pos, stop)))) return align64(pos) +
            __builtin_ctzll(elem));
  }
  return SIZE_MAX;
}

static size_t bitmap_get_last_bit(const struct bitmap *b, size_t start, size_t stop, bool v) {
  assert2(stop <= b->data_size);
  assert2(start <= stop);
  uint64_t elem;
  if (v) {
    _bitmap_op_loop_reverse(
        b, b, start, stop,
        if ((elem = _bitmap_elem(b, pos) & _rangemask(pos, stop))) return align64(pos) + 63 -
            __builtin_clzll(elem));
  } else {
    _bitmap_op_loop_reverse(
        b, b, start, stop,
        if ((elem = ~(_bitmap_elem(b, pos) | ~_rangemask(pos, stop)))) return align64(pos) + 63 -
            __builtin_clzll(elem));
  }
  return SIZE_MAX;
}

static void bitmap_set(struct bitmap *b, size_t start, size_t stop, bool v) {
  assert2(stop <= b->data_size);
  assert2(start <= stop);
  if (start % 8 || stop % 8) {
    if (v) {
      _bitmap_op_loop(b, b, start, stop, _bitmap_elem(b, pos) |= _rangemask(pos, stop));
    } else {
      _bitmap_op_loop(b, b, start, stop, _bitmap_elem(b, pos) &= ~_rangemask(pos, stop));
    }
  } else {
    memset(&((uint8_t *)_bitmap_data(b))[start / 8], v ? 0xff : 0, (stop / 8) - (start / 8));
  }
}

_define_range_op(xor, ^, &);
_define_range_op(or, |, &);
_define_range_op(and, &, | ~);

static void bitmap_init(struct bitmap *b, size_t data_size, bool v) {
  b->data_size = data_size;
  bitmap_set(b, 0, data_size, v);
}

#endif
