#include "assume.hpp"
#include "bitmap.hpp"

#define safe_memmove(dst, dst_off, dst_len, src, src_off, src_len, len)                            \
  do {                                                                                             \
    check_read(dst_off + len <= dst_len);                                                          \
    check_read(src_off + len <= src_len);                                                          \
    (dst == src ? memmove : memcpy)((void *)(dst + dst_off), (void *)(src + src_off), len);        \
  } while (0)

#define safe_memset(dst, dst_off, dst_len, val, len)                                               \
  do {                                                                                             \
    check_read(dst_off + len <= dst_len);                                                          \
    memset((void *)(dst + dst_off), val, len);                                                     \
  } while (0)

#define restrict __restrict__

typedef size_t sas_decompressor(const uint8_t *restrict in, size_t inlen, uint8_t *restrict out,
                                size_t outlen, const struct bitmap *restrict is_string_column_byte,
                                struct bitmap *restrict is_space_byte);

static size_t rle(const uint8_t *restrict in, size_t inlen, uint8_t *restrict out, size_t outlen,
                  const struct bitmap *restrict is_string_column_byte,
                  struct bitmap *restrict is_space_byte) {
  size_t inpos = 0;
  size_t outpos = 0;
  bitmap_set(is_space_byte, 0, bitmap_size(is_space_byte), false);
  while (inpos < inlen) {
    uint8_t ctrl = in[inpos] & 0xF0;
    uint8_t eob = in[inpos] & 0x0F;
    check_read(ctrl == 0xD0 || ctrl == 0xE0 || ctrl == 0xF0 || inpos + 1 < inlen);
    size_t nbytes;
    switch (ctrl) {
    case 0x00:
      nbytes = in[inpos + 1] + 64 + eob * 256;
      safe_memmove(out, outpos, outlen, in, inpos + 2, inlen, nbytes);
      inpos += 2 + nbytes;
      break;

    case 0x40:
      nbytes = in[inpos + 1] + 18 + eob * 256;
      check_read(inpos + 2 < inlen);
      safe_memset(out, outpos, outlen, in[inpos + 2], nbytes);
      inpos += 3;
      break;

    case 0x60:
    case 0x70:
      nbytes = in[inpos + 1] + 17 + eob * 256;
      // todo(perf) optimize: 50% of cases within single cell max 4% faster
      check_read(outpos + nbytes <= outlen);
      bitmap_or(is_space_byte, is_string_column_byte, outpos, outpos + nbytes);
      // todo(perf) skip memset?
      safe_memset(out, outpos, outlen, ctrl == 0x60 ? 0x20 : 0x00, nbytes);
      inpos += 2;
      break;

    case 0x80:
    case 0x90:
    case 0xA0:
    case 0xB0:
      nbytes = eob + (ctrl == 0x80 ? 1 : ctrl == 0x90 ? 17 : ctrl == 0xA0 ? 33 : 49);
      safe_memmove(out, outpos, outlen, in, inpos + 1, inlen, nbytes);
      inpos += 1 + nbytes;
      break;

    case 0xC0:
      nbytes = eob + 3;
      safe_memset(out, outpos, outlen, in[inpos + 1], nbytes);
      inpos += 2;
      break;

    case 0xD0:
      nbytes = eob + 2;
      safe_memset(out, outpos, outlen, 0x40, nbytes);
      inpos += 1;
      break;

    case 0xE0:
    case 0xF0:
      nbytes = eob + 2;
      check_read(outpos + nbytes <= outlen);
      bitmap_or(is_space_byte, is_string_column_byte, outpos, outpos + nbytes);
      safe_memset(out, outpos, outlen, ctrl == 0xE0 ? 0x20 : 0x00, nbytes);
      inpos += 1;
      break;

    default:
      assume(false, sas7bdat_error("Unknown control character"));
    }

    outpos += nbytes;
  }
  return outpos;
}

static size_t rdc(const uint8_t *restrict in, size_t inlen, uint8_t *restrict out, size_t outlen,
                  const struct bitmap *restrict is_string_column_byte,
                  struct bitmap *restrict is_space_byte) {
  size_t inpos = 0;
  size_t outpos = 0;
  while (inpos + 1 < inlen) {
    uint16_t ctrl = (in[inpos] << 8) + in[inpos + 1];
    inpos += 2;

    for (size_t i = 0; i < 16; ++i) {
      size_t nbytes;
      uint16_t current_bit = 1 << (15 - i);
      if ((ctrl & current_bit) == 0) {
        if (inpos >= inlen) {
          break;
        }
        nbytes = 1;
        safe_memmove(out, outpos, outlen, in, inpos, inlen, nbytes);
        inpos += 1;
      } else {
        check_read(inpos + 1 < inlen);

        uint8_t byte1 = in[inpos];
        uint8_t byte2 = in[inpos + 1];
        uint8_t byte1half1 = byte1 >> 4;
        uint8_t byte1half2 = byte1 & 0x0f;
        inpos += 2;

        switch (byte1half1) {
        case 0:
          nbytes = byte1 + 3;
          // todo(perf) is_space_byte
          safe_memset(out, outpos, outlen, byte2, nbytes);
          break;
        case 1:
          nbytes = byte1half2 + 19 + (byte2 << 4);
          check_read(inpos < inlen);
          // todo(perf) is_space_byte
          safe_memset(out, outpos, outlen, in[inpos], nbytes);
          inpos += 1;
          break;
        case 2: {
          size_t back_offset = byte1half2 + 3 + (byte2 << 4);
          check_read(inpos < inlen);
          nbytes = in[inpos] + 16;
          check_read(outpos >= back_offset);
          safe_memmove(out, outpos, outlen, out, outpos - back_offset, outpos, nbytes);
          inpos += 1;
          break;
        }
        default: {
          size_t back_offset = byte1half2 + 3 + (byte2 << 4);
          nbytes = byte1half1;
          check_read(outpos >= back_offset);
          safe_memmove(out, outpos, outlen, out, outpos - back_offset, outpos, nbytes);
          break;
        }
        }
      }
      outpos += nbytes;
    }
  }
  return outpos;
}
