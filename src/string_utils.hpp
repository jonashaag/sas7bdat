#ifndef STRING_UTILS_HPP
#define STRING_UTILS_HPP
#include <stdint.h>
#include <stdlib.h>

#define iswhitespace64(x)                                                                          \
  (((x) & ((uint64_t)0xffffffffffffffff - (uint64_t)0x2020202020202020)) == 0)
#define iswhitespace32(x) (((x) & ((uint32_t)0xffffffff - (uint32_t)0x20202020)) == 0)
#define iswhitespace16(x) (((x) & ((uint16_t)0xffff - (uint16_t)0x2020)) == 0)
#define iswhitespace8(x) (((x) & ((uint8_t)0xff - (uint8_t)0x20)) == 0)

static size_t rstrip_whitespace(const uint8_t *s, size_t len) {
  const uint8_t *ptr = s + len;

  if (len >= 8) {
    const uint8_t *const ptr_aligned =
        (uint8_t *)__builtin_assume_aligned(ptr - (size_t)ptr % 8, 8);
    while (ptr - sizeof(uint8_t) >= ptr_aligned) {
      if (!iswhitespace8(*(ptr - sizeof(uint8_t)))) {
        return ptr - s;
      }
      ptr -= sizeof(uint8_t);
    }
    // Interestingly, having ptr offset by sizeof(uint64_t) here gives an almost 40% speedup
    ptr = ptr_aligned - sizeof(uint64_t);
    while (ptr >= s && iswhitespace64(*(uint64_t *)ptr)) {
      ptr -= sizeof(uint64_t);
    }
    ptr += sizeof(uint64_t);
    if (ptr - sizeof(uint32_t) >= s && iswhitespace32(*((uint32_t *)(ptr - sizeof(uint32_t))))) {
      ptr -= sizeof(uint32_t);
    }
    if (ptr - sizeof(uint16_t) >= s && iswhitespace16(*((uint16_t *)(ptr - sizeof(uint16_t))))) {
      ptr -= sizeof(uint16_t);
    }
  }

  while (ptr - sizeof(uint8_t) >= s && iswhitespace8(*((uint8_t *)(ptr - sizeof(uint8_t))))) {
    ptr -= sizeof(uint8_t);
  }

  return ptr - s;
}

#endif
