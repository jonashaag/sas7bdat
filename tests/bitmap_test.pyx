from libc.stdlib cimport malloc
from numpy cimport uint8_t, uint64_t


cdef extern from "bitmap.h":
    struct bitmap:
        size_t data_size
        uint64_t *data
    size_t bitmap_compute_size(size_t data_size)
    size_t bitmap_size(bitmap *b)
    void bitmap_to_bytes(bitmap *b, uint8_t *out)
    void bitmap_set(bitmap *b, size_t start, size_t stop, bint v)
    void bitmap_xor(bitmap *b1, bitmap *b2, size_t start, size_t stop)
    void bitmap_and(bitmap *b1, bitmap *b2, size_t start, size_t stop)
    void bitmap_or(bitmap *b1, bitmap *b2, size_t start, size_t stop)
    size_t bitmap_get_first_bit(bitmap *b1, size_t start, size_t stop, bint v)
    size_t bitmap_get_last_bit(bitmap *b1, size_t start, size_t stop, bint v)


def compute_size(n):
    return bitmap_compute_size(n)


def set(bptr, start, stop, v): bitmap_set(<bitmap*><size_t>bptr, start, stop, v)
def get_first_bit(bptr, start, stop, v): return bitmap_get_first_bit(<bitmap*><size_t>bptr, start, stop, v)
def get_last_bit(bptr, start, stop, v): return bitmap_get_last_bit(<bitmap*><size_t>bptr, start, stop, v)


def xor (bptr1, bptr2, start, stop): bitmap_xor(<bitmap*><size_t>bptr1, <bitmap*><size_t>bptr2, start, stop)
def and_(bptr1, bptr2, start, stop): bitmap_and(<bitmap*><size_t>bptr1, <bitmap*><size_t>bptr2, start, stop)
def or_ (bptr1, bptr2, start, stop): bitmap_or (<bitmap*><size_t>bptr1, <bitmap*><size_t>bptr2, start, stop)


def mk(size, initial=0):
    cdef:
        bitmap *b = <bitmap *>malloc(bitmap_compute_size(size))
    b.data_size = size
    for i, v in enumerate([initial] * b.data_size if isinstance(initial, int) else initial):
        set(<size_t>b, i, i+1, v)
    return <size_t>b


def asarr(bptr):
    cdef bitmap *b = <bitmap*><size_t>bptr
    cdef uint8_t[:] res = bytearray(b.data_size)
    bitmap_to_bytes(b, &res[0])
    return list(res)
