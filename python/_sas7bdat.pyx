# cython: language_level=3, embedsignature=True, warn.undeclared=True, warn.maybe_uninitialized=True, warn.unused=True, warn.unused_arg=True, warn.unused_result=True, nonecheck=False
# TODO(perf) eliminate lists, none checks, ...
cimport cython
cimport numpy as np
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, strlen
from libcpp cimport bool
from numpy cimport (PyArray_GETPTR1, float32_t, float64_t, int8_t, int16_t,
                    int32_t, int64_t, uint8_t, uint32_t)

import codecs
import mmap
import os

import numpy as np


cdef extern from "<limits.h>":
    const size_t SIZE_MAX

cdef extern from "../include/sas7bdat.hpp":
    enum ColumnFormat:
        column_format_none,

        # Formats derived from SAS strings
        column_format_raw,     # Fixed-length raw bytes
        column_format_string,  # Variable-length bytes/str

        # Formats derived from SAS decimals
        column_format_double,
        column_format_float,
        column_format_bool,
        column_format_int8,
        column_format_int16,
        column_format_int32,
        column_format_int64,
        column_format_date,
        column_format_datetime

    void decimal2double(const uint8_t *buf, size_t len, bool file_is_little_endian, double *out)
    bool column_format_is_floating(ColumnFormat fmt)
    bool column_format_is_date_time(ColumnFormat fmt)
    bool is_sas_space(uint8_t value)

    enum Encoding:
        # Only relevant subset here
        encoding_infer = -2
        encoding_raw = -1
        encoding_default = 0
        encoding_utf_8 = 20
        encoding_ascii = 28
        encoding_iso_8859_1 = 29
        encoding_iso_8859_15 = 40
        encoding_windows_1252 = 62
        encoding_any = 204

    struct ColumnInfo:
        size_t offset
        uint32_t len
        const char *name
        ColumnFormat format

    struct FileInfo:
        bool is_32_bit
        bool is_little_endian
        bool need_byteswap
        Encoding encoding
        size_t page_count
        size_t row_count
        size_t column_count
        const ColumnInfo *columns

    struct FormatOverride:
        const char *column_name
        ColumnFormat format
        size_t len

    struct ParserConfig:
        void *userdata
        size_t filesize_override
        const FormatOverride *column_format_overrides
        size_t skip_rows
        void (*on_pagefault)(void *userdata, size_t requested_data_start, size_t requested_data_len, size_t *new_data_offset, const uint8_t **new_data, size_t *new_data_len);
        void (*on_metadata)(void *userdata, const FileInfo *)
        bool (*on_row)(void *userdata, const uint8_t *buf, bool have_fast_space_offsets)

    struct Parser:
        pass

    size_t parser_struct_size()
    void parser_init(Parser *parser, const ParserConfig *) except +
    bool parse(Parser *parser) except +
    void parser_deinit(Parser *parser) except +

    size_t column_last_known_space_offset(const Parser *, const ColumnInfo *)


# --- String processing ---

cdef extern from "Python.h":
    unicode PyUnicode_New(Py_ssize_t size, Py_UCS4 maxchar)
    unsigned char *PyUnicode_1BYTE_DATA(PyObject *o)
    unicode PyUnicode_FromStringAndSize(const char *u, Py_ssize_t size)

cdef extern from "../src/string_utils.hpp":
    size_t rstrip_whitespace(const uint8_t *s, size_t len)

cdef extern from "../src/encoding_utils.hpp":
    Encoding detect_iso_8859_15_variant(const uint8_t *s, size_t len)
    Encoding detect_windows_1252_variant(const uint8_t *s, size_t len)

cdef struct ConstBytestring:
    const uint8_t *data
    size_t len

cdef inline size_t rstrip_whitespace_fast(
    const Parser *parser,
    const uint8_t *buf,
    const ColumnInfo *colinfo,
) except? SIZE_MAX:
    # TODO(perf) optimize: 50% of cases within single cell max 4% faster
    cdef size_t last_known_space = column_last_known_space_offset(parser, colinfo)
    if last_known_space <= colinfo.offset or last_known_space == SIZE_MAX:
        return 0
    else:
        return last_known_space - colinfo.offset

cdef inline str fast_decode_ascii(ConstBytestring s):
    cdef:
        str out = PyUnicode_New(s.len, 127)
        unsigned char *data2 = PyUnicode_1BYTE_DATA(<PyObject *>out)
    memcpy(data2, s.data, s.len)
    return out

cdef inline object decode_string(ConstBytestring s, Encoding encoding, object fallback_decoder):
    # TODO(perf) caching, see eg. PyArrow memo
    if encoding == encoding_raw:
        return s.data[:s.len]
    elif s.len == 1 and not (s.data[0] & 0x80):
        # All encodings we support are ASCII compatible
        return PyUnicode_FromStringAndSize(<const char *>s.data, 1)
    else:
        # TODO(perf): fast variant for -1?
        if encoding == encoding_iso_8859_15:
            encoding = detect_iso_8859_15_variant(s.data, s.len)
        elif encoding == encoding_windows_1252:
            encoding = detect_windows_1252_variant(s.data, s.len)
        # Inlined version of: .decode(ENCODING_NAMES[encoding])
        if encoding == encoding_ascii:
            return fast_decode_ascii(s)
        elif encoding == encoding_iso_8859_1:
            return s.data[:s.len].decode("iso-8859-1")
        elif encoding == encoding_iso_8859_15:
            return s.data[:s.len].decode("iso-8859-15")
        elif encoding == encoding_utf_8:
            return s.data[:s.len].decode("utf-8")
        elif encoding == encoding_windows_1252:
            return s.data[:s.len].decode("windows-1252")
        else:
            return fallback_decoder(s.data[:s.len])[0]


# --- NumPy array utils ---

cdef extern from "numpy/npy_common.h":
    int64_t NPY_MIN_INT64

ctypedef fused setitem_type:
    bool
    int8_t
    int16_t
    int32_t
    int64_t
    float32_t
    float64_t
    ConstBytestring
    object

cdef inline np_setitem(np.ndarray ar, size_t idx, setitem_type value):
    assert idx < ar.shape[0]
    if setitem_type is object:
        # Python strings
        assert ar.ndim == 1
        Py_INCREF(value)
        (<void **>PyArray_GETPTR1(ar, idx))[0] = <void *>value
    elif setitem_type is ConstBytestring:
        # Raw strings
        assert ar.ndim == 2
        assert value.len > 0
        assert value.data != NULL
        assert value.len - 1 == ar.shape[1]
        memcpy(<uint8_t *>PyArray_GETPTR1(ar, idx), value.data, value.len)
    else:
        # Numbers
        assert ar.ndim == 1
        cython.cast(cython.typeof(&value), PyArray_GETPTR1(ar, idx))[0] = value


# --- Parser context ---

cdef extern from "math.h":
    bool isnan(double)

cdef make_np_array_for_column_format(fmt, col_len, row_count):
    dtype = {
        column_format_raw: f"S{col_len}",
        column_format_string: "object",
        column_format_double: "float64",
        column_format_float: "float32",
        column_format_bool: "bool",
        column_format_int8: "int8",
        column_format_int16: "int16",
        column_format_int32: "int32",
        column_format_int64: "int64",
        column_format_date: "datetime64[D]",
        column_format_datetime: "datetime64[s]",
    }[fmt]
    arr = np.ndarray(row_count, dtype=dtype)
    return (arr, arr.view("int64") if dtype.startswith("date") else arr)

ENCODING_NAMES = {
    encoding_infer: "infer",
    encoding_raw: "raw",
    encoding_default: "windows-1252",
    encoding_utf_8: "utf-8",
    encoding_ascii: "ascii",
    encoding_iso_8859_1: "iso-8859-1",
    encoding_iso_8859_15: "iso-8859-15",
    encoding_windows_1252: "windows-1252",
    encoding_any: "windows-1252",
}
ENCODING_ENUMS = {v: k for k, v in ENCODING_NAMES.items()}

cdef object np_nan = np.nan
cdef bytes empty_bytes = b""
cdef str empty_string = ""

cdef class Context:
    cdef:
        void *parser
        ParserConfig config

        object sas7bdat_data_buffer_iter
        size_t sas7bdat_data_len_so_far
        object sas7bdat_data_current_buffer
        const uint8_t[:] sas7bdat_data_current_buffer_view
        bool blank_as_nan
        bool copy_arrays
        size_t chunksize

        const FileInfo *fileinfo
        Encoding encoding
        object fallback_decoder  # f(bytes) -> (str, int)
        size_t current_row
        list col_arrs_read
        list col_arrs_write

    def __init__(
        self,
        sas7bdat_data_buffer_iter,
        *,
        blank_as_nan=True,
        chunksize=None,
        filesize_override=None,
        copy_arrays=True,
        encoding="infer",
    ):
        self.sas7bdat_data_buffer_iter = sas7bdat_data_buffer_iter
        self.sas7bdat_data_len_so_far = 0
        self.blank_as_nan = blank_as_nan
        self.chunksize = chunksize or 0
        self.copy_arrays = copy_arrays
        if isinstance(encoding, str):
            encoding = ENCODING_ENUMS[encoding]
        self.encoding = encoding
        self.current_row = 0

        self.config.userdata = <void *>self
        self.config.column_format_overrides = NULL
        self.config.filesize_override = filesize_override or 0
        self.config.on_pagefault = <void (*)(void *, size_t, size_t, size_t *, const uint8_t **, size_t *)>self.on_pagefault;
        self.config.on_metadata = <void (*)(void *, const FileInfo *)>self.on_metadata
        self.config.on_row = <bool (*)(void *, const uint8_t *, bool)>self.on_row

    cdef void on_pagefault(self, size_t requested_data_start, size_t requested_data_len, size_t *new_data_offset, const uint8_t **new_data, size_t *new_data_len) except *:
        # TODO(ref): move this logic into the parse_buffer_iter function like with http_parser?
        # TODO(ref): All of this is too complicated. Replace data_min_offset.
        assert requested_data_start < SIZE_MAX - requested_data_len
        assert requested_data_start + requested_data_len > self.sas7bdat_data_len_so_far
        bufs = []
        need_bytes = requested_data_len
        if requested_data_start < self.sas7bdat_data_len_so_far:
            bufs.append(self.sas7bdat_data_current_buffer[-(self.sas7bdat_data_len_so_far-requested_data_start):])
            gap = 0
        else:
            gap = requested_data_start - self.sas7bdat_data_len_so_far
        while sum(map(len, bufs)) < need_bytes + gap:
            chunk = next(self.sas7bdat_data_buffer_iter)
            if not chunk:
                raise RuntimeError("Failed to read more data, truncated input file?")
            bufs.append(chunk)
        self.sas7bdat_data_current_buffer = bufs[0] if len(bufs) == 1 else b"".join(bufs)
        self.sas7bdat_data_current_buffer_view = self.sas7bdat_data_current_buffer
        self.sas7bdat_data_len_so_far += len(self.sas7bdat_data_current_buffer) - (self.sas7bdat_data_len_so_far - requested_data_start if self.sas7bdat_data_len_so_far > requested_data_start else 0)

        new_data_offset[0] = gap
        new_data[0] = &self.sas7bdat_data_current_buffer_view[0]
        new_data_len[0] = len(self.sas7bdat_data_current_buffer)

    cdef void on_metadata(self, const FileInfo *fileinfo) except *:
        self.fileinfo = fileinfo
        if self.encoding == encoding_infer:
            self.encoding = fileinfo.encoding
        if self.encoding >= 0:
            self.fallback_decoder = codecs.getdecoder(ENCODING_NAMES[self.encoding])
        self.col_arrs_read, self.col_arrs_write = map(list, zip(*(
            make_np_array_for_column_format(
                fileinfo.columns[col_idx].format,
                fileinfo.columns[col_idx].len,
                min(self.chunksize, fileinfo.row_count) if self.chunksize else fileinfo.row_count,
            )
            for col_idx in range(fileinfo.column_count)
        )))

    cdef bool on_row(self, const uint8_t *buf, bool have_fast_space_offsets) except? True:
        cdef:
            size_t col_idx
            const ColumnInfo *colinfo

        assert self.current_row < len(self.col_arrs_write[0])

        for col_idx in range(self.fileinfo.column_count):
            colinfo = &self.fileinfo.columns[col_idx]
            if colinfo.format == column_format_raw:
                self._on_cell_raw(buf, self.current_row, col_idx, colinfo)
            elif colinfo.format == column_format_string:
                self._on_cell_string(buf, self.current_row, col_idx, colinfo, have_fast_space_offsets)
            else:
                self._on_cell_number(buf, self.current_row, col_idx, colinfo)

        self.current_row += 1
        return self.current_row == len(self.col_arrs_write[0])

    cdef inline bool _on_cell_raw(
        self,
        const uint8_t *buf,
        size_t row_idx,
        size_t col_idx,
        const ColumnInfo *colinfo,
    ) except False:
        np_setitem[ConstBytestring](
            self.col_arrs_write[col_idx],
            row_idx,
            ConstBytestring(&buf[colinfo.offset], colinfo.len),
        )
        return True

    cdef inline bool _on_cell_string(
        self,
        const uint8_t *buf,
        size_t row_idx,
        size_t col_idx,
        const ColumnInfo *colinfo,
        bool have_fast_space_offsets,
     ) except False:
        cdef:
            size_t str_len = colinfo.len
            object value
        if str_len > 0 and have_fast_space_offsets:
            str_len = rstrip_whitespace_fast(<Parser *>self.parser, buf, colinfo)
        if str_len > 0:
            str_len = rstrip_whitespace(&buf[colinfo.offset], str_len)
        if str_len > 0:
            value = decode_string(
                ConstBytestring(&buf[colinfo.offset], str_len),
                self.encoding,
                self.fallback_decoder,
            )
        else:
            if self.blank_as_nan:
                value = np_nan
            elif self.encoding == encoding_raw:
                value = empty_bytes
            else:
                value = empty_string
        np_setitem[object](self.col_arrs_write[col_idx], row_idx, value)
        return True

    cdef inline bool _on_cell_number(
        self,
        const uint8_t *buf,
        size_t row_idx,
        size_t col_idx,
        const ColumnInfo *colinfo,
    ) except False:
        cdef:
            float64_t value
            ColumnFormat colfmt = colinfo.format

        # Inline variant of most common path
        if colfmt == column_format_double and colinfo.len == 8 and not self.fileinfo.need_byteswap:
            memcpy(PyArray_GETPTR1(self.col_arrs_write[col_idx], row_idx), &buf[colinfo.offset], 8)
            return True

        if colinfo.len == 8 and not self.fileinfo.need_byteswap:
            # Fast path
            value = (<float64_t*>&buf[colinfo.offset])[0]
        else:
            decimal2double(&buf[colinfo.offset], colinfo.len, self.fileinfo.is_little_endian, &value)

        if isnan(value):
            if column_format_is_floating(colfmt):
                np_setitem[float64_t](self.col_arrs_write[col_idx], row_idx, value)
            elif column_format_is_date_time(colfmt):
                np_setitem[int64_t](self.col_arrs_write[col_idx], row_idx, NPY_MIN_INT64)
            else:
                raise ValueError("Unexpected NaN")
            return True

        if column_format_is_date_time(colfmt):
            np_setitem[int64_t](
                self.col_arrs_write[col_idx],
                row_idx,
                <int64_t>value - 3653 * (1 if colfmt == column_format_date else 24 * 3600),
            )
            return True

        if colfmt == column_format_double:
            np_setitem[float64_t](self.col_arrs_write[col_idx], row_idx, value)
        elif colfmt == column_format_float:
            np_setitem[float32_t](self.col_arrs_write[col_idx], row_idx, <float32_t>value)
        elif colfmt == column_format_bool:
            np_setitem[bool](self.col_arrs_write[col_idx], row_idx, <bool>value)
        elif colfmt == column_format_int8:
            np_setitem[int8_t](self.col_arrs_write[col_idx], row_idx, <int8_t>value)
        elif colfmt == column_format_int16:
            np_setitem[int16_t](self.col_arrs_write[col_idx], row_idx, <int16_t>value)
        elif colfmt == column_format_int32:
            np_setitem[int32_t](self.col_arrs_write[col_idx], row_idx, <int32_t>value)
        elif colfmt == column_format_int64:
            np_setitem[int64_t](self.col_arrs_write[col_idx], row_idx, <int64_t>value)
        else:
            raise ParserError(f"Unexpected column format {colfmt}")
        return True

    cdef reset_chunk(self):
        self.current_row = 0

    cdef get_current_chunk_dict(self):
        return dict(zip(
            self.get_column_names(),
            self.get_current_chunk_arrays(),
        ))

    cdef get_current_chunk_arrays(self):
        if self.copy_arrays:
            return [nd[:self.current_row] for nd in self.col_arrs_read]
        else:
            return [nd[:self.current_row].copy() for nd in self.col_arrs_read]

    cdef get_column_names(self):
        def _colname(i):
            cdef const char *name = self.fileinfo.columns[i].name
            return decode_string(
                ConstBytestring(<uint8_t*>name, strlen(name)),
                self.encoding,
                self.fallback_decoder,
            )
        return [_colname(i) for i in range(self.fileinfo.column_count)]


class ParserError(Exception):
    pass


def parse_file(filename: str | bytes | os.PathLike, fsize=None, use_mmap=True, **kwargs):
    with open(filename, "rb") as f:
        fileno = f.fileno()
        if fsize is None:
            fsize = os.fstat(fileno).st_size
        if use_mmap:
            with mmap.mmap(fileno, fsize, access=mmap.ACCESS_READ) as m:
                yield from parse_mmap(m, **kwargs)
        else:
            yield from parse_fileobj(f, fsize, **kwargs)


def parse_mmap(m: mmap.mmap, **kwargs):
    return parse_buffer_iter(iter([m]), filesize_override=m.size(), **kwargs)


def parse_fileobj(f, fsize=None, buffer_size=1024**3, **kwargs):
    def _chunks():
        bytes_read = 0
        chunk = None
        while (chunk is None or chunk) and (fsize is None or bytes_read < fsize):
            chunk = f.read(buffer_size if fsize is None else min(fsize - bytes_read, buffer_size))
            bytes_read += len(chunk)
            yield chunk
    return parse_buffer_iter(_chunks(), filesize_override=fsize, **kwargs)


def parse_buffer_iter(
    buffer_iter,
    *,
    size_t max_rows=SIZE_MAX,
    chunksize=None,
    filesize_override=None,
    **kwargs,
):
    cdef:
        Parser *parser = <Parser *>malloc(parser_struct_size())
        size_t retval = 1
        size_t chunk_idx = 0
    ctx = Context(buffer_iter, **kwargs, chunksize=chunksize, filesize_override=filesize_override)
    ctx.parser = parser
    # TODO(ref) move into context and rename context -> parser?
    parser_init(parser, &ctx.config)
    #     raise ParserError(f"Error initializing parser: {last_error()}")
    try:
        while retval:
            retval = parse(parser)
            # raise ParserError(f"Error parsing chunk {chunk_idx}: {last_error()}")
            if max_rows != SIZE_MAX:
                # TODO: properly implement max_rows
                yield {c: d[:max_rows] for c, d in ctx.get_current_chunk_dict().items()}
                break
            if ctx.current_row:
                yield ctx.get_current_chunk_dict()
            ctx.reset_chunk()
            chunk_idx += 1
    finally:
        # TODO(ref): should use with statement for cleanup
        parser_deinit(parser)
        free(parser)
