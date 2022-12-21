import mmap

import pandas as pd
import pandas.io.common as pd_io_common

import sas7bdat._sas7bdat


def read_sas(
    filepath_or_buffer,
    chunksize=None,
    encoding="infer",
    format=None,
    index=None,
    iterator=False,
    compression="infer",
    memory_map=False,
):
    """Almost drop-in replacement for `pd.read_sas()`."""
    if encoding != "infer":
        raise NotImplementedError("Encoding option must be 'infer'")
    if format not in {"sas7bdat", None}:
        raise NotImplementedError("Format option must be 'sas7bdat' or None")
    if iterator and not chunksize:
        chunksize = 1

    kwargs = {"chunksize": chunksize, "copy_arrays": False}
    handles = pd_io_common.get_handle(
        filepath_or_buffer,
        mode="rb",
        is_text=False,
        compression=compression,
        memory_map=memory_map,
    )
    if isinstance(handles.handle, pd_io_common._IOWrapper) and isinstance(
        handles.handle.buffer, mmap.mmap
    ):
        res = sas7bdat._sas7bdat.parse_mmap(handles.handle.buffer, **kwargs)
    else:
        res = sas7bdat._sas7bdat.parse_fileobj(handles.handle, **kwargs)
    chunks = _df_chunks(res, index)
    if chunksize:
        return chunks
    else:
        return next(chunks)


def _df_chunks(sas7bdat_iterator, index):
    pos = 0
    for data in sas7bdat_iterator:
        data_len = len(next(iter(data.values())))
        yield pd.DataFrame(
            data,
            pd.RangeIndex(pos, pos + data_len) if index is None else index,
            copy=True,
        )
        pos += data_len
