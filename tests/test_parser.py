import os
import pickle
import subprocess
import sys
import textwrap
from pathlib import Path

import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

TEST_FILES = [
    f
    for f in (Path(__file__).parent / "test-files").rglob("*.sas7bdat")
    if f.name not in {"zero_variables.sas7bdat"}  # empty
    and f.name not in {"ssocs.sas7bdat"}  # amd pages?
]


def parse(*args, **kwargs):
    marker = b"__pickle_marker__"
    test_prog = textwrap.dedent(
        f"""
        import pickle, sys, pyparser
        args, kwargs = pickle.load(sys.stdin.buffer)
        parsed = list(pyparser.parse_file(*args, **kwargs))
        sys.stdout.buffer.write({marker!r})
        pickle.dump(parsed, sys.stdout.buffer)
        sys.stdout.buffer.write({marker!r})
        """
    )
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            test_prog,
        ],
        capture_output=True,
        env={"DYLD_INSERT_LIBRARIES": os.environ["ASAN_DYLIB"]},
        input=pickle.dumps((args, kwargs)),
    )
    try:
        pickle_from = proc.stdout.index(marker)
        pickle_to = proc.stdout.rindex(marker)
        pickle_bytes = proc.stdout[pickle_from:pickle_to].removeprefix(marker)
        non_pickle_bytes = proc.stdout[:pickle_from] + proc.stdout[
            pickle_to:
        ].removeprefix(marker)
    except ValueError:
        pickle_bytes = b""
        non_pickle_bytes = proc.stdout
        failed = True
    else:
        try:
            result = pickle.loads(pickle_bytes)
            failed = False
        except:  # noqa
            failed = True
    if non_pickle_bytes:
        print(
            "Parser stdout:\n"
            + non_pickle_bytes.decode(sys.getdefaultencoding(), "ignore")
        )
    if proc.stderr:
        print(
            "Parser stderr:\n" + proc.stderr.decode(sys.getdefaultencoding(), "ignore"),
            file=sys.stderr,
        )
    if failed or proc.returncode:
        msg = "Parser failed"
        try:
            msg += ": " + proc.stderr.splitlines()[-1].decode("utf8", "coerce")
        except:  # noqa
            pass
        pytest.fail(msg)
    return result


def _to_pd(dicts):
    last_idx = 0
    for d in dicts:
        df = pd.DataFrame(
            {k.decode("latin1") if isinstance(k, bytes) else k: v for k, v in d.items()}
        )
        df.index = range(last_idx, last_idx + len(df))
        yield df
        last_idx += len(df)


@pytest.mark.parametrize("filename", TEST_FILES, ids=str)
def test_read_meta(filename):
    df = next(_to_pd(parse(filename, max_rows=0, chunksize=1, encoding="raw")))
    pd_df = next(pd.read_sas(filename, chunksize=1)).iloc[:0]
    pd.testing.assert_frame_equal(df, pd_df)


@pytest.mark.parametrize("filename", TEST_FILES, ids=str)
def test_read(filename):
    chunksize = 1000
    df = pd.concat(_to_pd(parse(filename, chunksize=chunksize, encoding="raw")))
    pd_df = pd.concat(pd.read_sas(filename, chunksize=chunksize))
    pd.testing.assert_frame_equal(df, pd_df)


@given(
    filename=st.sampled_from(TEST_FILES),
    chunksize=st.integers(
        1, 100
    ),  # TODO: test chunksize=0 but need a method to read row count
)
@settings(deadline=5000)
@pytest.mark.slow
def test_read_chunks(filename, chunksize):
    if "pss0910_pu.sas7bdat" in filename.name:
        # TODO slow
        return
    df1 = pd.concat(_to_pd(parse(filename, chunksize=chunksize, encoding="raw")))
    if chunksize < 10:
        # Special case because it's very slow with Pandas
        df2 = pd.concat(_to_pd(parse(filename, chunksize=1000, encoding="raw")))
    else:
        df2 = pd.concat(pd.read_sas(filename, chunksize=1000))
    pd.testing.assert_frame_equal(df1, df2)
