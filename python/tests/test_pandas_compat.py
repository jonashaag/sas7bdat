from pathlib import Path

import pandas as pd
import pytest

import sas7bdat.pandas_compat as spd


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"chunksize": 7},
        {"iterator": True},
        {"format": "sas7bdat"},
        # TODO: encoding="infer"
    ],
)
@pytest.mark.parametrize(
    "filename", ["pandas_test1.sas7bdat", "pandas_test1.sas7bdat.gz"]
)
def test_read_sas(kwargs, filename):
    test_file = Path(__file__).parent / filename
    pd_df = pd.read_sas(test_file, encoding="ascii", **kwargs)
    our_df = spd.read_sas(test_file, encoding="infer", **kwargs)
    if kwargs.get("chunksize") or kwargs.get("iterator"):
        pd_df = list(pd_df)
        our_df = list(our_df)
        assert len(pd_df) == len(our_df)
        for pd_df1, our_df1 in zip(pd_df, our_df):
            pd.testing.assert_frame_equal(pd_df1, our_df1)
    else:
        pd.testing.assert_frame_equal(pd_df, our_df)
