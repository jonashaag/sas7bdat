import operator

import numpy as np
import pyximport
from hypothesis import given, settings
from hypothesis import strategies as st

pyximport.install(setup_args={"include_dirs": np.get_include() + ":."})
import bitmap_test as B  # noqa

N = 130
EX = 100


bools = st.integers(0, 1)
lengths = st.integers(1, N).map(lambda n: n * 64)
ranges = st.shared(lengths, key="x").flatmap(
    lambda n: (
        st.tuples(st.just(n), st.integers(0, n - 1)).flatmap(
            lambda t: st.tuples(st.just(t[0]), st.just(t[1]), st.integers(t[1], t[0]))
        )
    )
)
bitmaps = (
    st.shared(lengths, key="x")
    .flatmap(lambda n: st.lists(bools, min_size=n, max_size=n))
    .map(lambda initial: (B.mk(len(initial), initial), initial))
)


@given(ranges, bitmaps, bools)
@settings(max_examples=EX)
def test_set1(r, bd, value):
    n, pos, _ = r
    b, initial = bd
    B.set(b, pos, pos + 1, value)
    expected = initial[:]
    expected[pos] = value
    assert B.asarr(b) == expected


@settings(max_examples=EX)
@given(ranges, bitmaps, bools)
def test_set(r, bd, value):
    n, start, stop = r
    b, initial = bd
    B.set(b, start, stop, value)
    expected = initial[:]
    expected[start:stop] = [value] * (stop - start)
    assert B.asarr(b) == expected


@given(st.sampled_from(("xor", "and_", "or_")), ranges, bitmaps, bitmaps)
@settings(max_examples=EX)
def test_op(op, r, bd1, bd2):
    n, start, stop = r
    b1, initial1 = bd1
    b2, initial2 = bd2
    getattr(B, op)(b1, b2, start, stop)
    expected = [
        getattr(operator, op)(a, b) if start <= i < stop else a
        for i, (a, b) in enumerate(zip(initial1, initial2))
    ]
    assert B.asarr(b1) == expected
    assert B.asarr(b2) == initial2


@given(ranges, bitmaps, bools, st.sampled_from(("first", "last")))
@settings(max_examples=EX)
def test_get_firstlast_bit(r, bd, value, firstlast):
    n, start, stop = r
    b, initial = bd
    maybe_reversed = (
        list if firstlast == "first" else (lambda x: list(reversed(list(x))))
    )
    try:
        firstlast_bit = maybe_reversed(
            idx for idx, v in enumerate(initial) if start <= idx < stop and v == value
        )[0]
    except IndexError:
        firstlast_bit = 2**64 - 1
    assert getattr(B, f"get_{firstlast}_bit")(b, start, stop, value) == firstlast_bit
