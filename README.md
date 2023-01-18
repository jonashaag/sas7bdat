# World's Fastest SAS7BDAT Parser

## Benchmarks

Based on some public and private benchmarks, this library reaches ~ 300–500 MB/s on single M1 MacBook Pro core.

|  | Times slower | Correctness |
|---|--:|---|
| This library | 1x | (See roadmap) |
| [cpp-sas7bdat]( ) | 3x | ? |
| [`pandas.read_sas`](https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html) in Pandas 2.0 | 10–20x | Less correct |
| `pandas.read_sas` in Pandas 1.5 | 15–30x | Less correct |
| `pandas.read_sas` in Pandas 1.4 | n/a | Broken |
| [pyreadstat]() | 5–10x<br>sometimes > 100x | More correct |
| [sas7bdat.py]() | > 100x | ? |

## Usage

### `read_sas`

Currently only the Pandas compatibility interface is considered stable:

```py
import sas7bdat.pandas_compat

df = sas7bdat.pandas_compat.read_sas("myfile.sas7bdat")
```

Options to `read_sas` are the same in [`pandas.read_sas`](https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html).

#### Installation

```
cd python
python setup.py install
```

## sas2parquet

Non-Python program to convert SAS7BDAT to Parquet, needs Arrow C++:

```
make sas2parquet
./sas2parquet input.sas7bdat output.parquet
```

## Roadmap

- Fix all the bugs
- Parser features:
  - Limiting the number of rows and pages to read
  - Efficient row and page skipping (useful for parallel reading)
  - Reading only a subset of columns
  - Automatically converting to best number type (eg., read integers instead of doubles)
- Pandas features:
  - Support for the [str extension type](https://pandas.pydata.org/docs/reference/api/pandas.StringDtype.html#pandas.StringDtype)
- New parsers:
  - Parsing directly to (Py)Arrow
  - Parsing directly to Parquet
- Performance ideas:
  - Try NumPy's masked arrays
  - Cache Python objects, similar to PyArrow's Memo

## License

Permission to use, copy, modify, and/or distribute this software for any **non-commercial** purpose without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
