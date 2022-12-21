import os
from pathlib import Path

import numpy as np
from Cython.Build import cythonize
from setuptools import Extension, setup

here = Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text()

CFLAGS = [
    "-O3",
    "-std=c++20",
    "-Wall",
    "-Wextra",
    "-Wno-unused-parameter",
    "-Wimplicit-fallthrough",
    "-Wno-missing-field-initializers",
    "-Wno-unused-const-variable",
    "-Wno-unused-function",
]
DEBUG = os.getenv("SAS7BDAT_DEBUG")
if DEBUG:
    CFLAGS += ["-O2", "-g"]
if os.getenv("SAS7BDAT_COMPILE_NATIVE"):
    CFLAGS += ["-mcpu=native"]
LDFLAGS = [flag for flag in CFLAGS if flag.startswith("-f")]

setup(
    name="sas7bdat",
    version="1.0.0",
    description="Python binding to sas7bdat parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jonashaag/sas7bdat",
    author="Jonas Haag",
    author_email="jonas@lophus.org",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: Free for non-commercial use",
    ],
    install_requires=["numpy", "pandas"],
    py_modules=["sas7bdat"],
    ext_modules=cythonize(
        Extension(
            name="sas7bdat._sas7bdat",
            sources=["_sas7bdat.pyx", "../src/sas7bdat.cpp"],
            language="c++",
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS,
        ),
        nthreads=os.cpu_count(),
        annotate=DEBUG,
        gdb_debug=DEBUG,
        verbose=True,
    ),
    include_dirs=np.get_include(),
    zip_safe=False,
)
