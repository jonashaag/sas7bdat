FILES := $(shell git ls-files)
FUZZ_ARGS	= -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION=1 -O2 -g -mcpu=native -std=c++20 -fsanitize=fuzzer,address,undefined -fprofile-instr-generate -fcoverage-mapping

py: ${FILES}
	cd python && python setup.py develop

fuzzer: ${FILES}
	${CXX} ${FUZZ_ARGS} fuzz.cpp -o fuzzer

bfuzzer: ${FILES}
	${CXX} ${FUZZ_ARGS} fuzz-bitmap.cpp -o bfuzzer

sas2parquet: ${FILES}
	${CXX} -O3 -I . -std=c++17 -larrow -lparquet src/sas2parquet.cpp src/sas7bdat.cpp -o sas2parquet ${CFLAGS} ${LDFLAGS}
