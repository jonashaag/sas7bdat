FILES := $(shell git ls-files)
FUZZ_ARGS	= -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION=1 -O2 -g -mcpu=native -std=c++20 -fsanitize=fuzzer,address,undefined -fprofile-instr-generate -fcoverage-mapping

py: ${FILES}
	cd python && python setup.py develop

fuzzer: ${FILES}
	${CXX} ${FUZZ_ARGS} fuzz.cpp -o fuzzer

bfuzzer: ${FILES}
	${CXX} ${FUZZ_ARGS} fuzz-bitmap.cpp -o bfuzzer
