c++ -O3 -I . -I ~/micromamba/envs/sas/include/ -std=c++17 -L ~/micromamba/envs/sas/lib/ -larrow -lparquet arrow_example.cpp src/sas7bdat.cpp -o arrow_example -Wno-tautological-constant-out-of-range-compare
DYLD_LIBRARY_PATH=~/micromamba/envs/sas/lib/ ./arrow_example (cat somefiles) confirm
