#ifndef ASSUME_HPP
#define ASSUME_HPP
#include <stdexcept>

#define assume(cond, exc)                                                                          \
  do {                                                                                             \
    if (!(__builtin_expect(!!(cond), 1))) {                                                        \
      throw exc;                                                                                   \
    }                                                                                              \
  } while (0)

// Cython undefines assert()
#define STR2(x) #x
#define STR(x) STR2(x)
#define assert2(x)                                                                                 \
  assume(x, std::runtime_error("Assertion failed in " STR(__func__) "() (file " STR(               \
                __FILE__) ", line " STR(__LINE__) ")"))
#endif

#define sas7bdat_error std::runtime_error

#define check_read(cond) assume(cond, sas7bdat_error("Out of bounds read"))
