#pragma once

#include <iostream>

// Minimal assertion macro shared by the test binaries (no external test
// framework dependency). Returns 1 from the enclosing function on failure.
#define CHECK(expr)                                                                      \
  do {                                                                                   \
    if (!(expr)) {                                                                       \
      std::cerr << "CHECK failed at " << __FILE__ << ":" << __LINE__ << ": " #expr "\n"; \
      return 1;                                                                          \
    }                                                                                    \
  } while (false)
