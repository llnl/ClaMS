// Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
// Project Developers. See the top-level COPYRIGHT file for details.


#pragma once

#include <chrono>

inline std::chrono::high_resolution_clock::time_point elapsed_time_sec() {
  return std::chrono::high_resolution_clock::now();
}

inline double elapsed_time_sec(
    const std::chrono::high_resolution_clock::time_point &tic) {
  auto duration_time = std::chrono::high_resolution_clock::now() - tic;
  return static_cast<double>(
             std::chrono::duration_cast<std::chrono::microseconds>(
                 duration_time)
                 .count()) /
         1e6;
}