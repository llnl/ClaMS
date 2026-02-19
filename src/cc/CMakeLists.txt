# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


add_basic_executable(run_cc run_cc.cpp)
setup_omp_target(run_cc)
setup_metall_target(run_cc)
