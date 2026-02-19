# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


add_basic_executable(build_amst build_amst.cpp)
setup_amst_target(build_amst)
setup_saltatlas_target(build_amst)

add_basic_executable(run_kruskal_mst run_kruskal_mst.cpp)
setup_omp_target(run_kruskal_mst)
setup_spdlog_target(run_kruskal_mst)
setup_metall_target(run_kruskal_mst)

add_basic_executable(serialize_pm_mst serialize_pm_mst.cpp)
setup_spdlog_target(serialize_pm_mst)
setup_metall_target(serialize_pm_mst)