// Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
// Project Developers. See the top-level COPYRIGHT file for details.


#pragma once

#include <unordered_map>
#include <vector>

#include <metall/utility/open_mp.hpp>

template<typename  graph_t>
std::pair<std::vector<id_t>, std::unordered_map<id_t, id_t>>
run_cc(const graph_t& graph) {
  std::vector<id_t> vertices; // Holds only active vertices
  std::unordered_map<id_t, id_t> cc_table; // key: vertex id, value: cc id
  const std::size_t num_vertices = graph.num_keys();
  vertices.reserve(num_vertices);
  cc_table.reserve(num_vertices);

  std::cout << "Init" << std::endl;
  for (auto vit = graph.keys_begin(); vit != graph.keys_end(); ++vit) {
    const auto &key = vit->first;
    cc_table[key] = key;
    vertices.push_back(key);
  }

  std::cout << "Start CC" << std::endl;
  std::size_t loop_count = 0;
  // Pull style CC using OMP to reduce the number of atomic operations
  while (true) {
    std::cout << "Loop: " << loop_count++ << std::endl;
    std::size_t num_updates = 0;
    OMP_DIRECTIVE(parallel for reduction(+ : num_updates))
    for (std::size_t i = 0; i < vertices.size(); ++i) {
      const auto vid = vertices[i];
      auto &cc = cc_table[vid];
      for (auto eit = graph.values_begin(vid); eit != graph.values_end(vid);
           ++eit) {
        const auto nid = eit->first;
        // As CC is a monotonically decreasing, we do not need any locks here
        const auto nbr_cc = cc_table[nid];
        if (cc > nbr_cc) {
          cc = nbr_cc;
          ++num_updates;
        }
      }
    }
    if (num_updates == 0) {
      break;
    }
  }
  std::cout << "Finished CC" << std::endl;

  return {vertices, cc_table};
}