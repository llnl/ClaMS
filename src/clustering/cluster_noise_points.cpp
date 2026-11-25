// Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
// Project Developers. See the top-level COPYRIGHT file for details.

// Assign cluster IDs to noise points by traversing the MST edges.
// Traverse the MST edges from each noise point in BFS manner until a point that
// belongs to a cluster is found.

#include <unistd.h>
#include <algorithm>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <stack>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>
#include <spdlog/stopwatch.h>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <metall/utility/metall_mpi_datastore.hpp>

#include "../common.hpp"
#include "../details/multithread_adjacency_list.hpp"

using namespace clams;

template <typename K>
using set_t = boost::unordered::unordered_flat_set<K>;

struct option {
  std::filesystem::path mst_edges_path;
  bool                  metall_mst{false};
  std::filesystem::path cluster_ids_input_path;
  bool                  metall_cluster_ids{false};
  std::filesystem::path cluster_ids_out_path;
};

void show_help() {
  std::cout
      << "<<Usage>>\n"
         "Required arguments:\n"
         "  -m <path> path to a single file or a directory that contains input "
         "MST.\n"
         "  -c <path> path to input cluster IDs.\n"
         "  -o <path> path to output cluster IDs.\n"
         "Optional arguments:\n"
         "  -M If specified, the MST input is either a single Metall datastore "
         "or a Metall MPI datastore.\n"
         "  -C If specified, the cluster IDs input is either a single Metall "
         "datastore or a Metall MPI datastore.\n"
         "  -h Show help."
      << std::endl;
}

void parse_option(int argc, char* argv[], option& opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "m:c:o:MCh")) != -1) {
    switch (opt_char) {
      case 'm':
        opt.mst_edges_path = optarg;
        break;
      case 'c':
        opt.cluster_ids_input_path = optarg;
        break;
      case 'o':
        opt.cluster_ids_out_path = optarg;
        break;
      case 'M':
        opt.metall_mst = true;
        break;
      case 'C':
        opt.metall_cluster_ids = true;
        break;
      case 'h':
        show_help();
        std::exit(EXIT_SUCCESS);
      default:
        show_help();
        std::exit(EXIT_FAILURE);
    }
  }
}

/// Get the partition size of a Metall MPI datastore.
/// Returns 0 if the partition size file does not exist.
/// TODO: implement helper functions on Metall side
int get_pm_datastore_partition_size(
    const std::filesystem::path& datastore_path) {
  const std::filesystem::path mpi_partition_size_file =
      datastore_path / "metall_mpi_adaptor_partition_size";
  if (std::filesystem::exists(mpi_partition_size_file)) {
    std::ifstream ifs(mpi_partition_size_file);
    int           partition_size;
    ifs >> partition_size;
    return partition_size;
  }
  return 0;
}

using mst_graph_t =
    multithread_adjacency_list<id_t, std::pair<id_t, distance_t>>;

void read_single_metall_edges(const std::filesystem::path& mst_edges_path,
                              mst_graph_t&                 mst_graph) {
  metall::manager metall_manager(metall::open_read_only, mst_edges_path);
  auto*           input_mst_edges =
      metall_manager.find<weighted_edge_list_t>(metall::unique_instance).first;
  if (!input_mst_edges) {
    spdlog::critical("Failed to find MST edges in Metall datastore at {}",
                     mst_edges_path.string());
    std::abort();
  }

  spdlog::trace("#of MST edges from path {} is {}", mst_edges_path.string(),
                input_mst_edges->size());

  OMP_DIRECTIVE(parallel for)
  for (size_t ei = 0; ei < input_mst_edges->size(); ++ei) {
    const auto& edge = input_mst_edges->at(ei);
    mst_graph.add(edge.ids[0], {edge.ids[1], edge.distance});
    mst_graph.add(edge.ids[1], {edge.ids[0], edge.distance});
  }
}

void read_mst_metall_edges(const std::filesystem::path& mst_edges_path,
                           mst_graph_t&                 mst_graph) {
  const int n_partitions = get_pm_datastore_partition_size(mst_edges_path);
  if (n_partitions > 0) {
    for (int i = 0; i < n_partitions; ++i) {
      const auto path = metall::utility::mpi_datastore::make_local_dir_path(
          mst_edges_path.string(), i);
      read_single_metall_edges(path, mst_graph);
    }
  } else {
    read_single_metall_edges(mst_edges_path, mst_graph);
  }
}

void read_single_metall_cluster_ids(
    const std::filesystem::path& cluster_ids_path, bool mpi_partition,
    pm_point_to_cluster_id_map_t& point_cluster_map) {
  metall::manager metall_manager(metall::open_read_only, cluster_ids_path);
  const auto*     input_cluster_id_list =
      mpi_partition
              ? metall_manager
                .find<pm_point_to_cluster_id_list_t>(metall::unique_instance)
                .first
              : metall_manager
                .find<pm_point_to_cluster_id_list_t>("point_to_cluster_id_list")
                .first;
  if (!input_cluster_id_list) {
    spdlog::critical("Failed to find cluster IDs in Metall datastore at {}",
                     cluster_ids_path.string());
    std::abort();
  }

  // The datastore closes at the end of this scope, so copy its entries.
  point_cluster_map.reserve(point_cluster_map.size() +
                            input_cluster_id_list->size());
  point_cluster_map.insert(input_cluster_id_list->begin(),
                           input_cluster_id_list->end());
}

void read_metall_cluster_ids(const std::filesystem::path&  cluster_ids_path,
                             pm_point_to_cluster_id_map_t& point_cluster_map) {
  const int n_partitions = get_pm_datastore_partition_size(cluster_ids_path);
  if (n_partitions > 0) {
    for (int i = 0; i < n_partitions; ++i) {
      const auto path = metall::utility::mpi_datastore::make_local_dir_path(
          cluster_ids_path.string(), i);
      read_single_metall_cluster_ids(path, true, point_cluster_map);
    }
  } else {
    read_single_metall_cluster_ids(cluster_ids_path, false, point_cluster_map);
  }
}

int main(int argc, char* argv[]) {
  option opt;
  parse_option(argc, argv, opt);

  mst_graph_t mst_graph;
  if (opt.metall_mst) {
    spdlog::info("Attaching MST in Metall datastore");
    read_mst_metall_edges(opt.mst_edges_path, mst_graph);
  } else {
    spdlog::info("Reading MST edges");
    weighted_edge_list_t input_mst_edges;
    read_edges(opt.mst_edges_path, input_mst_edges);
    spdlog::info("#of MST edges: {}", input_mst_edges.size());
    OMP_DIRECTIVE(parallel for)
    for (size_t i = 0; i < input_mst_edges.size(); ++i) {
      const auto& edge = input_mst_edges.at(i);
      mst_graph.add(edge.ids[0], {edge.ids[1], edge.distance});
      mst_graph.add(edge.ids[1], {edge.ids[0], edge.distance});
    }
  }

  if (mst_graph.empty()) {
    spdlog::warn("No MST edges found in the input file or directory: {}",
                 opt.mst_edges_path.string());
    return EXIT_SUCCESS;
  }

  pm_point_to_cluster_id_map_t point_cluster_map;
  if (opt.metall_cluster_ids) {
    spdlog::info("Attaching cluster IDs in Metall datastore");
    read_metall_cluster_ids(opt.cluster_ids_input_path, point_cluster_map);
  } else {
    read_cluster_ids(clams::find_files(opt.cluster_ids_input_path),
                     point_cluster_map);
  }
  spdlog::info("Read {} points' cluster IDs from {}", point_cluster_map.size(),
               opt.cluster_ids_input_path.string());

  std::vector<id_t> point_ids;
  point_ids.reserve(point_cluster_map.size());
  for (const auto& [point_id, cluster_id] : point_cluster_map) {
    point_ids.push_back(point_id);
  }

  spdlog::info(
      "Assigning cluster IDs to noise points by traversing the MST edges");
  // Start a timer to measure the time taken for assigning cluster IDs to noise
  // points
  auto kernel_timer = spdlog::stopwatch();

  std::size_t n_noise_points    = 0;
  std::size_t n_assigned_points = 0;
  // NOTE: point_cluster_map may be updated concurrently by multiple threads,
  // which causes data races, but it is acceptable for this use case. We employ
  // this algorithm because it is simple and fast.
  OMP_DIRECTIVE(parallel for reduction(+ : n_noise_points, n_assigned_points))
  for (size_t i = 0; i < point_ids.size(); ++i) {
    const auto point_id = point_ids.at(i);
    if (point_cluster_map.at(point_id) != k_noise_cluster_id) {
      continue;  // Already assigned to a cluster
    }
    ++n_noise_points;

    std::vector<std::pair<id_t, distance_t>> bfs_front;

    set_t<id_t> visited;
    bfs_front.push_back({point_id, 0});
    visited.insert(point_id);
    bool found_cluster = false;

    // Level-order traversal (BFS) to find the nearest cluster for the noise
    // point. Within each level, visit closer neighbors first
    while (!bfs_front.empty()) {
      std::vector<std::pair<id_t, distance_t>> next;
      for (const auto& [pid, _] : bfs_front) {
        // Traverse the neighbors of the current point in the MST
        for (auto nitr = mst_graph.values_begin(pid);
             nitr != mst_graph.values_end(pid); ++nitr) {
          const auto& [nid, ndist] = *nitr;
          if (visited.count(nid) > 0) {
            continue;  // Already visited, e.g., the node we came from
          }
          if (point_cluster_map.at(nid) != k_noise_cluster_id) {
            // Found a neighbor that belongs to a cluster
            // Update the table without locking, which may cause data races but
            // is acceptable for this use case
            point_cluster_map.at(point_id) = point_cluster_map.at(nid);
            found_cluster                  = true;
            ++n_assigned_points;
            goto BFS_COMPLETE;
          } else {
            visited.insert(nid);
            next.push_back({nid, ndist});
          }
        }
      }
      bfs_front.swap(next);
      // Sort the BFS front by distance to prioritize closer neighbors
      std::sort(
          bfs_front.begin(), bfs_front.end(),
          [](const auto& a, const auto& b) { return a.second < b.second; });
    }
  BFS_COMPLETE:

    if (!found_cluster) {
      spdlog::warn("Point {} could not be assigned to any cluster.", point_id);
    }
  }
  const auto kernel_elapsed_time = kernel_timer.elapsed();
  spdlog::info("Finished assigning cluster IDs to noise points {}s",
               kernel_elapsed_time.count());
  spdlog::info("Number of noise points in the original data: {}",
               n_noise_points);
  spdlog::info("Number of remaining noise points: {}",
               n_noise_points - n_assigned_points);

  dump_point_cluster_ids(point_cluster_map, opt.cluster_ids_out_path);
  spdlog::info("Dumped point cluster IDs to {}",
               opt.cluster_ids_out_path.string());

  return EXIT_SUCCESS;
}
