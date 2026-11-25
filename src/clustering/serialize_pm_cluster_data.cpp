// Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
// Project Developers. See the top-level COPYRIGHT file for details.

// Serialize point-to-cluster IDs from a Metall or Metall MPI datastore to a
// text file.
// Currently, this program targets the ouputs of the distributed clustering
// program.

#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

#include <spdlog/spdlog.h>
#include <metall/utility/metall_mpi_datastore.hpp>

#include "../common.hpp"

using namespace clams;

struct option {
  std::filesystem::path cluster_data_input_path;
  std::filesystem::path cluster_data_out_path;
};

void show_help(const char* program) {
  std::cout << "Usage: " << program
            << " -i <cluster_ids_metall_path> -o <output_file>\n"
               "  -i  Input Metall datastore or Metall MPI datastore "
               "containing point-to-cluster IDs\n"
               "  -o  Output text file (point ID and cluster ID per line)\n"
               "  -h  Show this help\n";
}

void parse_option(int argc, char* argv[], option& opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "i:o:h")) != -1) {
    switch (opt_char) {
      case 'i':
        opt.cluster_data_input_path = optarg;
        break;
      case 'o':
        opt.cluster_data_out_path = optarg;
        break;
      case 'h':
        show_help(argv[0]);
        std::exit(EXIT_SUCCESS);
      default:
        show_help(argv[0]);
        std::exit(EXIT_FAILURE);
    }
  }

  if (optind != argc || opt.cluster_data_input_path.empty() ||
      opt.cluster_data_out_path.empty()) {
    std::cerr << "Both -i and -o are required, with no positional arguments.\n";
    show_help(argv[0]);
    std::exit(EXIT_FAILURE);
  }
}

void read_cluster_ids(const std::filesystem::path&  cluster_ids_path,
                      bool                          mpi_partition,
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

  point_cluster_map.reserve(point_cluster_map.size() +
                            input_cluster_id_list->size());
  point_cluster_map.insert(input_cluster_id_list->begin(),
                           input_cluster_id_list->end());
}

int main(int argc, char* argv[]) {
  option opt;
  parse_option(argc, argv, opt);

  pm_point_to_cluster_id_map_t point_cluster_map;

  const auto partition_size_path =
      opt.cluster_data_input_path / "metall_mpi_adaptor_partition_size";
  if (std::filesystem::exists(partition_size_path)) {
    std::ifstream partition_size_file(partition_size_path);
    int           partition_size = 0;
    if (!(partition_size_file >> partition_size) || partition_size <= 0) {
      spdlog::critical("Failed to read MPI datastore partition size from {}",
                       partition_size_path.string());
      std::abort();
    }

    spdlog::info("Attaching cluster IDs in {} Metall MPI datastore partitions",
                 partition_size);
    for (int rank = 0; rank < partition_size; ++rank) {
      const auto local_path =
          metall::utility::mpi_datastore::make_local_dir_path(
              opt.cluster_data_input_path.string(), rank);
      read_cluster_ids(local_path, true, point_cluster_map);
    }
  } else {
    spdlog::info("Attaching cluster IDs in Metall datastore");
    read_cluster_ids(opt.cluster_data_input_path, false, point_cluster_map);
  }

  dump_point_cluster_ids(point_cluster_map, opt.cluster_data_out_path);
  spdlog::info("Serialized {} point-to-cluster IDs to {}",
               point_cluster_map.size(), opt.cluster_data_out_path.string());
  return EXIT_SUCCESS;
}
