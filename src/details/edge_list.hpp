// Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
// Project Developers. See the top-level COPYRIGHT file for details.


#pragma once

#include <cstddef>
#include <memory>
#include <iostream>
#include <utility>

#include <boost/container/vector.hpp>

namespace clams {

template <typename IdType, typename DistanceType>
struct weighted_edge {
  using id_type = IdType;
  using distance_type = DistanceType;

  weighted_edge() = default;
  weighted_edge(const id_type &id1, const id_type &id2,
                const distance_type &dist)
      : ids{id1, id2}, distance(dist) {}

  id_type ids[2];
  distance_type distance;
};

template <typename IdType, typename DistanceType>
std::ostream &operator<<(std::ostream &os,
                         const weighted_edge<IdType, DistanceType> &edge) {
  os << edge.ids[0] << "\t" << edge.ids[1] << "\t" << edge.distance;
  return os;
}

template <typename IdType, typename DistanceType,
          typename Allocator =
              std::allocator<weighted_edge<IdType, DistanceType>>>
using weighted_edge_list =
    boost::container::vector<weighted_edge<IdType, DistanceType>,
                             typename Allocator::template rebind<
                                 weighted_edge<IdType, DistanceType>>::other>;
}  // namespace clams