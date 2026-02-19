# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


import argparse
from collections import defaultdict

def read_labels(label_file):
    """Reads a label file with one numeric label per line."""
    labels = {}
    with open(label_file, 'r') as f:
        for idx, line in enumerate(f):
            label = line.strip()
            if label:
                labels[idx] = int(label)
    return labels

def read_neighbors(neighbor_file):
    """Reads neighbor file with two lines per point: neighbors and distances."""
    neighbors = {}
    with open(neighbor_file, 'r') as f:
        lines = f.readlines()
    assert len(lines) % 2 == 0, "Neighbor file must have even number of lines."

    for i in range(0, len(lines), 2):
        ids = list(map(int, lines[i].strip().split()))
        point_id = ids[0]
        neighbor_ids = ids[1:]
        neighbors[point_id] = neighbor_ids
    return neighbors

def validate_ids(neighbors, labels):
    """Ensure all point and neighbor IDs have labels."""
    missing_ids = set()
    for pid, nlist in neighbors.items():
        if pid not in labels:
            missing_ids.add(pid)
        for nid in nlist:
            if nid not in labels:
                missing_ids.add(nid)
    if missing_ids:
        raise ValueError(f"Missing labels for the following point IDs: {sorted(missing_ids)}")

def compute_accuracy_by_k(neighbors, labels):
    """Computes label agreement for top-k neighbors, from 1 to max_k."""
    max_k = max(len(nlist) for nlist in neighbors.values())
    results = []

    for k in range(1, max_k + 1):
        total_neighbors = 0
        same_label_neighbors = 0
        total_points = 0

        for pid, nlist in neighbors.items():
            point_label = labels[pid]
            top_k_neighbors = nlist[:k]

            match_count = sum(1 for nid in top_k_neighbors if labels[nid] == point_label)

            total_points += 1
            total_neighbors += len(top_k_neighbors)
            same_label_neighbors += match_count

        acc = same_label_neighbors / total_neighbors if total_neighbors else 0.0
        results.append((k, acc, total_points, total_neighbors))

    return results

def main(neighbor_file, label_file):
    labels = read_labels(label_file)
    neighbors = read_neighbors(neighbor_file)
    validate_ids(neighbors, labels)
    results = compute_accuracy_by_k(neighbors, labels)

    print("k\tAccuracy\tPoints\tNeighbors")
    for k, acc, npoints, nneigh in results:
        print(f"{k}\t{acc:.4f}\t\t{npoints}\t{nneigh}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("neighbor_file", help="Neighbor file (2 lines per point)")
    parser.add_argument("label_file", help="Label file (one numeric label per line)")
    args = parser.parse_args()

    main(args.neighbor_file, args.label_file)
