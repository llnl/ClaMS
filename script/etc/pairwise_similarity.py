# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


import argparse
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.preprocessing import normalize

def load_vectors(file_path):
    return np.loadtxt(file_path)  # defaults to whitespace-delimited

def compute_l2_distances(vectors):
    return pairwise_distances(vectors, metric='euclidean')

def compute_cosine_similarity(vectors):
    normed = normalize(vectors, norm='l2')
    return np.dot(normed, normed.T)

def main(input_file):
    vectors = load_vectors(input_file)

    print("Computing L2 distances...")
    l2_matrix = compute_l2_distances(vectors)
    print("L2 distance matrix:")
    print(l2_matrix)

    print("\nComputing cosine similarities...")
    cosine_matrix = compute_cosine_similarity(vectors)
    print("Cosine similarity matrix:")
    print(cosine_matrix)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="File with one whitespace-separated vector per line")
    args = parser.parse_args()

    main(args.input_file)
