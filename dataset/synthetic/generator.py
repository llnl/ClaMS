# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


# Generate synthetic datasets for clustering
# https://scikit-learn.org/1.5/auto_examples/cluster/plot_cluster_comparison.html

import argparse
import os
import random
import numpy as np
from sklearn import cluster, datasets, mixture
from sklearn.preprocessing import StandardScaler


def parse_options():
    parser = argparse.ArgumentParser(
        description='Generate synthetic datasets for clustering')

    parser.add_argument('-n', '--n_points',
                        dest='n_samples',
                        required=False, action='store', type=int,
                        default=10,
                        help='#of points to generate by the data generator')

    parser.add_argument('-s', '--seed',
                        dest='rnd_seed',
                        required=False, action='store', type=int,
                        default=random.randrange(1, 100),
                        help='Random seed')

    parser.add_argument('-o', '--out_prefix',
                        dest='out_prefix',
                        required=False, action='store', type=str,
                        default='./',
                        help='Prefix for output files')

    args = parser.parse_args()
    return args


opt = parse_options()

noisy_circles = datasets.make_circles(
    n_samples=opt.n_samples * 2, factor=0.5, noise=0.05,
    random_state=opt.rnd_seed
)
noisy_moons = datasets.make_moons(n_samples=opt.n_samples * 2, noise=0.05,
                                  random_state=opt.rnd_seed)
blobs = datasets.make_blobs(
    n_samples=[opt.n_samples, opt.n_samples, opt.n_samples],
    random_state=opt.rnd_seed)
rng = np.random.RandomState(opt.rnd_seed)
no_structure = rng.rand(opt.n_samples, 2), None

# Anisotropicly distributed data
random_state = 170
X, y = datasets.make_blobs(
    n_samples=[opt.n_samples, opt.n_samples, opt.n_samples],
    random_state=random_state)
transformation = [[0.6, -0.6], [-0.4, 0.8]]
X_aniso = np.dot(X, transformation)
aniso = (X_aniso, y)

# blobs with varied variances
blobs_samples = opt.n_samples
blobs_varied = datasets.make_blobs(
    n_samples=[blobs_samples, blobs_samples, blobs_samples, blobs_samples * 2,
               blobs_samples * 2, blobs_samples * 2],
    cluster_std=[0.75, 1.0, 1.5, 0.75, 1.0, 1.5],
    random_state=random_state,
    centers=[(-10, -5), (0, -5), (10, -5), (-10, 5), (0, 5), (10, 5)]
)

blobs_close = datasets.make_blobs(
    n_samples=[blobs_samples, blobs_samples, blobs_samples],
    random_state=random_state,
    cluster_std=[1.0, 1.0, 1.0],
    centers=[(-5, 0), (0, 0), (5, 0)]
)

datasets = [{'name': 'noisy_circles', 'data': noisy_circles},
            {'name': 'noisy_moons', 'data': noisy_moons},
            {'name': 'blobs_varied', 'data': blobs_varied},
            {'name': 'blobs_close', 'data': blobs_close},
            {'name': 'aniso', 'data': aniso},
            {'name': 'blobs', 'data': blobs},
            # {'name': 'no_structure', 'data': no_structure}
            ]

for dataset in datasets:
    X, y = dataset['data']
    # normalize dataset for easier parameter selection
    X = StandardScaler().fit_transform(X)

    print(f'Dataset: {dataset["name"]}')
    print(f'X shape: {X.shape}, y shape: {y.shape}')
    print(f'Unique labels: {np.unique(y)}')

    # Dump the data
    point_file = f'{opt.out_prefix}{dataset["name"]}_points.txt'
    label_file = f'{opt.out_prefix}{dataset["name"]}_labels.txt'
    print(f'Saving points to {point_file}')
    print(f'Saving labels to {label_file}')
    np.savetxt(point_file, X, fmt='%f')
    np.savetxt(label_file, y, fmt='%d')

    print()
    print()
