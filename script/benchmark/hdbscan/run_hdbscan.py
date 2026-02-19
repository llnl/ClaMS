# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


# Description: This script generates a synthetic dataset and runs HDBSCAN on it.
# It also can read a point file and run HDBSCAN on it.
# Usage:
# python run_hdbscan.py -n 1000 -m 5
# Or
# python run_hdbscan.py -p points.txt -m 5


from sklearn.datasets import make_blobs
import hdbscan
import os
import argparse
import numpy as np
from clustering_utilities import *


def parse_options():
    parser = argparse.ArgumentParser(
        description='Evaluate kNN index')

    # For input point (feature) data
    parser.add_argument('-p', '--point_data_path',
                        dest='point_data_path',
                        required=False, action='store', type=str,
                        help='Input point file path')

    parser.add_argument('-I', '--has_ids',
                        dest='has_ids',
                        required=False, action='store_true',
                        help='If specified, the input point file has point IDs')

    # For generate synthetic data
    parser.add_argument('-n', '--n_samples',
                        dest='n_samples',
                        required=False, action='store', type=int,
                        default=1000,
                        help='#of samples to generate by the data generator')
    parser.add_argument('-f', '--n_features',
                        dest='n_features',
                        required=False, action='store', type=int,
                        default=32,
                        help='#of features to generate by the data generator')

    # HDBSCAN parameters
    parser.add_argument('-m', '--min_cluster_size',
                        dest='min_cluster_size',
                        required=False, action='store', type=int,
                        default=5,
                        help='Minimum cluster size.')
    parser.add_argument('-s', '--min_samples',
                        dest='min_samples',
                        required=False, action='store', type=int,
                        default=None,
                        help='#of samples in a neighborhood for a point to be considered as a core point.'
                             'When None, defaults to min_cluster_size')

    # Output file paths
    parser.add_argument('-o', '--cluster_labels_out_path',
                        dest='cluster_labels_out_path',
                        required=False, action='store', type=str,
                        default='out_clusters.txt',
                        help='File path to store computed cluster IDs')
    parser.add_argument('-M', '--mst_out_path',
                        dest='mst_out_path',
                        required=False, action='store', type=str,
                        help='Output file path for intermediate MST data')
    parser.add_argument('-c', '--condensed_tree_out_path',
                        dest='condensed_tree_out_path',
                        required=False, action='store', type=str,
                        default=None,
                        help='File path to store internal cluster data (mainly for debugging)')
    parser.add_argument('-C', '--cluster_persistence_out_path',
                        dest='cluster_persistence_out_path',
                        required=False, action='store', type=str,
                        default=None,
                        help='File path to store cluster persistence data')

    # Ground truth file path
    parser.add_argument('-g', '--ground_truth',
                        dest='gt_file',
                        required=False, action='store', type=str,
                        help='Ground truth file path')

    args = parser.parse_args()
    return args


def run_hdbscan_kernel(points, min_cluster_size, min_samples,
                       gen_min_span_tree=False):
    clusters = hdbscan.HDBSCAN(gen_min_span_tree=gen_min_span_tree,
                               min_cluster_size=min_cluster_size,
                               min_samples=min_samples,
                               core_dist_n_jobs=-1)
    print(f'\nStart clustering')
    show_time_now()
    clusters.fit(points)
    print(f'Finish clustering')
    show_time_now()

    return clusters


def run_hdbscan(points, min_cluster_size, min_samples,
                cluster_labels_out_path='out_clusters.txt',
                gt_file=None,
                mst_out_path=None,
                condensed_tree_out_path=None,
                cluster_persistence_out_path=None,
                assign_cluster_to_noise=False):
    clusters = run_hdbscan_kernel(points, min_cluster_size, min_samples,
                                  gen_min_span_tree=(mst_out_path is not None))

    # Evaluate the clustering quality
    if gt_file:
        print('\nLoading ground truth data')
        gt_labels = read_label_data(gt_file)

        print('\nEvaluating clustering quality')
        eval_clusters(clusters.labels_, gt_labels)

        if assign_cluster_to_noise:
            print('\nAssigning a cluster ID to every noise point')
            no_noise_labels = assign_singleton_cluster_to_noise_point(
                clusters.labels_)
            eval_clusters(no_noise_labels, gt_labels)

    # Save the condensed tree data
    if condensed_tree_out_path:
        print(f'\nSaving condensed tree data in {condensed_tree_out_path}')
        clusters.condensed_tree_.to_pandas().to_csv(condensed_tree_out_path)

    # Save the MST data
    if mst_out_path:
        print(f'\nSaving MST data in {mst_out_path}')
        with open(mst_out_path, 'w') as fout_mst:
            mst = clusters.minimum_spanning_tree_.to_numpy()
            # Format: Point0, Point1, Distance
            for edge in mst:
                fout_mst.write(f'{int(edge[0])}\t{int(edge[1])}\t{edge[2]}\n')
            fout_mst.close()
            show_time_now()

    # Save the cluster IDs.
    if cluster_labels_out_path:
        print(f'\nSaving cluster IDs in {cluster_labels_out_path}')
        with open(cluster_labels_out_path, 'w') as fout:
            fout.write(f'# Node ID\tCluster ID\n')
            for i, label in enumerate(clusters.labels_):
                fout.write(f'{i}\t{label}\n')
            show_time_now()
            print(f'Cluster IDs are saved in {cluster_labels_out_path}')

    # Save the cluster persistence data
    if cluster_persistence_out_path:
        print(
            f'\nSaving cluster persistence data in {cluster_persistence_out_path}')
        with open(cluster_persistence_out_path, 'w') as fout:
            fout.write('Cluster ID\tPersistence\n')
            for i, persistence in enumerate(clusters.cluster_persistence_):
                fout.write(f'{i}\t{persistence}\n')


def main():
    opt = parse_options()

    show_time_now()
    if opt.point_data_path:
        points = read_point_data(opt.point_data_path, opt.has_ids)
    else:
        points, _ = make_blobs(n_samples=opt.n_samples,
                               n_features=opt.n_features,
                               centers=10)
    print(f'points data shape: {points.shape}')

    run_hdbscan(points, opt.min_cluster_size, opt.min_samples,
                gt_file=opt.gt_file,
                cluster_labels_out_path=opt.cluster_labels_out_path,
                mst_out_path=opt.mst_out_path,
                condensed_tree_out_path=opt.condensed_tree_out_path,
                cluster_persistence_out_path=opt.cluster_persistence_out_path)


if __name__ == '__main__':
    main()
