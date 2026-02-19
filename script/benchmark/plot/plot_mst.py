# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


"""
Plot MST
"""

import os
import argparse
import random
import numpy as np
import matplotlib.pyplot as plt


def parse_options():
    parser = argparse.ArgumentParser(
        description='Plot MST')

    parser.add_argument('-p', '--point',
                        dest='point_file',
                        required=True, action='store', type=str,
                        help='Path to file containing point data')
    parser.add_argument('-m', '--mst',
                        dest='mst_file',
                        required=True, action='store', type=str,
                        help='Path to file containing MST edges')
    parser.add_argument('-o', '--output',
                        dest='output_path',
                        required=False, action='store', type=str,
                        default='mst_plot.pdf',
                        help='Path to the output file')

    args = parser.parse_args()
    return args


def generate_thermal_colors(data, cmap=None):
    # Normalize data
    data_normalized = (data - np.min(data)) / (np.max(data) - np.min(data))

    # Get the colormap
    colormap = plt.get_cmap(cmap)

    # Generate colors
    thermal_colors = [colormap(value) for value in data_normalized]
    return thermal_colors


def generate_thermal_colors_2(data, cmap=None):
    data_with_index = [(i, value) for i, value in enumerate(data)]
    data_with_index.sort(key=lambda x: x[1])

    colormap = plt.get_cmap(cmap)
    thermal_colors = [None] * len(data)

    for i, value in data_with_index:
        thermal_colors[data_with_index[i][0]] = colormap(i / len(data))

    return thermal_colors


def main():
    opts = parse_options()

    points = np.loadtxt(opts.point_file)
    # Must be 2D
    if len(points.shape) != 2 or points.shape[1] != 2:
        print('Point data must be 2D')
        exit(1)

    mst_edges = np.loadtxt(opts.mst_file).astype(np.float32)
    # Each edge must have 2 or 3 elements: (src, dst) or (src, dst, weight)
    if len(mst_edges.shape) != 2 or not (mst_edges.shape[1] == 2 or mst_edges.shape[1] == 3):
        print('Invalid MST edge shape')

    # Extract weights
    weights = []
    if mst_edges.shape[1] == 3:
        weights = mst_edges[:, 2]
        mst_edges = mst_edges[:, :2]
    mst_edges = mst_edges.astype(np.int32)

    # Generate thermal colors
    thermal_colors = None
    if len(weights) > 0:
        thermal_colors = generate_thermal_colors(weights)
        print('Weights are used for edge colors')

    # Plot points
    plt.figure()
    plt.scatter(points[:, 0], points[:, 1], s=2, c='black', marker='o')

    # Plot MST edges
    for i, edge in enumerate(mst_edges):
        src, dst = edge
        color = thermal_colors[i] if len(thermal_colors) > 0 else 'black'
        plt.plot([points[src, 0], points[dst, 0]], [
                 points[src, 1], points[dst, 1]], c=color, linewidth=2)

    # Dump as p PDF file
    plt.savefig(opts.output_path)
    print(f'Plot is saved in {opts.output_path}')


if __name__ == '__main__':
    main()
