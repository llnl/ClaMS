# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


"""
Plot clustering result

This script reads point data and cluster labels from files and generates a scatter plot.
Point data must be 2D.
Cluster label data can either be a single column of labels or two columns where the second column contains the labels.
If there are tree columns, the first column is ignored, the second column is labels,
and the third one is lamda p value to represent how the point is strongly belongs to the cluster.
"""

import os
import argparse
import random
import numpy as np
import matplotlib.pyplot as plt


def parse_options():
    parser = argparse.ArgumentParser(
        description='Plot clustering result')

    parser.add_argument('-p', '--point',
                        dest='point_file',
                        required=True, action='store', type=str,
                        help='Path to file containing point data')
    parser.add_argument('-l', '--label',
                        dest='label_file',
                        required=True, action='store', type=str,
                        help='Path to file containing cluster labels')
    parser.add_argument('-o', '--output',
                        dest='output_path',
                        required=False, action='store', type=str,
                        default='data_plot.pdf',
                        help='Path to the output file')

    args = parser.parse_args()
    return args


def main():
    opts = parse_options()

    points = np.loadtxt(opts.point_file)
    # Must be 2D
    if len(points.shape) != 2 or points.shape[1] != 2:
        print('Point data must be 2D')
        exit(1)

    labels = np.loadtxt(opts.label_file)
    if len(labels) != len(points):
        print('The number of labels must be the same as the number of points')
        exit(1)

    # Represents how strongly a point belongs to a cluster
    strength = np.ones(len(points))

    if len(labels.shape) == 2 and labels.shape[1] == 2:
        labels = labels[:, 1]
    elif len(labels.shape) == 2 and labels.shape[1] == 3:
        lambda_values = labels[:, 2]
        # Normalize lambda values to [0, 1]
        minv = lambda_values.min()
        maxv = lambda_values.max()
        if maxv != minv:
            strength = (lambda_values - minv) / (maxv - minv)

        labels = labels[:, 1]
        print('Change alpha values to represent how strongly a point belongs to a cluster')
    elif len(labels.shape) != 1:
        print(f'Wrong label data format. Shape: {labels.shape}')
        exit(1)

    preset_colors = ['#0000FF', '#FFA500', '#008000', '#FF0000', '#800080',
                     '#A52A2A', '#FFC0CB', '#808000', '#00FFFF']
    gray = '#808080'

    colors = {}
    markers = {}
    unique_labels = np.unique(labels)
    for i, label in enumerate(unique_labels):
        if label == -1:
            colors[label] = gray
            markers[label] = 'x'
        else:
            if i < len(preset_colors):
                colors[label] = preset_colors[i]
            else:
                # Generate random color
                colors[label] = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            markers[label] = '.'

    # Create a figure and axis
    fig, ax = plt.subplots()

    # Create a scatter plot with the custom colors
    for i in range(len(points)):
        alpha = strength[i] if int(labels[i]) != -1 else 1
        ax.scatter(points[i][0], points[i][1], color=colors[int(labels[i])], s=20,
                   marker=markers[int(labels[i])], alpha=alpha)

    # Show text at the right bottom
    text = 'Noise: X'
    ax.text(0.99, 0.01, text, verticalalignment='bottom', horizontalalignment='right',
            transform=ax.transAxes, color=gray, fontsize=15)

    # Dump as p PDF file
    # Check if output_path's extension is pdf
    output_path = opts.output_path
    if output_path.split('.')[-1] != 'pdf':
        print('Output file must be a PDF file. Add .pdf extension to the output path.')
        output_path = output_path + '.pdf'

    plt.savefig(output_path)
    print(f'Plot is saved in {output_path}')


if __name__ == '__main__':
    main()
