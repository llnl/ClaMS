# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


# USPS dataset preprocessing script
# Original dataset source:
# https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#usps

# This script convert the labels to 0-based ones and merge the training
# and testing files into a single file

# Usage:
# cd dataset/usps
# Download the original files.
# python3 preprocess_usps_data.py

def merge_files(file1_path, file2_path, output_file_path):
    with open(file1_path, 'r') as file1, open(file2_path, 'r') as file2, open(
            output_file_path, 'w') as output_file:
        output_file.write(file1.read())
        output_file.write(file2.read())


# Merge training and testing files into a single file
merge_files('./original/usps', './original/usps.t', '/tmp/usps-merged')


def convert_file(input_filepath, labels_output_filepath,
                 values_output_filepath):
    with open(input_filepath, 'r') as file:
        labels = []
        values = []

        for line in file:
            parts = line.strip().split()
            if (len(parts) == 0):
                print(f'Empty line found at line {len(labels)}')
                continue
            labels.append(str(int(parts[0]) - 1))  # First element is the label

            # Skip the first element and remove the column number part
            line_values = [item.split(':')[1] for item in parts[1:]]
            values.append(' '.join(line_values))

    # Write the labels to the labels output file, changing the labels to 0-based ones
    with open(labels_output_filepath, 'w') as file:
        new_labels = dict()
        for label in labels:
            if label not in new_labels:
                new_labels[label] = len(new_labels)
            file.write(str(new_labels[label]) + '\n')

    # Write the values to the values output file
    with open(values_output_filepath, 'w') as file:
        for value_line in values:
            file.write(value_line + '\n')


# Convert the merged file into separate labels and points files
convert_file('/tmp/usps-merged', 'labels.txt', 'points.txt')
