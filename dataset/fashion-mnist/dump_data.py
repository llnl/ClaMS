# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


import mnist_reader
import numpy as np

X_train, y_train = mnist_reader.load_mnist('fashion-mnist/data/fashion', kind='train')
# X_test, y_test = mnist_reader.load_mnist('data/fashion', kind='t10k')

print(X_train.shape, y_train.shape)

np.savetxt('points.txt', X_train, fmt='%d')
print('Training data is saved in points.txt')

np.savetxt('labels.txt', y_train, fmt='%d')
print('Training labels are saved in labels.txt')

point_label_array = np.transpose(np.vstack((np.arange(y_train.shape[0]), y_train)))
print(point_label_array.shape)
np.savetxt('labels_with_point_ids.txt', point_label_array, fmt='%d')
print('File of with point ids and training labels is saved in labels_with_point_ids.txt')