# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


from sklearn.datasets import fetch_openml
mnist = fetch_openml("MNIST_784")

# Save the data
import numpy as np

print('Data shape:', mnist.data.shape)
print('Label shape:', mnist.target.shape)

point_label_array = np.transpose(np.vstack((np.arange(mnist.data.shape[0]), mnist.target)))
print(point_label_array.shape)
np.savetxt('./labels_with_point_ids.txt', point_label_array, fmt='%s')

np.savetxt('./points.txt', mnist.data, fmt='%d')
np.savetxt('./labels.txt', mnist.target, fmt='%s')