#!/bin/bash

# Usage:
# cd <path_to_repo>/build
# ../script/test_hdbscan_clustering.sh

mst_file="./blobs_mst.txt"
hdbscan_cluster_ids_file="./hdbscan_cluster_ids_file.txt"
num_samples=$((2**15))
min_cluster_size=5

echo "Running HDBSCAN"
python3 ../script/run_hdbscan.py --n_samples $num_samples --min_cluster_size $min_cluster_size --mst_out_path ${mst_file} --cluster_labels_out_path ${hdbscan_cluster_ids_file}


cluster_ids_file="out_cluster_ids.txt"
echo ""
./src/run_hdbscan_clustering -i ${mst_file} -o ${cluster_ids_file} -m $min_cluster_size

echo ""
echo "Reassigning cluster ids"
python3 ../script/reassign_sequential_cluster_ids.py -i ${hdbscan_cluster_ids_file} -o ${hdbscan_cluster_ids_file}

echo ""
echo "Reassigning cluster ids"
python3 ../script/reassign_sequential_cluster_ids.py -i ${cluster_ids_file} -o ${cluster_ids_file}

echo ""
echo "Running diff"
diff ${hdbscan_cluster_ids_file} ${cluster_ids_file} > diff.txt

echo "#of lines in diff.txt:"
wc -l diff.txt

echo "Done"