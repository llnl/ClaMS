# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


from pdf2image import convert_from_path
import os
from PIL import Image
import matplotlib.pyplot as plt


def combine_images_to_pdf(root_dir, param_names, dataset_names, algo_name,
                          output_pdf,
                          image_size=(40, 20), margin=0.1):
    image_dict = {}
    for param_name in param_names:
        for dataset_name in dataset_names:
            pdf_path = os.path.join(root_dir, param_name, dataset_name,
                                    f"{algo_name}.pdf")

            images = convert_from_path(pdf_path, dpi=800)
            image = images[0]
            if param_name not in image_dict:
                image_dict[param_name] = {}
            if dataset_name not in image_dict[param_name]:
                image_dict[param_name][dataset_name] = []

            image_dict[param_name][dataset_name] = image

    # Create a single-page image layout
    fig, axes = plt.subplots(len(image_dict),
                             max(len(v) for v in image_dict.values()),
                             figsize=image_size)
    fig.subplots_adjust(hspace=0.1, wspace=0.02)

    # Plot each image in a grid
    for row, (param, datasets) in enumerate(image_dict.items()):
        for col, (dataset, image) in enumerate(datasets.items()):
            if image:
                axes[row, col].imshow(image)
                axes[row, col].axis("off")
                # axes[row, col].set_title(f"{param} - {dataset}")
            else:
                print(f"Image not found for {param} - {dataset}")

    # Plot row labels (show text on the left side)
    for row, param in enumerate(image_dict.keys()):
        axes[row, 0].text(-0.01, 0.5, param, fontsize=20,
                          ha="center", va="center", rotation=90,
                          transform=axes[row, 0].transAxes)

    # Plot column labels
    for col, dataset in enumerate(image_dict[param_names[0]].keys()):
        axes[0, col].set_title(dataset, fontsize=15)

    # Save as PDF
    plt.savefig(output_pdf, bbox_inches="tight")
    print(f'Plot is saved in {output_pdf}')
    # plt.show()


def plot(algo_name):
    # List of PDF files to process
    parent_dir = "/Users/iwabuchi1/Downloads/out_synthetic_data_bench"
    param_names = ["m20_s5", "m30_s10", "m40_s15"]
    dataset_names = ["aniso", "blobs", "blobs_close", "blobs_varied",
                     "noisy_circles", "noisy_moons"]

    # Output file
    output_file = f"{algo_name}-combined.pdf"

    # Combine images into a single page PDF
    combine_images_to_pdf(parent_dir, param_names, dataset_names, algo_name,
                          output_file)


# Example usage:
if __name__ == "__main__":
    plot("clusters_gt")

    plot("clusters_hdbscan")
    plot("mst_hdbscan")

    plot("clusters_clms_hdbscan")

    plot("clusters_clms_mreach_dist")
    plot("mst_clms_mreach_dist")

    plot("clusters_clms_exact_mst")
    plot("mst_clms_exact_mst")

    plot("clusters_clms")
    plot("mst_amst")


