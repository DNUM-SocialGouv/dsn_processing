#!/bin/bash
set -e

# Check if year, month, and folder type are provided as arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <file_date> <folder_type>"
    exit 1
fi

file_date="$1"
year="${file_date::4}"
month="${file_date:5:2}"
folder_type="$2"

# Construct the source and destination paths
if [ "$folder_type" == "raw" ]; then
    src_folder="${WORKFLOW_RAW_DATA_PATH}/champollion_${year}${month}01"
    dst_folder="${WORKFLOW_RAW_DATA_PATH}/champollion_${year}-${month}-01"

    if [ -d "$dst_folder" ]; then
        rm -r "$dst_folder"
        echo "Remove $dst_folder"
    fi

    if [ -d "$src_folder" ]; then
        rm -r "$src_folder"
        echo "Remove $src_folder"
    fi

fi

