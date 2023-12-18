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
if [ "$folder_type" == "test" ]; then
    src_folder="${WORKFLOW_TEST_DATA_PATH}/champollion_${year}${month}01"
    dst_folder="${WORKFLOW_TEST_DATA_PATH}/champollion_${year}-${month}-01"
else
    src_folder="${WORKFLOW_RAW_DATA_PATH}/champollion_${year}${month}01"
    dst_folder="${WORKFLOW_RAW_DATA_PATH}/champollion_${year}-${month}-01"
fi

zip_folder="${WORKFLOW_ARCHIVES_DATA_PATH}/champollion_${year}${month}01.7z"

# Check if the destination folder exists
if [ -d "$dst_folder" ]; then
    echo "Destination folder already exists. Nothing to do."
else
    # Check if the source folder exists
    if [ -d "$src_folder" ]; then
        mv "$src_folder" "$dst_folder"
        echo "Moved folder $src_folder to $dst_folder."
    else
        # Check if the archive exists
        if [ "$folder_type" == "raw" ] && [ -e "$zip_folder" ]; then
            7za x "$zip_folder" -o"${WORKFLOW_RAW_DATA_PATH}" -p${WORKFLOW_ARCHIVES_PASSWORD}
            mv "$src_folder" "$dst_folder"
            echo "Extracted and moved folder $src_folder to $dst_folder."
        else
            echo "Source folder $src_folder and archive $zip_folder do not exist. Cannot perform the operation."
            exit 1
        fi
    fi
fi