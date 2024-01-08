set -e
if [ $# -ne 1 ]; then
    echo "Usage: $0 <year at format YY (ex : 23 for 2023)>"
    exit 1
fi

python ${DSN_PROCESSING_REPOSITORY_PATH}/core/python/raw_files_management/generate_static_table_files.py -y $1
python ${DSN_PROCESSING_REPOSITORY_PATH}/core/python/raw_files_management/generate_holiday_calendar.py