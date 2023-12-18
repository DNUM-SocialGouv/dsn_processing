#!/bin/bash
set -e

# Check if the required arguments are provided
if [ $# -ne 3 ]; then
  echo "Usage: $0 <start_date> <end_date> <folder_type>"
  exit 1
fi

start_date=$1
end_date=$2
folder_type=$3

current_month=$(date -d "$start_date" +%m | sed 's/^0//')
current_year=$(date -d "$start_date" +%Y)
end_month=$(date -d "$end_date" +%m | sed 's/^0//')
end_year=$(date -d "$end_date" +%Y)

while [[ "$current_year$current_month" -le "$(date -d "$end_date" +%Y%m)" ]]; do
  current_month_padded=$(printf "%02d" "$current_month")
  echo "Integration of month ${current_year}-${current_month_padded}-01"
  bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/dags/monthly_integration.sh ${current_year} ${current_month_padded} ${folder_type}
  if [[ "$current_month" -eq "$end_month" && "$current_year" -eq "$end_year" ]]; then
    break
  fi
  if [[ "$current_month" -eq "12" ]]; then
    current_month=1
    current_year=$((current_year+1))
  else
    current_month=$((current_month+1))
  fi
done
