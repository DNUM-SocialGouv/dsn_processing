#!/bin/bash

Help()
{
    # Display Help
    echo "Integrate historical data with Airflow."
    echo
    echo "Syntax: bash historical_integration.sh [--help|--start|--end|--database|--filetype|--log]"
    echo "options:"
    echo "--help          display this help and exit"
    echo "--start         first month to integrate (required)"
    echo "--end           last month to integrate (included) (required)"
    echo "--database      optional, data base connexion: 'champollion' (default), 'test' or 'mock'"
    echo "--filetype      optional, data path of DSN files: 'raw' (default) or 'test'"
    echo "--do_backup     optional, if added, backups are performed"
    echo "--log           optional, path directory to export log file"
}

ARGS=$(getopt -o '' --long help,start:,end:,database:,filetype:,do_backup,log: -n "$0" -- "$@")
if [ $? -ne 0 ]; then
    Help
fi

eval set -- "$ARGS"

while true; do
    case "$1" in
        --help)
            Help
            exit 1
            ;;
        --start)
            start_date=${2}
            shift 2
            ;;
        --end)
            end_date=${2}
            shift 2
            ;;
        --database)
            database=${2}
            shift 2
            ;;
        --filetype)
            filetype=${2}
            shift 2
            ;;
        --do_backup)
            do_backup=true
            shift 1
            ;;
        --log)
            log=${2}
            if ! [[ -d "$log" ]]; then
                echo "Error: The specified log storage path '$log' does not exist."
                exit 1
            fi
            shift 2
            ;;
        --)
            shift
            break
            ;;
    esac
done

if ! [[ -z "$log" ]]; then
    log_filename="$log/historical_integration_$(date +'%Y-%m-%d_%H-%M-%S').log"
    echo "Export logs to $log_filename"
    exec &>> "$log_filename"
fi

if [[ -z $start_date ]] || [[ -z $end_date ]]; then
    echo "Error:--start, and --end options are required."
    exit 1
fi

echo "Info: start date is $start_date."
echo "Info: end date is $end_date."

conf='{'
if ! [[ -z $database ]]; then
    conf=$conf'"db": "'$database'",'
    echo "Info: Configured database is $database."
fi

if ! [[ -z $do_backup ]]; then
    conf=$conf'"do_backup": true,'
    echo "Info: Backup activated."
else
    conf=$conf'"do_backup": false,'
    echo "Info: Backup deactivated."
fi

if ! [[ -z $filetype ]]; then
    conf=$conf'"filetype": "'$filetype'",'
    echo "Info: Configured filetype is $filetype."
fi

docker exec champollion-${COMPOSE_PROJECT_NAME}-airflow-webserver bash -c "airflow dags unpause monthly_integration"

current_month=$(date -d "$start_date" +%m | sed 's/^0//')
current_year=$(date -d "$start_date" +%Y)
end_month=$(date -d "$end_date" +%m | sed 's/^0//')
end_year=$(date -d "$end_date" +%Y)

while [[ "$current_year$current_month" -le "$(date -d "$end_date" +%Y%m)" ]]; do
    
    current_month_padded=$(printf "%02d" "$current_month")
    date="${current_year}-${current_month_padded}-01"
    tmp_conf=${conf}'"filedate": "'${date}'"}'

    docker exec champollion-${COMPOSE_PROJECT_NAME}-airflow-webserver bash -c "airflow dags trigger --conf='$tmp_conf' monthly_integration"
    if [ $? -ne 0 ]; then
        echo "Error: fail to execute job ($date)."
        exit 1
    fi

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