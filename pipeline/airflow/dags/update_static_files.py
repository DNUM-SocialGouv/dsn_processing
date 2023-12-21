import datetime as dt

from airflow import DAG  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from dsn_processing.core.python.raw_files_management.generate_holiday_calendar import (
    generate_holidays_calendar,
)
from dsn_processing.core.python.raw_files_management.generate_static_table_files import (
    generate_categories_juridiques_insee_file,
    generate_conventions_collectives_file,
    generate_motifs_recours_file,
    generate_naf_file,
    generate_natures_contrats_file,
)
from dsn_processing.pipeline.airflow.dags.utils import (
    START_DATE,
    get_conn_id,
    get_default_year,
)

POSTGRES_CONN_ID = get_conn_id()
DAG_ID = "update_static_files"

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Update the static files",
    schedule_interval="0 8 1 2 *",  # each february 1st of the year at 8:00
    max_active_runs=1,
    start_date=START_DATE,
    catchup=False,
    params={
        "year": Param(default=get_default_year(), type="string", regex="^(2[0-9]|30)$")
    },
) as dag:
    generate_categories_juridiques_insee_file_ = PythonOperator(
        task_id="generate_categories_juridiques_insee_file",
        python_callable=generate_categories_juridiques_insee_file,
    )

    generate_conventions_collectives_file_ = PythonOperator(
        task_id="generate_conventions_collectives_file",
        op_args=[
            "{{ params.year }}",
        ],
        python_callable=lambda year: generate_conventions_collectives_file(year=year),
    )

    generate_motifs_recours_file_ = PythonOperator(
        task_id="generate_motifs_recours_file",
        op_args=[
            "{{ params.year }}",
        ],
        python_callable=lambda year: generate_motifs_recours_file(year=year),
    )

    generate_naf_file_ = PythonOperator(
        task_id="generate_naf_file",
        op_args=[
            "{{ params.year }}",
        ],
        python_callable=lambda year: generate_naf_file(year=year),
    )

    generate_natures_contrats_file_ = PythonOperator(
        task_id="generate_natures_contrats_file",
        op_args=[
            "{{ params.year }}",
        ],
        python_callable=lambda year: generate_natures_contrats_file(year=year),
    )

    generate_holidays_calendar_ = PythonOperator(
        task_id="generate_holidays_calendar",
        python_callable=generate_holidays_calendar,
    )

(
    [
        generate_conventions_collectives_file_,
        generate_categories_juridiques_insee_file_,
        generate_motifs_recours_file_,
        generate_naf_file_,
        generate_natures_contrats_file_,
        generate_holidays_calendar_,
    ]
)  # type: ignore
