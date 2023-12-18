import datetime as dt

from airflow import DAG  # type: ignore
from airflow.models.baseoperator import chain  # type: ignore
from airflow.models.param import Param  # type: ignore

from dsn_processing.pipeline.airflow.dags.utils import (
    INTEGRATION_DATE_REGEX,
    INTEGRATION_DAY,
    SQL_TEMPLATE_SEARCHPATH,
    START_DATE,
    data_extraction,
    data_removal,
    database_backup,
    files_columns,
    files_delimiter,
    files_exist,
    files_qualifier,
    files_sizes,
    folder_exist,
    get_conn_id,
    get_default_declaration_date,
    get_start_task,
    get_successful_end_task,
    register_tasks,
)

POSTGRES_CONN_ID = get_conn_id()
DAG_ID = "monthly_integration"

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Monthly integration",
    schedule_interval=f"0 8 {INTEGRATION_DAY} * *",  # on intègre le 10 de chaque mois à 8h
    max_active_runs=1,
    start_date=START_DATE,
    template_searchpath=SQL_TEMPLATE_SEARCHPATH,
    catchup=False,
    params={
        "filedate": Param(
            default=get_default_declaration_date(),
            description="Date of the DSN files to integrate (must be yyyy-mm-01 and above 2019-01-01).",
            type="string",
            regex=INTEGRATION_DATE_REGEX,
        ),
        "filetype": Param(
            default="raw",
            description="Type of files to integrate (raw or test).",
            type="string",
            regex="^(raw|test)$",
        ),
        "db": Param(
            default="champollion",
            description="Database",
            type="string",
            regex="^(champollion|test|mock)$",
        ),
        "do_backup": Param(
            default=True,
            description="Should a backup be performed at the end.",
            type="boolean",
        ),
    },
) as dag:
    tasks = [
        get_start_task(postgres_conn_id=POSTGRES_CONN_ID),
        data_extraction,
        folder_exist,
        files_exist,
        files_sizes,
        # files_qualifier, -- some text columns have no qualifier unfortunately
        files_delimiter,
        files_columns,
    ]
    tasks = register_tasks(
        bash_filename="monthly_integration.sh",
        postgres_conn_id=POSTGRES_CONN_ID,
        tasks=tasks,
    )
    tasks += [
        database_backup,
        data_removal,
        get_successful_end_task(postgres_conn_id=POSTGRES_CONN_ID),
    ]

(chain(*tasks))
