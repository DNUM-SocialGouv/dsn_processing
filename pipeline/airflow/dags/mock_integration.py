import datetime as dt

from airflow import DAG  # type: ignore
from airflow.models.baseoperator import chain  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import PythonOperator

from dsn_processing.core.python.raw_files_management.generate_mock_table_files import (
    generate_mock_data_files,
)
from dsn_processing.pipeline.airflow.dags.utils import (
    SQL_TEMPLATE_SEARCHPATH,
    START_DATE,
    get_conn_id,
    register_tasks,
)

POSTGRES_CONN_ID = get_conn_id()
DAG_ID = "mock_integration"

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Integration of the mock data",
    schedule_interval=None,
    max_active_runs=1,
    start_date=START_DATE,
    template_searchpath=SQL_TEMPLATE_SEARCHPATH,
    catchup=False,
    params={
        "db": Param(
            default="mock",
            description="Database",
            type="string",
            regex="^(champollion|test|mock)$",
        ),
    },
) as dag:
    tasks = [
        PythonOperator(
            task_id="generate_mock_data_files",
            python_callable=generate_mock_data_files,
        )
    ]
    tasks = register_tasks(
        bash_filename="mock_integration.sh",
        postgres_conn_id=POSTGRES_CONN_ID,
        tasks=tasks,
    )
(chain(*tasks))
