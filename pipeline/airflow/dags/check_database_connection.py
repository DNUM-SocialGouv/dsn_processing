import datetime as dt

from airflow import DAG  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from dsn_processing.core.python.database_interactions.check_database_connection import (
    check_database_connection,
)
from dsn_processing.pipeline.airflow.dags.utils import START_DATE

DAG_ID = "check_database_connection"

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Check the connection of a database",
    schedule_interval=None,
    max_active_runs=1,
    start_date=START_DATE,
    catchup=False,
    params={
        "db": Param(
            default="champollion",
            description="Database",
            type="string",
            regex="^(champollion|test|mock)$",
        ),
    },
) as dag:
    ### START
    start = EmptyOperator(task_id="start")

    ### CHECK DATABASE CONNECTION
    check_database_connection = PythonOperator(
        task_id="check_database_connection",
        python_callable=check_database_connection,
        op_args=[
            "{{ params.db }}",
        ],
    )

start >> check_database_connection  # type:ignore
