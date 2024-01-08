import datetime as dt

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models.param import Param  # type: ignore

from dsn_processing.pipeline.airflow.dags.utils import (
    SQL_TEMPLATE_SEARCHPATH,
    START_DATE,
    get_conn_id,
    register_tasks,
)

POSTGRES_CONN_ID = get_conn_id()
DAG_ID = "update_database"

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Update of the static tables of the database",
    schedule_interval=None,
    start_date=START_DATE,
    max_active_runs=1,
    template_searchpath=SQL_TEMPLATE_SEARCHPATH,
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
    tasks = register_tasks(
        bash_filename="update_database.sh",
        postgres_conn_id=POSTGRES_CONN_ID,
    )

(chain(*tasks))
