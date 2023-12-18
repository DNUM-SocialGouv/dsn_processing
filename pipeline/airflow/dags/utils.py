import datetime as dt
import os

import pandas as pd
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator

from dsn_processing.core.python.raw_files_management.check_conformity_raw_files import (
    check_files_columns,
    check_files_delimiter,
    check_files_exist,
    check_files_qualifier,
    check_files_sizes,
    check_folder_exist,
)
from dsn_processing.pipeline.airflow.operators import PostgresOperator

AVAILABLE_CONN_ID_LIST = ["champollion", "test", "mock"]
SQL_TEMPLATE_SEARCHPATH = [
    os.path.join(
        os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
        "core",
        "sql",
    )
]
BASH_TEMPLATE_SEARCHPATH = os.path.join(
    os.environ["DSN_PROCESSING_REPOSITORY_PATH"], "pipeline", "bash", "dags"
)
INTEGRATION_DATE_REGEX = r"^\d{4}-(0[1-9]|1[0-2])-01$"
AUTHORIZED_DAGS = ["init_database.sh", "monthly_integration.sh", "update_database.sh"]
INTEGRATION_DAY = "10"
START_DATE = dt.datetime(2000, 1, 1)


def get_conn_id() -> str:
    return "{{ params.db }}"


def get_default_declaration_date() -> str:
    today = pd.Timestamp("today")
    ddate = None
    if today.day < int(INTEGRATION_DAY):
        ddate = str((today - pd.DateOffset(months=3)).replace(day=1).date())
    else:
        ddate = str((today - pd.DateOffset(months=2)).replace(day=1).date())
    return ddate


def get_default_year() -> str:
    today = pd.Timestamp("today")
    return str(today.year)[-2:]


def register_tasks(
    bash_filename: str,
    postgres_conn_id: str,
    tasks: list = [],
) -> list:
    file = open(os.path.join(BASH_TEMPLATE_SEARCHPATH, bash_filename), "r")
    for _, line in enumerate(file):
        # bash line
        parse = line.strip().split(sep=" ")
        if parse[0] == "python":
            # get python filename and args
            py_file = parse[1].split(sep="/")[-1]
            args = {}
            for j, arg in enumerate(parse):
                if arg.startswith("-"):
                    args[arg[1:]] = parse[j + 1]

            # execute sql file
            if "orchestrator.py" == py_file:
                sql_script = args["s"]
                task_id = sql_script.split("/")[-1]

                # date-dependent file sql script
                if "cmf" in args.keys():
                    filename = args["cmf"]
                    tasks.append(
                        PostgresOperator(
                            task_id=task_id,
                            sql=sql_script,
                            postgres_conn_id=postgres_conn_id,
                            autocommit=False,
                            params={
                                "filename": filename,
                                "filepath": "/champollion",
                                "foldername": "champollion",
                            },
                        )
                    )

                # static file sql script
                elif "csf" in args.keys():
                    filename = args["csf"]
                    tasks.append(
                        PostgresOperator(
                            task_id=task_id,
                            sql=sql_script,
                            postgres_conn_id=postgres_conn_id,
                            autocommit=False,
                            params={
                                "filename": filename,
                                "filepath": os.environ["WORKFLOW_SOURCES_DATA_PATH"],
                            },
                        )
                    )

                # no file sql script
                else:
                    tasks.append(
                        PostgresOperator(
                            task_id=task_id,
                            sql=sql_script,
                            postgres_conn_id=postgres_conn_id,
                            autocommit=False,
                        )
                    )

        # call another dag
        elif parse[0] == "bash":
            sh_file = parse[1].split(sep="/")[-1]
            if sh_file in AUTHORIZED_DAGS:
                tasks = register_tasks(
                    bash_filename=sh_file,
                    postgres_conn_id=postgres_conn_id,
                    tasks=tasks,
                )

    return tasks


data_extraction = BashOperator(
    task_id="data_extraction",
    bash_command="bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/common/extract_archives.sh {{ params.filedate }} {{ params.filetype }} ",  # mandatory last space here
)
data_removal = BashOperator(
    task_id="data_removal",
    bash_command="bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/common/remove_extracted.sh {{ params.filedate }} {{ params.filetype }} ",  # mandatory last space here
)
database_backup = BashOperator(
    task_id="database_backup",
    bash_command="bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/common/database_backup.sh {{ params.do_backup }} {{ params.db }} ",  # mandatory last space here
)

folder_exist = PythonOperator(
    task_id="check_folder_exist",
    python_callable=check_folder_exist,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)

files_exist = PythonOperator(
    task_id="check_files_exist",
    python_callable=check_files_exist,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)

files_sizes = PythonOperator(
    task_id="check_files_sizes",
    python_callable=check_files_sizes,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)

files_qualifier = PythonOperator(
    task_id="check_files_qualifier",
    python_callable=check_files_qualifier,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)

files_delimiter = PythonOperator(
    task_id="check_files_delimiter",
    python_callable=check_files_delimiter,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)

files_columns = PythonOperator(
    task_id="check_files_columns",
    python_callable=check_files_columns,
    op_args=["{{ params.filedate }}", "{{ params.filetype }}"],
)


def get_start_task(postgres_conn_id: str):
    start = PostgresOperator(
        task_id="start",
        sql="SELECT SET_STATUS_TO_ONGOING();",
        postgres_conn_id=postgres_conn_id,
        autocommit=False,
    )
    return start


def get_successful_end_task(postgres_conn_id: str):
    end = PostgresOperator(
        task_id="success",
        sql="SELECT SET_STATUS_TO_SUCCESS();",
        postgres_conn_id=postgres_conn_id,
        autocommit=False,
    )
    return end
