import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type:ignore

from dsn_processing.pipeline.airflow.dags.utils import AVAILABLE_CONN_ID_LIST


def check_database_connection(db: str):
    """
    Queries Postgres and returns a cursor to the results.
    """

    query = """
    SELECT 
        current_database() AS db_name,
        s.schema_name AS schema_name,
        t.table_name AS table_name,
        c.row_count AS row_count
    FROM 
        information_schema.schemata AS s
    JOIN 
        information_schema.tables AS t ON s.schema_name = t.table_schema
    LEFT JOIN (
        SELECT 
            schemaname AS schema_name,
            relname AS table_name,
            n_live_tup AS row_count
        FROM 
            pg_stat_all_tables
    ) AS c ON t.table_schema = c.schema_name AND t.table_name = c.table_name
    WHERE s.schema_name IN ('public', 'log', 'raw', 'source')
    ORDER BY schema_name, table_name;
    """

    assert (
        db in AVAILABLE_CONN_ID_LIST
    ), f'wrong value of variable db ("{db}" instead of "champollion", "test" or  "mock")'

    postgres = PostgresHook(postgres_conn_id=db)
    conn = postgres.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    logs = f"\nDatabase : {results[0][0]}"
    logs += f"\nCheck database connection : ok."
    if len(results) == 0:
        logs += "\nDatabase schemas: no schema found."
        print(logs)
        return

    df = pd.DataFrame(
        results, columns=["db_name", "schema_name", "table_name", "row_count"]
    )

    logs += f"\nDatabase schemas: {', '.join(df.schema_name.unique())}"
    for index, row in df.iterrows():
        logs += (
            f"\nRows in {row['schema_name']}.{row['table_name']}: {row['row_count']}"
        )

    print(logs)
    return
