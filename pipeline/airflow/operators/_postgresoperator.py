from airflow.providers.postgres.operators.postgres import (
    PostgresOperator as _PostgresOperator,  # type: ignore
)


class PostgresOperator(_PostgresOperator):
    template_fields = [*_PostgresOperator.template_fields, "conn_id"]
