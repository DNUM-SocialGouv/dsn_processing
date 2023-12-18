import functools
import gc
from typing import Optional

import pandas as pd

from dsn_processing._base import Base


class SQLConnector(Base):
    """
    Class to connect with a SQL database with psycopg2 for PostgreSQL databases.

    Parameters
    ----------
    verbose:bool, default=False
        Verbosity.
    log_level:str, default="info"
        Level of log trigger: "debug", "info", "warning", "error" or "critical".
    """

    def __init__(self, verbose: bool = False, log_level: str = "info"):
        super().__init__(verbose=verbose, log_level=log_level)
        self.type = type

    def open_connection(
        self,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
    ):
        """
        Open a SQL connection.

        Parameters
        ----------
        database : str, optional
            The database name
        user : str, optional
            User name used to authenticate.
        password : str, optional
            Password used to authenticate.
        host : str, optional
            Database host address (defaults to UNIX socket if not provided).
        port : str, optional
            Connection port number (defaults to 5432 if not provided).
        """

        # if a connexion is already open, close it
        if hasattr(self, "connection"):
            self.close_connection()

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.connection = self._get_postgres_connection()

        self._log("connection open", level="info")

    def _get_postgres_connection(self):
        global psycopg2
        import psycopg2

        connection = psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

        return connection

    def _check_connection(func):
        """
        Decorator to check if connection is open.
        """

        @functools.wraps(func)  # type: ignore
        def wrapper(self, *args, **kwargs):
            assert hasattr(self, "connection"), "there is no open connection."
            return func(self, *args, **kwargs)  # type: ignore

        return wrapper

    @_check_connection  # type: ignore
    def close_connection(self):
        """
        Close a SQL Server connection.
        """

        self.connection.close()

        del self.connection
        gc.collect()

        self._log("connection closed", level="info")

    @Base._timer
    @_check_connection  # type: ignore
    def execute_query(
        self,
        query: str,
        commit: bool = False,
        return_dataframe: bool = False,
    ):
        """
        Execute a SQL query.

        Parameters
        ----------
        query : str
            SQL query.
        commit : bool, optional
            Commit the query, by default False.
        return_dataframe : bool, optional
            Return data on a pandas.DataFrame object, by default False.

        Returns
        -------
        results : list or pd.DataFrame
            Query results.
        """

        with self.connection as connection:
            cursor = connection.cursor()
            cursor.execute(query)

            if commit:
                try:
                    self.connection.commit()
                except Exception as e:
                    self.connection.rollback()
                    self._log(e.__str__(), level="debug")

                cursor.close()
                return

            results = cursor.fetchall()
            results = [list(row) for row in results]

            if return_dataframe:
                columns = [row[0] for row in cursor.description]
                results = pd.DataFrame(results, columns=columns)

            cursor.close()
            return results
