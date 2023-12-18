import datetime as dt
import functools
import inspect
import logging
import time
from typing import Any, Callable, Optional


class Base:
    """
    Database class.

    Parameters
    ----------
    verbose : bool
        If True print log messages.
    log_level : str
        Logging level for filter log messages.
        Available levels are "debug", "info", "warning", "error" or "critical".
    """

    CATEGORIES_JURIDIQUES_INSEE_FILE = "categories_juridiques_insee"
    NATURES_CONTRATS_FILE = "natures_contrats"
    MOTIFS_RECOURS_FILE = "motifs_recours"
    NAF_FILE = "naf"
    CONVENTIONS_COLLECTIVES_FILE = "conventions_collectives"
    ENTREPRISES_FILE = "champollion_etablissement"
    ETABLISSEMENTS_FILE = "champollion_etablissement"
    SALARIES_FILE = "champollion_individu"
    CONTRATS_FILE = "champollion_contrat"
    CHANGEMENTS_SALARIES_FILE = "champollion_individuchangement"
    CHANGEMENTS_CONTRATS_FILE = "champollion_contratchangement"
    FINS_CONTRATS_FILE = "champollion_contratfin"
    ACTIVITES_FILE = "champollion_activite"
    REMUNERATIONS_FILE = "champollion_remuneration"
    VERSEMENTS_FILE = "champollion_versement"
    EXPECTED_ENTREPRISES_FILE = "test_database_expected_data_entreprises"
    EXPECTED_ETABLISSEMENTS_FILE = "test_database_expected_data_etablissements"
    EXPECTED_SALARIES_FILE = "test_database_expected_data_salaries"
    EXPECTED_CONTRATS_FILE = "test_database_expected_data_contrats"
    EXPECTED_POSTES_FILE = "test_database_expected_data_postes"
    EXPECTED_ACTIVITES_FILE = "test_database_expected_data_activites"
    EXPECTED_CONTRATS_COMPARISONS_FILE = "test_database_expected_data_contrats"
    EXPECTED_DATA_FILE = "source_file_test_data"
    MOCK_ENTREPRISES_FILE = "mock_database_entreprises"
    MOCK_ETABLISSEMENTS_FILE = "mock_database_etablissements"
    MOCK_SALARIES_FILE = "mock_database_salaries"
    MOCK_CONTRATS_FILE = "mock_database_contrats"
    MOCK_POSTES_FILE = "mock_database_postes"
    MOCK_ACTIVITES_FILE = "mock_database_activites"
    MOCK_DATA_FILE = "source_file_mock_data"
    ARCHIVE_FILE = "champollion"

    DATA_RETENTION_PERIOD = 72  # in months

    def __init__(self, verbose: bool, log_level: str) -> None:
        assert log_level in [
            "debug",
            "info",
            "warning",
            "error",
            "critical",
        ], "log level is not available."
        assert isinstance(verbose, bool), "argument of wrong type (bool require)"
        assert isinstance(log_level, str), "argument of wrong type (str require)"

        self.verbose = verbose
        self.log_level = log_level

        if self.verbose:
            logging.basicConfig(
                format="[%(levelname)s][%(asctime)s][%(function)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M",
                force=True,
            )
            logging.root.setLevel(self.log_level.upper())
            self.__logger = logging.getLogger()

    @staticmethod
    def _timer(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Function timer decorator.

        Parameters
        ----------
        func : Callable[..., Any]
            Function to decorate.

        Returns
        -------
        wrapper : Callable[..., Any]
            Function wrapped.
        """
        assert isinstance(func, Callable), "argument of wrong type (callable require)"

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            t0 = time.time()
            result = func(self, *args, **kwargs)

            self._log(
                f"execution time: {round(time.time() - t0, 2)} s",
                level="info",
                function="timer",
            )

            return result

        return wrapper

    def _log(self, msg: str, level: str, function: Optional[str] = None) -> None:
        """
        Print log message.

        Parameters
        ----------
        msg : str
            Message to print.
        level : str
            Importance of a given log message.
            Available logging levels are "debug", "info", "warning", "error" or "critical".
        function : Optional[str]
            Display the source function of the log message, by default display the current function.
        """
        assert level in [
            "debug",
            "info",
            "warning",
            "error",
            "critical",
        ], "log level is not available."
        assert isinstance(msg, str), "argument of wrong type (str require)"
        assert isinstance(level, str), "argument of wrong type (str require)"

        function = inspect.stack()[1].function if function is None else function
        extra = {"function": function}

        if self.verbose:
            if level == "debug":
                self.__logger.debug(msg, extra=extra)
            elif level == "info":
                self.__logger.info(msg, extra=extra)
            elif level == "warning":
                self.__logger.warning(msg, extra=extra)
            elif level == "error":
                self.__logger.error(msg, extra=extra)
            else:
                self.__logger.critical(msg, extra=extra)
