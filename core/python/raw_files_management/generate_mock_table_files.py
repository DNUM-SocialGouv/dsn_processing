import csv
import os

import pandas as pd

from dsn_processing._base import Base


def generate_mock_data_files() -> None:
    # read mock data excel
    file_name = Base.MOCK_DATA_FILE + ".xlsx"
    file_path = os.path.join(
        os.environ["DSN_PROCESSING_REPOSITORY_PATH"], "resources", file_name
    )

    df = pd.read_excel(file_path, sheet_name=None, dtype=str)

    # generate csv
    sheet_list = [
        "Output Entreprises",
        "Output Etablissements",
        "Output Salaries",
        "Output Contrats",
        "Output Postes",
        "Output Activites",
    ]

    for sheet in sheet_list:
        file_name = f"mock_database_{sheet.split()[1].lower()}.csv"
        file_path = os.path.join(os.environ["WORKFLOW_SOURCES_DATA_PATH"], file_name)

        # convert excel date format "%Y-%m-%d %H:%M:%S" to "%Y-%m-%d"
        df[sheet] = df[sheet].apply(
            lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S", errors="ignore")
        )

        # save csv
        df[sheet].to_csv(
            file_path,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )

        os.chmod(file_path, 0o777)


if __name__ == "__main__":
    generate_mock_data_files()
