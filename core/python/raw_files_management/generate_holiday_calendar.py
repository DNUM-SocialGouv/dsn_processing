import csv
import datetime as dt
import os

import pandas as pd
from jours_feries_france import JoursFeries


def generate_holidays_calendar() -> None:
    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"], "holidays_calendar.csv"
    )

    public_holidays = list()
    for year in range(2000, dt.date.today().year + 6, 1):
        res = JoursFeries.for_year(year)
        public_holidays += list(res.values())

    df = pd.DataFrame(
        {"holidays": [date.strftime("%Y-%m-%d") for date in public_holidays]}
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )


if __name__ == "__main__":
    generate_holidays_calendar()
