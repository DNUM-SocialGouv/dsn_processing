import argparse
import os
from datetime import datetime

from dsn_processing._sqlconnector import SQLConnector
from dsn_processing.pipeline.bash.utils import open_sql_script

# Script args
parser = argparse.ArgumentParser(description="Bash orchestrator")
parser.add_argument(
    "-s",
    "--script",
    type=str,
    help="Name of the SQL script to execute (<dag_name/script_name.sql>).",
    required=True,
)
parser.add_argument(
    "-cmf",
    "--copy_monthly_file",
    type=str,
    help="If importing SQL script, the name of the monthly csv file (<csv_name>).",
)
parser.add_argument(
    "-csf",
    "--copy_static_file",
    type=str,
    help="If importing SQL script, the name of the static csv file (<csv_name>).",
)
parser.add_argument(
    "-d", "--date", type=str, help="Declaration date (format : YYYY-MM-DD)."
)
parser.add_argument("-f", "--folder_type", type=str, help="Folder type (raw or test).")

args = parser.parse_args()

# Environment args
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
verbose = bool(os.environ["BASH_ORCHESTRATOR_VERBOSE"] == "True")
sql_path = "core/sql"

# Connection to the database
connector = SQLConnector(verbose=True, log_level="warning")
connector.open_connection(
    database=database, user=user, password=password, port=port, host=host
)

# Path
args.script = os.path.join(
    os.environ["DSN_PROCESSING_REPOSITORY_PATH"], sql_path, args.script
)
assert [args.copy_monthly_file, args.copy_static_file].count(None) >= 1
csv_path = (
    os.environ["WORKFLOW_SOURCES_DATA_PATH"]
    if args.copy_static_file
    else "/champollion"
) or ""
csv_type = (args.folder_type if args.copy_monthly_file else "") or ""
csv_folder = "champollion" if args.copy_monthly_file else ""
csv_name = args.copy_static_file or args.copy_monthly_file or ""
csv_date = args.date or ""

# Print
if verbose:
    now = datetime.now()
    print("When : ", now.strftime("%d/%m/%Y %H:%M:%S"))
    print("Script : ", args.script)
    if args.date:
        print("Declaration month : ", args.date)
    if args.copy_static_file or args.copy_monthly_file:
        print("File : ", csv_name)

# Write query
query = open_sql_script(
    file_path=args.script,
    csv_path=csv_path,
    csv_type=csv_type,
    csv_folder=csv_folder,
    csv_name=csv_name,
    csv_date=csv_date,
)

# Execute query
connector.execute_query(query, commit=True)
connector.close_connection()
