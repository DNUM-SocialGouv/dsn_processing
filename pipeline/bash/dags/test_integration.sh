#!/bin/bash
set -e
initial_postgres_db="$POSTGRES_DB"
export POSTGRES_DB="test"
export BASH_ORCHESTRATOR_VERBOSE=False
[ -z "$DSN_PROCESSING_REPOSITORY_PATH" ] && echo "Environment variable DSN_PROCESSING_REPOSITORY_PATH is not defined." && exit 0

echo "Info: generate test data files."
python ${DSN_PROCESSING_REPOSITORY_PATH}/core/python/raw_files_management/generate_test_data_files.py # add -i to re-generate input files as well

echo "Info: init test database and integrate test data."
bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/dags/init_database.sh
bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/dags/historical_integration.sh 2022-01-01 2022-09-01 test

echo "Info: integrate expected test data."
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/create_expected_tables.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_entreprises.sql -csf test_database_expected_data_entreprises
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_etablissements.sql -csf test_database_expected_data_etablissements
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_salaries.sql -csf test_database_expected_data_salaries
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_contrats.sql -csf test_database_expected_data_contrats
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_postes.sql -csf test_database_expected_data_postes
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_activites.sql -csf test_database_expected_data_activites
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s test_integration/load_expected_contrats_comparisons.sql -csf test_database_expected_data_contrats_comparisons

echo "Info: run tests."
pytest ${DSN_PROCESSING_REPOSITORY_PATH}/tests/tests.py