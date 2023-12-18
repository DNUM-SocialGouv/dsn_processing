set -e
initial_postgres_db="$POSTGRES_DB"
export POSTGRES_DB="mock"
python ${DSN_PROCESSING_REPOSITORY_PATH}/core/python/raw_files_management/generate_mock_table_files.py
bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/dags/init_database.sh
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_entreprises.sql -csf mock_database_entreprises
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_etablissements.sql -csf mock_database_etablissements
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_salaries.sql -csf mock_database_salaries
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_postes.sql -csf mock_database_postes
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_contrats.sql -csf mock_database_contrats
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s mock_integration/load_mock_activites.sql -csf mock_database_activites
export POSTGRES_DB=$initial_postgres_db