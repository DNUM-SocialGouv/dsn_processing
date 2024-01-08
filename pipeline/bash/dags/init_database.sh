set -e
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s init_database/create_permanent_tables.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s init_database/create_integration_tables.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s init_database/create_trigger_logs.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s init_database/create_dag_status_functions.sql
bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/dags/update_database.sh