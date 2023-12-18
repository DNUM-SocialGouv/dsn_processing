set -e

if [ $# -ne 3 ]; then
    echo "Usage: $0 <year> <month> <folder_type>"
    exit 1
fi

year="$1"
month="$2"
folder_type="$3"

bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/common/extract_archives.sh $year-$month-01 $folder_type

cleanup() {
  echo "Cleaning up before exiting..."
  bash ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/common/remove_extracted.sh $year-$month-01 $folder_type 
}

trap cleanup EXIT

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/integration_log_begin.sql -d $year-$month-01
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/remove_ctt.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/remove_stt.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/reindex_tables.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_etablissements.sql -cmf champollion_etablissement -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/set_dernier_mois_de_declaration_integre.sql -d $year-$month-01
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_entreprises.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_entreprises.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_etablissements.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_etablissements.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/modify_ouverture_etablissements.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_salaries.sql -cmf champollion_individu -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_salaries.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_salaries.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_contrats.sql -cmf champollion_contrat -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_postes.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_contrats.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_contrats.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_changements_salaries.sql -cmf champollion_individuchangement -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_changements_salaries.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/modify_changements_salaries.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_changements_contrats.sql -cmf champollion_contratchangement -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_changements_contrats.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/modify_changements_contrats.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_fins_contrats.sql -cmf champollion_contratfin -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_fins_contrats.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/modify_fins_contrats.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/modify_debuts_contrats.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_versements.sql -cmf champollion_versement -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_remunerations.sql -cmf champollion_remuneration -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/extract_activites.sql -cmf champollion_activite -d $year-$month-01 -f $folder_type
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/transform_activites.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/load_activites.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/remove_old_data.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/allocate_stt.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/allocate_ctt.sql

python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/monthly_sanity_checks.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s monthly_integration/integration_log_end.sql -d $year-$month-01