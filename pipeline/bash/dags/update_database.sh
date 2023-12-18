set -e
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_naf.sql -csf naf
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_conventions_collectives.sql -csf conventions_collectives
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_categories_juridiques_insee.sql -csf categories_juridiques_insee
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_motifs_recours.sql -csf motifs_recours
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_natures_contrats.sql -csf natures_contrats
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_calendar.sql
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_holidays.sql -csf holidays_calendar
python ${DSN_PROCESSING_REPOSITORY_PATH}/pipeline/bash/orchestrator.py -s update_database/update_zonage.sql -csf zonage