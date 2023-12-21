#!/bin/bash

Help()
{
   # Display Help
   echo "Build and push docker images of the repository."
   echo
   echo "Syntax: bash build.sh [-h|e|p|i]"
   echo "options:"
   echo "h          display this help and exit"
   echo "e          env file"
   echo "p          optional, push images to Nexus"
   echo "i          optional, ignore image to build and push ('common', 'postgres' or 'redis')"
}

while getopts "he:pi:" flag
do
    case "${flag}" in
        h)  Help && exit 1;;
        e)  env=${OPTARG};;
        p)  push=${OPTARG:-"enable"};;
        i)  ignore+=(${OPTARG});;
    esac
done

# export environment variables
if [[ -z $env ]]; then
    echo "-e argument is required. Help:" && Help && exit 0
elif ! [[ -f $env ]]; then
    echo "file specified with -e argument does not exists. Help:" && Help && exit 0
else
    export $(grep -v "#" $env | xargs)
fi

# ignore images
if [ ${#ignore[@]} -ge 1 ]; then
    for image in "${ignore[@]}"; do
        if [[ $image ==  "common" ]]; then
            ignore_common=true
        elif [[ $image ==  "postgres" ]]; then
            ignore_postgres=true
        elif [[ $image ==  "redis" ]]; then
            ignore_redis=true
        else
            echo "unavailable images specified by -i argument." && Help && exit 0
        fi
    done
fi

# build and push
if [[ -z $ignore_common ]]; then
    docker build --rm --file ${HOME}/${AIRFLOW_COMMON_BUILD_CONTEXT}/pipeline/airflow/common/Dockerfile \
    --build-arg AIRFLOW_UID=${AIRFLOW_UID} \
    --build-arg CHAMPOLLION_GID=${CHAMPOLLION_GID} \
    --build-arg HTTP_PROXY=${HTTP_PROXY} \
    --tag ${NEXUS_CHAMPOLLION_URL}/airflow/common:${AIRFLOW_COMMON_IMAGE_TAG} \
    ${HOME}/${AIRFLOW_COMMON_BUILD_CONTEXT}

    if ! [[ -z $push ]]; then
        docker image push ${NEXUS_CHAMPOLLION_URL}/airflow/common:${AIRFLOW_COMMON_IMAGE_TAG}
    fi
fi

if [[ -z $ignore_postgres ]];then
    docker build --rm --file ${HOME}/${AIRFLOW_POSTGRES_BUILD_CONTEXT}/Dockerfile \
    --tag ${NEXUS_CHAMPOLLION_URL}/airflow/postgres:${AIRFLOW_POSTGRES_IMAGE_TAG} \
    ${HOME}/${AIRFLOW_POSTGRES_BUILD_CONTEXT}

    if ! [[ -z $push ]]; then
        docker image push ${NEXUS_CHAMPOLLION_URL}/airflow/postgres:${AIRFLOW_POSTGRES_IMAGE_TAG}
    fi
fi

if [[ -z $ignore_redis ]];then
    docker build --rm --file ${HOME}/${AIRFLOW_REDIS_BUILD_CONTEXT}/Dockerfile \
    --tag ${NEXUS_CHAMPOLLION_URL}/airflow/redis:${AIRFLOW_REDIS_IMAGE_TAG} \
    ${HOME}/${AIRFLOW_REDIS_BUILD_CONTEXT}

    if ! [[ -z $push ]]; then
        docker image push ${NEXUS_CHAMPOLLION_URL}/airflow/redis:${AIRFLOW_REDIS_IMAGE_TAG}
    fi
fi
