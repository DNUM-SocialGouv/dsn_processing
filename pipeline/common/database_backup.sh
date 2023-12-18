#!/bin/bash

# Check if do_backup and db are given
if [ $# -ne 2 ]; then
    echo "Usage: $0 <do_backup> <db>"
    exit 1
fi

do_backup="$1"
db="$2"

# If do_backup is False or db != "champollion", skip
if [ "$do_backup" = "False" ] || [ "$db" != "champollion" ]; then
    echo "No backup."
    exit 0
fi

TIMESTAMP=$(date +"%Y%m%d%H%M%S")
POSTGRES_HOSTNAME=${POSTGRES_HOST:0:19}
BACKUP_DIRECTORY="/champollion/dumps/${POSTGRES_HOSTNAME}/backup_champollion_${TIMESTAMP}"
echo "Info: backup file ${BACKUP_FILE}"

cat <<EOF > ./dump.sh
#!/bin/bash

# check if ~/.pgpass file exists
if ! [[ -f ~/.pgpass ]]; then
    echo "Error: .pgpass file missing." && exit 1
fi
export PGPASSFILE=~/.pgpass 

# check if /champollion/dumps volume is mounted
if ! mountpoint -q /champollion/dumps; then
    echo "Error: /champollion/dumps is not mounted." && exit 1
fi

if ! [[ -d /champollion/dumps/${POSTGRES_HOSTNAME} ]]; then
    echo "Info: creation of /champollion/dumps/${POSTGRES_HOSTNAME} directory."
    mkdir /champollion/dumps/${POSTGRES_HOSTNAME}
fi

pg_dump -Fd -j 12 --dbname=champollion --host=localhost --username=${POSTGRES_USER} --port=${POSTGRES_PORT} --file=${BACKUP_DIRECTORY}

# Check if the dump was successful
if [ $? -eq 0 ]; then
     echo "Info: backup completed successfully."
else
    echo "Error: backup fail." && exit 1
fi

# Check the number of dumps
dump_count=\$(ls -1d /champollion/dumps/${POSTGRES_HOSTNAME}/backup_champollion_*[0-9] 2> /dev/null | wc -l) 
echo "Info: number of dumps: \$dump_count"

# Purge old dumps
if [ \$dump_count -gt 3 ]; then
    dump_to_delete=\$(ls -td /champollion/dumps/${POSTGRES_HOSTNAME}/backup_champollion_*[0-9] | tail -n +4)
    rm -rf \$dump_to_delete

    if [ $? -ne 0 ]; then
        echo "Error: purging old backups fail." && exit 1
    else
        echo "Info: the following dumps have been deleted:"
        echo "\$dump_to_delete"
    fi

else
    echo "Info: there are no more than 3 dumps to delete."
fi

rm ~/dump.sh
EOF

scp -o StrictHostKeyChecking=no -o PreferredAuthentications="publickey" ./dump.sh ${POSTGRES_HOST_USER}@${POSTGRES_HOST}:/exploit/${POSTGRES_HOST_USER}
if [ $? -ne 0 ]; then
    echo "Error: sending dump.sh file fail." && exit 1
fi

ssh -o StrictHostKeyChecking=no -o PreferredAuthentications="publickey" ${POSTGRES_HOST_USER}@${POSTGRES_HOST} bash /exploit/${POSTGRES_HOST_USER}/dump.sh
if [ $? -ne 0 ]; then
    echo "Error: executing dump.sh file fail." && exit 1
fi
rm ./dump.sh