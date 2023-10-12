#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'  
YEL='\033[0;33m'  
NC='\033[0m' # No Color


source_namespace="yash"
destination_namespace="yash1"
project_name="g-pilotdata-gl"
host="localhost:9100"

# list the jobs to be migrated
jobs=("sample_select"
    "weekly_hello_test_2"
    "daily_hello_test_1")

jobs_successfully_migrated=()
jobs_failed_migration=()

for value in "${jobs[@]}"; do
    echo ""
    echo -e "${YEL}> optimus job change-namespace ${NC}${GREEN}$value ${NC} ${YEL} -o=$source_namespace -n=$destination_namespace -p=$project_name --host=$host ${NC}"
    OPTIMUS_INSECURE=true ./optimus job change-namespace $value -o=$source_namespace -n=$destination_namespace -p=$project_name --host=$host
    if [ $? -ne 0 ]; then
        jobs_failed_migration+=($value)
    else 
        jobs_successfully_migrated+=($value)
    fi
done
echo ""

if [ ${#jobs_successfully_migrated[@]} -ne 0 ]; then
    echo -e "${GREEN}[OK] Successfully migrated jobs from namespace $source_namespace to $destination_namespace ${NC}"
    for job_name in "${jobs_successfully_migrated[@]}"; do
        echo -e "${GREEN}    $job_name ${NC}"
    done
fi
echo ""
echo ""

if [ ${#jobs_failed_migration[@]} -ne 0 ]; then
    echo -e "${RED}[ERROR] failed migrating: ${NC}"
    for job_name in "${jobs_failed_migration[@]}"; do
        echo -e "${RED}    $job_name ${NC}"
    done
    echo ""
    echo -e "${YEL}[INFO] Please consider migrating these jobs manually. ${NC}"
    echo ""
    echo -e "${YEL}[INFO] Reverting migration for jobs that failed migration. ${NC}"
    
    for job_name in "${jobs_failed_migration[@]}"; do
        echo ""
        echo -e "${YEL}> optimus job change-namespace ${NC}${GREEN}$value ${NC} ${YEL} -n=$source_namespace -o=$destination_namespace -p=$project_name --host=$host ${NC}"
        OPTIMUS_INSECURE=true ./optimus job change-namespace $value -n=$source_namespace -o=$destination_namespace -p=$project_name --host=$host
    done
fi