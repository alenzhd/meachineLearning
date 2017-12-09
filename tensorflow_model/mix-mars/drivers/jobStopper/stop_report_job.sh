#!/bin/bash

BASE_PATH=$(cd `dirname $0`; pwd)

AUDIENCE_ID=$1
FLAG=$2

LOG_PATH=logs/jobStopper/${AUDIENCE_ID}.log

MIX_AUDIENCE_SCRIPT=${BASE_PATH}/stop_report_job.py

echo 'Start to stop mixreport job...' >${LOG_PATH}
echo 'Start time: '$(date '+%F %T') >>${LOG_PATH}

if [ $# -ne 2 ];then
    echo 'Stop mixreport job failed' >>${LOG_PATH}
    echo 'Usage example: stop_report_job.sh audience_id flag' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 1
fi

echo "python ${MIX_AUDIENCE_SCRIPT} ${AUDIENCE_ID} ${FLAG}" >>${LOG_PATH}
python ${MIX_AUDIENCE_SCRIPT} ${AUDIENCE_ID} ${FLAG} >>${LOG_PATH} 2>&1

if [ $? -eq 0 ];then
    echo 'Stop mixreport job successfully' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 0
else
    echo 'Stop mixreport job failed' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 1
fi