#!/bin/bash

BASE_PATH=$(cd `dirname $0`; pwd)

PROVIENCES=$1
TAGS=$2
AUDIENCE_ID=$3
EMAIL=$4
AUDIENCE_TITLE=$5

# LOG_PATH='/data1/user/yuanjk/mix-report-api/drivers/audience/logs/create_mix_audience.log'
LOG_PATH=logs/audience/${AUDIENCE_ID}.log

# MIX_AUDIENCE_SCRIPT='/data1/user/yuanjk/mix-report-api/drivers/audience/create_mix_audience.py'
MIX_AUDIENCE_SCRIPT=${BASE_PATH}/create_mix_audience.py

echo 'Start to create mixdata audience...' >>${LOG_PATH}
echo 'Start time: '$(date '+%F %T')

if [ $# -ne 5 ];then
    echo 'Create mixdata audience failed' >>${LOG_PATH}
    echo 'Usage example: create_mix_audience.sh proviences tags audience_id email audience_title' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 1
fi



echo "python ${MIX_AUDIENCE_SCRIPT} ${PROVIENCES} ${TAGS} ${AUDIENCE_ID}" >>${LOG_PATH}
python ${MIX_AUDIENCE_SCRIPT} ${PROVIENCES} ${TAGS} ${AUDIENCE_ID} >>${LOG_PATH} 2>&1

#send_email
python ${BASE_PATH}/../common/send_email_util.py ${AUDIENCE_ID} ${AUDIENCE_TITLE} ${EMAIL} 'audience'

if [ $? -eq 0 ];then
    echo 'Create mixdata audience successfully' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 0
else
    echo 'Create mixdata audience failed' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 1
fi