#!/bin/bash

BASE_PATH=$(cd `dirname $0`; pwd)

AUDIENCE_ID=$1
FILE_NAME=$2
EMAIL=$3
AUDIENCE_TITLE=$4

# LOG_PATH='/data1/user/yuanjk/mix-report-api/drivers/audience/logs/create_user_audience.log'
LOG_PATH=logs/audience/${AUDIENCE_ID}.log

# USER_AUDIENCE_SCRIPT='/data1/user/yuanjk/mix-report-api/drivers/audience/create_user_audience.py'
USER_AUDIENCE_SCRIPT=${BASE_PATH}/create_user_audience.py


echo 'Start to create user audience...' >>${LOG_PATH}
echo 'Start time: '$(date '+%F %T') >>${LOG_PATH}

if [ $# -ne 4 ];then
    echo 'Create user audience failed' >>${LOG_PATH}
    echo 'Usage example: create_user_audience.sh audienceId fileName email audience_title' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T')
    exit 1
fi


echo "python ${USER_AUDIENCE_SCRIPT} ${AUDIENCE_ID} ${FILE_NAME} ${EMAIL}" >>${LOG_PATH}
python ${USER_AUDIENCE_SCRIPT} ${AUDIENCE_ID} ${FILE_NAME} ${EMAIL} >>${LOG_PATH} 2>&1

#send_email
python ${BASE_PATH}/../common/send_email_util.py ${AUDIENCE_ID} ${AUDIENCE_TITLE} ${EMAIL} 'audience'

if [ $? -eq 0 ];then
    echo 'Create user audience successfully' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 0
else
    echo 'Create user audience failed' >>${LOG_PATH}
    echo 'End time: '$(date '+%F %T') >>${LOG_PATH}
    exit 1
fi

