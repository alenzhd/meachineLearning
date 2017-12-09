#!/bin/sh
basepath=$(cd `dirname $0`; pwd)


PROVIENCES=$1

AUDIENCE_IDS=$2

REPORT_ID=$3

EMAIL=$4

REPORT_TITLE=$5

python $basepath/create_report.py $PROVIENCES $AUDIENCE_IDS $REPORT_ID > logs/report/${REPORT_ID}.log 2>&1

#send_email
python ${basepath}/../common/send_email_util.py ${REPORT_ID} ${REPORT_TITLE} ${EMAIL} 'report'
