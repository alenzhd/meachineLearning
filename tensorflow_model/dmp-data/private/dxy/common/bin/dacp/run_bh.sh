#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)
provider=$1
province=$2
net_type=$3
hour=$4
logbasedir=$5

#内容识别
sh $basepath/../../../../../src/ci/run.sh $provider $province $net_type $hour $hour
nohup python $basepath/../../../../../src/tools/logs/print_job_list.py dxy beijing mobile $logbasedir > /dev/null &

hadoop fs -rm -r .Trash/*

exit 0