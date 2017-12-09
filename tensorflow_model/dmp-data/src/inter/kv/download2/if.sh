#!/bin/bash
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD


i=2
n=$((`date -d "$1" +%j|awk '{print int($0)}'`%$i))

if [[ $n == "1" ]];then
end_day=$((`date -d "$1 +$((i-1)) day" +%Y%m%d`))
start_day=$1
echo "start_day:"$start_day
#nohup sh ./cp_um.sh $1 > logs/cp_um_${1}.log &
echo "end_day:"$end_day
else
exit -1
fi


cd -
