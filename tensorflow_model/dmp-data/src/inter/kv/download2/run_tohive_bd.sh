#!/bin/sh

hour_id=${1}00
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
echo $hour_id

./tohive.sh $hour_id

./dyhive.sh $hour_id

nohup sh ./cp_um.sh $1 > logs/cp_um_${1}.log &

cd -

