#!/bin/sh
hour_id=$1
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
java -jar dmp-download.jar $*
cd -

