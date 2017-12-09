#!/bin/sh
yyyymmdd=${1}
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

#参数检查
if [[ `expr length $yyyymmdd` -ne 8 ]]
then
		echo need parameter such as YYYYMMDD
		exit	3
fi
echo $PWD

cd $PWD

#清理48小时前的内容识别明细表
sh $PWD/clean_data.sh $yyyymmdd

cd -
