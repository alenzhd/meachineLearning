run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD

yyyymmddhh=`date --date=" -1 day" +%Y%m%d`00

nohup sh ./run_bh.sh ${yyyymmddhh} 0 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 1 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 2 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 3 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 4 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 5 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 6 >logs/${yyyymmddhh} 2>&1 &
nohup sh ./run_bh.sh ${yyyymmddhh} 7 >logs/${yyyymmddhh} 2>&1 &

cd -
