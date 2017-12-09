#!/bin/sh
hour=$1
yyyymm=${1:0:6}
yyyymmdd=${1:0:8}
yyyymmddhh=${1}
hh=${1:8:2}
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

#参数检查
if [[  `expr length $hour` -ne 10 ]]
then
		echo need parameter such as YYYYMMDDHH
		exit	3
fi
echo $PWD

#内容识别
python $PWD/../../../../../src/ci/dmp_ci.py lt jiangsu mobile $yyyymmddhh $yyyymmddhh

#上传处理
nohup python $PWD/../../../../../src/inter/kv/upload/kv1/dmp_kv.py lt jiangsu mobile dmp_uc_tags_m_bh $yyyymmddhh $yyyymmddhh &
nohup python $PWD/../../../../../src/inter/kv/upload/kv1/dmp_kv.py lt jiangsu mobile dmp_uc_otags_m_bh $yyyymmddhh $yyyymmddhh &
nohup python $PWD/../../../../../src/inter/kv/upload/kv1/dmp_kv.py lt jiangsu mobile dmp_ci_user_m_bh $yyyymmddhh $yyyymmddhh &
nohup python $PWD/../../../../../src/inter/kv/upload/kv1/dmp_kv.py lt jiangsu mobile dmp_uc_deeptags_m_bh $yyyymmddhh $yyyymmddhh &
nohup python $PWD/../../../../../src/inter/kv/upload/kv/dmp_kv.py lt jiangsu mobile dmp_mn_kpi_bh $yyyymmddhh $yyyymmddhh &

num=`ps -ef|grep "dmp_kv.py lt jiangsu .* $yyyymmddhh $yyyymmddhh"|grep -v grep|wc -l`

while [ $num -gt 0 ]
do
echo "kv-running:$num"
echo "sleep 10 s"
sleep 10 
num=`ps -ef|grep "dmp_kv.py lt jiangsu.* $yyyymmddhh $yyyymmddhh"|grep -v grep|wc -l`
done

#Combine User Tags
sh $PWD/trans_kv_bh $yyyymmddhh 0

sh $PWD/clean_kv.sh 
sh $PWD/mv_to_kv_bh.sh $yyyymmddhh
sh $PWD/kv_finish_bh.sh $yyyymmddhh

nohup sh $PWD/clean_data.sh $yyyymmdd &
