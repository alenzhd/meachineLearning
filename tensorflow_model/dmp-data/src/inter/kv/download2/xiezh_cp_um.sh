yyyymmdd=$1
yyyymmdd_data=`date -d "$yyyymmdd +1 day" +%Y%m%d`

run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
#i=8
#n=$((`date -d "$1" +%j`%$i))
#
#if [[ $n == "7" ]];then
#
#exit 0
#
#else

hive -e "
use dmp_online;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp_um_m_ht partition(provider='lt',province='jiangsu',day_id='${yyyymmdd}',hour_id='${yyyymmdd}00',mflag)
select mix_m_uid,m_id,id2uid_score,uid2id_score,mflag from dmp_um_m_ht where day_id=${yyyymmdd_data};
"
#fi
cd -

exit 0
