hour=$1
end_hour=$2
v_x=$3

python ../../../../../src/inter/kv/upload/kv2/dmp_kv.py lt jiangsu dmp_upload_inter_tab_bh_1 $hour $end_hour $v_x

yyyymmddhh=`date --date "-1 hour" +"%Y%m%d%H"`
yyyymmdd=${yyyymmddhh:0:8}

hive -e "use dmp_yx;insert overwrite table dmp_upload_inter_tab_bh  partition(day_id='$yyyymmdd',hour_id='$yyyymmddhh',tab='new1_${hour}_${v_x}',province='jiangsu') select key,value from dmp_upload_inter_tab_bh_2 where hour_id='${hour}' and tab like 'dmp_upload_inter_tab_bh_1_${v_x}%';"

echo -e "${hour}_${v_x}\0004finish" >>$PWD/flag/flag.${hour}

sleep 7200
