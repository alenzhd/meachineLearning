#!/bin/bash
#sleep 4h

day=`date +%Y%m%d`
echo $day

file=result_${0:0:19}.`date +%Y%m%d%H%M%S`

echo $file 
#result_bank.sh.20161028113734
#logfile=${file}.log
 
# get stdtags_dt last partitions
logfile=${file}.log
confile=${file}.cond
tab=dmp_ud_user_profile_dt
hadoop fs -du /user/hive/warehouse/dmp_online.db/${tab}/*dxy/*/*mobile |awk '{print $2}'>partitions
cat partitions | awk -F '/' '{print $7 "/" $8 "/" $9}' | sort -u > groups
for group in `cat groups`
do
partition=`cat partitions| grep "${group}"| tail -1`
echo ${partition} >> ${confile}
done
sed -i "s/^.\+${tab}./(/g" ${confile}
sed -i "s/=/=\'/g" ${confile}
sed -i "s/\//\' and /g" ${confile}
sed -i "s/$/') or /g" ${confile}
sed -i '1i (' ${confile}
sed -i '$a 1=2)' ${confile}

partitions_days_user_profile_dt=$(cat ${confile})
echo $partitions_days_user_profile_dt

hivesql="use dmp_online;
set hive.metastore.client.socket.timeout=2000;
set mapreduce.map.memory.mb=2048;set mapreduce.reduce.memory.mb=5120;
from(
    select mix_uid, type_id, tags
    from(
        select mix_uid, type_id, tags
        from dmp_ud_user_stdtags_dt
        where user_type = 'mobile' and type_id in ('b','c','d') and $partitions_days_user_profile_dt
        group by mix_uid, type_id, tags

        union all

        select t1.mix_uid, t1.type_id, t2.fa_cont_id as tags
        from(
            select mix_uid, profile_type as type_id, tags
            from dmp_ud_user_profile_dt
            where user_type = 'mobile' and profile_type = 'cont' and $partitions_days_user_profile_dt and tags like '8%'
            group by mix_uid, profile_type, tags
        )t1
        join(
            select cont_id, fa_cont_id
            from dmp_console.dim_cont
            lateral view explode(split(cont_id_path,'/')) adtable as fa_cont_id
            where state = '1' and fa_cont_id != '80005' and fa_cont_id != '80371'
        )t2
        on (t1.tags = t2.cont_id)

        union all

        select mix_uid, profile_type as type_id, tags
        from dmp_ud_user_profile_dt
        where user_type = 'mobile' and profile_type = 'dev' and $partitions_days_user_profile_dt
        group by mix_uid, profile_type, tags

        union all

        select mix_uid, profile_type as type_id, tags
        from dmp_ud_user_profile_dt
        where user_type = 'mobile' and profile_type = 'dg_gender' and $partitions_days_user_profile_dt
        group by mix_uid, profile_type, tags

        union all

        select mix_uid, profile_type as type_id, tags
        from dmp_ud_user_profile_dt
        where user_type = 'mobile' and profile_type = 'dg_age' and $partitions_days_user_profile_dt
        group by mix_uid, profile_type, tags
      
        union all

        select mix_uid, 'pubnum' as type_id, relobj_id as tags
        from dmp_um_user_relobj_dt
        where user_type = 'mobile' and relobj_type = 'sladuid_pubnum' and $partitions_days_user_profile_dt
        group by mix_uid, relobj_id

        union all

        select mix_uid, 'province' as type_id, province as tags
        from dmp_um_user_dt
        where user_type = 'mobile' and $partitions_days_user_profile_dt
        group by mix_uid, province
    )a
)b

insert overwrite table mixreport.dim_tag_count
partition (type = 'dev')
select tags, count(1)
where type_id = 'dev'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'site')
select tags, count(1)
where type_id = 'b'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'app')
select tags, count(1)
where type_id = 'c'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'kw')
select tags, count(1)
where type_id = 'd'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'age')
select tags, count(1)
where type_id = 'dg_age'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'gender')
select tags, count(1)
where type_id = 'dg_gender'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'cont')
select tags, count(1)
where type_id = 'cont'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'pubnum')
select tags, count(1)
where type_id = 'pubnum'
group by tags

insert overwrite table mixreport.dim_tag_count
partition (type = 'province')
select tags, count(1)
where type_id = 'province'
group by tags
"

echo "${hivesql}"
i=0

while [ ${i} -lt 600 ]
do
start_time=`/bin/date +%s`
hive -e "${hivesql}"
#sleep 10s
end_time=`/bin/date +%s`
i=`expr ${end_time} - ${start_time}`
if [ ${i} -lt 600 ]
then
echo 'repeat'
fi
done
