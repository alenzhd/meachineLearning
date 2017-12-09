#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)
yyyymmddhh=`date --date="-2 hour" +"%Y%m%d%H"`
yyyymmdd=`date --date="-2 hour" +"%Y%m%d"`

logbasedir="/data11/dacp/vendoryx/logs"
logdir=$logbasedir/${yyyymmdd}
mkdir -p $logdir

#固网运行省份调度
#adsl_provinces=`cat $basepath/../../../../../conf/online/dxy/common/adsl/provinces`
#adsl_provinces=`hadoop fs -text /user/vendoryx/dim_adsl/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_ADSL_PROVINCES_BH|awk '{print $3}'`
adsl_setting=`cat $basepath/../../../../../conf/online/dxy/common/adsl/setting.py|grep DIM_HOME|awk -F '=' '{print $2}'|tr -d "\'"`
adsl_provinces=`hadoop fs -text ${adsl_setting}/dim_adsl/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_ADSL_PROVINCES_BH|awk '{print $3}'`
arrs=$(echo $adsl_provinces|tr "#" " ")
for arr in $arrs
do 
province=${arr%%=*}
ni=${arr##*=}
n=${ni##*%}
i=${ni%%%*}
if [ $((`date --date="${yyyymmdd}" +%s`/3600/24%$n)) -eq $i ];then
#执行该省份
sh $basepath/run_bh.sh dxy $province adsl $yyyymmddhh 2>&1 |tee $logdir/adsl_${province}_${yyyymmddhh}.log & sleep 60
fi
done

#移网运行省份调度
#mobile_provinces=`cat $basepath/../../../../../conf/online/dxy/common/mobile/provinces`
#mobile_provinces=`hadoop fs -text /user/vendoryx/dim_mobile/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_MOBILE_PROVINCES_BH|awk '{print $3}'`
mobile_setting=`cat $basepath/../../../../../conf/online/dxy/common/mobile/setting.py|grep DIM_HOME|awk -F '=' '{print $2}'|tr -d "\'"`
mobile_provinces=`hadoop fs -text ${mobile_setting}/dim_mobile/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_MOBILE_PROVINCES_BH|awk '{print $3}'`
arrs=$(echo $mobile_provinces|tr "#" " ")
for arr in $arrs
do 
province=${arr%%=*}
ni=${arr##*=}
n=${ni##*%}
i=${ni%%%*}
if [ $((`date --date="${yyyymmdd}" +%s`/3600/24%$n)) -eq $i ];then
#执行该省份
sh $basepath/run_bh.sh dxy $province mobile $yyyymmddhh $logbasedir 2>&1 |tee $logdir/mobile_${province}_${yyyymmddhh}.log & sleep 60
fi

done

wait 
exit 0