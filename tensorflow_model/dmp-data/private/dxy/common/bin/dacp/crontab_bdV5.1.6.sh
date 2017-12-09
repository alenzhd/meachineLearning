#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)
yyyymmdd=`date --date="-1 day" +"%Y%m%d"`


logbasedir="/data11/dacp/vendoryx/logs"
logdir=$logbasedir/${yyyymmdd}
mkdir -p $logdir

#固网运行省份调度
#adsl_provinces=`cat $basepath/../../../../../conf/online/dxy/common/adsl/provinces`
#adsl_provinces=`hadoop fs -text /user/vendoryx/dim_adsl/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_ADSL_PROVINCES|awk '{print $3}'`
adsl_setting=`cat $basepath/../../../../../conf/online/dxy/common/adsl/setting.py|grep DIM_HOME|awk -F '=' '{print $2}'|tr -d "\'"`
adsl_provinces=`hadoop fs -text ${adsl_setting}/dim_adsl/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_ADSL_PROVINCES_BD|awk '{print $3}'`
arrs=$(echo $adsl_provinces|tr "#" " ")
for arr in $arrs
do 
province=${arr%%=*}
ni=${arr##*=}
n=${ni##*%}
i=${ni%%%*}
if [ $((`date --date="${yyyymmdd}" +%s`/3600/24%$n)) -eq $i ];then
#执行该省份
sh $basepath/run_bd.sh dxy $province adsl $yyyymmdd 2>&1 |tee $logdir/adsl_${province}_${yyyymmdd}.log & sleep 60 
fi
done

#移网运行省份调度
#mobile_provinces=`cat $basepath/../../../../../conf/online/dxy/common/mobile/provinces`
#mobile_provinces=`hadoop fs -text /user/vendoryx/dim_mobile/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_MOBILE_PROVINCES|awk '{print $3}'`
mobile_setting=`cat $basepath/../../../../../conf/online/dxy/common/mobile/setting.py|grep DIM_HOME|awk -F '=' '{print $2}'|tr -d "\'"`
mobile_provinces=`hadoop fs -text ${mobile_setting}/dim_mobile/dim_dmp_conf/dim_dmp_conf|grep DMP_CONF_MOBILE_PROVINCES_BD|awk '{print $3}'`
arrs=$(echo $mobile_provinces|tr "#" " ")
for arr in $arrs
do 
province=${arr%%=*}
ni=${arr##*=}
n=${ni##*%}
i=${ni%%%*}
if [ $((`date --date="${yyyymmdd}" +%s`/3600/24%$n)) -eq $i ];then
#执行该省份
sh $basepath/run_bd.sh dxy $province mobile $yyyymmdd 'IU' 2>&1 |tee $logdir/mobile_${province}_${yyyymmdd}.log & sleep 60 
fi
done
sh $basepath/run_bd.sh dxy beijing mobile $yyyymmdd 'S' 2>&1 |tee $logdir/mobile_idmapping_${yyyymmdd}.log &
wait 
exit 0