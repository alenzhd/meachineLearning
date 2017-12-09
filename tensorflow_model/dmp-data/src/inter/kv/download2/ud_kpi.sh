#!/bin/bash

hive -e "use dmp_online;select kpi,value from dmp_mn_kpi_bd where day_id=$1"> ~/data/tmp/dmp_mn_ud_kpi_bh_lt_jiangsu_mobile_${1}.txt
cp ~/data/tmp/dmp_mn_ud_kpi_bh_lt_jiangsu_mobile_${1}.txt ~/data/mn/
