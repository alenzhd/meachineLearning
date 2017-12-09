#!/bin/bash

yyyymmdd=$1

cat ~/logs/${yyyymmdd}/jslt_data_download*|grep cnt:|awk -F 'cnt:' -vOFS='\t' '{sum+=$2} END {print "kv|kv_1|||"'$yyyymmdd'"|jiangsu|lt|mobile|click|mobile|day",sum}' > ~/data/mn/dmp_mn_kpi_kv_bd_lt_jiangsu_mobile_${yyyymmdd}.txt
