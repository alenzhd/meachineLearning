#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

dates=$1

#人群脚本，参数出了dates,其他为固定参数(为了能够兼容settings.py，实际上脚本中这几个参数是没有用的)
#人群一天只跑一次，一次跑所有的人群
python $basepath/dmp_srv_crowd_bd.py  'dxy' 'shanghai' 'adsl' $dates
