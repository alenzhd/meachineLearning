#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 16:27:58 2015

@author: zhangqn
"""

import sys,os,string
import traceback

"""
    计算两点之间距离
    @param _lat1 - start纬度
    @param _lon1 - start经度
    @param _lat2 - end纬度
    @param _lon2 - end经度
    @return km(四舍五入)
"""
def getDistance(_lat1,_lon1,_lat2,_lon2):
    import math
    lat1=(math.pi/180)*_lat1
    lat2=(math.pi/180)*_lat2
    lon1=(math.pi/180)*_lon1
    lon2=(math.pi/180)*_lon2
    # 地球半径
    R = 6378.1
    d = math.sin(lat1)*math.sin(lat2)+math.cos(lat1)*math.cos(lat2)*math.cos(lon2-lon1)
    if d<=1 and d>=-1:
        d = math.acos(d)*R
    else:
        d = 0.0
    return d
    
try:
    for line in sys.stdin:
        temp = line.strip().split("\t") # temp[0]为lac_ci，temp[1]为同一lac_ci的"经度,纬度,geohash"集合，以"|"分隔
        locs = temp[1].split('|')
        lon = []
        lat = []
        geohash = []
        geohash_dict = dict()           # geohash字典，为了查找众数用，字典的key为geohash编码的前X位，value为编码的前X位出现的次数
        if len(locs)==0:
            continue
        for loc in locs:
            tmp=loc.split(",")          # tmp[0]经度，tmp[1]纬度，tmp[2]geohash
            lon.append(float(tmp[0]))
            lat.append(float(tmp[1]))
            head=tmp[2][0:5]            # 取geohash编码的前3位作为标识
            geohash.append(head)        
            if head in geohash_dict:
                geohash_dict[head] = geohash_dict[head]+1
            else:
                geohash_dict[head] = 1
        geohash_dict_items = sorted(geohash_dict.items(), key=lambda geohash_dict:geohash_dict[1],reverse=True)
        geohash_mode = geohash_dict_items[0][0]     # 出现次数最多的geohash编码的前X位
        num = geohash_dict_items[0][1]              # 出现次数
        center=''                                   # 中心点坐标字符串
        center_lon,center_lat=0.0,0.0               # 中心点经纬度
        radius=0.0                                  # 最大半径
        for i in range(len(lon)):
            if geohash[i]!=geohash_mode:
                continue
            center_lon=center_lon+lon[i]
            center_lat=center_lat+lat[i]
        center_lon=center_lon/num
        center_lat=center_lat/num
        center=str(center_lon)+","+str(center_lat)
        if num!=1:
            for i in range(len(lon)):
                if geohash[i]!=geohash_mode:
                    continue
                radius=max(radius,getDistance(center_lat,center_lon,lat[i],lon[i]))
        print (temp[0]+"\t"+center+"\t"+str(radius)+"\t"+str(num))
except Exception, e:
    print traceback.format_exc()
    
