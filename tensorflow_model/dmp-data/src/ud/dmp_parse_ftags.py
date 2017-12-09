#!/usr/bin/env python
# -*-coding:utf-8 -*-
##功能：解析深度标签，例如 f,v39:577:80496:505105742，保留原始数据，另外解析出三条数据
import sys,os,string
if __name__ == '__main__':
    #输入：mix_uid, type_id, tags, weight, hour_list, user_type
    #输出：mix_uid, type_id, tags, weight, hour_list, user_type
    # lines = ['T2V8U083X31RWV4UZ72149315DYB53TQ\tf\tv2:2850:500045024:\t1.0\t000000000100000000000000\tmobile']
    # for line in lines:
    handelWay1 = ["v2", "v24", "v32", "v38","v39","v37"]
    handelWay2 = ["v28", "v29"]
    for line in sys.stdin:
        try:
            mix_uid, type_id, tags, weight, hour_list, user_type = line.strip().split('\t')
            tagArr = tags.split(':');
            valueTypeId = tagArr[0]; siteId = tagArr[1]; contId1 = tagArr[2]; contId2 = tagArr[3];
            if(valueTypeId in handelWay1):
                if(siteId != ''):
                    print mix_uid + '\t' + 'b' + '\t' + siteId + '\t' + weight + '\t' + hour_list + '\t' + user_type
                if (contId1 != ''):
                    print mix_uid + '\t' + '0' + '\t' + contId1 + '\t' + weight + '\t' + hour_list + '\t' + user_type
                if(contId2 != ''):
                    print mix_uid + '\t' + '0' + '\t' + contId2 + '\t' + weight + '\t' + hour_list + '\t' + user_type
            elif (valueTypeId in handelWay2):
                if (siteId != ''):
                    print mix_uid + '\t' + 'b' + '\t' + siteId + '\t' + weight + '\t' + hour_list + '\t' + user_type
                if (contId1 != ''):
                    print mix_uid + '\t' + '0' + '\t' + contId1 + '\t' + weight + '\t' + hour_list + '\t' + user_type
            else:
                if (siteId != ''):
                    print mix_uid + '\t' + 'b' + '\t' + siteId + '\t' + weight + '\t' + hour_list + '\t' + user_type
                if (contId1 != ''):
                    print mix_uid + '\t' + '0' + '\t' + contId1 + '\t' + weight + '\t' + hour_list + '\t' + user_type
        except Exception, e:
            print e