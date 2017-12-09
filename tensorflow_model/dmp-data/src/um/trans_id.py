#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback
import hashlib

if __name__ == '__main__':
    #user_name的site字典
    siteDict=dict()
    siteDict["ali"]="7"
    siteDict["qq"]="4"
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            m_id = temp[1]
            if (temp[4]=='user_name'):
                siteName = m_id.split("_")[0]
                siteId = siteDict[siteName]
                tmp = 'sL$'+siteId+m_id[len(siteName):]
                tmp_md5 = hashlib.md5()
                tmp_md5.update(tmp)
                m_id=tmp_md5.hexdigest().upper()[8:24]+siteId
            print temp[0]+"\t"+m_id+"\t"+temp[2]+"\t"+temp[3]+"\t"+temp[4]+"\t"+temp[5]
        except Exception, e:
            continue
