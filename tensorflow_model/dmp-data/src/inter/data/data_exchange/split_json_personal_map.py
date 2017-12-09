#!/usr/bin/python
# coding: utf-8

import sys
import os
import json


if __name__ == '__main__':

    for line in sys.stdin:
        try:
            line=line.split('\t')
            key=line[0]
            keytype=line[1]
            if keytype=='1':
                keytype='imei'
            if keytype=='2':
                keytype='idfa'
            if keytype=='3': 
                keytype='aid'   
            createtime=line[3]
            ctype=line[4]
            if ctype=='cmc':
                provider='yd'
            if ctype=='cuc':
                provider='lt'
            if ctype=='ctc':
                provider='dx'
            province=line[5].replace('\r','').replace('\n','')
            keyvalue=line[2]
            newline=json.loads(keyvalue)
            for ln in newline:
                if (ln["typeid"]=='0'):
                    name= ln["name"]
                    value= ln["value"]
                    typeid=ln["typeid"]
                    rs=key+"\t"+name+"\t"+value+"\t"+createtime+"\t"+provider+"\t"+province+"\t"+typeid+"\t"+keytype
                    print rs
        except Exception, err:
            continue