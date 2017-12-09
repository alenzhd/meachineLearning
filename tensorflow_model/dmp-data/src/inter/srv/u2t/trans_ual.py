#!/usr/bin/python
# coding: utf-8
import sys
import os
import urllib2


def http_get(key, value):
    #online
    url = 'http://10.1.3.127:18090/behavior/save?key=' +key + '&value=' + value + '&version=1.2'
    #test
    #url = 'http://10.1.3.91:18090/behavior/save?key=' +key + '&value=' + value + '&version=1.2'
    response = urllib2.urlopen(url)   
    response.read()
    response.colse()                 


if __name__ == '__main__':

    last_key=''
    value=''
    values=[]
    flag=''
    last_flag=''
    for lines in sys.stdin:
        try:
            #input:flag,id,day_id,type_id,tags,weight
            line = lines.strip().split("\t")
            key=line[0]+'_'+line[1]+'_1_1_'+line[2]
            value=line[3]+','+line[4]+','+line[5]
            values.append(value)
            flag=line[0]
            if (key!=last_key and last_key!=''):
                del values[len(values)-1]
                v = '|'.join(values)
                #http_get(last_key,v)
                print last_key+'\t'+v+'\t'+last_flag
                values=[]
                value=line[3]+','+line[4]+','+line[5]
                values.append(value)
            last_key=key
            last_flag=flag
        except Exception,e:
            continue
    if(len(values)>0):
        v = '|'.join(values)
        #http_get(last_key,v)
        print last_key+'\t'+v+"\t"+last_flag
        values=[]
