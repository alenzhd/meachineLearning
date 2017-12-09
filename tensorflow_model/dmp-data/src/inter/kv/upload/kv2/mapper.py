#!/user/bin/env python
# -*-coding:utf-8 -*-
import sys,os,ConfigParser

reload(sys)
sys.setdefaultencoding('utf-8')

#current_path
current_path = os.path.dirname(os.path.abspath(__file__))
conf = ConfigParser.ConfigParser()
conf.read("kv.cfg")

sep1 = u""+conf.get('separator','sep1').strip().encode('utf-8')
sep2 = u""+conf.get('separator','sep2').strip().encode('utf-8')

sep1 = u'\u0005'
sep2 = u'\u0006'

max_len=10000000000
max_value_num=10000000
value_num=0 
values=[]

last_v_date=''
last_userid='mix'
#
if __name__ == '__main__':
    for lines in sys.stdin:
        try:
            #lines:tablename userid [type] content datatime[20141008|2014100818]
            lines=lines[:-1]
            words = lines.split('\t')
            tablename=words[0]
            userid = words[1]
            typeofvalue = conf.get(tablename,"type")
            if(typeofvalue=='none'):
                content = words[2]
                v_date = words[3]
                value = userid+sep1+content
                value_a = content
            else:
                type = conf.get("type",words[2])
                content = words[3]
                v_date = words[4]
                value = userid+sep1+type+sep2+content
                value_a = type+sep2+content
            value_len=len(value.encode('utf-8'))
            if(value_len > max_len):
                continue
            values.append(value_a)
            value_num = value_num+1 
            tmp = sep1.join(values)
            if(userid != last_userid and last_userid!=''):
                del values[len(values)-1]
                if(len(values)>0):
                        print last_userid+"\t"+sep1.join(values)+"\t"+last_v_date
                values=[]
                values.append(value_a)
                last_userid=userid
                last_v_date=v_date
                value_num=0
                continue
            #if(v_date != last_v_date and last_v_date!=''):#
            #    del values[len(values)-1]
            #    print last_userid+"\t"+sep1.join(values)+"\t"+last_v_date
            #    values=[]
            #    #values.append(userid)
            #    values.append(value_a)
            #    last_userid=userid
            #    last_v_date=v_date
            #    value_num=0
            #    continue
            if(len(tmp.encode('utf-8')) >= max_len):#
                del values[len(values)-1]
                print last_userid+"\t"+sep1.join(values)+"\t"+last_v_date
                values=[]
                #values.append(userid)
                values.append(value_a)
                last_userid=userid
                last_v_date=v_date
                value_num=0
                continue
            if(value_num==max_value_num):
                del values[len(values)-1]
                print last_userid+"\t"+sep1.join(values)+"\t"+last_v_date
                values=[]
                #values.append(userid)
                values.append(value_a)
                last_userid=userid
                last_v_date=v_date
                value_num=0
            last_userid=userid
            last_v_date=v_date
        except Exception as e:
            #print e 
            continue
    if(len(values)>0):
        print userid+"\t"+sep1.join(values)+"\t"+v_date
        values=[]
