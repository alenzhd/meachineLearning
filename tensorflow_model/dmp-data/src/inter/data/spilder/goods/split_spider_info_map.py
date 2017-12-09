#!/usr/bin/python
# coding: utf-8

import sys
import os
import json
reload(sys)  
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    for line in sys.stdin:
        try:
            line=line.replace("\\t"," ").strip()
            col=json.loads(line)
            goods_id=str(col["itemCode"])
            site_id=str(col["mpWebsitId"])
            site_cate_id=str(col["mpClassCode"])
            if("mpClassName" in col):
                site_cate_name=col["mpClassName"]
            else:
                site_cate_name=''
            title=col["title"]
            price=col["price"]
            if("brandCode" in col):
                brand_code=col["brandCode"]
            else:
                brand_code=''
            if("brandName" in col):
                brand_name=col["brandName"]
            else:
                brand_name=''
            if("stdClassCode" in col):
                std_cate_id=str(col["stdClassCode"])
            else:
                std_cate_id=''
            if("stdClassName" in col):
                std_cate_name=col["stdClassName"]
            else:
                std_cate_name=''
            update_date=str(col["collectDate"])
            site=goods_id.split('-')[0]
            rs = goods_id+"\t"+site_id+"\t"+site_cate_id+"\t"+site_cate_name+"\t"+title+"\t"+price+"\t"+brand_code+"\t"+brand_name+"\t"+std_cate_id+"\t"+std_cate_name+"\t"+update_date+"\t"+site
            print rs
        except Exception, err:
            continue
