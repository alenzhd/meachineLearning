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
            if (brand_code!='' and brand_name!='' and brand_code!='0' and brand_name!='null' and brand_name!='NULL' and '其他' not in brand_name):
                if site == 'suning' and len(brand_code) > 5:
                    rs = brand_code[5:]+"\t"+brand_name+"\t"+site
                else:
                    rs = brand_code+"\t"+brand_name+"\t"+site
                print rs
        except Exception, err:
            continue

