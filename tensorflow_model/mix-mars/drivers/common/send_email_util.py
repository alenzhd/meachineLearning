#!/usr/bin/env python
#-*- coding: utf-8 -*-
import sys,time,os
import smtplib 
import email
from email.header import Header 
from email.mime.text import MIMEText 
from email.mime.image import MIMEImage 
from email.mime.multipart import MIMEMultipart
mailto_list=["hulx@asiainfo-mixdata.com"]
mail_host="10.1.2.19"  #设置服务器
mail_user="idc-report"    #用户名
mail_pass="xxx"   #口令 
mail_postfix="mixdata.cn"  #发件箱的后缀

#读入图片
def addimg(src,imgid): #文件路径、图片id
    fp = open(src, 'rb')  #打开文件     
    msgImage = MIMEImage(fp.read()) #读入 msgImage 中
    fp.close() #关闭文件
    msgImage.add_header('Content-ID', imgid) 
    return msgImage

#发送邮件
def send_mail(to_list,sub,content):  #to_list：收件人；sub：主题；content：邮件内容
    me =mail_user+"@"+mail_postfix    #这里的hello可以任意设置，收到信后，将按照设置显示
    msgroot = MIMEMultipart('related')
    msgroot['Subject'] = sub    #设置主题
    msgroot['From'] = Header(me,'utf-8')

    msg = MIMEText(content,_subtype='html',_charset='utf-8')    #创建一个实例，这里设置为html格式邮件 
    msgroot.attach(msg)
    msgroot.attach(addimg(os.path.dirname(os.path.abspath(__file__))+"/mixdata.jpg","mixdata_jpg")) #全文件路径，后者为ID 根据ID在HTML中插入的位置
    #content="结果请见附件 <br>"
    #msg = MIMEText(content,_subtype='html',_charset='utf-8')
    #msgroot.attach(msg)


    h = Header(me,'utf-8')  
    msgroot['To'] = ";".join(to_list)

    try:  
        s = smtplib.SMTP(mail_host)  
        #s.connect(mail_host)  #连接smtp服务器
        #s.login(mail_user,mail_pass)  #登陆服务器
        s.ehlo()
        #s.starttls() 
        s.sendmail(me, to_list, msgroot.as_string())  #发送邮件
        s.close()  
        return True  
    except Exception, e:  
        print str(e)  
        return False  

def audience_mail(audience_id, audience_name, email_address):
    mailto_list = []
    mailto_list.append(email_address)

    content='''
        <body>
          <p> 您的人群数据：%(audience_name)s 已生成 </p>
          <p> 请点击： </p>
          <a href="http://report.mixdata.com.cn/mix-report-web/#/app/mix/audience/detail?audience_id=%(audience_id)s">
          http://report.mixdata.com.cn/mix-report-web/#/app/mix/audience/detail?audience_id=%(audience_id)s </a> <br>
          <p> 查看您的人群数据 </p>
          <br>
          <br>
          <p> <strong> 北京亚信品联信息技术有限公司 </strong></p>
          <p> <strong> 北京市朝阳区乐成中心A座9层 </strong></p>
          <IMG src="cid:mixdata_jpg" width="300" height="300">
        </body>
    ''' % vars ()
    if send_mail(mailto_list,u"MixData人群数据生成完成",content):
        print "success"
    else:
        print "failed"

def report_mail(report_id, report_name, email_address):
    mailto_list = []
    mailto_list.append(email_address)

    content='''
        <body>
          <p> 您的分析报告：%(report_name)s 已生成 </p>
          <p> 请点击： </p>
          <a href="http://report.mixdata.com.cn/mix-report-web/#/app/mix/report/detail?report_id=%(report_id)s">
          http://report.mixdata.com.cn/mix-report-web/#/app/mix/report/detail?report_id=%(report_id)s </a> <br>
          <p> 查看您的分析报告 </p>
          <br>
          <br>
          <p> <strong> 北京亚信品联信息技术有限公司 </strong></p>
          <p> <strong> 北京市朝阳区乐成中心A座9层 </strong></p>
          <IMG src="cid:mixdata_jpg" width="300" height="300">
        </body>
    ''' % vars ()
    if send_mail(mailto_list,u"MixData分析报告生成完成",content):
        print "success"
    else:
        print "failed"

if __name__ == '__main__':
    pass
    #TEST
    id = sys.argv[1]
    name = sys.argv[2]
    email = sys.argv[3]
    type = sys.argv[4]

    if type == 'audience':
        audience_mail(id,name,email)
    elif type == 'report':
        report_mail(id,name,email)




