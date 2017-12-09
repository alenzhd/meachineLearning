# -*-coding:utf-8 -*-
'''
Created on 2017年5月4日

@author: Administrator
'''

import sys,chardet
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')


def getDbConn(host="10.1.3.3",user="mars_test",passwd="mars",db="mars", cht='utf8'):
# def getDbConn(host="10.1.3.3",user="mixreport",passwd="wCSt5XkX",db="mixreport", cht='utf8'):
    "获取数据库连接 "
    count = 0;
    while True:
        try:
            db_conn = MySQLdb.connect(host, user, passwd, db, charset='utf8')
            print '数据库已连接：', 'host='+host, 'user='+user, 'password='+passwd, 'db='+db
            return db_conn
        except Exception, e:
            count += 1
            print '第'+str(count)+'次尝试连接数据库失败'
            print e
            if count == 10:
                print '无法创建数据库连接'
                return 0

def closeDbConn(db):
    "提交更新并关闭数据库"
    try:
        db.commit()
        db.close()
        print '数据库已关闭'
    except:
        print '关闭数据库失败'
                
                
def updateTableValue(table, column1, value, column2, cond, host="10.1.3.3",user="mars",passwd="mars",db="mars_test", charset="utf-8"):
# def updateTableValue(table, column1, value, column2, cond, host="10.1.3.3",user="mixreport",passwd="wCSt5XkX",db="mixreport", charset="utf-8"):
    "更新指定表中数据"
    db_conn = getDbConn(host, user, passwd, db, charset)
#     sql = 'UPDATE '+table+' SET '+column1+'=\''+value+'\' WHERE '+column2+'=\''+cond+'\''
    sql = "UPDATE %s SET %s='%s' WHERE %s='%s'" % (table, column1, value.decode('utf-8').encode('utf-8'), column2, cond )
    print sql
    try:
        cursor = db_conn.cursor()
        # 执行SQL语句
        cursor.execute(sql)
        # 关闭数据库
        cursor.close
        closeDbConn(db_conn)
    except Exception, e:
        print 'MySQL更新失败：', sql
        print e

if __name__ == '__main__':
    args = sys.argv
    vr="中国文"
#     updateTableValue('audience', 'state', '100', 'audience_id', 'yuanjk@asiainfo-mixdata.com$user$20170425233233')
    updateTableValue('report', 'description', vr, 'report_id', '5_20170519181624')
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
