# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 10:48:05 2017

@author: Yixiang Zhang
"""

import sshtunnel,pymysql,time
import pandas as pd

import yorencorrespanallawson_db.yorencorrespanallawson_db


#输入数据库的用户名和密码
username = 'shlawson'
password = 'Lawson2017'


server = sshtunnel.SSHTunnelForwarder(('115.29.237.50', 22),ssh_password="3vkxgKqX7D",ssh_username="ydoor",remote_bind_address=('rdsnfjbfyfquanq.mysql.rds.aliyuncs.com', 3306))
server.start()
connection = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user=username, passwd=password, db='lawson_db', charset='utf8')
cursor = connection.cursor()
print(time.ctime(),'连接成功，开始查询')


df1=yorencorrespanallawson_db.yorencorrespanallawson_db.commodity_sell_report(large_name='米饭',begindate='20170701',enddate='20170702',region_block_code='sh-lawson',cursor=cursor)
df3=yorencorrespanallawson_db.yorencorrespanallawson_db.record_for_CA(large_name='米饭',begindate='20170701',enddate='20170702',region_block_code='sh-lawson',cursor=cursor)


results=pd.merge(df3,df1,on='commodity_cd',how='left')
results=results[['SKU(name)','gender&age']]

plotdata=yorencorrespanallawson_db.yorencorrespanallawson_db.CA(results)


connection.close()
