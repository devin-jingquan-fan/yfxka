# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 14:41:21 2020

@author: ptmind
"""
import psycopg2
import logging
import os
import pandas as pd
import json

from io import StringIO

# 数据库连接串
pgsql_db = 'bidb'
#pgsql_user = 'jpprod_devin_rw'
#pgsql_pass = 's7HLND98ewh&'
pgsql_user = 'jpprodscheduler_rw'
pgsql_pass = 'T0p$2Ga@T7QE'
pgsql_host = '172.19.4.21'
pgsql_port = '5432'



# 连接pgsql数据库
def running_pgsql (a) :
    conn = psycopg2.connect(database=pgsql_db, user=pgsql_user, password=pgsql_pass, host=pgsql_host, port=pgsql_port)
    cur = conn.cursor()
    cur.execute(a)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    a = pd.DataFrame(rows)
    return a 
    



#从数据库中取字段信息
field_detail_sql = """
select table_catalog, table_schema, table_name, column_name
from information_schema.columns
where  table_catalog ='bidb'
and table_schema in ('dm','app','exp','dim')
-- and table_schema in ('exp')
and table_name not like 'apppip%'
"""
field_detail = running_pgsql(field_detail_sql)
field_detail = field_detail.rename(columns = {0:'table_catalog',1:'table_schema',2:'table_name',3:'column_name'})

#从BIDB的临时表中取注释信息
comment_detail_sql = """
select
field_name, comment
from dim.tmp_filed_comment
"""
comment_detail = running_pgsql(comment_detail_sql)
comment_detail = comment_detail.rename(columns = {0:'field_name',1:'comment'})

#信息整合
new_info_detail = field_detail.merge(comment_detail,how = 'left',left_on = 'column_name',right_on = 'field_name' )
#new_info_detail.to_csv('C:/aaa/growth/support/new_info_detail.csv')

def grant_comment(info_df):
    res_df = pd.DataFrame(columns = ('table_schema','table_name','column_name','comment','type','error'))
    conn = psycopg2.connect(database=pgsql_db, user=pgsql_user, password=pgsql_pass, host=pgsql_host, port=pgsql_port)
    cur = conn.cursor()

    for row in info_df.itertuples():
        schedule = getattr(row,'table_schema')
        table_name = getattr(row,'table_name')
        field_name = getattr(row,'column_name')
        comment = getattr(row,'comment')
        str_comment = 'COMMENT ON COLUMN %s.%s.%s IS \'%s\';'%(schedule,table_name,field_name,comment)
#        print(str_comment)
        
        try:
            cur.execute(str_comment)
        except Exception as e:
            res_df = res_df.append({'table_schema':schedule,'table_name':table_name,'column_name':field_name,'comment':comment,'type':'failed','error':e}, ignore_index=True)
            print('注释失败')
        else:
            res_df = res_df.append({'table_schema':schedule,'table_name':table_name,'column_name':field_name,'comment':comment,'type':'succeed','error':''}, ignore_index=True)
            print('注释成功')
        print(str_comment)
            

    conn.commit()
    cur.close()
    conn.close()
    return res_df



res_data = grant_comment(new_info_detail)
#res_data.to_csv('C:/aaa/growth/support/res_data.csv') 




##v3目标表和表字段
target_table = 'system.sys_monitor_fields_comment_cover'
#target_columns = ('table_schema','table_name','column_name','comment','type','error')
#v3目标表数据清空sql
delete_v3_sql = """
truncate system.sys_monitor_fields_comment_cover;
"""
##临时文件存储区
#file_path = os.getcwd() 
##res_data.to_csv('C:/aaa/growth/support/res_data.csv') 


def insert_data_to_table(df,target_table,delete_v3_sql) :
    df.to_csv(os.getcwd()+'/df.csv',sep='~') 
    #数据库连接
    conn = psycopg2.connect(database=pgsql_db, user=pgsql_user, password=pgsql_pass, host=pgsql_host, port=pgsql_port)
    cur = conn.cursor()
    #首先清空原始数据
    cur.execute(delete_v3_sql)
    #copy数据到目标表
    file = open(os.getcwd()+'/df.csv', 'r',encoding='utf-8')
    file.readline()  
    cur.copy_expert("COPY %s FROM STDIN DELIMITERS '~' csv "%target_table,file)
    conn.commit()
    cur.close()
    conn.close()
    print(target_table + '数据插入成功')


insert_data_to_table(res_data,target_table,delete_v3_sql)






