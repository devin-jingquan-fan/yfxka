# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 15:32:06 2020

@author: ptmind
"""
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 15:59:37 2020

@author: Devin
"""


import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
from pandas.core.frame import DataFrame
import psycopg2


pysqldf = lambda q: sqldf(q, globals())



def select_pgsql (a) :
    #连接pgsql数据库
#    conn = psycopg2.connect(database='ptbi',user='jingquan.fan',password='i1uqAXdB2l',host='ptbipg.ptengine.jp',port='5432')
    conn = psycopg2.connect(database='ptbi',user='jingquan.fan',password='i1uqAXdB2l',host='172.19.4.21',port='5432')
#    conn = psycopg2.connect(database='ptbi',user='jpprod_devin_rw',password='s7HLND98ewh&',host='172.19.4.21',port='15432')
    cur = conn.cursor()
    cur.execute(a)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    a = DataFrame(rows)
    return a 
    



#分列后，选取日期
def columns_category (a):
    a= a.iloc[:,:1]
    return a  
    
#对原始数据进行切分，并将时间变为日期格式
def etl_message(df_info):
    #提取需要的数据
    main_info = df_info[['User ID', 'User Email','Message title',  'Message channel','Folder / campaign name','Message style / email template','Sent at', 'Opened at','Clicked at', 'Hit goal at', 'Replied / responded at','Unsubscribed at', 'Bounced at']]
    #合成新列名的新字典
    main_old_columns = main_info.columns.values.tolist()
    main_new_columns = ['uid','email','title', 'message_channel','campaign_name','style','Sent', 'Opened','Clicked', 'goal', 'Replied','Unsubscribed', 'Bounced']
    dictionary = dict(zip(main_old_columns, main_new_columns))
    
    main_info = main_info.rename(columns = dictionary )
    
    #切分时间字符串
    sent = pd.DataFrame(main_info['Sent'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Sent']  = sent[0]
    
    sent = pd.DataFrame(main_info['Opened'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Opened']  = sent[0]
    
    sent = pd.DataFrame(main_info['Clicked'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Clicked']  = sent[0]
    
    sent = pd.DataFrame(main_info['goal'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['goal']  = sent[0]
    
    sent = pd.DataFrame(main_info['Replied'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Replied']  = sent[0]
    
    sent = pd.DataFrame(main_info['Unsubscribed'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Unsubscribed']  = sent[0]
    
    sent = pd.DataFrame(main_info['Bounced'].str.split(' ',expand=True))
    sent = columns_category (sent)
    main_info['Bounced']  = sent[0]
    
#    将切分后的字符串变成时间格式
    main_info['goal'] = pd.to_datetime(main_info['goal'])
    main_info['Sent'] = pd.to_datetime(main_info['Sent'])
    main_info['Opened'] = pd.to_datetime(main_info['Opened'])
    main_info['Clicked'] = pd.to_datetime(main_info['Clicked'])
    main_info['Replied'] = pd.to_datetime(main_info['Replied'])
    main_info['Unsubscribed'] = pd.to_datetime(main_info['Unsubscribed'])
    main_info['Bounced'] = pd.to_datetime(main_info['Bounced'])
    
    return main_info
        
#根据邮件名称转换引导系列
def create_category(info):
    a = pd.DataFrame()
    a['title'] = info['title']
    
    a['title'].replace('1d no datain [No profile]','create_profile',inplace = True)
    
    
    info['category'] = a['title']

    return info

def goal_open_etl(info):
    a = info
    a = a.dropna(subset=['Opened'])
    a = a.groupby(['email','category','goal']).agg({'Sent':'max'})
    a = a.rename_axis(None, axis=1).reset_index()
    a['last_open_goal'] = a['goal']
    info = pd.merge(info,a,how = 'left',left_on =['email','category','goal','Sent'],right_on =['email','category','goal','Sent'])
    
    return info

def goal_click_etl(info):
    a = info
    a = a.dropna(subset=['Clicked'])
    a = a.groupby(['email','category','goal']).agg({'Sent':'max'})
    a = a.rename_axis(None, axis=1).reset_index()
    a['last_click_goal'] = a['goal']
    info = pd.merge(info,a,how = 'left',left_on =['email','category','goal','Sent'],right_on =['email','category','goal','Sent'])
    
    return info

def ETL_message(message_info):
    message_info = etl_message(message_info)
    message_info = create_category(message_info)
    message_info = goal_open_etl(message_info)
    message_info = goal_click_etl(message_info)
    return message_info


#---------------------------------------------------
#20.8.10 CallBack info
info1 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info1.csv',dtype = str)
info2 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info2.csv',dtype = str)
info3 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info3.csv',dtype = str)
info4 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info4.csv',dtype = str)
info5 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info5.csv',dtype = str)
info6 = pd.read_csv('C:/aaa/growth/8_31CallBack_part2/info6.csv',dtype = str)



info1_after_etl = ETL_message(info1)
info1_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info1.txt')
info2_after_etl = ETL_message(info2)
info2_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info2.txt')
info3_after_etl = ETL_message(info3)
info3_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info3.txt')
info4_after_etl = ETL_message(info4)
info4_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info4.txt')
info5_after_etl = ETL_message(info5)
info5_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info5.txt')
info6_after_etl = ETL_message(info6)
info6_after_etl.to_csv('C:/aaa/growth/8_31CallBack_part2/info6.txt')


all_info_sql ="""
select 
*
from info1_after_etl
union all 

select 
*
from info2_after_etl
union all 

select 
*
from info3_after_etl
union all 

select 
*
from info4_after_etl
union all 

select 
*
from info5_after_etl
union all 

select 
*
from info6_after_etl

"""
all_info = pysqldf(all_info_sql)

activate_cohort_info_sql = """
select
account_id, stat_week,source_type, reg_time, w0_active_day_cnt, w1_active_day_cnt, w2_active_day_cnt, w3_active_day_cnt, w4_active_day_cnt
from ptbi_app.app_ptmind_account_active_cohort_wi
where stat_week >= '2020-07-27'
and account_category = 1
and area = '0'

"""
activate_cohort_info = select_pgsql(activate_cohort_info_sql)
activate_cohort_info = activate_cohort_info.rename(columns = {0:'account_id',1:'stat_week',2:'source_type',3:'reg_time',4:'w0_active_day_cnt',5:'w1_active_day_cnt',6:'w2_active_day_cnt',7:'w3_active_day_cnt',8:'w4_active_day_cnt'})


#join cohort信息
join_sql = """

select 
a.*

,c.account_id
,c.stat_week
,c.source_type
,c.reg_time
,case when c.w0_active_day_cnt = 0 then null else c.w0_active_day_cnt end AS w0_active_day_cnt
,case when c.w1_active_day_cnt = 0 then null else c.w1_active_day_cnt end AS w1_active_day_cnt
,case when c.w2_active_day_cnt = 0 then null else c.w2_active_day_cnt end AS w2_active_day_cnt
,case when c.w3_active_day_cnt = 0 then null else c.w3_active_day_cnt end AS w3_active_day_cnt
,case when c.w4_active_day_cnt = 0 then null else c.w4_active_day_cnt end AS w4_active_day_cnt
from all_info a 
left join 
(
    select
    a.uid
    ,min(b.stat_week) as stat_week
    from all_info a 
    left join activate_cohort_info b 
    on a.uid = b.account_id
    where w0_active_day_cnt >0
    group by 1
) b 
on a.uid = b.uid
left join activate_cohort_info c 
on b.uid = c.account_id
    and b.stat_week = c.stat_week

"""

resault = pysqldf(join_sql)

resault.to_csv('C:/aaa/growth/8_31CallBack_part2/Callback_resault.txt')

resault.to_csv('C:/aaa/growth/8_31CallBack_part2/Callback_resault.csv')


















