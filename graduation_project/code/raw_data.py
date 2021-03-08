# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 23:29:14 2019

@author: Devin
"""

from sklearn.cluster import KMeans
from sklearn.externals import joblib
from sklearn import cluster

import numpy as np 
import stockstats as stkstat
import tushare as ts
import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
import time 

time_start = time.time()

pysqldf = lambda q: sqldf(q, globals())
meat = load_meat()
births = load_births() 

#tushare api调用
ts.set_token('ce252aca832765dc55242d4b12b5107480df87294f6d60a3029f58c1')
pro = ts.pro_api()

# 测试时间准确性
print (time.strftime("%Y-%m-%d", time.localtime())  )
now_date = time.strftime("%Y-%m-%d", time.localtime())

raw_data = pro.daily(trade_date='20190408')

#df = pro.daily(ts_code='000001.SZ', start_date='20190410', end_date='20190410')
#df = pro.query('daily', ts_code='000001.SZ', start_date='20180701', end_date='20180718')


#income_data 利润表
#  operate_profit	营业利润
income_raw_data = pro.income(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,ann_date,basic_eps,diluted_eps,operate_profit,total_profit,n_income,ebit,distable_profit')
#balancesheet 资产负债表
balancesheet_raw_data = pro.balancesheet(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,ann_date,  total_share,money_cap,notes_receiv,accounts_receiv,div_receiv,int_receiv,sett_rsrv,total_cur_assets,oth_assets,fix_assets,intan_assets,r_and_d,lt_borr,st_borr,loan_oth_bank,acct_payable,div_payable,estimated_liab,total_liab,acc_receivable,payables')
#cashflow 现金流量表
cashflow_raw_data = pro.cashflow(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,ann_date,    net_profit,finan_exp,free_cashflow,others,im_net_cashflow_oper_act,fa_fnc_leases')
#forecast 业绩预告表
forecast_raw_data = pro.forecast(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201', fields = 'ts_code,ann_date,    type,p_change_min,p_change_max')
#express 业绩快报
express_raw_data = pro.express(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201', fields = 'ts_code,ann_date,    yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,yoy_equity')
#fina_indicator 财务指标
fina_indicator_raw_data = pro.fina_indicator(ts_code = '600000.SH',start_date = '20190101',end_date = '20191201', fields = 'ts_code,ann_date,    current_ratio,quick_ratio,cash_ratio,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,profit_to_gr,gc_of_gr,roe,op_yoy,equity_yoy')

#get inome data
ts_coode_list = raw_data['ts_code']

# =============================================================================
# def get_income1(data):
#     a=pd.DataFrame()
#     for i in range(len(data)):
#         income_raw_data = pro.income(ts_code = data[i],start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,f_ann_date,basic_eps,diluted_eps,operate_profit,total_profit,n_income,ebit,distable_profit')
#         time.sleep(0.3)
#         a=a.append(income_raw_data,ignore_index=True)
#     return a
# =============================================================================

def get_income(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        income_raw_data = pro.income(ts_code = data[i],start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,report_type,ann_date,f_ann_date,basic_eps,diluted_eps,operate_profit,total_profit,n_income')
        time.sleep(0.31)
        a=a.append(income_raw_data,ignore_index=True)
    return a
#get balancesheet data
def get_balancesheet(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        balancesheet_raw_data = pro.balancesheet(ts_code = data[i],start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,ann_date,f_ann_date,  total_share,money_cap,notes_receiv,accounts_receiv,total_cur_assets,intan_assets,st_borr,total_liab')
        time.sleep(0.31)
        a=a.append(balancesheet_raw_data,ignore_index=True)
    return a
#get cashflow data
def get_cashflow(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        cashflow_raw_data = pro.cashflow(ts_code = data[i],start_date = '20190101',end_date = '20191201',report_type='1', fields = 'ts_code,comp_type,ann_date,f_ann_date,    net_profit,finan_exp,free_cashflow,im_net_cashflow_oper_act')
        time.sleep(0.31)
        a=a.append(cashflow_raw_data,ignore_index=True)
    return a
#get forecast data
def get_forecast(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        forecast_raw_data = pro.forecast(ts_code = data[i],start_date = '20180101', fields = 'ts_code,ann_date,    type,p_change_min,p_change_max')
        time.sleep(0.31)
        a=a.append(forecast_raw_data,ignore_index=True)
    return a
#get express data 
def get_express(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        express_raw_data = pro.express(ts_code = data[i],start_date = '2017 0101',end_date = '20191201', fields = 'ts_code,ann_date,end_date,    revenue,operate_profit,n_income,total_assets,diluted_roe')
        time.sleep(0.31)
        a=a.append(express_raw_data,ignore_index=True)
    return a
#get fina_indicator data
def get_fina_indicator(data):
    a=pd.DataFrame()
    for i in range(len(data)):
        fina_indicator_raw_data = pro.fina_indicator(ts_code = data[i],start_date = '20170101',end_date = '20191201', fields = 'ts_code,ann_date,end_date ,    current_ratio,quick_ratio,cash_ratio,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,profit_to_gr,gc_of_gr,roe,op_yoy,equity_yoy')
        time.sleep(0.31)
        a=a.append(fina_indicator_raw_data,ignore_index=True)
    return a

	

#print(get_income1(test))
#income_etl_raw  = get_income(ts_coode_list)
#income_etl_raw.to_csv('C:/Users/Devin/Desktop/graduation_project/data/income_etl_raw.csv')
#
#balancesheet_etl_raw  = get_balancesheet(ts_coode_list)
#balancesheet_etl_raw.to_csv('C:/Users/Devin/Desktop/graduation_project/data/balancesheet_etl_raw.csv')
#
#cashflow_etl_raw  = get_cashflow(ts_coode_list)
#cashflow_etl_raw.to_csv('C:/Users/Devin/Desktop/graduation_project/data/cashflow_etl_raw.csv')
#
#forecast_etl_raw  = get_forecast(ts_coode_list)
#forecast_etl_raw.to_csv('C:/Users/Devin/Desktop/graduation_project/data/forecast_etl_raw.csv')
#
#express_etl_raw  = get_express(ts_coode_list)
#express_etl_raw.to_csv('C:/Users/Devin/Desktop/gra duation_project/data/express_etl_raw.csv')
#
#fina_indicator_etl_raw  = get_fina_indicator(ts_coode_list)
#fina_indicator_etl_raw.to_csv('C:/Users/Devin/Desktop/graduation_project/data/fina_indicator_etl_raw.csv')



#get balancesheet data


# =============================================================================
# test = pro.forecast(ann_date='20190131',start_date = '20190101',end_date = '20191201', fields = 'ts_code,ann_date,    type,p_change_min,p_change_max')
# 
# 
# #将客户行为聚类
# 
# #data = np.random.rand(1000,6)
# data1 = test.iloc[:,3:]
# data_sql = """select case when p_change_min is null then 0 else cast( p_change_min as int) end,
# case when p_change_max is null then 0 else cast(p_change_max as int) end from data1"""
# data = pysqldf(data_sql)
# estimator=KMeans(n_clusters=10)
# res=estimator.fit_predict(data)
# lable_pred=estimator.labels_
# centroids=estimator.cluster_centers_
# inertia=estimator.inertia_
# #print res
# 
# def all_np(arr):
#     arr = np.array(arr)
#     key = np.unique(arr)
#     result = {}
#     for k in key:
#         mask = (arr == k)
#         arr_new = arr[mask]
#         v = arr_new.size
#         result[k] = v
#     return result
# 
# 
# print (all_np(lable_pred))
# print (centroids)
# print (inertia)
# 
# 
# =============================================================================


pct_chg_greater_5= raw_data[raw_data["pct_chg"]>5]
#print(df2)

#    if gegu["change"] >= '5':
#        a.append[gegu]
#    else : 
#        print(1)

time_end = time.time()
print (time_end - time_start)