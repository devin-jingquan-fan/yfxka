# -*- coding: utf-8 -*-
"""
Created on Tue May 14 00:24:58 2019

@author: Devin
"""
from sklearn.cluster import KMeans
from sklearn.externals import joblib
from sklearn import cluster
from sklearn.cluster import AgglomerativeClustering

import scipy
from scipy.cluster.vq import vq,kmeans,whiten
import scipy.cluster.hierarchy as sch

import numpy as np 
import stockstats as stkstat
import tushare as ts
import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
import time 

time_start = time.time()
now_date = time.strftime("%Y-%m-%d", time.localtime())

pysqldf = lambda q: sqldf(q, globals())

ts.set_token('ce252aca832765dc55242d4b12b5107480df87294f6d60a3029f58c1')
pro = ts.pro_api()
raw_data_list = pro.daily(trade_date='20190408')
ts_code_list = raw_data_list['ts_code']
#ts_code_list = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/ts_code_list.csv',index_col=0,header = 0,encoding='utf-8')

def all_np(arr):
    arr = np.array(arr)
    key = np.unique(arr)
    result = {}
    for k in key:
        mask = (arr == k)
        arr_new = arr[mask]
        v = arr_new.size
        result[k] = v
    return result

# =============================================================================
# #tushare api调用
# ts.set_token('ce252aca832765dc55242d4b12b5107480df87294f6d60a3029f58c1')
# pro = ts.pro_api()
# =============================================================================



income_etl_raw  = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/income_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')

balancesheet_etl_raw  = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/balancesheet_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')
balancesheet_etl_raw  = balancesheet_etl_raw.dropna(axis=0, how='any')
#
cashflow_etl_raw  = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/cashflow_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')
cashflow_etl_raw  = cashflow_etl_raw.dropna(axis=0, how='any')
#
forecast_etl_raw = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/forecast_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')
forecast_etl_raw  = forecast_etl_raw.dropna(axis=0, how='any')
#
express_etl_raw  = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/express_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')
express_etl_raw  = express_etl_raw.dropna(axis=0, how='any')
#
fina_indicator_etl_raw = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/fina_indicator_etl_raw.csv',index_col=0,header = 0,encoding='utf-8')
fina_indicator_etl_raw  = fina_indicator_etl_raw.dropna(axis=0, how='any')


income_etl_sql  = """
select t2.*
from 
(
    select b.ts_code,b.f_ann_date,max(b.n_income) n_income
    from 
        (
        select ts_code,max(f_ann_date) f_ann_date
        from income_etl_raw
        group by ts_code
        )a 
    join income_etl_raw b 
    	on a.ts_code = b.ts_code and a.f_ann_date = b.f_ann_date
	group by b.ts_code,b.f_ann_date
) t1
join income_etl_raw t2
	on t1.ts_code = t2.ts_code and t1.f_ann_date = t2.f_ann_date
		and t1.n_income =t2.n_income
where t2.operate_profit != ''

group by 1,2,3,4,5,6,7,8,9
"""
balancesheet_etl_sql = """
select c.*
from 
(
select ts_code,max(f_ann_date) f_ann_date
from balancesheet_etl_raw 
group by ts_code) a
join (
select ts_code,max(total_liab) total_liab
from balancesheet_etl_raw 
group by ts_code) b  on a.ts_code = b.ts_code
join balancesheet_etl_raw c 
on a.ts_code = c.ts_code and a.f_ann_date = c.f_ann_date and b.total_liab = c.total_liab
group by 1,2,3,4,5,6,7,8,9,10,11,12
"""
cashflow_etl_sql = """
select c.*
from 
(
select ts_code,max(f_ann_date) f_ann_date
from cashflow_etl_raw 
group by ts_code) a
join (
select ts_code,max(free_cashflow) free_cashflow
from cashflow_etl_raw 
group by ts_code) b  on a.ts_code = b.ts_code
join cashflow_etl_raw c 
on a.ts_code = c.ts_code and a.f_ann_date = c.f_ann_date and b.free_cashflow = c.free_cashflow
group by 1,2,3,4,5,6,7,8"""
forecast_etl_sql = """
select b.ts_code,b.ann_date,
(case when b.type = '续盈' then 400 
	  when b.type = '略增' then 300 
	  when b.type = '预增' then 200 
      when b.type = '扭亏' then 100
      when b.type = '不确定' then 0 
      when b.type = '首亏' then -100 
	  when b.type = '略减' then -200
	  when b.type = '预减' then -300 
	  when b.type = '续亏' then -400 
	  else 0 end) type,b.p_change_min,b.p_change_max
from 
(
select ts_code,max(ann_date) ann_date
from forecast_etl_raw 
group by ts_code
) a join forecast_etl_raw b
on a.ts_code = b.ts_code and a.ann_date = b.ann_date
"""
express_etl_sql ="""
select b.*
from
(
select ts_code,max(ann_date) ann_date
from express_etl_raw
group by ts_code)a
join express_etl_raw b
on a.ts_code = b.ts_code and a.ann_date = b.ann_date
"""
fina_indicator_etl_sql ="""
select b.*
from
(
select ts_code,max(end_date) end_date
from fina_indicator_etl_raw
group by ts_code)a
join fina_indicator_etl_raw b
on a.ts_code = b.ts_code and a.end_date = b.end_date
"""
income_data = pysqldf(income_etl_sql)
balancesheet_data = pysqldf(balancesheet_etl_sql)
cashflow_data = pysqldf(cashflow_etl_sql)
forecast_data = pysqldf(forecast_etl_sql)
express_data = pysqldf(express_etl_sql)
fina_indicator_data = pysqldf(fina_indicator_etl_sql)

income_data_k = income_data.iloc[:,4:]
income_data_k = income_data_k.fillna(0)
balancesheet_data_k = balancesheet_data.iloc[:,4:]
cashflow_data_k = cashflow_data.iloc[:,4:]
forecast_data_k = forecast_data.iloc[:,2:]
express_data_k = express_data.iloc[:,3:]
fina_indicator_data_k = fina_indicator_data.iloc[:,3:]

#income_data.to_csv('C:/Users/Devin/Desktop/graduation_project/data/income_data.csv')

# #data = np.random.rand(1000,6)
# data1 = test.iloc[:,3:]
# data_sql = """select case when p_change_min is null then 0 else cast( p_change_min as int) end,
# case when p_change_max is null then 0 else cast(p_change_max as int) end from data1"""
# data = pysqldf(data_sql)

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#生成点与点之间的距离矩阵,这里用的欧氏距离:
income_disMat = sch.distance.pdist(income_data_k,'euclidean') 
balancesheet_disMat = sch.distance.pdist(balancesheet_data_k,'euclidean') 
cashflow_disMat = sch.distance.pdist(cashflow_data_k,'euclidean') 
forecast_disMat = sch.distance.pdist(forecast_data_k,'euclidean') 
express_disMat = sch.distance.pdist(express_data_k,'euclidean')
fina_indicator_disMat = sch.distance.pdist(fina_indicator_data_k,'euclidean')
#进行层次聚类:
income_Z=sch.linkage(income_disMat,method='average') 
balancesheet_Z=sch.linkage(balancesheet_disMat,method='average') 
cashflow_Z=sch.linkage(cashflow_disMat,method='average') 
forecast_Z=sch.linkage(forecast_disMat,method='average') 
express_Z=sch.linkage(express_disMat,method='average') 
fina_indicator_Z=sch.linkage(fina_indicator_disMat,method='average') 
#将层级聚类结果以树状图表示出来并保存为plot_dendrogram.png
#P=sch.dendrogram(Z)
#根据linkage matrix Z得到聚类结果:
income_cluster= sch.fcluster(income_Z,criterion='inconsistent',depth=40, t=20)
income_cluster = pd.DataFrame(income_cluster)
income_sort = pd.DataFrame()
income_sort["tscode"] = income_data['ts_code'] 
income_sort["income_sort"] = income_cluster

balancesheet_cluster= sch.fcluster(balancesheet_Z,criterion='inconsistent',depth=12, t=6)
balancesheet_cluster = pd.DataFrame(balancesheet_cluster)
balancesheet_sort = pd.DataFrame()
balancesheet_sort["tscode"] = balancesheet_data['ts_code'] 
balancesheet_sort["balancesheet_sort"] = balancesheet_cluster

cashflow_cluster= sch.fcluster(cashflow_Z,criterion='inconsistent',depth=35, t=12)
cashflow_cluster = pd.DataFrame(cashflow_cluster)
cashflow_sort = pd.DataFrame()
cashflow_sort["tscode"] = cashflow_data['ts_code'] 
cashflow_sort["cashflow_sort"] = cashflow_cluster

#调参
forecast_cluster= sch.fcluster(forecast_Z,criterion='inconsistent',depth=35, t=12)
forecast_cluster = pd.DataFrame(forecast_cluster)
forecast_sort = pd.DataFrame()
forecast_sort["tscode"] = forecast_data['ts_code'] 
forecast_sort["forecast_sort"] = forecast_cluster

express_cluster= sch.fcluster(express_Z,criterion='inconsistent',depth=35, t=12)
express_cluster = pd.DataFrame(express_cluster)
express_sort = pd.DataFrame()
express_sort["tscode"] = express_data['ts_code'] 
express_sort["express_sort"] = express_cluster

fina_indicator_cluster= sch.fcluster(fina_indicator_Z,criterion='inconsistent',depth=35, t=11)
fina_indicator_cluster = pd.DataFrame(fina_indicator_cluster)
fina_indicator_sort = pd.DataFrame()
fina_indicator_sort["tscode"] = fina_indicator_data['ts_code'] 
fina_indicator_sort["fina_indicator_sort"] = fina_indicator_cluster

ts.set_token('ce252aca832765dc55242d4b12b5107480df87294f6d60a3029f58c1')
pro = ts.pro_api()
raw_data = pro.daily(trade_date='20190417')
#raw_data.to_csv('C:/Users/Devin/Desktop/graduation_project/data/raw_data.csv')

raw_data_sql = """
select ts_code,(case when transaction_sort = '111' then 1
					 when transaction_sort = '112' then 2
					 when transaction_sort = '121' then 3
					 when transaction_sort = '122' then 4
					 when transaction_sort = '211' then 5
					 when transaction_sort = '212' then 6
					 when transaction_sort = '221' then 7
					 when transaction_sort = '222' then 8 else null end )transaction_sort ,pct_chg
from			
(
select  ts_code,
		(case when close <=13 then 1 else 2 end )||
		(case when vol <=77000 then 1 else 2 end )||
	    (case when amount <=100000 then 1 else 2 end )transaction_sort,
	    (case when pct_chg <= 8 then 0 else 1 end )pct_chg
from raw_data
)a"""
raw_data = pysqldf(raw_data_sql)


result_sql = """
select a.ts_code,substr(a.ts_code,1,6 ) ts_code_num,b.income_sort,c.balancesheet_sort,d.cashflow_sort
,e.forecast_sort,f.express_sort,g.fina_indicator_sort,
h.transaction_sort,h.pct_chg
from ts_code_list a 
left join income_sort b on a.ts_code = b.tscode
left join balancesheet_sort c on a.ts_code = c.tscode
left join cashflow_sort d on a.ts_code = d.tscode
left join forecast_sort e on a.ts_code = e.tscode
left join express_sort f on a.ts_code = f.tscode
left join fina_indicator_sort g on a.ts_code = g.tscode
left join raw_data h on a.ts_code = h.ts_code
"""
result_sort = pysqldf(result_sql)

result_sort  = result_sort.fillna(0)
result_sort.to_csv('C:/Users/Devin/Desktop/graduation_project/data/result_sort.csv')

# =============================================================================
# #sklearn 的 层次聚类
# ac = AgglomerativeClustering(n_clusters=19,affinity="euclidean",linkage="average")
# labels = ac.fit_predict(cashflow_data_k)
# print(all_np(labels))
# =============================================================================


# =============================================================================
# #k-men
# estimator=KMeans(n_clusters=40)
# res=estimator.fit_predict(income_data_k)
# lable_pred=estimator.labels_
# centroids=estimator.cluster_centers_
# inertia=estimator.inertia_
# #print re
# print (all_np(lable_pred))
# #print (centroids)
# #print (inertia)
# =============================================================================


time_end = time.time()
print (time_end - time_start)
