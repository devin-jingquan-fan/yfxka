# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 20:20:52 2019

@author: ptmind
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics
from fbprophet import Prophet
from pandasql import PandaSQL,sqldf,load_meat,load_births
import datetime

today=datetime.date.today()
starttime = datetime.datetime.now()


pysqldf = lambda q: sqldf(q, globals())

prophet = Prophet()


# 导入原始结果集，数据为空
result_raw = pd.read_csv('C:/Users/ptmind/Desktop/code/predict_sale_car/evaluation_public.csv')
#导入数据集集
train_sales_data_v1 = pd.read_csv('C:/Users/ptmind/Desktop/code/predict_sale_car/train_sales_data.csv')
train_search_data_v1 = pd.read_csv('C:/Users/ptmind/Desktop/code/predict_sale_car/train_search_data.csv')
train_user_reply_data_v1 = pd.read_csv('C:/Users/ptmind/Desktop/code/predict_sale_car/train_user_reply_data.csv')

sales_sql = """


select 
 * ,
 ( cast(regYear as string) || '-'|| cast(regMonth as string) || '-' || '01' ) statdt 
--  str_to_date(  ( cast(regYear as string) || '-'|| cast(regMonth as string) || '-' || '01' ) ,'%Y-%m-%d') as a 

from train_sales_data_v1

"""
train_sales_data = pysqldf(sales_sql)
train_sales_data['statdt'] = train_sales_data['statdt'].apply(pd.to_datetime)




shandong = train_sales_data[train_sales_data['province'] == '山东']
shandong_3c974920a76ac9c1 = shandong[shandong['model'] == '3c974920a76ac9c1']
shandong_3c974920a76ac9c1_fit = shandong_3c974920a76ac9c1[['statdt','salesVolume']]
shandong_3c974920a76ac9c1_fit.columns = ['ds','y']
shandong_3c974920a76ac9c1_fit = shandong_3c974920a76ac9c1_fit.reset_index(drop=True)

#test = pd.date_range('2016-01-01','2016-01-24')
#test = pd.DataFrame(test)
#test.columns = ['ds']
#test['y'] = pd.Series(shandong_3c974920a76ac9c1_fit['y'])


m= Prophet(yearly_seasonality = 20 ).fit(shandong_3c974920a76ac9c1_fit)

#a = plot_yearly(m)
#future = prophet.make_future_dataframe(periods=365)
#future.tail()

future = pd.date_range('2018-01-01','2019-01-01', freq = 'MS')
future = pd.DataFrame(future)
future.columns = ['ds']

#future = pd.date_range('2016-01-01','2016-02-01')
#future = pd.DataFrame(future)
#future.columns = ['ds']

forecast = m.predict(future)
forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].head()



fig1 = prophet.plot(forecast)
print(fig1)

fig2 = prophet.plot_components(forecast)
print(fig2)











