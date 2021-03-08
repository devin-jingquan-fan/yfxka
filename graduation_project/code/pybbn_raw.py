# -*- coding: utf-8 -*-
"""
Created on Fri May 17 22:51:47 2019

@author: Devin
"""

from pybbn.graph.dag import Bbn
from pybbn.graph.edge import Edge, EdgeType
from pybbn.graph.jointree import EvidenceBuilder
from pybbn.graph.node import BbnNode
from pybbn.graph.variable import Variable
from pybbn.pptc.inferencecontroller import InferenceController

import numpy as np 
import stockstats as stkstat
import tushare as ts
import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
import time 

ts.set_token('ce252aca832765dc55242d4b12b5107480df87294f6d60a3029f58c1')
pro = ts.pro_api()
#raw_data = pro.daily(trade_date='20190517')
#raw_data.to_csv('C:/Users/Devin/Desktop/graduation_project/data/raw_data.csv')
#raw_data = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/raw_data.csv')

result_sort = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/result_sort.csv')
income_re = result_sort["income_sort"]
forecast_re = result_sort["forecast_sort"]
express_re = result_sort["express_sort"]
cashflow_re = result_sort["cashflow_sort"]
balancesheet_re = result_sort["balancesheet_sort"]
fina_indicator_re = result_sort["fina_indicator_sort"]
transaction_re = result_sort["transaction_sort"]
pct_chg_re = result_sort["pct_chg"]
#参数学习
def zero_studay(data,sort_num):
    result_proportion = []
    data = pd.DataFrame(data)
    for i in np.arange(0,sort_num):
        a =int((data == i).sum())
        b = a/len(data)
        result_proportion.append(b)
    return result_proportion

def two_studay(data1,data2,data3,sort_num1,sort_num2,sort_num3):
    result_proportion = []
    data_sum = pd.DataFrame()
    data_sum["a"]=data1
    data_sum["b"]=data2
    data_sum["c"]=data3
    for i1 in np.arange(0,sort_num1):
        for i2 in np.arange(0,sort_num2):
            for i3 in np.arange(0,sort_num3):
                a =int(len(data_sum[(data_sum["a"] == i1) & (data_sum["b"] == i2 )& (data_sum["c"] == i3)]))
                b = a/len(data_sum)
                result_proportion.append(b)
    return result_proportion

def three_studay(data1,data2,data3,data4,sort_num1,sort_num2,sort_num3,sort_num4):
    result_proportion = []
    data_sum = pd.DataFrame()
    data_sum["a"]=data1
    data_sum["b"]=data2
    data_sum["c"]=data3
    data_sum["d"]=data4
    for i1 in np.arange(0,sort_num1):
        for i2 in np.arange(0,sort_num2):
            for i3 in np.arange(0,sort_num3):
                for i4 in np.arange(0,sort_num4):
                                a =int(len(data_sum[(data_sum["a"] == i1) &(data_sum["b"] == i2)&(data_sum["c"] == i3)&(data_sum["d"]==i4) ]))
                                b = a/len(data_sum)
                                result_proportion.append(b)
    return result_proportion

def six_studay(data1,data2,data3,data4,data5,data6,data7,sort_num1,sort_num2,sort_num3,sort_num4,sort_num5,sort_num6,sort_num7):
    result_proportion = []
    data_sum = pd.DataFrame()
    data_sum["a"]=data1
    data_sum["b"]=data2
    data_sum["c"]=data3
    data_sum["d"]=data4
    data_sum["e"]=data5
    data_sum["f"]=data6
    data_sum["h"]=data7
    for i1 in np.arange(0,sort_num1):
        for i2 in np.arange(0,sort_num2):
            for i3 in np.arange(0,sort_num3):
                for i4 in np.arange(0,sort_num4):
                    for i5 in np.arange(0,sort_num5):
                        for i6 in np.arange(0,sort_num6):
                            for i7 in np.arange(0,sort_num7):
                                a =int(len(data_sum[(data_sum["a"] == i1) &(data_sum["b"] == i2)&(data_sum["c"] == i3)&(data_sum["d"]==i4)&(data_sum["e"]==i5)&(data_sum["f"]==i6)&(data_sum["h"]==i7) ]))
                                b = a/len(data_sum)
                                result_proportion.append(b)
    return result_proportion





test = [0.5,0.5]
# create the nodes
income = BbnNode(Variable(0, 'income', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15']), zero_studay(income_re,16))
forecast = BbnNode(Variable(1, 'forecast', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20']), two_studay(income_re,balancesheet_re,forecast_re,16,19,21))
express = BbnNode(Variable(2, 'express', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15','16']), two_studay(fina_indicator_re,cashflow_re,express_re,19,19,17))
balancesheet = BbnNode(Variable(3, 'balancesheet', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18']), zero_studay(balancesheet_re,19))
fina_indicator = BbnNode(Variable(4, 'fina_indicator', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18']), zero_studay(fina_indicator_re,19))
cashflow = BbnNode(Variable(5, 'cashflow', ['0','1', '2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18']),zero_studay(cashflow_re,19))
#transaction = BbnNode(Variable(6, 'transaction', ['0','1', '2','3','4','5','6','7','8']), six_studay(income_re,balancesheet_re,forecast_re,cashflow_re,fina_indicator_re,express_re,transaction_re,16,19,21,19,19,17,9))
transaction = BbnNode(Variable(6, 'transaction', ['0','1', '2','3','4','5','6','7','8']), two_studay(forecast_re,express_re,transaction_re,21,17,9))
pct_chg = BbnNode(Variable(7, 'pct_chg', ['0', '1']), two_studay(transaction_re,express_re,income_re,9,17,2))

# create the network structure

bbn = Bbn() \
    .add_node(income) \
    .add_node(forecast) \
    .add_node(express) \
    .add_node(balancesheet) \
    .add_node(fina_indicator) \
    .add_node(cashflow) \
    .add_node(transaction) \
    .add_node(pct_chg) \
    .add_edge(Edge(income, forecast, EdgeType.DIRECTED)) \
    .add_edge(Edge(balancesheet, forecast, EdgeType.DIRECTED)) \
    .add_edge(Edge(fina_indicator, express, EdgeType.DIRECTED)) \
    .add_edge(Edge(cashflow, express, EdgeType.DIRECTED)) \
    .add_edge(Edge(forecast, transaction, EdgeType.DIRECTED)) \
    .add_edge(Edge(express, transaction, EdgeType.DIRECTED)) \
    .add_edge(Edge(transaction, pct_chg, EdgeType.DIRECTED)) \
    .add_edge(Edge(express, pct_chg, EdgeType.DIRECTED))
#bbn = Bbn() \
#    .add_node(income) \
#    .add_node(forecast) \
#    .add_node(express) \
#    .add_node(balancesheet) \
#    .add_node(fina_indicator) \
#    .add_node(cashflow) \
#    .add_node(transaction) \
#    .add_node(pct_chg) \
#    .add_edge(Edge(income, forecast, EdgeType.DIRECTED)) \
#    .add_edge(Edge(income, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(balancesheet, forecast, EdgeType.DIRECTED)) \
#    .add_edge(Edge(balancesheet, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(forecast, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(cashflow, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(fina_indicator, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(express, transaction, EdgeType.DIRECTED)) \
#    .add_edge(Edge(transaction, pct_chg, EdgeType.DIRECTED)) \
#    .add_edge(Edge(express, pct_chg, EdgeType.DIRECTED))

# convert the BBN to a join tree
join_tree = InferenceController.apply(bbn)
##get today_data by tushare 
today_data = ts.get_today_all()
today_data.to_csv('C:/Users/Devin/Desktop/graduation_project/data/today_data.csv')
today_data = pd.read_csv('C:/Users/Devin/Desktop/graduation_project/data/today_data.csv')

#get stock transaction sort by today data
def get_stock_info(ts_code):  
#    '000042.SZ' 601021
    info = result_sort[result_sort["ts_code_num"]==ts_code]
    a = today_data[today_data["code"]== ts_code]
    if int(int(a["trade"]) <= 13 and a["volume"]) <= 77000 and int(a["amount"]) <= 100000  :
        info["transaction_sort"] = 1#111
    elif int(int(a["trade"]) <= 13 and a["volume"]) <= 77000 and int(a["amount"]) > 100000 :
        info["transaction_sort"] = 2#112
    elif int(int(a["trade"]) <= 13 and a["volume"]) > 77000 and int(a["amount"]) <= 100000  :
        info["transaction_sort"] = 3#121
    elif int(int(a["trade"]) <= 13 and a["volume"]) > 77000 and int(a["amount"]) > 100000  :
        info["transaction_sort"] = 4#122
    elif int(int(a["trade"]) > 13 and a["volume"]) <= 77000 and int(a["amount"]) <= 100000  :
        info["transaction_sort"] = 5#211
    elif int(int(a["trade"]) > 13 and a["volume"]) <= 77000 and int(a["amount"]) > 100000  :
        info["transaction_sort"] = 6#212
    elif int(int(a["trade"]) > 13 and a["volume"]) > 77000 and int(a["amount"]) <= 100000  :
        info["transaction_sort"] = 7#221
    else:
        info["transaction_sort"] = 8#222
    return info

#predict stock pct_chg(涨跌幅)
#    300174  300550
def get_stock_pct_chg(tscode):
    a = get_stock_info(tscode)
    ev = EvidenceBuilder() \
        .with_node(join_tree.get_bbn_node_by_name('income')) \
        .with_evidence(str(int(a["income_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('forecast')) \
        .with_evidence(str(int(a["forecast_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('express')) \
        .with_evidence(str(int(a["express_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('fina_indicator')) \
        .with_evidence(str(int(a["fina_indicator_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('cashflow')) \
        .with_evidence(str(int(a["cashflow_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('transaction')) \
        .with_evidence(str(int(a["transaction_sort"])), 1.0) \
        .with_node(join_tree.get_bbn_node_by_name('balancesheet')) \
        .with_evidence(str(int(a["balancesheet_sort"])), 1.0) \
        .build()
    join_tree.set_observation(ev)
    
    for node in join_tree.get_bbn_nodes():
        potential = join_tree.get_bbn_potential(node)
        print(node)
        print(potential)
    
    
# =============================================================================
#     
# # insert an observation evidence
# ev = EvidenceBuilder() \
#     .with_node(join_tree.get_bbn_node_by_name('income')) \
#     .with_evidence('1', 1.0) \
#     .build()
# join_tree.set_observation(ev)
# 
# # print the marginal probabilities
# for node in join_tree.get_bbn_nodes():
#     potential = join_tree.get_bbn_potential(node)
#     print(node)
#     print(potential)
# =============================================================================

