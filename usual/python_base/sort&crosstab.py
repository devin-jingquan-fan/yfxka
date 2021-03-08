#!/usr/bin/env python
# _*_ UTF-8 _*_
'''
@project:GBDT+LR-Demo
@author:xiaofei
'''

import pandas as pd
import numpy as np

if  __name__ == '__main__':
    df = pd.read_excel('/Users/fanjingquan/Desktop/test1.xlsx', sheetname='Sheet1', index_col='udid')
    dftest = pd.DataFrame(df)



# ================================================三种排序方式========================================================

    dfindex = dftest.sort_index(axis=1)

# =============================================================================
#     df2.sort_values(by='b')对df2按照b列排序，
# df2.sort_values(by=['b','a'])对df2按照b列排序后如有相同的再按照a列排序，
# df2.sort_values(by=['a','b'])对df2按照a列排序后如有相同的再按照b列排序，
# =============================================================================
    dfvalues = dftest.sort_values(by=['udid','event'])

# =============================================================================
# 升序的话，从1开始排名，如果有重复的，重复的排名相加除以重复的个数。如下：
# 四个4分别排名为5,6,7,8，则(5+6+7+8)/4=6.5,后面的则从9开始排名。
# =============================================================================
# 将数值分组再排序
    dfrank = dftest.rank(axis=1)
    
#数据透视表 （交叉表）crosstab只计数，不能求和

    dfcrosstab = pd.crosstab(dfindex.body_source,dfindex.event,margins=True)


# 数据透视表pivot_table  aggfunc支持numpy计算，可以调用numpy的公式
# =============================================================================
# print(pd.pivot_table(df, values = 'values', index = 'date', columns = 'key', aggfunc=np.sum))  # 也可以写 aggfunc='sum'
# print('-----')
# # data：DataFrame对象
# # values：要聚合的列或列的列表
# # index：数据透视表的index，从原数据的列中筛选
# # columns：数据透视表的columns，从原数据的列中筛选
# # aggfunc：用于聚合的函数，默认为numpy.mean，支持numpy计算方法
# 
# print(pd.pivot_table(df, values = 'values', index = ['date','key'], aggfunc=len))
# print('-----')
# # 这里就分别以date、key共同做数据透视，值为values：统计不同（date，key）情况下values的平均值
# # aggfunc=len(或者count)：计数
# =============================================================================




# 另存为，index_label 是索引，加不加都可以
    dfvalues.to_excel("/Users/fanjingquan/Desktop/test2.xlsx",index_label="t1.udid")
#另存为数据透视表
    dfcrosstab.to_excel("/Users/fanjingquan/Desktop/test3.xlsx")

