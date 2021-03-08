#!/usr/bin/env python
# _*_ UTF-8 _*_
'''
@project:GBDT+LR-Demo
@author:xiaofei
'''

import pandas as pd
import numpy as np

if  __name__ == '__main__':

    df = pd.read_csv('/Users/fanjingquan/Desktop/test1.csv',  names=['udid','event','a','b'])

    dftest = pd.DataFrame(df)

# ==========================================数据分组==================================================
# =============================================================================
#     # 排序依据
#     bins =[0,5,10,15,20]
#     # 组名
#     group_name =['a','b','c','d']
#     #cut函数，数组列名，分组区间，对应组名
#     dftest['group'] = pd.cut(dftest['event'],bins,labels=group_name)
# =============================================================================
    


# ==========================================数据分列==================================================

    df_final = dftest['udid'].str.split('-', 1, True)
    df_final.columns = ['band','a']





    df_final.to_csv("/Users/fanjingquan/Desktop/test3.csv")
