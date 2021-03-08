from bayesianpy.network import Builder as builder
import bayesianpy.network

nt = bayesianpy.network.create_network()

# where df is your dataframe
task = builder.create_discrete_variable(nt, df, 'task')

size = builder.create_continuous_variable(nt, 'size')
grasp_pose = builder.create_continuous_variable(nt, 'GraspPose')

builder.create_link(nt, size, grasp_pose)
builder.create_link(nt, task, grasp_pose)

for v in ['fill level', 'object shape', 'side graspable']:
    va = builder.create_discrete_variable(nt, df, v)
    builder.create_link(nt, va, grasp_pose)
    builder.create_link(nt, task, va)

# write df to data store
with bayesianpy.data.DataSet(df, bayesianpy.utils.get_path_to_parent_dir(__file__), logger) as dataset:
    model = bayesianpy.model.NetworkModel(nt, logger)
    model.train(dataset)

    # to query model multi-threaded
    results = model.batch_query(dataset, [bayesianpy.model.QueryModelStatistics()], append_to_df=False)







import numpy as npy

# 贝叶斯分类：

class Bayes:
    def __init__(self):
        # -1表示测试方法没有做，表示没有进行训练。
        self.length = -1
        # 分类的类别标签
        self.labelcount = dict()
        self.vectorcount = dict()
    # 训练函数：(dataSet:list 训练集指定为list类型)
    def fit(self, dataSet:list, labels:list):
        if(len(dataSet)!=len(labels)):
            raise ValueError("您输入的测试数组跟类别数组长度不一致~")
        self.length = len(dataSet[0]) # 测试数据特征值的长度。
        # 所有类别的数据
        labelsnum = len(labels)
        # 不重复的类别的数量
        norepeatlabel = set(labels)
        # 以此遍历各个类别
        for item in norepeatlabel:
            # 计算当前类别占总类别的比例：
            # thislabel为当前类别
            thislabel = item
            # 当前类别在总类别中的比例;
            labelcount[thislabel]= labels.count(thislabel)/labelsnum
        for vector, label in zip(dataSet, labels):
            if(label not in vectorcount):
                self.vectorcount[label]= []
            self.vectorcount[label].append(vector)
        print("训练结束~")
        return self
    # 测试数据：
    def btest(self, TestData, labelsSet):
        if(self, length==-1):
            raise ValueError("您还没有进行训练，请先训练~~")
        # 计算testdata分别为各个类别的概率：
        lbDict = dict()
        for thislb in labelsSet:
            p = 1
            # 当前类别占总类别的比例：
            alllabel = self.labelcount[thislb]
            # 当前类别中的所有向量：
            allvector = self.vectorcount[thislb]
            # 当前类别一共有多少个向量：
            vnum = len(allvector)
            # 数组转置
            allvector =npy.array(allvector).T
            for index in range(0, len(TestData)):
                vector = list(allvector[index])
                p* = vector.count(TestData[index])/vnum
            lbDict[thislb] = p*alllabel
        thislabel = sorted(lbDict, key=lambda x:lbDict[x], reverse=True)[0]
        return thislabel


>>> from sklearn import datasets
>>> iris = datasets.load_iris()
>>> iris.feature_names  # 四个特征的名字
['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']
>>> iris.data
array([[ 5.1,  3.5,  1.4,  0.2],
       [ 4.9,  3. ,  1.4,  0.2],
       [ 4.7,  3.2,  1.3,  0.2],
       [ 4.6,  3.1,  1.5,  0.2],
       [ 5. ,  3.6,  1.4,  0.2],
       [ 5.4,  3.9,  1.7,  0.4],
       [ 4.6,  3.4,  1.4,  0.3],
       [ 5. ,  3.4,  1.5,  0.2],
       ......
       [ 6.5,  3. ,  5.2,  2. ],
       [ 6.2,  3.4,  5.4,  2.3],
       [ 5.9,  3. ,  5.1,  1.8]]) #类型是numpy.array
>>> iris.data.size  
600  #共600/4=150个样本
>>> iris.target_names
array(['setosa', 'versicolor', 'virginica'], 
      dtype='|S10')
>>> iris.target
array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,....., 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ......, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2])
>>> iris.target.size
150
>>> from sklearn.naive_bayes import GaussianNB
>>> clf = GaussianNB()
>>> clf.fit(iris.data, iris.target)
>>> clf.predict(iris.data[0])
array([0])   # 预测正确
>>> clf.predict(iris.data[149])
array([2])   # 预测正确
>>> data = numpy.array([6,4,6,2])
>>> clf.predict(data)
array([2])  # 预测结果很合理





# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 19:07:29 2019

@author: ptmind
"""

import numpy as np 
import seaborn as sns
import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
pysqldf = lambda q: sqldf(q, globals())
meat = load_meat()
births = load_births()

#s = np.random.dirichlet((10, 5, 3), 20).transpose()
lost_customer_name =['account_id','owner_acct_id','owner_email','user_acct_id','profile_id']
lost_customer_info = pd.read_csv  ('C:/Users/ptmind/Desktop/work/lost_customer_info.csv',names = lost_customer_name)
lost_customer_info.head()




