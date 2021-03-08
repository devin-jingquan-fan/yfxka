# -*- coding: utf-8 -*-

import pandas as pd
from pandasql import PandaSQL,sqldf,load_meat,load_births
import matplotlib.pyplot as plt
import matplotlib

#sql函数
pysqldf = lambda q: sqldf(q, globals())

#插入数据并转换为df
Emp = pd.DataFrame()
Emp_staff_id = ['A1','A1','A2','A3','A3','A4']
Emp_post_id = ['G1','G2','G1','G3','G5','G4']
Emp_post_name = ['Gn1','Gn2','Gn1','Gn3','Gn5','Gn4']
Emp['Emp_staff_id'] = Emp_staff_id
Emp['Emp_post_id'] = Emp_post_id
Emp['Emp_post_name'] = Emp_post_name


#数据清洗，使用SQL完成
etl_sql = """
select
    Emp_post_id
    ,count(Emp_post_id) as emp_cnts
from Emp
group by Emp_post_id
"""

res = pysqldf(etl_sql)


# 绘图
x_vals = res['Emp_post_id'].values.tolist()
y_vals = res['emp_cnts'].values.tolist()


plt.bar(x_vals, y_vals, align = 'center', color = 'steelblue', alpha = 0.8)
plt.ylim(0, 5)     # y轴取值范围
plt.ylabel("人数")
plt.title('staff count by every post')
for x,y in enumerate(y_vals):
    plt.text(x, y+1, '%s' %round(y,1), ha='center')
plt.show