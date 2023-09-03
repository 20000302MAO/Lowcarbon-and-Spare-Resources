import re

import matplotlib
import matplotlib.pyplot as plt


import sqlite3

# 连接到数据库（如果不存在，则会创建一个新的数据库文件）
import pandas as pd

conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')  # 替换为你的数据库文件路径

# 创建一个游标对象
cursor = conn.cursor()

# 执行查询语句
query = r"select * from all_c_sps order by time ASC"


cursor.execute(query)
result_list = cursor.fetchall()

print(result_list)
print(len(result_list))
timeline = set()
instance_types = set()
for item in result_list:
    timeline.add(item[2])
    instance_types.add(item[0])

multiseries = pd.DataFrame(-1, index=list(timeline), columns=list(instance_types))
print(multiseries)
print(multiseries.shape)

for i in result_list:
    #rint(multiseries.at[i[2], i[0]])
    multiseries.at[i[2], i[0]] = i[1]
    #print(multiseries.at[i[2], i[0]])


print(multiseries)
print(multiseries.index)
multiseries = multiseries.rename_axis('time', axis='index')
multiseries.to_csv('sps_c.csv')






cursor.close()
conn.close()

print(result_list)
print(len(result_list))
print(type(result_list[0]))
print(len(result_list))

x = []
y = []
pattern = r'\b(?:[01]\d|2[0-3]):(00|30):[0-5]\d\b'
for item in result_list:
    # x.append(item[1][8:])
    # y.append(item[0])
    # if re.match('^2023-06-0.*', item[1]):
    #     x.append(item[1][8:13])
    #     y.append(item[0])
    res = re.findall(pattern, item[1])
    if len(res) > 0:
        x.append(item[1][8:16])
        #x.append(item[1])
        y.append(item[0])

# x = x[::3]
# y = y[::3]
print(x)
print(len(x))

#matplotlib.use('TkAgg')
plt.figure(figsize=(30, 14))
plt.plot(x, y, marker='o', linestyle='-', color='b')
#plt.scatter(x, y)

# 添加标题和标签
plt.title('avg_sps_m_30min_may', fontsize=30)
plt.xlabel('time', fontsize=20)
plt.ylabel('sps', fontsize=20)
plt.yticks([2.40, 2.45, 2.50, 2.55, 2.60, 2.65], fontsize=20)
plt.xticks(x[::48], rotation=30, fontsize=20)

# 添加网格线
plt.grid(True)

# 显示图例
plt.legend()

# 显示图表
plt.show()

