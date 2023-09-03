import re

import matplotlib.pyplot as plt

import sqlite3

conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')  # 替换为你的数据库文件路径
cursor = conn.cursor()

query = r"select * from all_m_avg order by time ASC"

cursor.execute(query)
result_list = cursor.fetchall()

cursor.close()
conn.close()

print(result_list)
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

plt.title('avg_sps_m_30min_june', fontsize=30)
plt.xlabel('time', fontsize=20)
plt.ylabel('sps', fontsize=20)
plt.yticks([2.30, 2.35, 2.40, 2.45, 2.50, 2.55], fontsize=20)
plt.xticks(x[::48], rotation=30, fontsize=20)

plt.grid(True)

plt.legend()

plt.show()

