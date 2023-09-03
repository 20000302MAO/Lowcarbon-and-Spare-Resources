import matplotlib.pyplot as plt
import re

import sqlite3

# 连接到数据库（如果不存在，则会创建一个新的数据库文件）
conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')  # 替换为你的数据库文件路径

# 创建一个游标对象
cursor = conn.cursor()

# 执行查询语句
query = r'select time, score, "Actual Carbon Intensity (gCO2/kWh)" from june_total order by time ASC'

cursor.execute(query)
result_list = cursor.fetchall()

print(result_list)

cursor.close()
conn.close()


# Example data for the two plot lines
x_data = []
y_data1 = []
y_data2 = []

for item in result_list:
    if re.match('^24.*', item[0][8:16]):
        x_data.append(item[0][8:16])
        y_data1.append(item[1])
        y_data2.append(item[2] / 400 + 2.3)


x_data = x_data[:48]
y_data1 = y_data1[:48]
y_data2 = y_data2[:48]
print(x_data)

plt.figure(figsize=(25, 14))
# Plot the first line
plt.plot(x_data, y_data1, label='sps', color='blue', linestyle='--', marker='o')

# Plot the second line
plt.plot(x_data, y_data2, label='carbon_intensity', color='red', linestyle='-', marker='s')

# Add labels and legend
plt.xticks(x_data, rotation=45, fontsize=20)
plt.yticks([2.5, 2.6, 2.7, 2.8, 2.9, 3.0], fontsize=20)
plt.xlabel('time', fontsize=23)
plt.ylabel('score', fontsize=23)
plt.title('June_24', fontsize=23)
plt.legend()

# Show the plot
plt.show()

