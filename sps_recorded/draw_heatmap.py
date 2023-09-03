import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
import sqlite3
import re

# 连接到数据库（如果不存在，则会创建一个新的数据库文件）
conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')  # 替换为你的数据库文件路径



def get_data(family):
    '''
    :param family: c/r/m ,str type
    :return: corresponding x&y lists with a 30 minutes interval
    '''
    pattern = r'\b(?:[01]\d|2[0-3]):(00|30):[0-5]\d\b'
    query = r'select * from all_'+family+r'_avg order by time ASC'
    cursor = conn.cursor()
    cursor.execute(query)
    result_list_raw = cursor.fetchall()
    res_x = []
    res_y = []
    for item in result_list_raw:
        res = re.findall(pattern, item[1])
        if len(res) > 0:
            res_x.append(item[1][8:16])
            # x.append(item[1])
            res_y.append(item[0])

    cursor.close()

    return res_x, res_y

data = {}
columns_name = []
for f in ['m', 'c', 'r']:
    columns_name = get_data(f)[0]
    data[f] = get_data(f)[1]

print(columns_name)

dataf = pd.DataFrame(data)
df_transposed = dataf.transpose()
df_transposed.columns = columns_name
print(df_transposed)

conn.close()

plt.figure(figsize=(20, 12))
heatmap = sb.heatmap(df_transposed)
heatmap.set_yticklabels(['m', 'c', 'r'], rotation='horizontal', fontsize=30)
heatmap.set_xlabel('days in June, 2022.', fontsize=30)
heatmap.set_ylabel('Instance Classes', fontsize=30)
plt.show()