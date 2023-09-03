import sqlite3
import pandas as pd

def change_time(old_timestamp):
  return '2023'+old_timestamp[4:]

def get_workloads_times():
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    query = r"select timestamp from multiple_nodes_new order by timestamp ASC"
    raw_res = pd.read_sql_query(query, conn)
    conn.close()
    res_df = raw_res.applymap(change_time)
    return res_df

print(get_workloads_times())


