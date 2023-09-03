import pandas as pd
import openpyxl

# Replace 'your_file.xlsx' with the path to your actual XLSX file
file_path = '../sps_c_july_new.xlsx'

# Read the XLSX file using read_excel()
df = pd.read_excel(file_path)

# Print the DataFrame (optional)
#print(df)
df.columns = ['InstanceType', 'az', 'sps', 'time']
print(df)




