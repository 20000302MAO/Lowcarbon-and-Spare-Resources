import os
import sqlite3
import zipfile
import gzip
import csv
import os.path

BASE_DIR = '/Users/maojingyi/Downloads/scout-master/dataset'
db_path = os.path.join(BASE_DIR, "MscProject.db")
#with sqlite3.connect(db_path) as db:
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
#/Users/maojingyi/Downloads/scout-master/dataset
#print(os.getcwd()) //show the current working folder

file_path = '/Users/maojingyi/Downloads/scout-master/sps_recorded/aws-2023-05'

i = 0
for root, dirs, files in os.walk(file_path):
    print(i, '........................................')
    # if i == 2:
    #     break
    # print(root, dirs, files)
    i += 1

    if len(dirs) > 1:
        for z in dirs:
            #print(z) current folder is z
            # if z not in ['01']:
            #     continue
            current_file_path = root + r'/' + z
            #input_file_path = root + r'/' + z
            # print(input_file_path)
            # output_file_path = root + r'/' + z[:8] + r'.csv'
            # print(output_file_path)

            for croot, cdirs, cfiles in os.walk(current_file_path):
                print(croot, cdirs, cfiles)
                if len(cfiles) > 1:
                    for f in cfiles:
                        input_file_path = croot + r'/' + f
                        print(input_file_path)
                        output_file_path = croot + '/' + f[:12]
                        print(output_file_path)
                        with gzip.open(input_file_path, 'rb') as gz_file:
                            with open(output_file_path, 'wb') as csv_file:
                                csv_file.write(gz_file.read())
                                csv_data = csv.reader(output_file_path)
                                # next(csv_data)  # Skip the header row if it exists
                                # print(csv_data)
                                # cursor.executemany('INSERT INTO sps_tracker VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', csv_data)

                        # conn.commit()
                        # conn.close()
                        os.remove(input_file_path)







            # with gzip.open(input_file_path, 'rb') as gz_file:
            #     with open(output_file_path, 'wb') as csv_file:
            #         csv_file.write(gz_file.read())
            #         csv_data = csv.reader(output_file_path)
            #         next(csv_data)  # Skip the header row if it exists
            #         cursor.executemany('INSERT INTO sps_tracker VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', csv_data)
            #
            # os.remove(input_file_path)


# with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#     # Extract all the files in the ZIP file to a specified directory
#     zip_ref.extractall('/Users/maojingyi/Downloads/scout-master/sps_recorded')



# input_file = 'path/to/your/input_file.csv.gz'
# output_file = 'path/to/your/output_file.csv'
#
# with gzip.open(input_file, 'rb') as gz_file:
#     with open(output_file, 'wb') as csv_file:
#         csv_file.write(gz_file.read())
