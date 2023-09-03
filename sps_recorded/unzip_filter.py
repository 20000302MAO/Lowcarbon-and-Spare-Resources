import zipfile

zip_path = '/Users/maojingyi/Downloads/aws-2023-06.zip'

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    # Extract all the files in the ZIP file to a specified directory
    zip_ref.extractall('/Users/maojingyi/Downloads/scout-master/sps_recorded')


