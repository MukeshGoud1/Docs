import re
import sys
import time
import boto3
import logging
import pandas as pd
import traceback
from io import StringIO
from pathlib import Path
from awsglue.utils import getResolvedOptions
import subprocess


s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
list1=[]
#bucket name

def split_s3_path(s3_path):
    try:
        bucket = s3_path.split("s3://")[-1].split('/')[0]
        key = s3_path.split("s3://" + bucket + '/')[-1]
        return bucket, key
    except Exception as e:
        raise e

args = getResolvedOptions(sys.argv, ['config','folder_path'])
config_file_path = args["config"]
folder_location = args["folder_path"]

print(config_file_path,folder_location)

try:    
    config_file_bucket, config_file_key = split_s3_path(config_file_path)
    data = s3.get_object(Bucket=config_file_bucket, Key=config_file_key)
    config_df = pd.read_csv(data['Body'])
    
    #Req_File = config_df["SOURCE_FILES"].values.tolist()
    req_file = config_df[config_df["HEADER_AVAILABLE_FLAG"] == "N" ][["SOURCE_FILES","HEADERS"]]
    
    mandatory_file = req_file["SOURCE_FILES"].values.tolist()
    print(mandatory_file)
    
    total_file_bucket, total_file_key = split_s3_path(folder_location)
    my_bucket = s3_resource.Bucket(total_file_bucket)
    
    s3_files = []
    for object_summary in my_bucket.objects.filter(Prefix=total_file_key):
        file_name = object_summary.key.split("/")[-1]
        if len(file_name) >= 1:
            s3_files.append(file_name)    

    for file in s3_files:
        filename = Path(file).stem
        x = filename.split("_")
        srcFileaName = "_"
        if x[-1].isdigit():
            srcFileaName = srcFileaName.join(x[0:-1])
        else:
            srcFileaName = srcFileaName.join(x)
        if srcFileaName in mandatory_file:
            filedata = s3.get_object(Bucket=total_file_bucket, Key=total_file_key+file)
            file_columns = pd.read_csv(filedata['Body'],nrows=0).columns.tolist()
            headers = [i.strip() for i in req_file[req_file["SOURCE_FILES"] == srcFileaName ].HEADERS.values[0].split(',')]
            print("file_columns-", file_columns)
            print("headers from config file-", headers)
            if len(list(set(file_columns).difference(set(headers))))>3:
                print("headers need to append in file")
                cmd = f'echo "{",".join(headers)}" > headers.csv'
                print("\n",cmd)
                cmd1 = f'aws s3 cp s3://{total_file_bucket}/{total_file_key+file} - >> headers.csv'
                print("\n",cmd1)
                cmd2 = f'aws s3 cp headers.csv s3://{total_file_bucket}/{total_file_key+file}'
                print("\n",cmd2)
                cmd_res = subprocess.call(cmd, shell=True)
                cmd1_res = subprocess.call(cmd1, shell=True)
                cmd2_res = subprocess.call(cmd2, shell=True)
                if cmd_res == 0 and cmd1_res == 0 and cmd2_res == 0:
                    print("headers has been appended to file -", file)
                else:
                    print("failed to append headers")
            else:
                print('headers already present in file -', file)
                pass
except Exception as ex:
    print("ERROR: ", ex)
    raise Exception(ex)        
        
        
        
