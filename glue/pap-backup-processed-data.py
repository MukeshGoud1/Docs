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
import concurrent.futures
from datetime import datetime, timedelta


args = getResolvedOptions(sys.argv, ['source_bucket','PROCESSED_TARGET_KEY', 'EXCLUDED_PREFIXES', 'ARCHIVE_LOCATION','BATCH_DATE'])

print('args', args)

def clear_folder(s3_res, bucket_name, folder_prefix):
    try:
        print(bucket_name, folder_prefix)
        bucket = s3_res.Bucket(bucket_name)
        response = bucket.objects.filter(Prefix=folder_prefix).delete()
        if response:
            print("latest backup folder deleted successfully")
            return 1
        else:
            print("deletion failed - latest backup folder not found")
            return None
    except Exception as ex:
        print("ERROR: ", ex)
        raise Exception(ex)
        
        
def is_excluded_prefix(object_key,excluded_prefixes):
    for prefix in excluded_prefixes:
        if object_key.lower().startswith(prefix.lower()):
            return True
    return False

def execute_command(cmd):
    try:
        cmd_res = subprocess.call(cmd, shell=True)
        if cmd_res == 0:
            # successfully_copied_objects.append(obj)
            print(f"Successfully copied - {cmd} ")
        else:
            print(f"Not able to move - {cmd}")
    except Exception as ex:
        print("ERROR: ", ex)
        raise Exception(ex)   
        
def call_thread_pool(cmd_list):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers = 25) as exec:
            exec.map(execute_command, cmd_list)
        return 1
    except Exception as ex:
        print("ERROR: ", ex)
        raise Exception(ex)  
        
try:    
    s3_client = boto3.client('s3')
    s3_res = boto3.resource('s3')
    source_bucket =args['source_bucket']
    target_key = args['PROCESSED_TARGET_KEY']
    excluded_prefixes = eval(args['EXCLUDED_PREFIXES'])
    batch_date = args['BATCH_DATE']
    # batch_date = (datetime.strptime(args['BATCH_DATE'], '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
    
    archive_location = args['ARCHIVE_LOCATION'] + batch_date
    
    successfully_copied_objects = []
    failed_to_copy_objects = []

    # delete old backup data from latest backup folder
    delete_result= clear_folder(s3_res, source_bucket, target_key)
    print("existing rpd/latest/ backup folder has been deleted Successfully")
    
    # copy processed bucket data to backup location
    response = s3_client.list_objects_v2(Bucket = source_bucket, Delimiter = '/')
    if 'CommonPrefixes' in response:
        objects = [obj['Prefix'] for obj in response['CommonPrefixes'] if not is_excluded_prefix(obj['Prefix'],excluded_prefixes)]
        print("objects", objects)
        archive_cp_cmds = [f"aws s3 cp s3://{source_bucket}/{obj} s3://{source_bucket}/{archive_location}/{obj} --recursive" for obj in objects]
        cp_cmds = [f"aws s3 cp s3://{source_bucket}/{obj} s3://{source_bucket}/{target_key}{obj} --recursive" for obj in objects]
        cp_cmds.extend(archive_cp_cmds)
        
        print("cp commands", cp_cmds)
        res = call_thread_pool(cp_cmds)
        print("backup creation completed")
    
    #logic to delete old batch date partition
    

except Exception as ex:
    print("ERROR: ", ex)
    raise Exception(ex)        
        
        
        
