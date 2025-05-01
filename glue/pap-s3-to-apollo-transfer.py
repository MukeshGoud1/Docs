import sys
import pysftp
import boto3
import os
from pythonping import ping
import boto3
import traceback
from awsglue.utils import getResolvedOptions
import ast
import subprocess

glue_args = getResolvedOptions(sys.argv, ['SFTP_FILE_PATH'])
print(glue_args)
SFTP_FILE_PATH = eval(glue_args['SFTP_FILE_PATH'])
print(SFTP_FILE_PATH)

client = boto3.client('glue')
redshift_secret_name = "pap-apollo-server"
region_name = os.environ['AWS_DEFAULT_REGION']

session = boto3.session.Session()
client = session.client(service_name='secretsmanager',region_name=region_name)
try:
    get_redshift_secret = client.get_secret_value(SecretId=redshift_secret_name)
except Exception as e:
    print("Error While Fetching secrets")
    raise e
args = ast.literal_eval(get_redshift_secret['SecretString'])
print(args)

hostname =args["hostname"]
username=args["username"]
password= args["password"]
port=args["port"]

def execute_command(cmd):
    try:
        cmd_res = subprocess.call(cmd, shell=True)
        if cmd_res == 0:
            # successfully_copied_objects.append(obj)
            print(f"Successfully copied to local- {cmd} ")
        else:
            print(f"Not able to copy to local - {cmd}")
    except Exception as ex:
        print("ERROR: ", ex)
        raise Exception(ex) 
        
def download_s3_files_to_local(src_folder_path, target_folder_name):
    try:
        src_query  = f"aws s3 cp '{src_folder_path}' './{target_folder_name}/' --recursive"
        print("src_query", src_query)
        execute_command(src_query)
    except Exception as e:
        print("Error ", e)
        raise e
try:
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    
    sftp = pysftp.Connection(hostname, username=username, password=password,cnopts=cnopts)
    print("Connection successfully established ... ")
    for i in SFTP_FILE_PATH:
        download_s3_files_to_local(i['src_path'],i['folder_name'])
        local_copy_folder = f"./{i['folder_name']}/"
        sftp.put_r(local_copy_folder, i['target_path'])
        print("Copy completed - ", i['folder_name'])
    print("copy completed")
except Exception as e:
    print("Error ", e)
    raise e