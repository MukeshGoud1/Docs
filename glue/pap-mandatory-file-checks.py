import re
import sys
import time
import boto3
import logging
import pandas as pd
import traceback
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
import subprocess

import yaml
from botocore.exceptions import ClientError
from string import Template
import json
from awsglue.utils import getResolvedOptions

def get_config(bucket, object_key):
    s3_resource = boto3.resource('s3')
    try:
        obj = s3_resource.Object(bucket, object_key)
        content = (obj.get()['Body']).read().decode('utf-8')
        if content is None or len(content) == 0 or not content:
            ValueError('Missing configuration file')
        config = yaml.safe_load(content)
        return config
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            ValueError("Config file does not exist or cannot be accessed!!!")
    except yaml.YAMLError as e:
        ValueError('Parsing YAML configuration file failed!!!')


def send_email(email):
    for key in (
            'aws_region', 'sender', 'recipient_list', 'msg_charset', 'msg_subject', 'msg_body_html', 'msg_body_text'):
        if email.get(key) is None:
            ValueError(f"Missing email parameter -{key}")

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=email["aws_region"])
    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': email["recipient_list"],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': email["msg_charset"],
                        'Data': email["msg_body_html"],
                    },
                    'Text': {
                        'Charset': email["msg_charset"],
                        'Data': email["msg_body_text"],
                    },
                },
                'Subject': {
                    'Charset': email["msg_charset"],
                    'Data': email["msg_subject"],
                },
            },
            Source=email["sender"],
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def send_email_notification(env_name,batch_date, run_status, description):
    print('inside email notification function')
    dict_email = {}
    # parameterize env
    dict_email['ENV'] = env_name
    dict_email['APPLICATION_NAME'] = 'PAP'
    dict_email['PROCESS_RUN_STATE'] = run_status
    dict_email['BATCH_DATE'] = batch_date
    dict_email['RAW_LANDING_LOCATION'] = folder_location
    dict_email['ARCHIVE_LOCATION'] = archive_location = args['archive_path']

    # needs to be populated message and process url
    dict_email['DESCRIPTION'] = description

    # parameterize email temp
    email_template = \
        get_config(CODE_ARTIFACT_BUCKET, "configuration/dags/email"
                                                                              "/mandatory_file_check_email_notification.yml")[
            'templates'][0]
    print(email_template)
    enriched_email = Template(json.dumps(email_template)).safe_substitute(dict_email)
    print(enriched_email)
    send_email(json.loads(enriched_email))


def split_s3_path(s3_path):
    try:
        bucket = s3_path.split("s3://")[-1].split('/')[0]
        key = s3_path.split("s3://" + bucket + '/')[-1]
        return bucket, key
    except Exception as e:
        raise e


def get_s3_files(s3_resource, bucket_name, key):
    try:
        my_bucket = s3_resource.Bucket(bucket_name)
        landing_files = [object_summary.key.split("/")[-1] for object_summary in my_bucket.objects.filter(Prefix=key) if
                         len(object_summary.key.split("/")[-1]) >= 1]
        source_files = []
        file_dict = {}
        print("\n landing files - ", landing_files)
        for file in landing_files:
            x = Path(file).stem.split("_")
            if x[-1].isdigit():
                filename = '_'.join(x[0:-1])
            else:
                filename = '_'.join(x)
            source_files.append(filename)
            if filename in file_dict:
                file_dict[filename].append(file)
            else:
                file_dict[filename]=[file]

        print("\n\n file_dict", file_dict)
        return landing_files, source_files, file_dict
    except Exception as e:
        print(e)
        raise e


def get_latest_batch_date(s3, bucket_name, key):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key, Delimiter='/')
        all_batch_dates = []
        for i in response['CommonPrefixes']:
            val = i['Prefix'].strip('/').split('/')[-1]
            if len(val) == 8 and val.isdigit():
                all_batch_dates.append(datetime.strptime(val, '%Y%m%d'))
            elif len(val) == 12 and val.isdigit():
                all_batch_dates.append(datetime.strptime(val, '%Y%m%d%H%M'))
            else:
                pass
        if all_batch_dates:
            max_date = max(all_batch_dates)
            if max_date.minute == 0 and max_date.hour == 0:
                return max_date.strftime('%Y%m%d')
            if max_date.minute != 0 or max_date.hour != 0:
                return max_date.strftime('%Y%m%d%H%M')
        else:
            return None, "No Archival batch date folder found"

    except Exception as ex:
        print("ERROR: ", ex)
        print(traceback.format_exc())
        # TODO: make connection to DB and store error logs
        raise Exception(ex)


def execute_command(cmd):
    try:
        cmd_result = subprocess.call(cmd, shell=True)
        print(cmd_result)
        if cmd_result == 0:
            # successfully_copied_objects.append(obj)
            print(f"Successfully copied - {cmd} ")
        else:
            print(f"Not able to move - {cmd}")
        return cmd_result
    except Exception as ex:
        print("ERROR: ", ex)
        raise Exception(ex)


args = getResolvedOptions(sys.argv, ['config','folder_path','batch_date', 'config_bucket', 'archive_path', 'env_name'])
print(args)

CODE_ARTIFACT_BUCKET = args['config_bucket']
config_file_path = args['config']
folder_location = args['folder_path']
batch_date = args['batch_date']
archive_location = args['archive_path']
ENV_NAME = args['env_name'].upper()

try:
    # aws_session = return_aws_connection()
    sns_client = boto3.client('sns')
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    config_file_bucket, config_file_key = split_s3_path(config_file_path)
    data = s3.get_object(Bucket=config_file_bucket, Key=config_file_key)
    config_df = pd.read_csv(data['Body'])

    mandatory_files = config_df[config_df["MANDATORY_FLAG"] == "Y"]["SOURCE_FILES"].values.tolist()
    # print(mandatory_files)
    cleanup_file_list = config_df[config_df["FILE_CLEANUP_FLAG"] == "N"]["SOURCE_FILES"].values.tolist()

    total_file_bucket, total_file_key = split_s3_path(folder_location)

    landing_files, source_files, landing_file_dict = get_s3_files(s3_resource, total_file_bucket, total_file_key)

    missing_file = set(mandatory_files).difference(set(source_files))

    print('\nsource files from landing path', source_files)
    print('\n\nmissing files ', missing_file)

    non_mandatory_missing_files = [i for i in cleanup_file_list if i in list(missing_file)]
    print("\n\n non_mandatory_missing_files", non_mandatory_missing_files)

    # check for non mandatory missing file in archive location in latest batch partition
    archive_bucket, archive_key = split_s3_path(archive_location)
    print(archive_bucket, archive_key)

    latest_batch = get_latest_batch_date(s3, archive_bucket, archive_key)
    copied_files = []
    if latest_batch:
        archive_key = archive_key + latest_batch + '/'
        print("\n latest batch date ", latest_batch)
        archive_files, archive_source_files, archive_file_dict = get_s3_files(s3_resource, archive_bucket, archive_key)
        
        print("\n \n archive_file_dict", archive_file_dict)
        for file, filename_list in archive_file_dict.items():
            if file in non_mandatory_missing_files:
                print("\n missing file ", file)
                for k in filename_list:
                    cp_cmd = f"aws s3 cp 's3://{archive_bucket}/{archive_key}{k}' s3://{total_file_bucket}/{total_file_key}"
                    print(cp_cmd)
                    cmd_res = execute_command(cp_cmd)
                    if cmd_res==0:
                        copied_files.append(k)
                        missing_file.discard(file)

    print(missing_file)
    missing_file = list(missing_file)
    if missing_file:

        description = f"""Following are the missing file in landing location - {missing_file}. """ 
        if copied_files:
            description = description + f""" <br>Following are the non-mandatory missing files and have been copied from archive location to raw landing location – {copied_files}"""
        send_email_notification(ENV_NAME, batch_date, "Failed", description)
        raise Exception(f"Following Mandatory Files are missing {missing_file}")

    else:
        print("Success, all files present")
        description = f"""All the mandatory and non-mandatory files are present in landing location. """
        if copied_files:
            description = description + f"""<br> Following are the non-mandatory missing files and have been copied from archive location to raw landing location – {copied_files}"""
        send_email_notification(ENV_NAME, batch_date, "Succeeded", description)

except Exception as ex:
    print("ERROR: ", ex)
    print(traceback.format_exc())
    # TODO: make connection to DB and store error logs
    raise Exception(ex)
