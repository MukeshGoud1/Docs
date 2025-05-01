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

sns_client = boto3.client('sns')


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

    
config_file_bucket, config_file_key = split_s3_path(config_file_path)
data = s3.get_object(Bucket=config_file_bucket, Key=config_file_key)
config_df = pd.read_csv(data['Body'])

#Req_File = config_df["SOURCE_FILES"].values.tolist()
req_file = config_df[config_df["MANDATORY_FLAG"] == "Y" ]["SOURCE_FILES"].values.tolist()
print(req_file)

total_file_bucket, total_file_key = split_s3_path(folder_location)
my_bucket = s3_resource.Bucket(total_file_bucket)

for object_summary in my_bucket.objects.filter(Prefix=total_file_key):
    file_name=object_summary.key.split("/")[-1]
    if len(file_name)>=1:
        list1.append(file_name)
    
print(list1)

# req_file=["b.txt","A.txt","s.txt"]

list2= [x for x in req_file]


def subscribe(topic, protocol, endpoint):
    """
    :param topic: The topic to subscribe to.
    :param protocol: The protocol of the endpoint, such as 'sms' or 'email'.
    :param endpoint: The endpoint that receives messages, such as a phone number
                     (in E.164 format) for SMS messages, or an email address for
                     email messages.
    :return: The newly added subscription.
    """
    subscription = sns_client.subscribe(
            TopicArn=topic, Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True)
    return subscription

def create_topic(name):
    """
    Creates a notification topic.

    :param name: The name of the topic to create.
    :return: The newly created topic.
    """
    topic = sns_client.create_topic(Name=name)
    return topic['TopicArn']


def file_check(landing_file,mandatory_file):

    source_files=[]
    
    for file in landing_file:
        filename =Path(file).stem
        x=filename.split("_")
        s="_"
        if x[-1].isdigit():
            s=s.join(x[0:-1])
        else:
            s=s.join(x)
        source_files.append(s)
        
    #CONVERTING list of mandatory files to set
    mandatory_file=set(mandatory_file)
    #CONVERTING list of files present in raw landing bucket to set
    source_files=set(source_files)
    
    #set difference gives mandatory files that are not present in raw landing
    missing_file = mandatory_file.difference(source_files)
    missing_file=list(missing_file)
    #missing_file=[]
    if missing_file:
        print(f"FAIL \nmissing files : {missing_file}")
        
        return False , missing_file
        #fail dag
    else:
        print("success")
        return True , True
    #result =  all(elem in list1  for elem in list2)
    #return(result)
    
    #filename =Path('stamped_file_name_08537376767677.csv').stem
    #filename = re.sub(r"_\d{14}$", "", filename)
    #print(filename)
try:
    
    file_check_status , missing_file_list = file_check(list1,list2)
    
    if file_check_status:
        print("Success, all files present")
    else :
        topic_name = f'raw-missing-files-topic-{time.time_ns()}'

        print(f"Creating topic {topic_name}.")
        #Create topic
        topicArn = create_topic(topic_name)
        topicArn="arn:aws:sns:us-west-2:285936246797:mandatory-file-check"
        #Create email subscription
        mail_id = config_df["PERSON_MAIL_ID"].values.tolist()
        #mail_id = ['rushikesh.sakhare@gilead.com']
        response = subscribe(topicArn, "email", mail_id[0])
        
        #Publish to topic
        sns_client.publish(TopicArn=topicArn,
            Message=f"Following Mandatory Files are missing : {missing_file_list}",
            Subject="Missing Mandatory Files")
        
        raise Exception(f"Following Mandatory Files are missing {missing_file_list}")

except Exception as ex:
    print("ERROR: ", ex)
    print(traceback.format_exc())
    # TODO: make connection to DB and store error logs
    raise Exception(ex)
