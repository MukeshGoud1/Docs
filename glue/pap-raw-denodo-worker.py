import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import os
import ast
from datetime import date, datetime

glue_args = getResolvedOptions(sys.argv, ['JOB_NAME','config_bucket'])


sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(glue_args['JOB_NAME'], glue_args)

client = boto3.client('glue')
redshift_secret_name = "pap-secret-denodo"
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

try:
    df = spark.read.jdbc(args['DENODO_JDBC_URL'], 'worker_l1_vw', properties = {"user":args['DENODO_USER'], "password":args['DENODO_PASSWORD'], "driver":args['DENODO_DRIVER']} )
    
    # df = spark.read.jdbc('jdbc:vdb://10.22.181.22:9999/ent', 'worker_l1_vw', properties = {"user":'svc_hra', "password":'Fm967E^n!@#$', "driver":'com.denodo.vdp.jdbc.Driver'} )
    print(df.count())
    
    current_datetime_format=datetime.now().strftime('%m%d%Y%H%M%S')
    print(glue_args) 
    write_bucket_name=glue_args['config_bucket']
    print(write_bucket_name)
    write_object_key=f"landing/denodo_worker_l1_vw_{current_datetime_format}.txt"
    write_path=f"s3://{write_bucket_name}/{write_object_key}"
    tmp_path=f"s3://{write_bucket_name}/landing/tmp/denodo_worker_l1_vw/"
    print(write_path)
    
    # df.write.option("header","true").mode("overwrite").csv(write_path)
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI(f"s3://{write_bucket_name}"), sc._jsc.hadoopConfiguration())
    df.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save(tmp_path)
    
    # rename created file
    created_file_path = fs.globStatus(Path(tmp_path + "part*.csv"))[0].getPath()
    fs.rename(created_file_path,Path(write_path))

except Exception as e:
    print("Error - ",str(e))
    raise e
job.commit()