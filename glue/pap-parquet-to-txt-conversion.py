import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
import os
from datetime import datetime, timedelta

glue_args = getResolvedOptions(sys.argv, ['JOB_NAME','input_output_path_list','batch_date'])
print(glue_args)
input_output_path_list = eval(glue_args['input_output_path_list'])
print(input_output_path_list)

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(glue_args['JOB_NAME'], glue_args)

def split_s3_path(s3_path):
    try:
        bucket = s3_path.split("s3://")[-1].split('/')[0]
        key = s3_path.split("s3://" + bucket + '/')[-1]
        return bucket, key
    except Exception as e:
        raise e

try:
    for i in input_output_path_list:
        df = spark.read.parquet(i['input_path'])
        df = df.select([col(column).alias(column.upper()) for column in df.columns])
        print(df.count())
        if "BATCH_DATE" in df.columns:
            df = df.drop("BATCH_DATE")
        #current_datetime_format=(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        print(i['output_path'])
        write_path=f"{i['output_path']}{i['filename']}_{glue_args['batch_date']}.txt"
        tmp_path=f"{i['output_path']}tmp/{i['filename']}/"
        print(write_path)
        write_bucket_name, write_path_key = split_s3_path(i['output_path'])
        URI = sc._gateway.jvm.java.net.URI
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(URI(f"s3://{write_bucket_name}"), sc._jsc.hadoopConfiguration())
        df.printSchema()
        df.coalesce(1).write.option("sep","||").option("encoding", "UTF-8").option("emptyValue", None).option("nullValue", None).format("csv").mode("overwrite").option("header","true").save(tmp_path)
        
        # rename created file
        created_file_path = fs.globStatus(Path(tmp_path + "part*.csv"))[0].getPath()
        s3Client = boto3.client('s3')
        write_bucket_name, write_path_key = split_s3_path(write_path)
        print(f"Deleting the file {write_bucket_name}/{write_path_key}")
        s3Client.delete_object(Bucket=write_bucket_name, Key=write_path_key)

        fs.rename(created_file_path,Path(write_path))
except Exception as e:
    raise e
job.commit()