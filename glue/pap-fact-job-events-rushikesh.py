'''
Template for etlcore framework. 
Consumer: Developer team
'''

from etlcore import etlcore
from awsglue.utils import getResolvedOptions
import sys, datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import lit
import os
import json
from string import Template
import uuid
from datetime import date,timedelta
import boto3 #temporary
from botocore.errorfactory import ClientError #temporary
import logging
import pandas as pd
from dateutil import parser
import datetime
import pandas as pd
from datetime import timedelta
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from awsglue.job import Job
import numpy as np
from datetime import datetime as dt
#sc1 = SparkContext()
#sc1 = SparkContext.getOrCreate()
#glue_context1 = GlueContext(sc1)
#spark1 = glue_context1.spark_session
#job1 = Job(glue_context1)

handle = None

#use custom_play method only when a given etl job cannot be implemented by generic framework method - play. 
'''
def custom_play(handle):
        """
        Orchestrates sequence of task - extract, transform and load
        """
        if handle.config.get("jobs") is not None:
            for job in handle.config["jobs"]:
                handle.logger.info("The job name is (%s)" %(job["name"]))
                handle.extract(job)               #Extract from Sources
                handle.apply_transformation(job)  #Transforms
                handle.load(job)                  #Load to Target
        else:
            handle.logger.log("There is no jobs property. Therefore skipping...")
'''
 
def DAY_BETWEEN(sdate,edate):
    if sdate is not None and edate is not None:
        biz_days = np.busday_count(sdate,edate) 
    else:
        biz_days = 0
    return int(biz_days)

    
try:
    handle=etlcore.Executor()
    args=getResolvedOptions(sys.argv, ['batch_date'])
    handle.batch_date=dt.strptime(args['batch_date'],'%Y-%m-%d').date()
    handle.initialize()
    handle.logger.setLevel(logging.DEBUG)
    overall_start = datetime.datetime.now()
    #sc1 = SparkContext.getOrCreate()
    #glue_context1 = GlueContext(sc1)
    #spark1 = glue_context1.spark_session
    #handle.spark.udf.register("DAY_BETWEEN", DAY_BETWEEN, IntegerType())
    handle.play()
    # source_qualifier_iasg_st = handle.spark.sql('select * from source_qualifier_iasg_st')
    # print(source_qualifier_iasg_st.count())
    
    # tv_stg_wkday_events_union_iasg_st  = handle.spark.sql('select * from tv_stg_wkday_events_union_iasg_st')
    # print(tv_stg_wkday_events_union_iasg_st.count())
    
    overall_finish = datetime.datetime.now()
    handle.logger.info("Total time taken : %s" % (str(overall_finish-overall_start)))
    handle.wind_up()
    

except Exception as e:
    handle.logger.error("Error occured while executing the job on etlcore framework!!!")
    handle.audit_job_record['status']="FAILED"
    handle.audit_job_record['et']=datetime.datetime.now()
    handle.audit_job_record['msg']=str(e)
    handle.log_job_dtl()
    raise #re-raise