'''
Template for etlcore framework v0.1.2 
Consumer: Developer team

'''

from etlcore import etlcore
from awsglue.utils import getResolvedOptions
import sys, datetime
from pyspark.context import SparkContext
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import lit
#import os
#import json
#from string import Template
#import uuid
from datetime import date,timedelta
from datetime import datetime as dt

import logging

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
try:
    handle=etlcore.Executor()
    args=getResolvedOptions(sys.argv, ['batch_date'])
    handle.batch_date=dt.strptime(args['batch_date'],'%Y%m%d').date()
    handle.initialize()
    handle.logger.setLevel(logging.DEBUG)
    overall_start = datetime.datetime.now()
    handle.play()
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