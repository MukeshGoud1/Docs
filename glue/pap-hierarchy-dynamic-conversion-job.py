'''
Template for etlcore framework v0.1.2 
Consumer: Developer team

'''

from etlcore import etlcore
from awsglue.utils import getResolvedOptions
import sys, datetime
from pyspark.context import SparkContext
from datetime import date,timedelta
from datetime import datetime as dt

import json
import logging

handle = None

def create_sql(column, hier_start_date, hier_end_date, date_column , use_dim_cal ,count, table_name):
    dim_type = ''
    if column == "CC_HIER_KEY" or column == "CC_HIER_PRV_KEY": 
        lookup_hashmap = 'cost_center_hashmap'
        dim_type = 'CC'
    elif column == "DEPT_HIER_KEY" or column == "DEPT_HIER_PRV_KEY":
        lookup_hashmap = 'department_hier_hashmap'
        dim_type = 'DEPT'
    elif column == "LOC_HIER_KEY" or column == "LOC_HIER_PRV_KEY":
        lookup_hashmap = 'location_hashmap'
        dim_type = 'LOC'
    elif column == "LE_HIER_KEY" or column == "LE_HIER_PRV_KEY":
        lookup_hashmap = 'legal_entity_hashmap'
        dim_type = 'LE'
    elif column == "DEPT_KEY" or column == "DEPT_PRV_KEY":
        lookup_hashmap = 'department_hashmap'
        dim_type = 'DEPT'
    if table_name in ["fact-rcrtmnt-events","fact-employee-survey","fact-award","fact-empl-upward-feedback","fact-equity","fact-employee-review","fact-position-events","fact-job-events-snapshot","fact-job-events"]:
        if use_dim_cal == "TRUE":
            part_query = f" CASE WHEN ((TO_DATE('{hier_start_date[dim_type]}','yyyy-MM-dd') <= cal.calendar_dt AND cal.calendar_dt <=  TO_DATE('{hier_end_date[dim_type]}','yyyy-MM-dd')) AND cch_{count}.UPDATED_KEY != 0) THEN cch_{count}.UPDATED_KEY ELSE ot.{column} END AS new_{column} "
        else:
            part_query = f" CASE WHEN ((TO_DATE('{hier_start_date[dim_type]}','yyyy-MM-dd') <= ot.{date_column} AND ot.{date_column} <=  TO_DATE('{hier_end_date[dim_type]}','yyyy-MM-dd')) AND cch_{count}.UPDATED_KEY != 0) THEN cch_{count}.UPDATED_KEY ELSE ot.{column} END AS new_{column} "
    else:
        if use_dim_cal == "TRUE":
            part_query = f" CASE WHEN (TO_DATE('{hier_start_date[dim_type]}','yyyy-MM-dd') <= cal.calendar_dt AND cal.calendar_dt <=  TO_DATE('{hier_end_date[dim_type]}','yyyy-MM-dd')) THEN cch_{count}.UPDATED_KEY ELSE ot.{column} END AS new_{column} "
        else:
            part_query = f" CASE WHEN (TO_DATE('{hier_start_date[dim_type]}','yyyy-MM-dd') <= ot.{date_column} AND ot.{date_column} <=  TO_DATE('{hier_end_date[dim_type]}','yyyy-MM-dd')) THEN cch_{count}.UPDATED_KEY ELSE ot.{column} END AS new_{column} "

    part_join = f" LEFT OUTER JOIN {lookup_hashmap} cch_{count} ON ot.{column} = cch_{count}.ORIG_KEY "
    return part_query,part_join
#use custom_play method only when a given etl job cannot be implemented by generic framework method - play. 
def custom_play(handle):
        """
        Orchestrates sequence of task - extract, transform and load
        """
        if handle.config.get("jobs") is not None:
            for job in handle.config["jobs"]:
                #original_table_df = handle.spark.read.parquet(f"s3://{handle.PROCESSED_BUCKET}/backup/pransh/{args['table_name']}/")
                sql_dict = {}
                handle.logger.info("The job name is (%s)" %(job["name"]))
                if job["name"] == "PAP_Hierarchy_Dynamic_Conversion_Job_Backup":
                    print("1")
                    job["sources"].append({'object': f's3://{handle.PROCESSED_BUCKET}/{args["table_name"]}/', 'view': 'original_table_view','lookup':'target'})
                    print("2")
                    print(job)
                    handle.extract(job)               #Extract from Sources
                    job["targets"]["target_location"] = job["targets"]["target_location"] + f"{args['table_name']}-before-conv/"
                    handle.load(job)                  #Load to Target
                else:
                    hier_start_date = json.loads(handle.app_config["environment"]["HIER_CONVERSION_START_DT"])
                    hier_end_date = json.loads(handle.app_config["environment"]["HIER_CONVERSION_END_DT"])
                    table_column_list = args["column_names"].split(",")
                    part_query_final,part_join_final = "",""
                    if args["use_dim_calendar"] == "TRUE":
                        part_join_final = f" LEFT OUTER JOIN dim_calendar cal ON ot.{args['date_column_name']} = cal.CAL_DATE_KEY "
                    column_list_count = len(table_column_list)
                    cnt = 0 
                    for column in table_column_list:
                        cnt += 1
                        part_query,part_join = create_sql(column,hier_start_date,hier_end_date, args["date_column_name"], args["use_dim_calendar"], str(cnt),args['table_name'])
                        part_query_final += part_query
                        part_join_final += part_join
                        if cnt != column_list_count:
                            part_query_final += ","

                    sql_dict['sql'] = f'CREATE TEMPORARY VIEW intermediate_hierarchy_output AS SELECT ot.*,{part_query_final} FROM original_table_view ot {part_join_final}'
                    job['transforms'].append(sql_dict)
                    handle.extract(job)               #Extract from Sources
                    handle.apply_transformation(job)  #Transforms
                    updated_df = handle.spark.sql("SELECT * FROM intermediate_hierarchy_output")
                    drop_column_list = []
                    for column in table_column_list:
                        new_column = f"new_{column}"
                        drop_column_list.append(new_column)
                        updated_df=updated_df.withColumn(column, updated_df[f"{new_column}"])
                    print(tuple(drop_column_list))
                    updated_df=updated_df.drop(*(tuple(drop_column_list)))
                    print(f"COLUMN COUNT: {len(updated_df.columns)}")
                    updated_df.createOrReplaceTempView("final_hierarchy_output")
                    job["targets"]["target_location"] = job["targets"]["target_location"] + f"{args['table_name']}/"
                    handle.load(job)                  #Load to Target
        else:
            handle.logger.log("There is no jobs property. Therefore skipping...")
try:
    handle=etlcore.Executor()
    args=getResolvedOptions(sys.argv, ['batch_date','table_name','column_names','use_dim_calendar','date_column_name'])
    handle.batch_date=dt.strptime(args['batch_date'],'%Y%m%d').date()
    handle.initialize()
    handle.logger.setLevel(logging.DEBUG)
    overall_start = datetime.datetime.now()
    #handle.play()
    custom_play(handle)
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