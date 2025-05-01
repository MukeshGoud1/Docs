import os
import ast
import json
import traceback
import boto3
from datetime import date
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,col,when
from pyspark.sql.types import DecimalType,StringType,DateType,TimestampType

'''
1. Input Args: Either pass specifc path in input_file_path variable or assign it to be empty and all folders in data-
migration folder will be copied.
'''
tbl_dict = {}  # Create dictionary to track table name and progress
ENV_NAME = ''

def Copy_To_Processed_Bucket(df, path):
	print(f"Copying files to new location: {path}")
	df.write.mode("overwrite").parquet(f"{path}")

def Create_Hist_Record(s3_client):
	# This will create a JSON file which will have detail for hist run
	hist_record = {"batch_date": "20230314", "batch_id": "96b78b32-72d6-48f3-b1b2-5bb96f00bd1e", "batch_name": "PAP", 
	"status": "FINISHED", "start_time": "2023-03-14T17:02:30+00:00", "end_time": "2023-03-14T19:21:27+00:00"}
	s3_client.put_object(
		Body=json.dumps(hist_record),
		Bucket=f"gilead-gna-hr-{ENV_NAME['name']}-us-west-2-curated",
		Key=f'audit-table/pap-log-batch-dtl/batch_date={hist_record["batch_date"]}/PAP_log.json'
	)

def Update_Column_Name(df):
	df_column_list = df.columns
	correct_column_name = 'RECEIVED_FILE_NAME'
	if "RECEIEVED_FILE_ID" in df_column_list and "RECEIEVED_FILE_NAME" in df_column_list:
		print("Both RECEIEVED_FILE_ID & RECEIVED_FILE_NAME present")
		df = df.withColumnRenamed('RECEIEVED_FILE_NAME', correct_column_name)
		df = df.drop("RECEIEVED_FILE_ID")
	elif "RECEIEVED_FILE_NAME" in df_column_list:
		print("Only RECEIEVED_FILE_NAME present")
		df = df.withColumnRenamed('RECEIEVED_FILE_NAME', correct_column_name)
	elif "RECEIEVED_FILE_ID" in df_column_list:
		print("Only RECEIEVED_FILE_ID present")
		df = df.withColumn("RECEIEVED_FILE_ID",col("RECEIEVED_FILE_ID").cast(StringType())).withColumnRenamed('RECEIEVED_FILE_ID', correct_column_name)
	return df

def Add_SCD_Type_Two_Columns(df, name):
	scd_type_2_table_list = ["dim-pay-grade","dim-job"]
	if name in scd_type_2_table_list:
		print("SCD type 2 are needed, starting the process to add the columns")
		df = df.withColumn("start_dt_wid",lit("19000101").cast(DecimalType(10,0))).withColumn("end_dt_wid",lit("29991231").cast(DecimalType(10,0)))
		print("SCD type 2 column: start_dt_wid & end_dt_wid are added")
	return df

def Add_New_Columns_Prst(df, name):
	prst_list = ['fact-job-events','fact-job-events-snapshot'] #'stg-wkday-events-custom-prst','stg-wkday-events-job-prst','stg-wkday-events-mgr-prst',
	if name in prst_list:
		print("New Columns are needed for PRST tables, starting the process to add the columns")
		df = df.withColumn("TOTAL_SALARY_AND_ALLOWANCES",lit(None).cast(StringType())).withColumn("TARGET_BONUS_PERCENT",lit(None).cast(StringType())).withColumn("ANNUAL_BONUS_TARGET_AMOUNT",lit(None).cast(StringType())).withColumn("STOCK_TARGET_AMOUNT",lit(None).cast(StringType())).withColumn("STOCK_ACTUAL_RSU",lit(None).cast(StringType())).withColumn("STOCK_ACTUAL_PSU",lit(None).cast(StringType())).withColumn("STOCK_ACTUAL_NQ",lit(None).cast(StringType())).withColumn("STOCK_ACTUAL_PROMOTION_PSU",lit(None).cast(StringType())).withColumn("STOCK_CURRENCY",lit(None).cast(StringType()))
		print("9 New columns: TOTAL_SALARY_AND_ALLOWANCES, TARGET_BONUS_PERCENT, ANNUAL_BONUS_TARGET_AMOUNT, STOCK_TARGET_AMOUNT, STOCK_ACTUAL_RSU, STOCK_ACTUAL_PSU, STOCK_ACTUAL_NQ, STOCK_ACTUAL_PROMOTION_PSU, STOCK_CURRENCY are added")
	return df
	
def Add_New_Columns_Specific(df, name):
	if name == 'dim-cost-center-hier':
		print("New Columns are needed for Dim_COST_CENTER_HIER")
		df = df.withColumn("cc_desc",lit(None).cast(StringType())).withColumn("cc_legal_entity",lit(None).cast(StringType())).withColumn("cc_owner",lit(None).cast(StringType())).withColumn("functional_area",lit(None).cast(StringType())).withColumn("currency",lit(None).cast(StringType())).withColumn("product_link",lit(None).cast(StringType())).withColumn("market_associated",lit(None).cast(StringType())).withColumn("level8_val",lit(None).cast(StringType())).withColumn("level8_desc",lit(None).cast(StringType())).withColumn("hryid",lit(None).cast(StringType())).withColumn("level1_val",lit("Z0001").cast(StringType())).withColumn("level1_desc",lit("Total Cost Centers").cast(StringType())).withColumn("active_flag",lit(None).cast(StringType())).withColumn("cc_master_name",lit(None).cast(StringType())).withColumn("effective_end_date",lit(None).cast(DateType())).withColumn("functional_area_desc",lit(None).cast(StringType())).withColumn("company",lit(None).cast(StringType()))
	if name == 'dim-legal-entity-hier':
		print("New Columns are needed for Dim_Legal_Entity_Hier")
		df = df.withColumn("location",lit(None).cast(StringType())).withColumn("country",lit(None).cast(StringType())).withColumn("company",lit(None).cast(StringType())).withColumn("currency",lit(None).cast(StringType()))
	if name == 'dim-product-hier':
		print("Adding HRYID,level6_value,level6_desc columns to dim-product-hier")
		df = df.withColumn("hryid",lit(None).cast(StringType())).withColumn("level6_value",lit(None).cast(StringType())).withColumn("level6_desc",lit(None).cast(StringType())).withColumn("level1_value",lit("TOTALPROD").cast(StringType())).withColumn("level1_desc",lit("Total Products").cast(StringType()))
	if name == 'dim-stock-price':
		print("Casting  DATASOURCE_NUM_ID column of dim-stock-price")
		df = df.withColumn("datasource_num_id",df["datasource_num_id"].cast(DecimalType(10,0)))
	if name == 'dim-upd-feedback-survey':
		print("Casting SORT_KEY column of dim-upd-feedback-survey")
		df = df.withColumn("sort_key",df["sort_key"].cast(DecimalType(3,0)))
	if name == 'dim-worker':
		print("Casting ethnic_group_custom_sort_order column of dim-worker")
		df = df.withColumn("ethnic_group_custom_sort_order",df["ethnic_group_custom_sort_order"].cast(DecimalType(5,0))).withColumn("curr_org_size",lit(None).cast(DecimalType(20,0))).withColumn("curr_indirect_org",lit(None).cast(DecimalType(20,0)))
	if name == 'dim-job':
		print("Adding new column in dim-job")
		df = df.withColumn("scd2_active_flg",lit(None).cast(StringType()))
	if name == 'fact-mobility-expenses':
		print("Casting Row_ID column of fact-mobility-expenses")
		df = df.withColumn("row_id",df["row_id"].cast(DecimalType(10,0)))
	if name == "fact-job-events":
		print("Casting Small int column of fact-job-events")
		column_list = ["worker_evt_ind","supervisor_evt_ind","mgrlead_ind","prom_event_ind","hire_event_ind","term_event_ind",
		"event_ind","bldg_change_ind","scheduled_reviewcycle_ind","outofcycle_review_ind","annual_compensation_ind","midyear_compensation_ind",
		"le_change_ind","cc_change_ind","employment_change_ind","position_change_ind","rehire_event_ind","job_change_ind","location_change_ind",
		"dept_change_ind","supervisor_change_ind","internal_filled_req_ind","grade_change_ind","snapshot_ind","snapshot_month_start_ind",
		"snapshot_month_end_ind","last_month_in_qtr_ind","last_month_in_year_ind","emp_ind","max_seq_ind","leave_status",
		"payroll_status","grade_decrease_flag","term_status_ind"]
		for column in column_list:
			df = df.withColumn(column,df[column].cast(DecimalType(1,0)))
	if name == "fact-job-events-snapshot":
		print("Casting Small int column of fact-job-events-snapshot")
		column_list = ["worker_evt_ind","supervisor_evt_ind","mgrlead_ind","prom_event_ind","hire_event_ind","term_event_ind",
		"event_ind","bldg_change_ind","scheduled_reviewcycle_ind","outofcycle_review_ind","annual_compensation_ind","midyear_compensation_ind",
		"le_change_ind","cc_change_ind","employment_change_ind","position_change_ind","rehire_event_ind","job_change_ind","location_change_ind",
		"dept_change_ind","supervisor_change_ind","internal_filled_req_ind","grade_change_ind","snapshot_ind","snapshot_month_start_ind",
		"snapshot_month_end_ind","last_month_in_qtr_ind","last_month_in_year_ind","contractor_conversion_flg","leave_status","payroll_status",
		"grade_decrease_flag","movement_ind","emp_ind","max_seq_ind","term_status_ind"]
		for column in column_list:
			df = df.withColumn(column,df[column].cast(DecimalType(1,0)))
	if name == "fact-rcrtmnt-events-slates-t1":
		print("Casting Small int column of fact-rcrtmnt-events-slates-t1")
		column_list = ["us_score","nonus_score","female_score","ethnic_group_score","ethnic_hispanic_score","ethnic_black_score",
		"ethnic_asian_score","disability_score","veteranstatus_score","ethnic_amind_aknat_score","ethnic_nathi_othpacisn_score","ethnic_two_or_more_races_score",
		"lesbian_score","gay_score","bisexual_score","bisexual_score","transgender_score"]
		for column in column_list:
			df = df.withColumn(column,df[column].cast(DecimalType(1,0)))
		df = df.withColumn("worker_type_key",df["worker_type_key"].cast(DecimalType(10,0))).withColumn("replacement_worker_key",df["replacement_worker_key"].cast(DecimalType(20,0)))
	if name == "dim-pay-grade":
		print("Updating PAY_GROUP_GROUP column in dim-pay-grade table")
		df = df.withColumn("pay_grade_group",when(df["pay_grade_code"] == "85",lit("35+").cast(StringType())).when(df["pay_grade_code"] == "90",lit("35+").cast(StringType()))
		.when(df["pay_grade_code"] == "95",lit("35+").cast(StringType())).otherwise(df["pay_grade_group"]))
	if name == "mv-fact-vfw-badge-dtl":
		print("Casting leave_status,payroll_status column in mv-fact-vfw-badge-dtl")
		df = df.withColumn("leave_status",df["leave_status"].cast(DecimalType(1,0))).withColumn("payroll_status",df["payroll_status"].cast(DecimalType(1,0)))
	if name == "w-exch-rate-g":
		print("Casting start_dt_key & end_dt_key column in w-exch-rate-g")
		df = df.withColumn("start_dt_key",df["start_dt_key"].cast(DecimalType(19,0))).withColumn("end_dt_key",df["end_dt_key"].cast(DecimalType(19,0)))
	if name == "stg-wkday-events-job-prst":
		df_column_list = df.columns
		columns_list = ["ANNUAL_BONUS_TARGET_AMT","ANNUAL_BONUS_TARGET_PER","STOCK_ACTUAL_AMOUNT"]
		if "ANNUAL_BONUS_TARGET_AMT" in df_column_list and "ANNUAL_BONUS_TARGET_PER" in df_column_list and "STOCK_ACTUAL_AMOUNT" in df_column_list:
			df = df.drop("ANNUAL_BONUS_TARGET_AMT","ANNUAL_BONUS_TARGET_PER","STOCK_ACTUAL_AMOUNT")
	if name == "stg-wkday-rqstn-merge":
		print("Adding new column in stg-wkday-rqstn-merge")
		df = df.withColumn("recruiting_start_date",lit(None).cast(TimestampType())).withColumn("hashdiff",lit(None).cast(StringType())).withColumn("legal_entity",lit(None).cast(StringType()))
	if name == "dim-rqstn":
		print("Adding new column in dim-rqstn")
		df = df.withColumn("recruiting_start_date",lit(None).cast(TimestampType())).withColumn("confidential_job",lit(None).cast(StringType()))
	if name == "fact-rcrtmnt-events":
		print("Adding new column in fact-rcrtmnt-events")
		df = df.withColumn("recruiting_start_date",lit(None).cast(TimestampType()))
	if name == "stg-wkdy-rqsn-app-to-req-merge":
		print("Adding new column in stg-wkdy-rqsn-app-to-req-merge")
		df = df.withColumn("recruiting_start_date",lit(None).cast(TimestampType())).withColumn("legal_entity",lit(None).cast(StringType())).withColumn("rq_cc_hier_key",lit(None).cast(DecimalType(10,0)))
	return df

def Add_Datasource_Num_Id(df, name):
	if name == 'dim-source':
		print("Adding new datasource_num_id for EHANA in dim-source Table")
		columns = ["datasource_num_id","source_name","source_type","source_full_name","datasource_name","created_by_key","created_dt","updated_by_key","updated_dt"]
		newRow = spark.createDataFrame([(13,"EHANA","ATHENA","EHANA","MANUAL",100000001,'',100000001,''),(14,"RADFORD","FILE","RADFORD","MANUAL",100000001,'',100000001,'')], columns)
		newRow=newRow.withColumn("created_dt", F.current_timestamp()).withColumn("updated_dt", F.current_timestamp())
		newRow=newRow.withColumn("datasource_num_id", newRow["datasource_num_id"].cast(DecimalType(10,0))).withColumn("created_by_key", newRow["created_by_key"].cast(DecimalType(10,0))).withColumn("updated_by_key", newRow["updated_by_key"].cast(DecimalType(10,0)))
		df = df.union(newRow)
	return df

def Add_Additional_Data(dim_misc_attributes_original_df, name):
	# loading data from two files to dim-misc-attributes
	if name != "dim-misc-attributes":
		return dim_misc_attributes_original_df
	else:
		print(str(dim_misc_attributes_original_df.count()))
		dim_misc_attributes_original_df.createOrReplaceTempView("dim_misc_attributes_original")
		survey_df = spark.read.option("header",True).option("quote", "\"").option("escape", "\"").csv(f"s3://gilead-gna-hr-{ENV_NAME['name']}-us-west-2-code-artifacts/configuration/setup/dim-misc-attributes-extra-data/survey/")
		survey_df.createOrReplaceTempView("survey_data")
		print(f"add_data_df COUNT:{str(survey_df.count())}")
		df_original_survey = spark.sql('''
					WITH MAX_KEY AS (SELECT MAX(CAST(attribute_key AS DECIMAL(10,0))) AS MAX_ATTRIBUTE_KEY FROM dim_misc_attributes_original) 
					SELECT 
						CAST(attribute_key AS DECIMAL(10,0)) AS attribute_key,
						attribute_type,
						attr_misc_code,
						attr_misc_desc,
						datasource_num_id,
						created_dt,
						updated_dt,
						effective_start_dt,
						effective_end_dt 
					FROM dim_misc_attributes_original 
						UNION ALL   
					SELECT  
						CAST(ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id()) + 
						(SELECT MAX_ATTRIBUTE_KEY FROM MAX_KEY) AS DECIMAL(10,0)) AS attribute_key,
						CAST(SURVEY_TYPE AS STRING) AS attribute_type,
						CAST(KCS_SHORT_ITEM AS STRING) AS attr_misc_code,
						CAST(SURVEY_QUES_LONG_DESCR AS STRING) AS attr_misc_desc,
						CAST('5' AS DECIMAL(10,0)) AS datasource_num_id,
						current_timestamp() AS created_dt,
						current_timestamp() AS updated_dt,
						TO_TIMESTAMP('01-01-1900 00:00:00','MM-dd-yyyy HH:mm:ss') AS effective_start_dt,
						TO_TIMESTAMP('12-31-2099 00:00:00','MM-dd-yyyy HH:mm:ss') AS effective_end_dt 
					from survey_data''') 
		#df_original_survey.write.parquet("s3a://gilead-gna-hr-dev-us-west-2-processed/backup/pransh/misc-output/",mode="overwrite")
		print(f"df_original_survey COUNT:{str(df_original_survey.count())}")
		df_original_survey.createOrReplaceTempView("original_survey_data")
		classification_df = spark.read.option("header",True).option("quote", "\"").option("escape", "\"").csv(f"s3://gilead-gna-hr-{ENV_NAME['name']}-us-west-2-code-artifacts/configuration/setup/dim-misc-attributes-extra-data/classification/")
		classification_df.createOrReplaceTempView("classification_data")
		print(f"classification_df COUNT:{str(classification_df.count())}")
		df_original_survey_classification = spark.sql('''
					WITH MAX_KEY AS (SELECT MAX(CAST(attribute_key AS DECIMAL(10,0))) AS MAX_ATTRIBUTE_KEY FROM original_survey_data) 
					SELECT 
						CAST(attribute_key AS DECIMAL(10,0)) AS attribute_key,
						attribute_type,
						attr_misc_code,
						attr_misc_desc,
						datasource_num_id,
						created_dt,
						updated_dt,
						effective_start_dt,
						effective_end_dt 
					FROM original_survey_data 
						UNION ALL   
					SELECT  
						CAST(ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id()) + 
						(SELECT MAX_ATTRIBUTE_KEY FROM MAX_KEY) AS DECIMAL(10,0)) AS attribute_key,
						CAST('Upward_Feedback_Item_Category' AS STRING) AS attribute_type,
						CAST(BUSINESS_QUESTION AS STRING) AS attr_misc_code,
						CAST(CATEGORY AS STRING) AS attr_misc_desc,
						CAST('5' AS DECIMAL(10,0)) AS datasource_num_id,
						current_timestamp() AS created_dt,
						current_timestamp() AS updated_dt,
						TO_TIMESTAMP('01-01-1900 00:00:00','MM-dd-yyyy HH:mm:ss') AS effective_start_dt,
						TO_TIMESTAMP('12-31-2099 00:00:00','MM-dd-yyyy HH:mm:ss') AS effective_end_dt 
					from classification_data''')
		print(f"df_original_survey_classification COUNT:{str(df_original_survey_classification.count())}")
		return df_original_survey_classification

def DataType_Correction(df, name):
	prst_list = ['stg-wkday-events-custom-prst','stg-wkday-events-job-prst','stg-wkday-events-mgr-prst']
	fact_list = ['fact-employee-review']
	if name in prst_list:
		df= df.withColumn("TOTAL_SALARY_AND_ALLOWANCES",df["TOTAL_SALARY_AND_ALLOWANCES"].cast(StringType())).withColumn("TARGET_BONUS_PERCENT",df["TARGET_BONUS_PERCENT"].cast(StringType())).withColumn("ANNUAL_BONUS_TARGET_AMOUNT",df["ANNUAL_BONUS_TARGET_AMOUNT"].cast(StringType())).withColumn("STOCK_TARGET_AMOUNT",df["STOCK_TARGET_AMOUNT"].cast(StringType())).withColumn("STOCK_ACTUAL_RSU",df["STOCK_ACTUAL_RSU"].cast(StringType())).withColumn("STOCK_ACTUAL_PSU",df["STOCK_ACTUAL_PSU"].cast(StringType())).withColumn("STOCK_ACTUAL_NQ",df["STOCK_ACTUAL_NQ"].cast(StringType())).withColumn("STOCK_ACTUAL_PROMOTION_PSU",df["STOCK_ACTUAL_PROMOTION_PSU"].cast(StringType())).withColumn("STOCK_CURRENCY",df["STOCK_CURRENCY"].cast(StringType()))
	elif name in fact_list:
		df = df.withColumn("SUP_HIER_KEY",df["SUP_HIER_KEY"].cast(DecimalType(10,0)))
	return df


try:

	try:   
		region_name = os.environ['AWS_DEFAULT_REGION']
		session = boto3.session.Session()
		client = session.client(service_name='secretsmanager',region_name=region_name)
		get_role_secret = client.get_secret_value(SecretId='pap-secret-env-name')
		ENV_NAME = ast.literal_eval(get_role_secret['SecretString'])
	except Exception as e:
		print("Error While Fetching secrets")
		raise e
	s3client = boto3.client('s3')
	response = s3client.get_bucket_encryption(
		Bucket=f"gilead-gna-hr-{ENV_NAME['name']}-us-west-2-processed"
	)
	processed_bucket_kms_key = response["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"]
	sc = SparkContext()
	conf = sc.getConf()
	conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
	conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
	conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
	conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
	conf.set("spark.hadoop.fs.s3a.server-side-encryption.key",processed_bucket_kms_key)
	conf.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm","SSE-KMS")
	sc.stop()
	sc = SparkContext.getOrCreate(conf=conf)
	glueContext = GlueContext(sc)
	spark = glueContext.spark_session
	job = Job(glueContext)

	bucket_name=f"gilead-gna-hr-{ENV_NAME['name']}-us-west-2-processed"
	input_key = "data-migration/"
	
	batch_date = date.today()
	# IMP: BE CAREFUL WHILE RUNNING THIS JOB WITH 'input_file_path' = '' AS THIS WILL OVERWRITE THE PROCESSED BUCKET DATA 
	#input_file_path = "s3://gilead-gna-hr-dev-us-west-2-processed/data-migration/dim-cost-center-hier/"
	#input_file_path = "s3://gilead-gna-hr-dev-us-west-2-processed/data-migration-pransh/dim-rqstn/"
	#input_file_path = "s3://gilead-gna-hr-dev-us-west-2-processed/data-migration/fact-job-events/"
	#input_file_path =""
	input_file_path = "s3://gilead-gna-hr-tst-us-west-2-processed/data-migration/stg-wkdy-rqsn-app-to-req-merge/"
	if input_file_path:
		tbl_dict[input_file_path]= "IN PROGRESS"
		df = spark.read.parquet(input_file_path)
		df = Update_Column_Name(df)
		table_name = input_file_path.split('/')[4]
		output_path = f"s3a://{bucket_name}/{table_name}/batch_date={batch_date}/"
		#output_path = f"s3a://{bucket_name}/backup/pransh/{table_name}/batch_date={batch_date}/"

		df = Add_SCD_Type_Two_Columns(df, table_name)
		df = Add_New_Columns_Prst(df, table_name)
		df = Add_New_Columns_Specific(df, table_name)
		df = Add_Datasource_Num_Id(df, table_name)
		df = Add_Additional_Data(df, table_name)
		df = DataType_Correction(df, table_name)
		Copy_To_Processed_Bucket(df, output_path)	
		print(f"Process Completed for {input_file_path}")
		tbl_dict[input_file_path]= output_path
	else:
		s3_client = boto3.client('s3')
		Create_Hist_Record(s3_client)
		response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{input_key}", Delimiter= "/")
		if response["KeyCount"] == 0:
			raise Exception(f"File Path {bucket_name}/{input_key} does not exist.")
		for item in response["CommonPrefixes"]:
			tbl_dict[f"s3a://{bucket_name}/{item['Prefix']}"]= "IN PROGRESS"
			df = spark.read.parquet(f"s3a://{bucket_name}/{item['Prefix']}")
			df = Update_Column_Name(df)
			table_name = item['Prefix'].split("/")[1]
			output_path = f"s3a://{bucket_name}/{item['Prefix']}batch_date={batch_date}/".replace(input_key,"")
			df = Add_SCD_Type_Two_Columns(df,table_name)
			df = Add_New_Columns_Prst(df,table_name)
			df = Add_New_Columns_Specific(df, table_name)
			df = Add_Datasource_Num_Id(df, table_name)
			df = Add_Additional_Data(df, table_name)
			df = DataType_Correction(df, table_name)
			Copy_To_Processed_Bucket(df, output_path)
			print(f"Process Completed for {item['Prefix']}")
			tbl_dict[f"s3a://{bucket_name}/{item['Prefix']}"]= output_path
except Exception as ex:
	print("ERROR: ",ex)
	print(traceback.format_exc())
	raise Exception(ex)
finally:
	print(f"\n SUMMARY:{tbl_dict}")