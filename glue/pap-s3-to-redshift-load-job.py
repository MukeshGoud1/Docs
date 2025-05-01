import os
import sys
import boto3
import ast
import psycopg2
from awsglue.utils import getResolvedOptions

def Get_Redshift_Connection():
  client = boto3.client('glue')
  redshift_secret_name = "pap-secret-redshift"
  role_secret_name = "pap-secret-redshift-copy-role-arn"
  region_name = os.environ['AWS_DEFAULT_REGION']
  session = boto3.session.Session()
  client = session.client(service_name='secretsmanager',region_name=region_name)
  try:
        get_redshift_secret = client.get_secret_value(SecretId=redshift_secret_name)
        get_role_secret = client.get_secret_value(SecretId=role_secret_name)
  except Exception as e:
        print("Error While Fetching secrets")
        raise e
  connection_properties = ast.literal_eval(get_redshift_secret['SecretString'])
  role_arn = ast.literal_eval(get_role_secret['SecretString'])
  database = connection_properties["database"]
  user = connection_properties["username"]
  password = connection_properties["password"]
  port = connection_properties["port"]
  host=connection_properties["host"]
  try:
    conn = psycopg2.connect(
      dbname=database,
      user=user,
      password=password,
      port=port,
      host=host)
    return conn,role_arn['role_arn']
  except Exception as ERROR:
    print("Issue while connecting to Redshift: " + str(ERROR))

def Validate_Path(name, schema = "", cursor=""):
    if cursor:
        exists_query = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{name}')"
        print(exists_query)
        cursor.execute(exists_query)
        if not cursor.fetchone()[0]:
            raise Exception(f"ERROR:{name} Table does not exist in Redshift")
    else:
        # s3://gilead-gna-hr-dev-us-west-2-processed/fact-rcrtmnt-events/
        s3_client = boto3.client('s3')
        corrected_s3_path, nameArray = "", name.split("/")
        pref_key = ('/').join(nameArray[3:])
        response = s3_client.list_objects_v2(Bucket=nameArray[2], Prefix=f"{pref_key}")
        if response["KeyCount"] == 0:
            raise Exception(f"ERROR:File Path {name} does not exist.")
        for item in response["Contents"]:
            if ".parquet" in item["Key"]:
                split_item_key = item['Key'].split('/')
                corrected_s3_path = f"s3://{nameArray[2]}/{'/'.join(split_item_key[:len(split_item_key)-1])}/"
                break
        return corrected_s3_path

try:
    table_dictionary_dms = {"dim_department":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-department/",
"dim_supervisor_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-supervisor-hier/",
"dim_cost_center_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-cost-center-hier/",
"dim_department_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-department-hier/",
"dim_pay_grade":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-pay-grade/",
"dim_worker":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-worker/",
"dim_wrkfc_event_type":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-wrkfc-event-type/",
"dim_disposition_reasons":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-disposition-reasons/",
"dim_job":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-job/",
"dim_legal_entity_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-legal-entity-hier/",
"dim_product_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-product-hier/",
"dim_rqstn":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-rqstn/",
"dim_source":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-source/",
"dim_status":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-status/",
"dim_worker_ethnicity":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-worker-ethnicity/",
"dim_worker_type":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-worker-type/",
"dim_emp_demographics":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-emp-demographics/",
"dim_location_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-location-hier/",
"dim_position":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-position/",
"dim_sec_user_loc":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sec-user-loc/",
"dim_service_band":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-service-band/",
"dim_supervisor_hier_curr":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-supervisor-hier-curr/",
"lnk_dim_rcrtmnt_source":"s3://gilead-gna-hr-tst-us-west-2-processed/lnk-dim-rcrtmnt-source/",
"dim_age_band":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-age-band/",
"dim_calendar":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-calendar/",
"dim_intl_assign":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-intl-assign/",
"dim_misc_attributes":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-misc-attributes/",
"dim_perf_band":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-perf-band/",
"dim_sec_user_leader":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sec-user-leader/",
"dim_sec_user_resp":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sec-user-resp/",
"lnk_dim_rcrtmnt_event_type":"s3://gilead-gna-hr-tst-us-west-2-processed/lnk-dim-rcrtmnt-event-type/",
"dim_award_plan":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-award-plan/",
"dim_budget":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-budget/",
"dim_equity_cancelled":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-equity-cancelled/",
"dim_gl_account":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-gl-account/",
"dim_gl_departments_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-gl-departments-hier/",
"dim_job_grade_salranges":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-job-grade-salranges/",
"dim_learning_course_vw":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-learning-course-vw/",
"dim_mobility_demographics":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-mobility-demographics/",
"dim_mobility_glaccount":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-mobility-glaccount/",
"dim_mobility_servicescores":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-mobility-servicescores/",
"dim_numofdirects_vw":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-numofdirects-vw/",
"dim_review_category":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-review-category/",
"dim_sec_gm_exception":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sec-gm-exception/",
"dim_sec_user_resp_vw":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sec-user-resp-vw/",
"dim_stock_price":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-stock-price/",
"dim_sup_hier_rcrt":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-sup-hier-rcrt/",
"dim_survey_questions":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-survey-questions/",
"dim_term_theme":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-term-theme/",
"dim_upd_feedback_survey":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-upd-feedback-survey/",
"dim_upward_feedback":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-upward-feedback/",
"dim_worker_term_themes":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-worker-term-themes/",
"dim_employee_hier":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-employee-hier/",
"dim_people_manager_vw":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-people-manager-vw/",
"dim_upd_feedback_comm":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-upd-feedback-comm/",
"fact_job_events":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-job-events/",
"fact_mobility_expenses":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-mobility-expenses/",
"fact_position_events":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-position-events/",
"dim_worker_desk_loc":"s3://gilead-gna-hr-tst-us-west-2-processed/dim-worker-desk-loc/",
"fact_employee_survey":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-employee-survey/",
"fact_award":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-award/",
"fact_equity":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-equity/",
"fact_job_events_snapshot":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-job-events-snapshot/",
"fact_rcrtmnt_events":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-rcrtmnt-events/",
"fact_upd_feedback_survey":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-upd-feedback-survey/",
"fact_rcrtmnt_events_slates_t1":"s3://gilead-gna-hr-tst-us-west-2-processed/fact-rcrtmnt-events-slates-t1/",
"mv_fact_vfw_badge_dtl":"s3://gilead-gna-hr-tst-us-west-2-processed/mv-fact-vfw-badge-dtl/","mv_job_tiercode_country":"s3://gilead-gna-hr-tst-us-west-2-processed/mv-job-tiercode-country/","w_exch_rate_g":"s3://gilead-gna-hr-tst-us-west-2-processed/w-exch-rate-g/"}
    table_dictionary = {"dim_department":"s3://gilead-gna-hr-prd-us-west-2-processed/dim-department/"}
    schema = "gna_load_schema"
    redshift_conn = ""
    override_default_args = True
    if override_default_args:
        args = getResolvedOptions(sys.argv,['s3-path','table-name','schema-name'])
        schema,table_name,s3_path,redshift_conn = args['schema_name'].strip(),args['table_name'].strip(),args['s3_path'].strip(),""
        if schema == "" or table_name == "" or s3_path == "":
            print("Input argument/s are Empty.")
            raise Exception("ERROR:Empty Arguments.")
        table_dictionary = {table_name.lower().replace("-","_"): s3_path}

    redshift_conn,iam_role_arn = Get_Redshift_Connection()
    for table_name in table_dictionary:
        s3_path_with_batch = ""
        table_name = table_name.lower().replace("-","_")
        cursor = redshift_conn.cursor()
        print(f"Table:{table_name}")
        Validate_Path(table_name,schema,cursor)
        truncate_query = f"TRUNCATE TABLE {schema}.{table_name}"
        if table_name == "pap_log_batch_dtl":
            print("Updating DAG Status to Table pap_log_batch_dtl")
            copy_from_s3_query = f"COPY {schema}.{table_name} FROM '{table_dictionary[table_name]}' IAM_ROLE '{iam_role_arn}' JSON 'auto ignorecase'"
        else:
            s3_path_with_batch = Validate_Path(table_dictionary[table_name],"","")
            copy_from_s3_query = f"COPY {schema}.{table_name} FROM '{s3_path_with_batch}' IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET"
        print(f"truncate_query:{truncate_query}")
        print(f"copy_from_s3_query:{copy_from_s3_query}")
        cursor.execute(truncate_query)
        cursor.execute(copy_from_s3_query)
        redshift_conn.commit()
        print(f"{table_name} table moved to redshift.")
    print("JOB IS COMPLETED")
except Exception as ERROR:
      print("Execution Issue: " + str(ERROR))
      exit(1)
finally:
    if redshift_conn:
          redshift_conn.close()