#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Publish Handler
#  Purpose             :   This module will perform data copy to publish layer
#  Input Parameters    :
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   8 June 2018
#  Last changed by     :   Amal Kurup
#  Reason for change   :   Enhancement to publish files at batch level
# ######################################################################################################################

# Library and external modules declaration
import os
import traceback
import sys
#import pydoop.hdfs.path as hpath

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from S3MoveCopyUtility import S3MoveCopyUtility
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager

from awsglue.utils import getResolvedOptions

# all module level constants are defined here
MODULE_NAME = "PublishHandler"
PROCESS_NAME = "Publish"


class PublishHandler:

    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the publish copy (main function)
    # Input              :   file_master_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list
    # Output             :   NA
    # ##################################################################################################################
    def execute_publish_copy(self, file_master_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        skip_publish_flag = None
        file_name = None
        log_info['process_name'] = PROCESS_NAME
        file_process_status = ""
        update_log_file_dtl_flag = True
        try:
            status_message = "Executing copy of files to publish layer"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if not file_master_id:
                raise Exception(CommonConstants.DATASET_ID_NOT_PROVIDED)
            elif not batch_id:
                raise Exception(CommonConstants.BATCH_ID_NOT_PROVIDED)
            elif not cluster_id:
                raise Exception(CommonConstants.CLUSTER_ID_NOT_PROVIDED)
            elif not workflow_id:
                raise Exception(CommonConstants.WORKFLOW_ID_NOT_PROVIDED)
            elif not process_id:
                raise Exception(CommonConstants.PROCESS_ID_NOT_PROVIDED)
            elif not batch_id_list:
                raise Exception(CommonConstants.BATCH_ID_LIST_NOT_PROVIDED)

            self.execution_context.set_context({"file_master_id": file_master_id})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get post dqm and publish location from MySQL table for given dataset id
            data_set_info = CommonUtils().get_dataset_info(file_master_id)

            # Validate if dataset exists for provided dataset id
            if not data_set_info:
                raise Exception(CommonConstants.DATASET_NOT_PRESENT_FOR_ID)

            publish_flag = data_set_info['dataset_publish_flag']

            # Fetch post dqm location and validate if it is empty
            fetch_handle_empty_file = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "handle_empty_file"])
            handle_empty_file: bool = False if fetch_handle_empty_file is None else fetch_handle_empty_file
            post_dqm_location = data_set_info['s3_post_dqm_location'] if handle_empty_file is False else data_set_info['s3_landing_location']

            # Fetch publish location and validate if it is empty
            publish_location = data_set_info['publish_s3_path']

            if publish_flag != 'Y':
                skip_publish_flag = 'Y'
            else:
                if not publish_location:
                    raise Exception(CommonConstants.PUBLISH_LOC_NOT_PROVIDED)
                if not post_dqm_location:
                    raise Exception(CommonConstants.POST_DQM_LOC_NOT_PROVIDED)

            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, file_master_id)

            # Validate if file details exist for current batch
            if not file_details:
                raise Exception(CommonConstants.MISSING_FILE_DETAILS_FOR_BATCH_ID)

            # write a for loop here to make entry in the table for all files under the batch as In-Progress
            file_failed_flag = False
            file_not_processed_flag = False
            for file_detail in file_details:
                # Get detail for each file
                # Logic added for restartability
                file_id = file_detail['file_id']
                file_name = file_detail['file_name']

                if not file_id or not file_name:
                    raise Exception(CommonConstants.INVALID_FILE_AUDIT_DETAILS)
                log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                            CommonConstants.FILE_NAME: file_name,
                            CommonConstants.DATASET_ID: file_master_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }

                # Logic added for restartability
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{publish}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=file_master_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         publish=CommonConstants.FILE_PROCESS_NAME_PUBLISH)

                result = MySQLConnectionManager().execute_query_mysql(query)
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    if file_process_status == "":
                        file_not_processed_flag = True
                        # Creating process log entry
                        CommonUtils().create_process_log_entry(self.audit_db, log_info)
                        status_message = "Created process log entry for file id " + str(file_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True

            # move files to s3 at batch level
            try:
                if file_failed_flag or file_not_processed_flag:

                    if skip_publish_flag != 'Y':
                        post_dqm_url = post_dqm_location.rstrip('/').__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))
                        publish_url = publish_location.rstrip('/').__add__('/').__add__(
                            CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id))

                        # Invoke Publish Utility
                        #if not hpath.exists(post_dqm_url)
                            #raise Exception("Post DQM location " + str(post_dqm_url) + " doesn't exist")

                        #publish_cpy_cmd = "python ClusterToS3LoadHandler.py  -s " + post_dqm_url + " -t " + publish_url
                        #CommonUtils().execute_shell_command(publish_cpy_cmd)

                        #S3MoveCopyUtility(self.execution_context).copy_s3_to_s3_recursive_with_s3cmd(post_dqm_url,
                        #                                                                   str(publish_url))


                        module_name_for_spark = str(MODULE_NAME + "_dataset_" + file_master_id + "_process_" + process_id)
                        spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark)
                        df = spark_context.read.parquet(post_dqm_url)

                        if (df.count() == 0) and handle_empty_file :
                            df.write.mode('overwrite').format('parquet').save(publish_url)
                        else:
                            df.write.mode('overwrite').format('parquet').partitionBy(CommonConstants.PARTITION_FILE_ID).save(publish_url)



                        status_message = "Publish step is completed successfully"
                        logger.debug(status_message, extra=self.execution_context.get_context())

                        log_info[CommonConstants.BATCH_ID] = curr_batch_id
                        log_info['status_id'] = CommonConstants.SUCCESS_ID
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                        log_info['message'] = status_message
                        record_count = PySparkUtility(self.execution_context).get_parquet_record_count(post_dqm_url)
                        log_info['record_count'] = record_count
                    else:
                        status_message = "Skipped publish step"
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        log_info[CommonConstants.PROCESS_STATUS] = "SKIPPED"
                        log_info['status_id'] = CommonConstants.SUCCESS_ID
                        record_count = str(0)
                        log_info['record_count'] = str(record_count)
                        log_info['message'] = status_message

                else:
                    update_log_file_dtl_flag = False
                    status_message = 'Batch ID : ' + str(
                        batch_id) + ' is already processed'
                    logger.debug(status_message, extra=self.execution_context.get_context())

            except Exception as exception:
                status_message = "Publish Failed For Batch Id " + str(batch_id)
                self.execution_context.set_context({"batch_id": batch_id})
                batches = batch_id_list.split(",")
                for batch in batches:
                    log_info[CommonConstants.BATCH_ID] = batch
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info['record_count'] = str(0)
                    log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                          "").replace(
                        '"', '')

                    CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                    CommonUtils().update_batch_status(self.audit_db, log_info)
                log_info[CommonConstants.BATCH_ID] = batch_id
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                      "").replace(
                    '"',
                    '')
                error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                        " ERROR MESSAGE: " + str(traceback.format_exc())
                self.execution_context.set_context({"traceback": error})
                logger.error(status_message, extra=self.execution_context.get_context())
                self.execution_context.set_context({"traceback": ""})
                self.exc_info = sys.exc_info()
                raise exception
            finally:
                status_message = "In  Finally Block for Publish at Batch Level"
                logger.debug(status_message, extra=self.execution_context.get_context())
                if update_log_file_dtl_flag:
                    # updating process log entry
                    log_info[CommonConstants.BATCH_ID] = curr_batch_id
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                    status_message = "Updated process log entry for all files in batch id" + str(batch_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "Files were processed in a previous run, hence process log was not updated"
                    logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Completed copy of files to publish layer"
            logger.debug(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.BATCH_ID] = batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                           '')
            CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
            batches = batch_id_list.split(",")
            for batch in batches:
                log_info[CommonConstants.BATCH_ID] = batch
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                    '"',
                    '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(e)}
            raise e


if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                              ['dataset_id', 'workflow_id', 'process_id', 'batch_id', 'batches'])
    file_master_id = args["dataset_id"]
    cluster_id = CommonConstants.GLUE_CLUSTER_ID
    workflow_id = args["workflow_id"]
    process_id = args["process_id"]
    batch_id = args["batch_id"]
    batch_id_list = args["batches"]
    
    if file_master_id is None or cluster_id is None or workflow_id is None or process_id is None or batch_id is None or batch_id_list is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    batch_id = str(batch_id).rstrip("\n\r")
    publish_handler = PublishHandler()
    publish_handler.execute_publish_copy(file_master_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list)
    sys.stdout.write("batch_id=" + batch_id)