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

import logging
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.sql.types import *
from graphframes.lib import Pregel
from datetime import datetime as dt



handle = None
vertColSchema = StructType().add("dist", DoubleType()).add("node", StringType()).add("path", ArrayType(StringType(), True))

#use custom_play method only when a given etl job cannot be implemented by generic framework method - play. 
vertColSchema = StructType().add("dist", DoubleType()).add("node", StringType()).add("path", ArrayType(StringType(), True))

def vertexProgram(vd, msg):
    if msg == None or vd.__getitem__(0) < msg.__getitem__(0):
        return (vd.__getitem__(0), vd.__getitem__(1), vd.__getitem__(2))
    else:
        return (msg.__getitem__(0), vd.__getitem__(1), msg.__getitem__(2))

vertexProgramUdf = F.udf(vertexProgram, vertColSchema)

def sendMsgToDst(src, dst):
    srcDist = src.__getitem__(0)
    dstDist = dst.__getitem__(0)
    if srcDist < (dstDist - 1):
        return (srcDist + 1, src.__getitem__(1), src.__getitem__(2) + [dst.__getitem__(1)])
    else:
        return None

sendMsgToDstUdf = F.udf(sendMsgToDst, vertColSchema)

def aggMsgs(agg):
    shortest_dist = sorted(agg, key=lambda tup: tup[1])[0]
    return (shortest_dist.__getitem__(0), shortest_dist.__getitem__(1), shortest_dist.__getitem__(2))

aggMsgsUdf = F.udf(aggMsgs, vertColSchema)

def get_hierarchy(self,graph_config):
    vertices,edges,startswith_columnname,startswith_columnvalue,max_iter,output_view_name, output_view_columns,graph =[None]*8
    print("1")
    try:
        self.logger.debug(graph_config)
        self.logger.info("Invoking graphframe to generate hierarchy ...")
        print("2")    
        if graph_config.get("name") is not None:
            print("3")
            self.logger.info("Executing task as per graph configuration %s" % (str(graph_config["name"])))  
        else:
           raise ValueError("Missing name in graph job configuration!") 
        
        if graph_config.get("vertices") is not None:  
            print("4")
            vertices = self.spark.sql("select * from %s" %(str(graph_config["vertices"])))
        else:
            raise ValueError("Missing vertices in graph job configuration!")
        
        if graph_config.get("edges") is not None:  
            print("5")
            edges = self.spark.sql("select * from %s" %(str(graph_config["edges"])))
        else:
            raise ValueError("Missing edges in graph job configuration!")
        
        print("6")
        graph = GraphFrame(vertices, edges)
        
        if graph_config.get("startswith") is not None:
            print("7")
            if graph_config.get("startswith").get("column") is None:
                raise ValueError("Missing column name in startswith graph job configuration!")
            if graph_config.get("startswith").get("value") is  None:
                raise ValueError("Missing column value in startswith graph job configuration!")
            if graph_config.get("startswith").get("max_iter") is  None:
                raise ValueError("Missing max interation in graph job configuration!")
            if graph_config.get("startswith").get("checkpoint_interval") is None:
               raise ValueError("Missing checkpont_interval in graph job configuration!")
        else:
            raise ValueError("Missing startswith in graph job configuration!")
        
        result = graph.pregel \
                      .withVertexColumn( \
                          colName = "vertCol", \
                          initialExpr =   F.when( \
                                                  F.col(str(graph_config["startswith"]["column"]))==(F.lit(graph_config["startswith"]["value"])),  \
                                                  F.struct(F.lit(0.0), F.col("node"), F.array(F.col("node"))) \
                                              ) \
                                          .otherwise(F.struct(F.lit(float("inf")), F.col("node"), F.array(F.lit("")))) \
                                          .cast(vertColSchema), \
                          updateAfterAggMsgsExpr = vertexProgramUdf(F.col("vertCol"), Pregel.msg()) \
                       ) \
                      .sendMsgToDst(sendMsgToDstUdf(F.col("src.vertCol"), Pregel.dst("vertCol"))) \
                      .aggMsgs(aggMsgsUdf(F.collect_list(Pregel.msg()))) \
                      .setMaxIter(graph_config["startswith"]["max_iter"]) \
                      .setCheckpointInterval(graph_config["startswith"]["checkpoint_interval"]) \
                      .run()
        print("8")
        if graph_config.get("output") is not None:
            if graph_config.get("output").get("view") is None:
                raise ValueError("Missing view name in output graph job configuration!")
            if graph_config.get("output").get("columns") is None:
                raise ValueError("Missing view columns in output graph job configuration!")
        else:
            raise ValueError("Missing output in graph job configuration!")
        
        rename_result=result.select("node","level",F.reverse('vertCol.path').alias('path'),F.col('vertCol.dist').alias('dist')).distinct()
        rename_result.createOrReplaceTempView(graph_config["output"]["view"])
        print("9")
    except Exception as e:
                    self.logger.error('Parsing spark Graph SQL query failed!!!')
                    raise #re-raise it
    return graph_config["output"]["view"]

def custom_play(handle):
        """
        Orchestrates sequence of task - extract, transform and load
        """
        if handle.config.get("jobs") is not None:
            for job in handle.config["jobs"]:
                handle.logger.info("The job name is (%s)" %(job["name"]))
                if job["name"] == "PAP_Dim_Employee_Hier_1":
                    handle.extract(job)               #Extract from Sources
                    handle.apply_transformation(job)  #Transforms
                    print("1")
                    for transform in job["transforms"]:
                        if transform.get("graph") is not None:
                            #print(type(transform))
                            graph= transform["graph"]
                            #print(graph)
                            #print(type(graph))
                            get_hierarchy(handle,graph)
                    #handle.load(job)                  #Load to Target
                    
                    hier_incorrect_manager_df = handle.spark.sql("SELECT * FROM hierarchy_output WHERE dist = 'Infinity'")
                    persistent_stg_supervisor_hier_df = handle.spark.sql("SELECT * FROM filtered_input")

                    schema = StructType([
                              StructField('node', StringType(), True),
                              StructField('newPath', ArrayType(StringType()), True)
                              ])
                    columns = ['node', 'newPath']
                    null_manager_path_df = handle.spark.createDataFrame([], schema)
                    print("Count of hier_incorrect_manager_df:" + str(hier_incorrect_manager_df.count()))
                    #exit(1)
                    for iterator in hier_incorrect_manager_df.collect():
                        is_base_manager = True
                        latest_manager = iterator["node"]
                        manager_list = []
                        print(f"Looking manager for {str(latest_manager)}")
                        while is_base_manager:
                            manager_id_df = handle.spark.sql(f"SELECT currentnode FROM filtered_input WHERE childnode ='{latest_manager}'")
                            if manager_id_df.count() > 0:
                                for manager in manager_id_df.collect():
                                    if latest_manager == manager['currentnode'] or manager['currentnode'] in manager_list or iterator["node"] == manager['currentnode']:
                                        # it is at root node or at cyclic dependency
                                        is_base_manager = False
                                    latest_manager = manager['currentnode']
                                    manager_list.append(latest_manager)
                            else:
                                # no more manager present
                                is_base_manager = False
                        null_manager_path_df = null_manager_path_df.union(handle.spark.createDataFrame([(iterator["node"],manager_list)], columns))
                    #null_manager_path_df.show(100,False)
                    null_manager_path_df.createOrReplaceTempView("null_manager_path_view")
                else:
                    handle.extract(job)               #Extract from Sources
                    handle.apply_transformation(job)  #Transforms
                    handle.load(job)                  #Load to Target
        else:
            handle.logger.log("There is no jobs property. Therefore skipping...")

try:
    handle=etlcore.Executor()
    args=getResolvedOptions(sys.argv, ['batch_date'])
    handle.batch_date=dt.strptime(args['batch_date'],'%Y%m%d').date()
    handle.initialize()
    handle.sc.setCheckpointDir(f"s3://{handle.PROCESSED_BUCKET}/checkpoint/")
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