import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
import boto3
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client = boto3.client('lambda', region_name='us-west-2')
glue = boto3.client(service_name='glue', region_name='us-west-2',
                    endpoint_url='https://glue.us-west-2.amazonaws.com')
current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")


										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
                                        
## impact connection
impact_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="impact",transformation_ctx="impact_ds")
impact_regDF = impact_ds.toDF()
impact_regDF = impact_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
impact_dynDF = DynamicFrame.fromDF(impact_regDF, glueContext, "nested")

## fuel connection
fuel_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="fuel",transformation_ctx="fuel_ds")
fuel_regDF = fuel_ds.toDF()
fuel_regDF = fuel_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
fuel_dynDF = DynamicFrame.fromDF(fuel_regDF, glueContext, "nested")

## tire connection
tire_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="tire",transformation_ctx="tire_ds")
tire_regDF = tire_ds.toDF()
tire_regDF = tire_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
tire_dynDF = DynamicFrame.fromDF(tire_regDF, glueContext, "nested")

## tiredetail connection
tiredet_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="tiredetail",transformation_ctx="tiredet_ds")
tiredet_regDF = tiredet_ds.toDF()
tiredet_regDF = tiredet_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
tiredet_dynDF = DynamicFrame.fromDF(tiredet_regDF, glueContext, "nested")

## tiresourcetype connection
tirest_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="tiresourcetype",transformation_ctx="tirest_ds")
tirest_regDF = tirest_ds.toDF()
tirest_regDF = tirest_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
tirest_dynDF = DynamicFrame.fromDF(tirest_regDF, glueContext, "nested")

## tiretype connection
tiretype_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="tiretype",transformation_ctx="tiretype_ds")
tiretype_regDF = tiretype_ds.toDF()
tiretype_regDF = tiretype_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("audtdateadded", lit(current_timestamp))
tiretype_dynDF = DynamicFrame.fromDF(tiretype_regDF, glueContext, "nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################

## impact mapping
impact_mappingcombined = ApplyMapping.apply(frame=impact_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("createdtimestamp", "string", "createdtimestamp", "timestamp"),
    ("rovertimestamp", "string", "rovertimestamp", "timestamp"),
    ("mjid", "long", "mjid", "long"),
    ("chid", "string", "chid", "string"),
    ("sensorid1", "long", "sensorid1", "long"),
    ("ximpact1", "long", "ximpact1", "long"),
    ("yimpact1", "long", "yimpact1", "long"),
    ("zimpact1", "long", "zimpact1", "long"),
    ("sensorid2", "long", "sensorid2", "long"),
    ("ximpact2", "long", "ximpact2", "long"),
    ("yimpact2", "long", "yimpact2", "long"),
    ("zimpact2", "long", "zimpact2", "long"),
    ("sensorid3", "long", "sensorid3", "long"),
    ("ximpact3", "long", "ximpact3", "long"),
    ("yimpact3", "long", "yimpact3", "long"),
    ("zimpact3", "long", "zimpact3", "long"),
    ("sensorid4", "long", "sensorid4", "long"),
    ("ximpact4", "long", "ximpact4", "long"),
    ("yimpact4", "long", "yimpact4", "long"),
    ("zimpact4", "long", "zimpact4", "long"),
    ("flags", "long", "flags", "long")
],
transformation_ctx="impact_mappingcombined")

## fuel mapping
fuel_mappingcombined = ApplyMapping.apply(frame=fuel_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("createdtimestamp", "string", "createdtimestamp", "timestamp"),
    ("rovertimestamp", "string", "rovertimestamp", "timestamp"),
    ("mjid", "long", "mjid", "long"),
    ("chid", "string", "chid", "string"),
    ("level1", "long", "level1", "long"),
    ("level2", "long", "level2", "long"),
    ("urealevel", "long", "urealevel", "long"),
    ("odometer", "long", "odometer", "long"),
    ("flags", "long", "flags", "long"),
    ("fuelboard1analog3v", "long", "fuelboard1analog3v", "long"),
    ("fuelboard1analog11v", "long", "fuelboard1analog11v", "long"),
    ("fuelboard1analog30v", "long", "fuelboard1analog30v", "long"),
    ("fuelboard2analog3v", "long", "fuelboard2analog3v", "long"),
    ("fuelboard2analog11v", "long", "fuelboard2analog11v", "long"),
    ("fuelboard2analog30v", "long", "fuelboard2analog30v", "long")
],
transformation_ctx="fuel_mappingcombined")

## tire mapping
tire_mappingcombined = ApplyMapping.apply(frame=tire_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("servertimestamp","string","servertimestamp","timestamp"),
	("rovertimestamp","string","rovertimestamp","timestamp"),
	("chid","string","chid","string"),
	("tiresavailableflags","long","tiresavailableflags","long"),
	("flags","long","flags","long"),
	("datasource","long","datasource","long"),
	("x","long","x","long"),
	("y","long","y","long")
],
transformation_ctx="tire_mappingcombined")

## tiredetail mapping
tiredet_mappingcombined = ApplyMapping.apply(frame=tiredet_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("servertimestamp","string","servertimestamp","timestamp"),
	("rovertimestamp","string","rovertimestamp","timestamp"),
	("chid","string","chid","string"),
	("dataavailable","long","dataavailable","long"),
	("serialnumber","string","serialnumber","string"),
	("pressure","long","pressure","long"),
	("temperature","long","temperature","long"),
	("signalstrength","long","signalstrength","long"),
	("flags","long","flags","long"),
	("internalflags","long","internalflags","long")
],
transformation_ctx="tiredet_mappingcombined")

## tiresourcetype mapping
tirest_mappingcombined = ApplyMapping.apply(frame=tirest_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("tiresourcetypeid","long","tiresourcetypeid","long"),
	("description","string","description","string")
],
transformation_ctx="tirest_mappingcombined")


## tiretype mapping
tiretype_mappingcombined = ApplyMapping.apply(frame=tiretype_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("tiretypeid","long","tiretypeid","long"),
	("description","string","description","string")
],
transformation_ctx="tiretype_mappingcombined")

                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
                                        
## impact datasink
impact_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=impact_mappingcombined, connection_type="s3", connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/impact"}, format="parquet", transformation_ctx="impact_datasinkcombined")

## fuel datasink
fuel_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=fuel_mappingcombined, connection_type="s3", connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/fuel"}, format="parquet", transformation_ctx="fuel_datasinkcombined")

## tire datasink
tire_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=tire_mappingcombined, connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/tire"}, format="parquet", transformation_ctx="tire_datasinkcombined")

## tiredetail datasink
tiredet_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=tiredet_mappingcombined, connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/tiredetail"}, format="parquet", transformation_ctx="tiredet_datasinkcombined")

## tiresourcetype datasink
tirest_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=tirest_mappingcombined, connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/tiresourcetype"}, format="parquet", transformation_ctx="tirest_datasinkcombined")

## tiretype datasink
tiretype_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=tiretype_mappingcombined, connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/tiretype"}, format="parquet", transformation_ctx="tiretype_datasinkcombined")


# start Parquet processing
glue.start_job_run(JobName = 'mistar-pnct-fm-parquet')

job.commit()