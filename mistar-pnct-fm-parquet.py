import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,current_timestamp, expr,from_utc_timestamp,lit,to_date
from pyspark.sql.types import StringType
import time
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client = boto3.client('glue', region_name='us-west-2')


										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
										
## tire connection
tire_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "tire", transformation_ctx = "tire_DS")
tire_regDF = tire_DS.toDF()
tire_regDF = tire_regDF.withColumn('sourcesystemtimestamp',from_utc_timestamp('rovertimestamp','America/New_York')).withColumn('sourcesystemdate',to_date(from_utc_timestamp('rovertimestamp','America/New_York')))
tire_noDel = tire_regDF.filter(tire_regDF['dboperationtype'] != 'D')
tire_drop = tire_noDel.drop('dboperationtype').drop('audtdateadded')
tire_distDF = tire_drop.distinct()
tire_dynDF = DynamicFrame.fromDF(tire_distDF, glueContext, "nested")					
			
			
## tiredetail connection
tiredet_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_combined", table_name="tiredetail",transformation_ctx="tiredet_ds")
tiredet_regDF = tiredet_ds.toDF()
tiredet_regDF = tiredet_regDF.withColumn('sourcesystemtimestamp',from_utc_timestamp('rovertimestamp','America/New_York')).withColumn('sourcesystemdate',to_date(from_utc_timestamp('rovertimestamp','America/New_York')))
tiredet_noDel = tiredet_regDF.filter(tiredet_regDF['dboperationtype'] != 'D')
tiredet_drop = tiredet_noDel.drop('dboperationtype').drop('audtdateadded')
tiredet_distDF = tiredet_drop.distinct()
tiredet_dynDF = DynamicFrame.fromDF(tiredet_distDF, glueContext, "nested")

## tiresourcetype connection
#DF1
tirest_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_combined", table_name="tiresourcetype",transformation_ctx="tirest_ds")
tirest_regDF = tirest_ds.toDF()
tirest_noDel = tirest_regDF.filter(tirest_regDF['dboperationtype'] != 'D')
tirest_agg = tirest_noDel.groupBy("tiresourcetypeid").agg({"audtdateadded":"max"})
tirest_agg = tirest_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
tirest_distDF = tirest_agg.distinct()

#DF2
tirest_ds2 = glueContext.create_dynamic_frame.from_catalog(database="staging_combined", table_name="tiresourcetype",transformation_ctx="tirest_ds2")
tirest_regDF2 = tirest_ds2.toDF()
tirest_noDel2 = tirest_regDF2.filter(tirest_regDF2['dboperationtype'] != 'D')
tirest_distDF2 = tirest_noDel2.distinct()

#JOIN
tirest_join = tirest_distDF2.join(tirest_distDF,(tirest_distDF2.tiresourcetypeid == tirest_distDF.tiresourcetypeid) & (tirest_distDF2.audtdateadded == tirest_distDF.audtdateadded)).drop(tirest_distDF.tiresourcetypeid).drop(tirest_distDF.audtdateadded)
tirest_drop = tirest_join.drop('dboperationtype')
tirest_joinDist = tirest_drop.distinct()
tirest_dynDF = DynamicFrame.fromDF(tirest_joinDist, glueContext, "nested")

## tiretype connection
#DF1
tiretype_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_combined", table_name="tiretype",transformation_ctx="tiretype_ds")
tiretype_regDF = tiretype_ds.toDF()
tiretype_noDel = tiretype_regDF.filter(tiretype_regDF['dboperationtype'] != 'D')
tiretype_agg = tiretype_noDel.groupBy("tiretypeid").agg({"audtdateadded":"max"})
tiretype_agg = tiretype_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
tiretype_distDF = tiretype_agg.distinct()

#DF2
tiretype_ds2 = glueContext.create_dynamic_frame.from_catalog(database="staging_combined", table_name="tiretype",transformation_ctx="tiretype_ds2")
tiretype_regDF2 = tiretype_ds2.toDF()
tiretype_noDel2 = tiretype_regDF2.filter(tiretype_regDF2['dboperationtype'] != 'D')
tiretype_distDF2 = tiretype_noDel2.distinct()

#JOIN
tiretype_join = tiretype_distDF2.join(tiretype_distDF,(tiretype_distDF2.tiretypeid == tiretype_distDF.tiretypeid) & (tiretype_distDF2.audtdateadded == tiretype_distDF.audtdateadded)).drop(tiretype_distDF.tiretypeid).drop(tiretype_distDF.audtdateadded)
tiretype_drop = tiretype_join.drop('dboperationtype')
tiretype_joinDist = tiretype_drop.distinct()
tiretype_dynDF = DynamicFrame.fromDF(tiretype_joinDist, glueContext, "nested")

## fuel connection
fuel_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "fuel", transformation_ctx = "fuel_DS")
fuel_regDF = fuel_DS.toDF()
fuel_regDF.createOrReplaceTempView("distfuel")
fuel_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
							createdtimestamp, \
							rovertimestamp, \
							from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
							cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate,\
							mjid, \
							chid, \
							level1, \
							level2, \
							urealevel, \
							odometer, \
							flags, \
							fuelboard1analog3v, \
							fuelboard1analog11v, \
							fuelboard1analog30v, \
							fuelboard2analog3v, \
							fuelboard2analog11v, \
							fuelboard2analog30v \
						FROM distfuel \
						WHERE dboperationtype <> 'D'")
fuel_dynDF = DynamicFrame.fromDF(fuel_distDF, glueContext, "nested")					
					
## impact connection
impact_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "impact", transformation_ctx = "impact_DS")
impact_regDF = impact_DS.toDF()
impact_regDF.createOrReplaceTempView("distimpact")
impact_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
								createdtimestamp, \
								rovertimestamp, \
								from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
								cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate,\
								mjid, \
								chid, \
								sensorid1, \
								ximpact1, \
								yimpact1, \
								zimpact1, \
								sensorid2, \
								ximpact2, \
								yimpact2, \
								zimpact2, \
								sensorid3, \
								ximpact3, \
								yimpact3, \
								zimpact3, \
								sensorid4, \
								ximpact4, \
								yimpact4, \
								zimpact4, \
								flags \
							FROM distimpact \
					    	WHERE dboperationtype <> 'D'")
impact_dynDF = DynamicFrame.fromDF(impact_distDF, glueContext, "nested")	          
					
					
										####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################

## tire mapping
tire_mapping = ApplyMapping.apply(frame = tire_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("servertimestamp", "timestamp", "servertimestamp", "timestamp"), 
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"), ("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"),("chid", "string", "chid", "string"), 
("tiresavailableflags","long","tiresavailableflags","long"),("flags", "long", "flags", "long"),("datasource","long","datasource","long"),("x","long","x","long"),("y","long","y","long"),
("sourcesystemdate","date","sourcesystemdate","date")], 
transformation_ctx = "tire_mapping")

## tiredetail mapping
tiredet_mapping = ApplyMapping.apply(frame=tiredet_dynDF, mappings=[("sourcesystem", "string", "sourcesystem", "string"), ("servertimestamp", "timestamp", "servertimestamp", "timestamp"), 
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"), ("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"),("chid","string","chid","string"),("dataavailable","long","dataavailable","long"),
("serialnumber","string","serialnumber","string"),("pressure","long","pressure","long"),("temperature","long","temperature","long"),("signalstrength","long","signalstrength","long"),("flags","long","flags","long"),
("internalflags","long","internalflags","long"),("sourcesystemdate","date","sourcesystemdate","date")]
,transformation_ctx="tiredet_mapping")

## tiresourcetype mapping
tirest_mapping = ApplyMapping.apply(frame=tirest_dynDF, mappings=[("sourcesystem", "string", "sourcesystem", "string"),("audtdateadded", "timestamp", "audtdateadded", "timestamp"),
("tiresourcetypeid","long","tiresourcetypeid","long"),("description","string","description","string")]
,transformation_ctx="tirest_mapping")

## tiretype mapping
tiretype_mapping = ApplyMapping.apply(frame=tiretype_dynDF, mappings=[("sourcesystem", "string", "sourcesystem", "string"),("audtdateadded", "timestamp", "audtdateadded", "timestamp"),("tiretypeid","long","tiretypeid","long"),
("description","string","description","string")]
,transformation_ctx="tiretype_mapping")

## fuel mapping
fuel_mapping = ApplyMapping.apply(frame = fuel_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("createdtimestamp", "timestamp", "createdtimestamp", "timestamp"), 
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"), ("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"), ("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), 
("level1", "long", "level1", "long"), ("level2", "long", "level2", "long"), ("urealevel", "long", "urealevel", "long"), ("odometer", "long", "odometer", "long"), ("flags", "long", "flags", "long"), 
("fuelboard1analog3v", "long", "fuelboard1analog3v", "long"), ("fuelboard1analog11v", "long", "fuelboard1analog11v", "long"), ("fuelboard1analog30v", "long", "fuelboard1analog30v", "long"), 
("fuelboard2analog3v", "long", "fuelboard2analog3v", "long"), ("fuelboard2analog11v", "long", "fuelboard2analog11v", "long"), ("fuelboard2analog30v", "long", "fuelboard2analog30v", "long"),
("sourcesystemdate","date","sourcesystemdate","date")]
, transformation_ctx = "fuel_mapping")

## impact mapping
impact_mapping = ApplyMapping.apply(frame = impact_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),  ("createdtimestamp", "timestamp", "createdtimestamp", "timestamp"), 
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"), ("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"), ("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), 
("sensorid1", "long", "sensorid1", "long"), ("ximpact1", "long", "ximpact1", "long"), ("yimpact1", "long", "yimpact1", "long"), ("zimpact1", "long", "zimpact1", "long"), ("sensorid2", "long", "sensorid2", "long"), 
("ximpact2", "long", "ximpact2", "long"), ("yimpact2", "long", "yimpact2", "long"), ("zimpact2", "long", "zimpact2", "long"), ("sensorid3", "long", "sensorid3", "long"), ("ximpact3", "long", "ximpact3", "long"), 
("yimpact3", "long", "yimpact3", "long"), ("zimpact3", "long", "zimpact3", "long"), ("sensorid4", "long", "sensorid4", "long"), ("ximpact4", "long", "ximpact4", "long"), ("yimpact4", "long", "yimpact4", "long"), 
("zimpact4", "long", "zimpact4", "long"), ("flags", "long", "flags", "long"),("sourcesystemdate","date","sourcesystemdate","date")]
, transformation_ctx = "impact_mapping")

										
										####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## tire datasink
tire_datasink = glueContext.write_dynamic_frame.from_options(frame = tire_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/tire", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "tire_datasink")

## tiredetail datasink
tiredetail_datasink = glueContext.write_dynamic_frame.from_options(frame = tiredet_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/tiredetail", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "tiredetail_datasink")

## tiresourcetype datasink
tiresourcetype_datasink = glueContext.write_dynamic_frame.from_options(frame = tirest_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/tiresourcetype"}, format = "parquet", transformation_ctx = "tiresourcetype_datasink")

## tiretype datasink
tiretype_datasink = glueContext.write_dynamic_frame.from_options(frame = tiretype_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/tiretype"}, format = "parquet", transformation_ctx = "tiretype_datasink")

## fuel datasink
fuel_datasink = glueContext.write_dynamic_frame.from_options(frame = fuel_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/fuel", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "fuel_datasink")

## impact datasink
impact_datasink = glueContext.write_dynamic_frame.from_options(frame = impact_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/impact", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "impact_datasink")


# Start Crawler to update Athena Partitions
response = client.start_crawler(
    Name='dl-mistar-fm-parquet-partitioned'
)

job.commit()




										