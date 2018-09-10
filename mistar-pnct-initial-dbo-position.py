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

current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")



# position connection
position_ds = glueContext.create_dynamic_frame.from_catalog(database ="staging_initial", table_name ="position",transformation_ctx = "position_ds")
position_regDF = position_ds.toDF()
position_regDF = position_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
position_distDF = position_regDF.distinct()
position_dynDF = DynamicFrame.fromDF(position_distDF, glueContext, "nested")


# position mapping
position_mappingcombined = ApplyMapping.apply(frame=position_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("servertimestamp", "string", "servertimestamp", "timestamp"),
    ("rovertimestamp", "string", "rovertimestamp", "timestamp"),
    ("mjid", "long", "mjid", "long"),
    ("chid", "string", "chid", "string"),
    ("azimuth", "long", "azimuth", "long"),
    ("localx", "long", "localx", "long"),
    ("localy", "long", "localy", "long"),
    ("spreaderazimuth", "long", "spreaderazimuth", "long"),
    ("spreaderx", "long", "spreaderx", "long"),
    ("spreadery", "long", "spreadery", "long"),
    ("gpsstatustypeid", "long", "gpsstatustypeid", "long"),
    ("gpsdiagnosticdata", "long", "gpsdiagnosticdata", "long"),
    ("navstatustypeid", "long", "navstatustypeid", "long"),
    ("latchtypeid", "long", "latchtypeid", "long"),
    ("vehicletypeid", "long", "vehicletypeid", "long"),
    ("macaddress", "string", "macaddress", "string"),
    ("signalstrength", "long", "signalstrength", "long"),
    ("sdx", "long", "sdx", "long"),
    ("sdy", "long", "sdy", "long"),
    ("sdaz", "long", "sdaz", "long"),
    ("velocity", "long", "velocity", "long"),
    ("spreaderlength", "long", "spreaderlength", "long"),
    ("spreaderheight", "long", "spreaderheight", "long"),
    ("flags", "long", "flags", "long"),
    ("flags2", "long", "flags2", "long"),
    ("workzonetypeid", "long", "workzonetypeid", "long"),
    ("workzonelength", "long", "workzonelength", "long"),
    ("workzonewidth", "long", "workzonewidth", "long"),
    ("activeoridletime", "long", "activeoridletime", "long"),
    ("basedatanotreceived","long","basedatanotreceived","long")
]
,transformation_ctx="position_mappingcombined")


# position datasink
position_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = position_mappingcombined, connection_type ="s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/position"},format = "parquet",transformation_ctx = "position_datasinkcombined")

job.commit()