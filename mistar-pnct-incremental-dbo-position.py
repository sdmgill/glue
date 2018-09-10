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

## @type: DataSource
## @args: [database = "staging_incremental", table_name = "position", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="staging_incremental", table_name="position",
                                                            transformation_ctx="datasource0")

regDF = datasource0.toDF()
regDF = regDF.withColumn("sourcesystem", lit("PNCT"))

dynDF = DynamicFrame.fromDF(regDF, glueContext, "nested")

## @type: ApplyMapping
## @args: [mapping = [("dboperationtype", "string", "dboperationtype", "string"), ("servertimestamp", "string", "servertimestamp", "string"), ("rovertimestamp", "string", "rovertimestamp", "string"), ("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), ("azimuth", "long", "azimuth", "long"), ("localx", "long", "localx", "long"), ("localy", "long", "localy", "long"), ("spreaderazimuth", "long", "spreaderazimuth", "long"), ("spreaderx", "long", "spreaderx", "long"), ("spreadery", "long", "spreadery", "long"), ("gpsstatustypeid", "long", "gpsstatustypeid", "long"), ("gpsdiagnosticdata", "long", "gpsdiagnosticdata", "long"), ("navstatustypeid", "long", "navstatustypeid", "long"), ("latchtypeid", "long", "latchtypeid", "long"), ("vehicletypeid", "long", "vehicletypeid", "long"), ("macaddress", "string", "macaddress", "string"), ("signalstrength", "long", "signalstrength", "long"), ("sdx", "long", "sdx", "long"), ("sdy", "long", "sdy", "long"), ("sdaz", "long", "sdaz", "long"), ("velocity", "long", "velocity", "long"), ("spreaderlength", "long", "spreaderlength", "long"), ("spreaderheight", "long", "spreaderheight", "long"), ("flags", "long", "flags", "long"), ("flags2", "long", "flags2", "long"), ("workzonetypeid", "long", "workzonetypeid", "long"), ("workzonelength", "long", "workzonelength", "long"), ("workzonewidth", "long", "workzonewidth", "long"), ("activeoridletime", "long", "activeoridletime", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymappingraw = ApplyMapping.apply(frame=dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("servertimestamp", "string", "servertimestamp", "string"),
    ("rovertimestamp", "string", "rovertimestamp", "string"),
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
    ("activeoridletime", "long", "activeoridletime", "long")
],
                                     transformation_ctx="applymappingraw")

## @type: ApplyMapping
## @args: [mapping = [("dboperationtype", "string", "dboperationtype", "string"), ("servertimestamp", "string", "servertimestamp", "string"), ("rovertimestamp", "string", "rovertimestamp", "string"), ("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), ("azimuth", "long", "azimuth", "long"), ("localx", "long", "localx", "long"), ("localy", "long", "localy", "long"), ("spreaderazimuth", "long", "spreaderazimuth", "long"), ("spreaderx", "long", "spreaderx", "long"), ("spreadery", "long", "spreadery", "long"), ("gpsstatustypeid", "long", "gpsstatustypeid", "long"), ("gpsdiagnosticdata", "long", "gpsdiagnosticdata", "long"), ("navstatustypeid", "long", "navstatustypeid", "long"), ("latchtypeid", "long", "latchtypeid", "long"), ("vehicletypeid", "long", "vehicletypeid", "long"), ("macaddress", "string", "macaddress", "string"), ("signalstrength", "long", "signalstrength", "long"), ("sdx", "long", "sdx", "long"), ("sdy", "long", "sdy", "long"), ("sdaz", "long", "sdaz", "long"), ("velocity", "long", "velocity", "long"), ("spreaderlength", "long", "spreaderlength", "long"), ("spreaderheight", "long", "spreaderheight", "long"), ("flags", "long", "flags", "long"), ("flags2", "long", "flags2", "long"), ("workzonetypeid", "long", "workzonetypeid", "long"), ("workzonelength", "long", "workzonelength", "long"), ("workzonewidth", "long", "workzonewidth", "long"), ("activeoridletime", "long", "activeoridletime", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymappingparquet = ApplyMapping.apply(frame=dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
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
    ("activeoridletime", "long", "activeoridletime", "long")
],
                                         transformation_ctx="applymappingparquet")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/raw/mistar/position"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame=applymappingraw, connection_type="s3",
                                                         connection_options={
                                                             "path": "s3://data-lake-us-west-2-062519970039/raw/mistar/position"},
                                                         format="csv", transformation_ctx="datasink2")
datasink3 = glueContext.write_dynamic_frame.from_options(frame=applymappingparquet, connection_type="s3",
                                                         connection_options={
                                                             "path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/position"},
                                                         format="parquet", transformation_ctx="datasink3")

## Make sure to change the S3_FOLDER variable to proper location
client.update_function_configuration(
    FunctionName='mistar-pnct-incremental-position-cleanup',
    Environment={
        'Variables': {
            'S3_FOLDER': 'mistar/pnct/incremental/dbo/position/',
            'SOURCE_BUCKET': 'pa-dms-staging',
            'TARGET_BUCKET': 'pa-processed'
        }
    }
)

client.invoke(FunctionName='mistar-pnct-incremental-position-cleanup')

#time.sleep(60)

#client.invoke(FunctionName='dms-start-mistar-pnct-position-table-task')

job.commit()