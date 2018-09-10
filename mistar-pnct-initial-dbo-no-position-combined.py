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
import time
import boto3


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client = boto3.client('lambda', region_name='us-west-2')

current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

                                        ######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################

# latch connection
latch_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_initial", table_name="latch",transformation_ctx="latch_ds")
latch_regDF = latch_ds.toDF()
latch_regDF = latch_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
latch_distDF = latch_regDF.distinct()
latch_dynDF = DynamicFrame.fromDF(latch_distDF, glueContext, "nested")

# roverdetails connection
roverdetails_ds = glueContext.create_dynamic_frame.from_catalog(database="staging_initial",table_name="roverdetails",transformation_ctx="roverdetails_ds")
roverdetails_regDF = roverdetails_ds.toDF()
roverdetails_regDF = roverdetails_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded", lit(current_timestamp))
roverdetails_distDF =  roverdetails_regDF.distinct()
roverdetails_dynDF = DynamicFrame.fromDF(roverdetails_distDF, glueContext, "nested")

# mapdata connection
mapdata_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "mapdata",transformation_ctx = "mapdata_ds")
mapdata_regDF = mapdata_ds.toDF()
mapdata_regDF = mapdata_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
mapdata_distDF =  mapdata_regDF.distinct()
mapdata_dynDF = DynamicFrame.fromDF(mapdata_distDF,glueContext,"nested")

# latchtype connection
latchtype_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "latchtype",transformation_ctx = "latchtype_ds")
latchtype_regDF = latchtype_ds.toDF()
latchtype_regDF = latchtype_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
latchtype_distDF =  latchtype_regDF.distinct()
latchtype_dynDF = DynamicFrame.fromDF(latchtype_distDF,glueContext,"nested")

# utctimezoneoffsets connection
offset_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "utctimezoneoffsets",transformation_ctx = "offset_ds")
offset_regDF = offset_ds.toDF()
offset_regDF = offset_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn( "audtdateadded",lit(current_timestamp))
offset_distDF =  offset_regDF.distinct()
offset_dynDF = DynamicFrame.fromDF(offset_distDF,glueContext,"nested")

# vehicletype connection
vehicletype_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "vehicletype",transformation_ctx = "vehicletype_ds")
vehicletype_regDF = vehicletype_ds.toDF()
vehicletype_regDF = vehicletype_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
vehicletype_distDF =  vehicletype_regDF.distinct()
vehicletype_dynDF = DynamicFrame.fromDF(vehicletype_distDF,glueContext,"nested")

# flagdescription connection
flagdesc_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "flagdescription",transformation_ctx = "flagdesc_ds")
flagdesc_regDF = flagdesc_ds.toDF()
flagdesc_regDF = flagdesc_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
flagdesc_distDF=  flagdesc_regDF.distinct()
flagdesc_dynDF = DynamicFrame.fromDF(flagdesc_distDF,glueContext,"nested")

# maptype connection
maptype_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "maptype", transformation_ctx = "maptype_ds")
maptype_regDF = maptype_ds.toDF()
maptype_regDF = maptype_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
maptype_distDF =  maptype_regDF.distinct()
maptype_dynDF = DynamicFrame.fromDF(maptype_distDF,glueContext,"nested")

## vehicledetails connection
vdetails_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "vehicledetails", transformation_ctx = "vdetails_DS")
vdetails_regDF = vdetails_DS.toDF()
vdetails_regDF = vdetails_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
vdetails_distDF = vdetails_regDF.distinct()
vdetails_dynDF = DynamicFrame.fromDF(vdetails_distDF,glueContext,"nested")


                                        ####################################
                                        ####        MAPPING BLOCK      ####
                                        ####################################

# latch mapping
latch_mappingcombined = ApplyMapping.apply(frame=latch_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("servertimestamp", "string", "servertimestamp", "timestamp"),
    ("rovertimestamp", "string", "rovertimestamp", "timestamp"),
    ("mjid", "long", "mjid", "long"),
    ("chid", "string", "chid", "string"),
    ("spreaderazimuth", "long", "spreaderazimuth", "long"),
    ("spreaderheight", "long", "spreaderheight", "long"),
    ("spreader1x", "long", "spreader1x", "long"),
    ("spreader1y", "long", "spreader1y", "long"),
    ("spreader2x", "long", "spreader2x", "long"),
    ("spreader2y", "long", "spreader2y", "long"),
    ("latchtypeid", "long", "latchtypeid", "long"),
    ("containerlot", "string", "containerlot", "string"),
    ("containerrow", "string", "containerrow", "string"),
    ("containertier", "long", "containertier", "long"),
    ("container1spot", "string", "container1spot", "string"),
    ("container1id", "string", "container1id", "string"),
    ("container2spot", "string", "container2spot", "string"),
    ("container2id", "string", "container2id", "string"),
    ("matchedchid", "string", "matchedchid", "string"),
    ("vehicletypeid", "long", "vehicletypeid", "long"),
    ("distancetravelled", "long", "distancetravelled", "long"),
    ("duration", "long", "duration", "long"),
    ("inserttypeid", "long", "inserttypeid", "long"),
    ("spreaderlength", "long", "spreaderlength", "long"),
    ("weight", "long", "weight", "long"),
    ("flags", "long", "flags", "long"),
    ("gpsstatustypeid", "long", "gpsstatustypeid", "long"),
    ("navstatustypeid", "long", "navstatustypeid", "long"),
    ("senttotos", "boolean", "senttotos", "boolean"),
    ("sdx", "long", "sdx", "long"),
    ("sdy", "long", "sdy", "long"),
    ("sdaz", "long", "sdaz", "long")
]
, transformation_ctx="latch_mappingcombined")

# roverdetails mapping
roverdetails_mappingcombined = ApplyMapping.apply(frame=roverdetails_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("mjid", "long", "mjid", "long"),
    ("chid", "string", "chid", "string"),
    ("lastdetailtimestamp", "string", "lastdetailtimestamp", "timestamp"),
    ("ipaddress", "string", "ipaddress", "string"),
    ("softwareversion", "string", "softwareversion", "string"),
    ("navversion", "string", "navversion", "string"),
    ("updateversion", "long", "updateversion", "long"),
    ("gpshwversion1", "string", "gpshwversion1", "string"),
    ("gpshwversion2", "string", "gpshwversion2", "string"),
    ("powerhwversion", "long", "powerhwversion", "long"),
    ("powerfwversion", "long", "powerfwversion", "long"),
    ("startuptimestamp", "string", "startuptimestamp", "timestamp"),
    ("fleetmanagementversion", "string", "fleetmanagementversion", "string"),
    ("keepaliveversion", "string", "keepaliveversion", "string"),
    ("vehicleviewversion", "string", "vehicleviewversion", "string"),
    ("bootloaderfwversion", "long", "bootloaderfwversion", "long"),
    ("gyroserialnumber", "long", "gyroserialnumber", "long"),
    ("flags", "long", "flags", "long"),
    ("navbootloaderversion","long","navbootloaderversion","long")
]
, transformation_ctx="roverdetails_mappingcombined")

# Mapdata mapping
mapdata_mappingcombined = ApplyMapping.apply(frame=mapdata_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("maptypeid", "long", "maptypeid", "long"),
    ("position1x", "double", "position1x", "double"),
    ("position1y", "double", "position1y", "double"),
    ("position3x", "double", "position3x", "double"),
    ("position2x", "double", "position2x", "double"),
    ("position2y", "double", "position2y", "double"),
    ("position3y", "double", "position3y", "double"),
    ("parkingrowlength", "double", "parkingrowlength", "double"),
    ("rowheight", "double", "rowheight", "double"),
    ("parkingrowwidth", "double", "parkingrowwidth", "double"),
    ("columncount", "long", "columncount", "long"),
    ("rowcount", "long", "rowcount", "long"),
    ("startingrownumber", "long", "startingrownumber", "long"),
    ("incrementby", "long", "incrementby", "long"),
    ("lot", "string", "lot", "string"),
    ("row", "string", "row", "string"),
    ("startingcolumnnumber", "long", "startingcolumnnumber", "long"),
    ("slotdigits", "long", "slotdigits", "long"),
    ("rowangle", "double", "rowangle", "double"),
    ("flags", "long", "flags", "long"),
    ("stringformat", "string", "stringformat", "string"),
    ("mapconstrainttypeid", "long", "mapconstrainttypeid", "long"),
    ("isactive", "long", "isactive", "long"),
    ("mapgroupid", "long", "mapgroupid", "long")
]
, transformation_ctx="mapdata_mappingcombined")

# latchtype mapping
latchtype_mappingcombined = ApplyMapping.apply(frame=latchtype_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("latchtypeid", "long", "latchtypeid", "long"),
    ("description", "string", "description", "string")
]
, transformation_ctx="latchtype_mappingcombined")

# utctimezoneoffsets mapping
offset_mappingcombined = ApplyMapping.apply(frame=offset_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("daylightsavingstimestart", "string", "daylightsavingstimestart", "timestamp"),
    ("daylightsavingstimeend", "string", "daylightsavingstimeend", "timestamp"),
    ("standardtimestart1", "string", "standardtimestart1", "timestamp"),
    ("standardtimeend1", "string", "standardtimeend1", "timestamp"),
    ("standardtimestart2", "string", "standardtimestart2", "timestamp"),
    ("standardtimeend2", "string", "standardtimeend2", "timestamp"),
    ("easternutcoffsetinsidedst", "long", "easternutcoffsetinsidedst", "long"),
    ("easternutcoffsetoutsidedst", "long", "easternutcoffsetoutsidedst", "long"),
    ("centralutcoffsetinsidedst", "long", "centralutcoffsetinsidedst", "long"),
    ("centralutcoffsetoutsidedst", "long", "centralutcoffsetoutsidedst", "long"),
    ("mountainutcoffsetinsidedst", "long", "mountainutcoffsetinsidedst", "long"),
    ("mountainutcoffsetoutsidedst", "long", "mountainutcoffsetoutsidedst", "long"),
    ("pacificutcoffsetinsidedst", "long", "pacificutcoffsetinsidedst", "long"),
    ("pacificutcoffsetoutsidedst", "long", "pacificutcoffsetoutsidedst", "long"),
    ("arizonautcoffsetinsidedst", "long", "arizonautcoffsetinsidedst", "long"),
    ("arizonautcoffsetoutsidedst", "long", "arizonautcoffsetoutsidedst", "long")
]
, transformation_ctx="offset_mappingcombined")

# vehicletype mapping
vehicletype_mappingcombined = ApplyMapping.apply(frame=vehicletype_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("vehicletypeid", "long", "vehicletypeid", "long"),
    ("description", "string", "description", "string"),
    ("mapconstrainttypeid", "long", "mapconstrainttypeid", "long"),
    ("title", "string", "title", "string")
]
, transformation_ctx="vehicletype_mappingcombined")

# flagdescription mapping
flagdesc_mappingcombined = ApplyMapping.apply(frame=flagdesc_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("flagdescriptionid", "long", "flagdescriptionid", "long"),
    ("flagvalue", "long", "flagvalue", "long"),
    ("description", "string", "description", "string"),
    ("tablename", "string", "tablename", "string"),
    ("columnname", "string", "columnname", "string"),
    ("schema", "string", "schema", "string"),
    ("active", "boolean", "active", "boolean")
]
,transformation_ctx="flagdesc_mappingcombined")

# maptype mapping
maptype_mappingcombined = ApplyMapping.apply(frame=maptype_dynDF, mappings=[
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("dboperationtype", "string", "dboperationtype", "string"),
    ("audtdateadded", "string", "audtdateadded", "timestamp"),
    ("maptypeid", "long", "maptypeid", "long"),
    ("description", "string", "description", "string")
]
, transformation_ctx="maptype_mappingcombined")

## vehicledetails mapping
vdetails_applymapping = ApplyMapping.apply(frame = vdetails_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), ("lastdetailtimestamp", "string", "lastdetailtimestamp", "timestamp"), ("lastfueltimestamp", "string", "lastfueltimestamp", "timestamp"), ("lastrefueltimestamp", "string", "lastrefueltimestamp", "timestamp"), ("lasttiretimestamp", "string", "lasttiretimestamp", "timestamp"), ("lastimpacttimestamp", "string", "lastimpacttimestamp", "timestamp"), ("vehicletypeid", "long", "vehicletypeid", "long"), ("vehiclesubtypeid", "long", "vehiclesubtypeid", "long"), ("trailcolor", "long", "trailcolor", "long"), ("status", "string", "status", "string"), ("tosserveripaddress", "string", "tosserveripaddress", "string"), ("lastplctimestamp", "string", "lastplctimestamp", "timestamp")], transformation_ctx = "vdetails_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
# latch datasink
latch_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=latch_mappingcombined, connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/latch"}, format="parquet", transformation_ctx="latch_datasinkcombined")

# roverdetails datasink
roverdetails_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame=roverdetails_mappingcombined,connection_type="s3",connection_options={"path": "s3://pa-dms-staging/mistar/pnct/combined/roverdetails"},format="parquet",transformation_ctx="roverdetails_datasinkcombined")

# mapdata datasink
mapdata_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = mapdata_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/mapdata"},format = "parquet", transformation_ctx = "mapdata_datasinkcombined")

# latchtype datasink
latchtype_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = latchtype_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/latchtype"},format = "parquet", transformation_ctx = "latchtype_datasinkcombined")

# utctimezoneoffsets datasink
offset_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = offset_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/utctimezoneoffsets"},format = "parquet", transformation_ctx = "offset_datasinkcombined")

# vehicletype datasink
vehicletype_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = vehicletype_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/vehicletype"},format = "parquet", transformation_ctx = "vehicletype_datasinkcombined")

# flagdescription datasink
flagdesc_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = flagdesc_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/flagdescription"},format = "parquet", transformation_ctx = "flagdesc_datasinkcombined")

# maptype datasink
maptype_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = maptype_mappingcombined, connection_type = "s3",connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/maptype"},format = "parquet", transformation_ctx = "maptype_datasinkcombined")

## vehicledetails datasink
vdetails_datasink = glueContext.write_dynamic_frame.from_options(frame = vdetails_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mistar/pnct/combined/vehicledetails"}, format = "parquet", transformation_ctx = "vdetails_datasink")
                                        

job.commit()