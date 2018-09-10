import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,current_timestamp, expr
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

client = boto3.client('glue', region_name='us-west-2')

										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################

## latch connection
## get max
latch_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "latch", transformation_ctx = "latch_DS")
latch_regDF = latch_DS.toDF()
latch_regDF.createOrReplaceTempView("distlatch")
latch_distDF = spark.sql("SELECT DISTINCT sourcesystem,\
                                    servertimestamp, \
                                    rovertimestamp, \
                                    from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
									cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate,\
                                    mjid, \
                                    chid,\
                                    spreaderazimuth,\
                                    spreaderheight,\
                                    spreader1x,\
                                    spreader1y,\
                                    spreader2x,\
                                    spreader2y,\
                                    latchtypeid,\
                                    containerlot,\
                                    containerrow, \
                                    containertier,\
                                    container1spot,\
                                    container1id,\
                                    container2spot,\
                                    container2id,\
                                    matchedchid,\
                                    vehicletypeid,\
                                    distancetravelled,\
                                    duration,\
                                    inserttypeid, \
                                    spreaderlength,\
                                    weight, \
									flags,\
                                    gpsstatustypeid,\
                                    navstatustypeid,\
                                    senttotos,\
                                    sdx,\
                                    sdy,\
                                    sdaz \
                    FROM distlatch \
                    WHERE dboperationtype <> 'D'")
latch_dynDF = DynamicFrame.fromDF(latch_distDF, glueContext, "nested")

## position connection
position_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "position", transformation_ctx = "position_DS")
position_regDF = position_DS.toDF()
position_regDF.createOrReplaceTempView("distpos")
position_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
											servertimestamp, \
											rovertimestamp, \
											from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
											cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate,\
											mjid, \
											chid, \
											azimuth, \
											localx, \
											localy, \
											spreaderazimuth, \
											spreaderx, \
											spreadery, \
											gpsstatustypeid, \
											gpsdiagnosticdata, \
											navstatustypeid, \
											latchtypeid, \
											vehicletypeid, \
											macaddress, \
											signalstrength, \
											sdx, \
											sdy, \
											sdaz, \
											velocity, \
											spreaderlength, \
											spreaderheight, \
											flags, \
											flags2, \
											workzonetypeid, \
											workzonelength, \
											workzonewidth, \
											activeoridletime, \
											basedatanotreceived \
							FROM distpos \
                            WHERE dboperationtype <> 'D' \
                            and rovertimestamp >= cast('2018-05-01' as timestamp)")
position_dynDF = DynamicFrame.fromDF(position_distDF, glueContext, "nested")

## roverdetails connection
## get max
rover_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "roverdetails", transformation_ctx = "rover_DS")
rover_regDF = rover_DS.toDF()
rover_regDF.createOrReplaceTempView("distrover")
rover_distDF = spark.sql("WITH maxrovd \
							AS ( \
							   SELECT dr.mjid, \
									  MAX(dr.lastdetailtimestamp) lastdetailtimestamp \
							   FROM distrover dr \
							   WHERE dboperationtype <> 'D' \
							   GROUP BY dr.mjid \
							   ) \
							SELECT DISTINCT \
								   rd.sourcesystem, \
								   rd.mjid, \
								   rd.chid, \
								   rd.lastdetailtimestamp, \
								   from_utc_timestamp(rd.lastdetailtimestamp, 'America/New_York') sourcesystemtimestamp, \
								   rd.ipaddress, \
								   rd.softwareversion, \
								   rd.navversion, \
								   rd.updateversion, \
								   rd.gpshwversion1, \
								   rd.gpshwversion2, \
								   rd.powerhwversion, \
								   rd.powerfwversion, \
								   rd.startuptimestamp, \
								   rd.fleetmanagementversion, \
								   rd.keepaliveversion, \
								   rd.vehicleviewversion, \
								   rd.bootloaderfwversion, \
								   rd.gyroserialnumber, \
								   rd.flags, \
								   rd.navbootloaderversion \
							FROM distrover rd \
							INNER JOIN maxrovd mr ON rd.mjid = mr.mjid \
								AND rd.lastdetailtimestamp = mr.lastdetailtimestamp")
rover_dynDF = DynamicFrame.fromDF(rover_distDF, glueContext, "nested")

## utctimezoneoffsets connection
utc_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "utctimezoneoffsets", transformation_ctx = "utc_DS")
utc_regDF = utc_DS.toDF()
utc_regDF.createOrReplaceTempView("distutc")
utc_distDF = spark.sql("SELECT DISTINCT \
					   sourcesystem, \
					   MAX(audtdateadded) audtdateadded, \
					   daylightsavingstimestart, \
					   daylightsavingstimeend, \
					   standardtimestart1, \
					   standardtimeend1, \
					   standardtimestart2, \
					   standardtimeend2, \
					   easternutcoffsetinsidedst, \
					   easternutcoffsetoutsidedst, \
					   centralutcoffsetinsidedst, \
					   centralutcoffsetoutsidedst, \
					   mountainutcoffsetinsidedst, \
					   mountainutcoffsetoutsidedst, \
					   pacificutcoffsetinsidedst, \
					   pacificutcoffsetoutsidedst, \
					   arizonautcoffsetinsidedst, \
					   arizonautcoffsetoutsidedst \
				FROM distutc \
				GROUP BY sourcesystem, \
						 daylightsavingstimestart, \
						 daylightsavingstimeend, \
						 standardtimestart1, \
						 standardtimeend1, \
						 standardtimestart2, \
						 standardtimeend2, \
						 easternutcoffsetinsidedst, \
						 easternutcoffsetoutsidedst, \
						 centralutcoffsetinsidedst, \
						 centralutcoffsetoutsidedst, \
						 mountainutcoffsetinsidedst, \
						 mountainutcoffsetoutsidedst, \
						 pacificutcoffsetinsidedst, \
						 pacificutcoffsetoutsidedst, \
						 arizonautcoffsetinsidedst, \
						 arizonautcoffsetoutsidedst")
utc_dynDF = DynamicFrame.fromDF(utc_distDF, glueContext, "nested")

## mapdata connection
mapdata_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mapdata", transformation_ctx = "mapdata_DS")
mapdata_regDF = mapdata_DS.toDF()
mapdata_regDF.createOrReplaceTempView("distmapdata")
mapdata_distDF = spark.sql("SELECT DISTINCT \
                            	   sourcesystem, \
                            	   audtdateadded, \
                            	   maptypeid, \
                            	   position1x, \
                            	   position1y, \
                            	   position3x, \
                            	   position2x, \
                            	   position2y, \
                            	   position3y, \
                            	   parkingrowlength, \
                            	   rowheight, \
                            	   parkingrowwidth, \
                            	   columncount, \
                            	   rowcount, \
                            	   startingrownumber, \
                            	   incrementby, \
                            	   lot, \
                            	   row, \
                            	   startingcolumnnumber, \
                            	   slotdigits, \
                            	   rowangle, \
                            	   flags, \
                            	   stringformat, \
                            	   mapconstrainttypeid, \
                            	   isactive, \
                            	   mapgroupid \
                            FROM distmapdata \
                            WHERE audtdateadded = (SELECT max(audtdateadded) from distmapdata) \
                            and dboperationtype <> 'D'")
mapdata_dynDF = DynamicFrame.fromDF(mapdata_distDF, glueContext, "nested")

## latchtype connection
latchtype_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "latchtype", transformation_ctx = "latchtype_DS")
latchtype_regDF = latchtype_DS.toDF()
latchtype_regDF.createOrReplaceTempView("distlatchtype")
latchtype_distDF = spark.sql("SELECT DISTINCT  \
									   sourcesystem, \
									   MAX(audtdateadded) audtdateadded, \
									   latchtypeid, \
									   description \
								FROM distlatchtype \
								WHERE dboperationtype <> 'D' \
								GROUP BY sourcesystem, \
										 latchtypeid, \
										 description")
latchtype_dynDF = DynamicFrame.fromDF(latchtype_distDF, glueContext, "nested")

## maptype connection
maptype_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "maptype", transformation_ctx = "maptype_DS")
maptype_regDF = maptype_DS.toDF()
maptype_regDF.createOrReplaceTempView("distmaptype")
maptype_distDF = spark.sql("SELECT DISTINCT \
								   sourcesystem, \
								   MAX(audtdateadded) audtdateadded, \
								   maptypeid, \
								   description \
							FROM distmaptype \
							WHERE dboperationtype <> 'D' \
							GROUP BY sourcesystem, \
									 maptypeid, \
									 description")
maptype_dynDF = DynamicFrame.fromDF(maptype_distDF, glueContext, "nested")

## vehicletype connection
vehicletype_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vehicletype", transformation_ctx = "vehicletype_DS")
vehicletype_regDF = vehicletype_DS.toDF()
vehicletype_regDF.createOrReplaceTempView("distvehicletype")
vehicletype_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
									   MAX(audtdateadded) audtdateadded, \
									   vehicletypeid, \
									   description, \
									   mapconstrainttypeid, \
									   title \
								FROM distvehicletype \
								WHERE dboperationtype <> 'D' \
								GROUP BY sourcesystem, \
										 vehicletypeid, \
										 description, \
										 mapconstrainttypeid, \
										 title")
vehicletype_dynDF = DynamicFrame.fromDF(vehicletype_distDF, glueContext, "nested")

## flagdescription connection
flagdesc_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "flagdescription", transformation_ctx = "flagdesc_DS")
flagdesc_regDF = flagdesc_DS.toDF()
flagdesc_regDF.createOrReplaceTempView("distflagdescription")
flagdesc_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
							MAX(audtdateadded) audtdateadded, \
							flagdescriptionid, \
							flagvalue, \
							description, \
							tablename, \
							columnname, \
							schema, \
							active \
					FROM distflagdescription \
					WHERE dboperationtype <> 'D' \
					GROUP BY sourcesystem, \
							flagdescriptionid, \
							flagvalue, \
							description, \
							tablename, \
							columnname, \
							schema, \
							active")
flagdesc_dynDF = DynamicFrame.fromDF(flagdesc_distDF, glueContext, "nested")

## vehicledetails connection
##DF1
vdetails_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vehicledetails", transformation_ctx = "vdetails_DS")
vdetails_regDF = vdetails_DS.toDF()
vdetails_regDFa = vdetails_regDF.na.fill({'lastplctimestamp':'1900-01-01'})
vdetails_regDFb = vdetails_regDFa.filter(vdetails_regDF['dboperationtype'] !='D')
vdetails_grouped = vdetails_regDFb.groupBy("chid").agg({"lastdetailtimestamp":'max',"lastplctimestamp":'max'})
vdetails_agg = vdetails_grouped.withColumnRenamed('max(lastdetailtimestamp)', 'lastdetailtimestamp').withColumnRenamed('max(lastplctimestamp)', 'lastplctimestamp')

##DF2
vdetails_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vehicledetails", transformation_ctx = "vdetails_DS2")
vdetails_regDF2 = vdetails_DS2.toDF()
vdetails_regDF2a = vdetails_regDF2.na.fill({'lastplctimestamp':'1900-01-01'})
vdetails_regDF2b = vdetails_regDF2a.filter(vdetails_regDF2['dboperationtype'] !='D')
vdetails_dist = vdetails_regDF2b.dropDuplicates()

##Join
vdetails_join = vdetails_dist.join(vdetails_agg, (vdetails_dist.chid == vdetails_agg.chid) & (vdetails_dist.lastdetailtimestamp == vdetails_agg.lastdetailtimestamp) & (vdetails_dist.lastplctimestamp == vdetails_agg.lastplctimestamp)).drop(vdetails_agg.chid).drop(vdetails_agg.lastdetailtimestamp).drop(vdetails_agg.lastplctimestamp)
vdetails_drop = vdetails_join.drop('dboperationtype').drop('audtdateadded')
vdetails_final = vdetails_drop.distinct()
vdetails_dynDF = DynamicFrame.fromDF(vdetails_final,glueContext,"nested")

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## latch mapping
latch_mapping = ApplyMapping.apply(frame=latch_dynDF, mappings=[("sourcesystem", "string", "sourcesystem", "string"),("servertimestamp", "timestamp", "servertimestamp", "timestamp"),
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"),("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"),
("mjid", "long", "mjid", "long"),("chid", "string", "chid", "string"),("spreaderazimuth", "long", "spreaderazimuth", "long"),("spreaderheight", "long", "spreaderheight", "long"),("spreader1x", "long", "spreader1x", "long"),
("spreader1y", "long", "spreader1y", "long"),("spreader2x", "long", "spreader2x", "long"),("spreader2y", "long", "spreader2y", "long"),("latchtypeid", "long", "latchtypeid", "long"),
("containerlot", "string", "containerlot", "string"),("containerrow", "string", "containerrow", "string"),("containertier", "long", "containertier", "long"),("container1spot", "string", "container1spot", "string"),
("container1id", "string", "container1id", "string"),("container2spot", "string", "container2spot", "string"),("container2id", "string", "container2id", "string"),("matchedchid", "string", "matchedchid", "string"),
("vehicletypeid", "long", "vehicletypeid", "long"),("distancetravelled", "long", "distancetravelled", "long"),("duration", "long", "duration", "long"),("inserttypeid", "long", "inserttypeid", "long"),
("spreaderlength", "long", "spreaderlength", "long"),("weight", "long", "weight", "long"),("flags", "long", "flags", "long"),("gpsstatustypeid", "long", "gpsstatustypeid", "long"),
("navstatustypeid", "long", "navstatustypeid", "long"),("senttotos", "boolean", "senttotos", "boolean"),("sdx", "long", "sdx", "long"),("sdy", "long", "sdy", "long"),("sdaz", "long", "sdaz", "long"), 
("sourcesystemdate", "date", "sourcesystemdate", "date")]
, transformation_ctx="latch_mapping")

## position mapping
position_mapping = ApplyMapping.apply(frame = position_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("servertimestamp", "timestamp", "servertimestamp", "timestamp"), 
("rovertimestamp", "timestamp", "rovertimestamp", "timestamp"), ("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"), 
("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), ("azimuth", "long", "azimuth", "long"), ("localx", "long", "localx", "long"), ("localy", "long", "localy", "long"), 
("spreaderazimuth", "long", "spreaderazimuth", "long"), ("spreaderx", "long", "spreaderx", "long"), ("spreadery", "long", "spreadery", "long"), ("gpsstatustypeid", "long", "gpsstatustypeid", "long"), 
("gpsdiagnosticdata", "long", "gpsdiagnosticdata", "long"), ("navstatustypeid", "long", "navstatustypeid", "long"), ("latchtypeid", "long", "latchtypeid", "long"), ("vehicletypeid", "long", "vehicletypeid", "long"), 
("macaddress", "string", "macaddress", "string"), ("signalstrength", "long", "signalstrength", "long"), ("sdx", "long", "sdx", "long"), ("sdy", "long", "sdy", "long"), ("sdaz", "long", "sdaz", "long"), 
("velocity", "long", "velocity", "long"), ("spreaderlength", "long", "spreaderlength", "long"), ("spreaderheight", "long", "spreaderheight", "long"), ("flags", "long", "flags", "long"), ("flags2", "long", "flags2", "long"), 
("workzonetypeid", "long", "workzonetypeid", "long"), ("workzonelength", "long", "workzonelength", "long"), ("workzonewidth", "long", "workzonewidth", "long"), ("activeoridletime", "long", "activeoridletime", "long"),
("basedatanotreceived","long","basedatanotreceived","long"), ("sourcesystemdate", "date", "sourcesystemdate", "date")]
, transformation_ctx = "position_mapping")

## roverdetails mapping
rover_mapping = ApplyMapping.apply(frame = rover_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), 
("lastdetailtimestamp", "timestamp", "lastdetailtimestamp", "timestamp"),("sourcesystemtimestamp", "timestamp", "sourcesystemtimestamp", "timestamp"), ("ipaddress", "string", "ipaddress", "string"), 
("softwareversion", "string", "softwareversion", "string"), ("navversion", "string", "navversion", "string"), ("updateversion", "long", "updateversion", "long"), ("gpshwversion1", "string", "gpshwversion1", "string"), 
("gpshwversion2", "string", "gpshwversion2", "string"), ("powerhwversion", "long", "powerhwversion", "long"), ("powerfwversion", "long", "powerfwversion", "long"), 
("startuptimestamp", "timestamp", "startuptimestamp", "timestamp"), ("fleetmanagementversion", "string", "fleetmanagementversion", "string"), ("keepaliveversion", "string", "keepaliveversion", "string"), 
("vehicleviewversion", "string", "vehicleviewversion", "string"), ("bootloaderfwversion", "long", "bootloaderfwversion", "long"), ("gyroserialnumber", "long", "gyroserialnumber", "long"), ("flags", "long", "flags", "long"),
("navbootloaderversion","long","navbootloaderversion","long")]
, transformation_ctx = "rover_mapping")

## utctimezoneoffsets mapping
utc_mapping = ApplyMapping.apply(frame = utc_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("daylightsavingstimestart", "timestamp", "daylightsavingstimestart", "timestamp"), ("daylightsavingstimeend", "timestamp", "daylightsavingstimeend", "timestamp"), 
("standardtimestart1", "timestamp", "standardtimestart1", "timestamp"), ("standardtimeend1", "timestamp", "standardtimeend1", "timestamp"), ("standardtimestart2", "timestamp", "standardtimestart2", "timestamp"), 
("standardtimeend2", "timestamp", "standardtimeend2", "timestamp"), ("easternutcoffsetinsidedst", "long", "easternutcoffsetinsidedst", "long"), ("easternutcoffsetoutsidedst", "long", "easternutcoffsetoutsidedst", "long"), 
("centralutcoffsetinsidedst", "long", "centralutcoffsetinsidedst", "long"), ("centralutcoffsetoutsidedst", "long", "centralutcoffsetoutsidedst", "long"), 
("mountainutcoffsetinsidedst", "long", "mountainutcoffsetinsidedst", "long"), ("mountainutcoffsetoutsidedst", "long", "mountainutcoffsetoutsidedst", "long"), 
("pacificutcoffsetinsidedst", "long", "pacificutcoffsetinsidedst", "long"), ("pacificutcoffsetoutsidedst", "long", "pacificutcoffsetoutsidedst", "long"), ("arizonautcoffsetinsidedst", "long", "arizonautcoffsetinsidedst", "long"), 
("arizonautcoffsetoutsidedst", "long", "arizonautcoffsetoutsidedst", "long")]
, transformation_ctx = "utc_mapping")

## mapdata mapping
mapdata_mapping = ApplyMapping.apply(frame = mapdata_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("maptypeid", "long", "maptypeid", "long"), ("position1x", "double", "position1x", "double"), ("position1y", "double", "position1y", "double"), ("position3x", "double", "position3x", "double"), 
("position2x", "double", "position2x", "double"), ("position2y", "double", "position2y", "double"), ("position3y", "double", "position3y", "double"), ("parkingrowlength", "double", "parkingrowlength", "double"), 
("rowheight", "double", "rowheight", "double"), ("parkingrowwidth", "double", "parkingrowwidth", "double"), ("columncount", "long", "columncount", "long"), ("rowcount", "long", "rowcount", "long"), 
("startingrownumber", "long", "startingrownumber", "long"), ("incrementby", "long", "incrementby", "long"), ("lot", "string", "lot", "string"), ("row", "string", "row", "string"), 
("startingcolumnnumber", "long", "startingcolumnnumber", "long"), ("slotdigits", "long", "slotdigits", "long"), ("rowangle", "double", "rowangle", "double"), ("flags", "long", "flags", "long"), 
("stringformat", "string", "stringformat", "string"), ("mapconstrainttypeid", "long", "mapconstrainttypeid", "long"), ("isactive", "long", "isactive", "long"), ("mapgroupid", "long", "mapgroupid", "long")]
, transformation_ctx = "mapdata_mapping")

## latchtype mapping
latchtype_mapping = ApplyMapping.apply(frame = latchtype_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("latchtypeid", "long", "latchtypeid", "long"), ("description", "string", "description", "string")]
, transformation_ctx = "latchtype_mapping")

## maptype mapping
maptype_mapping = ApplyMapping.apply(frame = maptype_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("maptypeid", "long", "maptypeid", "long"), ("description", "string", "description", "string")]
, transformation_ctx = "maptype_mapping")

## vehicletype mapping
vehicletype_mapping = ApplyMapping.apply(frame = vehicletype_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("vehicletypeid", "long", "vehicletypeid", "long"), ("description", "string", "description", "string"), ("mapconstrainttypeid", "long", "mapconstrainttypeid", "long"), ("title", "string", "title", "string")]
, transformation_ctx = "vehicletype_mapping")

## flagdescription mapping
flagdesc_mapping = ApplyMapping.apply(frame = flagdesc_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("flagdescriptionid", "long", "flagdescriptionid", "long"), ("flagvalue", "long", "flagvalue", "long"), ("description", "string", "description", "string"), ("tablename", "string", "tablename", "string"), 
("columnname", "string", "columnname", "string"), ("schema", "string", "schema", "string"), ("active", "boolean", "active", "boolean")]
, transformation_ctx = "flagdesc_mapping")

## vehicledetails mapping
vdetails_applymapping = ApplyMapping.apply(frame = vdetails_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("mjid", "long", "mjid", "long"), ("chid", "string", "chid", "string"), 
("lastdetailtimestamp", "timestamp", "lastdetailtimestamp", "timestamp"), ("lastfueltimestamp", "timestamp", "lastfueltimestamp", "timestamp"), ("lastrefueltimestamp", "timestamp", "lastrefueltimestamp", "timestamp"), 
("lasttiretimestamp", "timestamp", "lasttiretimestamp", "timestamp"), ("lastimpacttimestamp", "timestamp", "lastimpacttimestamp", "timestamp"), ("vehicletypeid", "long", "vehicletypeid", "long"), 
("vehiclesubtypeid", "long", "vehiclesubtypeid", "long"), ("trailcolor", "long", "trailcolor", "long"), ("status", "string", "status", "string"), ("tosserveripaddress", "string", "tosserveripaddress", "string"), 
("lastplctimestamp", "timestamp", "lastplctimestamp", "timestamp")]
, transformation_ctx = "vdetails_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################

## latch datasink
latch_datasink = glueContext.write_dynamic_frame.from_options(frame = latch_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/latch", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "latch_datasink")

## position datasink
position_datasink = glueContext.write_dynamic_frame.from_options(frame = position_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/position", "partitionKeys": ["sourcesystemdate"]}, format = "parquet", transformation_ctx = "position_datasink")

## roverdetails datasink
rover_datasink = glueContext.write_dynamic_frame.from_options(frame = rover_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/roverdetails"}, format = "parquet", transformation_ctx = "rover_datasink")

## utctimezoneoffsets datasink
utc_datasink = glueContext.write_dynamic_frame.from_options(frame = utc_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/utctimezoneoffsets"}, format = "parquet", transformation_ctx = "utc_datasink")

## mapdata datasink
mapdata_datasink = glueContext.write_dynamic_frame.from_options(frame = mapdata_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/mapdata"}, format = "parquet", transformation_ctx = "mapdata_datasink")

## latchtype datasink
latchtype_datasink = glueContext.write_dynamic_frame.from_options(frame = latchtype_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/latchtype"}, format = "parquet", transformation_ctx = "latchtype_datasink")

## maptype datasink
maptype_datasink = glueContext.write_dynamic_frame.from_options(frame = maptype_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/maptype"}, format = "parquet", transformation_ctx = "maptype_datasink")

## vehicletype datasink
vehicletype_datasink = glueContext.write_dynamic_frame.from_options(frame = vehicletype_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/vehicletype"}, format = "parquet", transformation_ctx = "vehicletype_datasink")

## flagdescription datasink
flagdesc_datasink = glueContext.write_dynamic_frame.from_options(frame = flagdesc_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/flagdescription"}, format = "parquet", transformation_ctx = "flagdesc_datasink")

## vehicledetails datasink
vdetails_datasink = glueContext.write_dynamic_frame.from_options(frame = vdetails_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mistar/vehicledetails"}, format = "parquet", transformation_ctx = "vdetails_datasink")

# Start Crawler to update Athena Partitions
response = client.start_crawler(
    Name='dl-mistar-dbo-parquet-partitioned'
)
 
job.commit()
