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

current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
## department connection
dep_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "department", transformation_ctx = "dep_DS")
dep_regDF = dep_DS.toDF()
dep_regDF = dep_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
dep_dynDF = DynamicFrame.fromDF(dep_regDF,glueContext,"nested")

## location connection
loc_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "location", transformation_ctx = "loc_DS")
loc_regDF = loc_DS.toDF()
loc_regDF = loc_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
loc_dynDF = DynamicFrame.fromDF(loc_regDF,glueContext,"nested")

## lookuptable Connection
lupt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "lookuptable", transformation_ctx = "lupt_DS")
lupt_regDF = lupt_DS.toDF()
lupt_regDF = lupt_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
lupt_dynDF = DynamicFrame.fromDF(lupt_regDF,glueContext,"nested")

## operationalview connection
opview_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "operationalview", transformation_ctx = "opview_DS")
opview_regDF = opview_DS.toDF()
opview_regDF = opview_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
opview_dynDF = DynamicFrame.fromDF(opview_regDF,glueContext,"nested")

## worktype connection
workt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "worktype", transformation_ctx = "workt_DS")
workt_regDF = workt_DS.toDF()
workt_regDF = workt_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
workt_dynDF = DynamicFrame.fromDF(workt_regDF,glueContext,"nested")

## table attribute connection
tabatt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "tableattribute", transformation_ctx = "tabatt_DS")
tabatt_regDF = tabatt_DS.toDF()
tabatt_regDF = tabatt_regDF.withColumn("sourcesystem",lit("MAINPAC")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
tabatt_dynDF = DynamicFrame.fromDF(tabatt_regDF,glueContext,"nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## department mapping
dep_applymapping = ApplyMapping.apply(frame = dep_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"), ("departmentid", "long", "departmentid", "long"), ("departmentname", "string", "departmentname", "string"), ("departmentdescription", "string", "departmentdescription", "string"), ("departmenttype", "string", "departmenttype", "string"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string"), ("expendituretypeid", "long", "expendituretypeid", "long")], transformation_ctx = "dep_applymapping")

## location mapping
loc_applymapping = ApplyMapping.apply(frame = loc_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"), ("locationid", "long", "locationid", "long"), ("locationname", "string", "locationname", "string"), ("locationdescription", "string", "locationdescription", "string"), ("gpslatitude", "double", "gpslatitude", "double"), ("gpslongitude", "double", "gpslongitude", "double"), ("mapreference", "string", "mapreference", "string"), ("rfid", "string", "rfid", "string"), ("locationclass1id", "long", "locationclass1id", "long"), ("locationclass2id", "long", "locationclass2id", "long"), ("locationclass3id", "long", "locationclass3id", "long"), ("locationclasstext1", "string", "locationclasstext1", "string"), ("locationclasstext2", "string", "locationclasstext2", "string"), ("locationclasstext3", "string", "locationclasstext3", "string"), ("locationclassvalue1", "double", "locationclassvalue1", "double"), ("locationclassvalue2", "double", "locationclassvalue2", "double"), ("locationclassdate1", "string", "locationclassdate1", "timestamp"), ("locationclassdate2", "string", "locationclassdate2", "timestamp"), ("comments", "string", "comments", "string"), ("addressid", "long", "addressid", "long"), ("locationtype", "string", "locationtype", "string"), ("alternativelocationid", "long", "alternativelocationid", "long"), ("gisforeignkey", "string", "gisforeignkey", "string"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("operationalviewid", "long", "operationalviewid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("pointlocation", "string", "pointlocation", "string"), ("geolocation", "string", "geolocation", "string"), ("layer", "long", "layer", "long"), ("gridlocationx", "double", "gridlocationx", "double"), ("gridlocationy", "double", "gridlocationy", "double"), ("segment", "string", "segment", "string")], transformation_ctx = "loc_applymapping")

## lookuptable Mapping
lupt_applymapping = ApplyMapping.apply(frame = lupt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"), ("tableid", "long", "tableid", "long"), ("tablekey", "string", "tablekey", "string"), ("tablename", "string", "tablename", "string"), ("tabledescription", "string", "tabledescription", "string"), ("tablevalue", "double", "tablevalue", "double"), ("tablevalue2", "double", "tablevalue2", "double"), ("tabletext", "string", "tabletext", "string"), ("tablecomments", "string", "tablecomments", "string"), ("tableclasstext1", "string", "tableclasstext1", "string"), ("tableclasstext2", "string", "tableclasstext2", "string"), ("tableclasstext3", "string", "tableclasstext3", "string"), ("tableclassvalue1", "double", "tableclassvalue1", "double"), ("tableclassvalue2", "double", "tableclassvalue2", "double"), ("tableclassdate1", "string", "tableclassdate1", "timestamp"), ("tableclassdate2", "string", "tableclassdate2", "timestamp"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string")], transformation_ctx = "lupt_applymapping")

## operationalview mapping
opview_applymapping = ApplyMapping.apply(frame = opview_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"), ("operationalviewid", "long", "operationalviewid", "long"), ("alternatekey", "string", "alternatekey", "string"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("type", "string", "type", "string"), ("currencyid", "long", "currencyid", "long"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("companypassword", "string", "companypassword", "string")], transformation_ctx = "opview_applymapping")


## worktype mapping
workt_applymapping = ApplyMapping.apply(frame = workt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"), ("worktypeid", "long", "worktypeid", "long"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("category", "string", "category", "string"), ("iscompliance", "boolean", "iscompliance", "boolean"), ("isshutdown", "boolean", "isshutdown", "boolean"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string")], transformation_ctx = "workt_applymapping")

## table attribute mapping
tabatt_applymapping = ApplyMapping.apply(frame = tabatt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("tableattributeid", "long", "tableattributeid", "long"), ("attributekey", "string", "attributekey", "string"), ("keyid", "long", "keyid", "long"), ("sequence", "long", "sequence", "long"), ("attributenameid", "long", "attributenameid", "long"), ("datavalue", "string", "datavalue", "string"), ("ismandatory", "boolean", "ismandatory", "boolean"), ("createtime", "string", "createtime", "string"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "string"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "long", "rowstatusid", "long")], transformation_ctx = "tabatt_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
## department datasink
dep_datasink = glueContext.write_dynamic_frame.from_options(frame = dep_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/department"}, format = "parquet", transformation_ctx = "dep_datasink")

## location datasink
loc_datasink = glueContext.write_dynamic_frame.from_options(frame = loc_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/location"}, format = "parquet", transformation_ctx = "loc_datasink")

## lookuptable Datasink
lupt_datasink = glueContext.write_dynamic_frame.from_options(frame = lupt_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/lookuptable"}, format = "parquet", transformation_ctx = "lupt_datasink")

## operationalview datasink
opview_datasink = glueContext.write_dynamic_frame.from_options(frame = opview_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/operationalview"}, format = "parquet", transformation_ctx = "opview_datasink")

## worktype datasink
workt_datasink = glueContext.write_dynamic_frame.from_options(frame = workt_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/worktype"}, format = "parquet", transformation_ctx = "workt_datasink")

## table attribute datasink
tabatt_datasink = glueContext.write_dynamic_frame.from_options(frame = tabatt_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/mainpac/combined/tableattribute"}, format = "parquet", transformation_ctx = "tabatt_datasink")


job.commit()

