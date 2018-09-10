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
#1st DF
dep_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "department", transformation_ctx = "dep_DS")
dep_regDF = dep_DS.toDF()
dep_regDF = dep_regDF.filter(dep_regDF['dboperationtype'] !='D')
dep_grouped = dep_regDF.groupBy("departmentid").agg({"updatetime":'max'})
dep_agg = dep_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
dep_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "department", transformation_ctx = "dep_DS2")
dep_regDF2 = dep_DS2.toDF()
dep_regDF2 = dep_regDF2.filter(dep_regDF2['dboperationtype'] !='D')
dep_dist = dep_regDF2.dropDuplicates()
#Join
dep_join = dep_dist.join(dep_agg, (dep_dist.departmentid == dep_agg.departmentid) & (dep_dist.updatetime == dep_agg.updatetime)).drop(dep_agg.departmentid).drop(dep_agg.updatetime)
dep_drop = dep_join.drop('dboperationtype')
dep_final = dep_drop.distinct()
dep_dynDF = DynamicFrame.fromDF(dep_final,glueContext,"nested")

    ###############

## location connection
#1st DF
loc_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "location", transformation_ctx = "loc_DS")
loc_regDF = loc_DS.toDF()
loc_regDF = loc_regDF.filter(loc_regDF['dboperationtype'] !='D')
loc_grouped = loc_regDF.groupBy("locationid").agg({"updatetime":'max'})
loc_agg = loc_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
loc_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "location", transformation_ctx = "loc_DS2")
loc_regDF2 = loc_DS2.toDF()
loc_regDF2 = loc_regDF2.filter(loc_regDF2['dboperationtype'] !='D')
loc_dist = loc_regDF2.dropDuplicates()
#Join
loc_join = loc_dist.join(loc_agg, (loc_dist.locationid == loc_agg.locationid) & (loc_dist.updatetime == loc_agg.updatetime)).drop(loc_agg.locationid).drop(loc_agg.updatetime)
loc_drop = loc_join.drop('dboperationtype')
loc_final = loc_drop.distinct()
loc_dynDF = DynamicFrame.fromDF(loc_final,glueContext,"nested")

    ###############

## lookuptable Connection
#1st DF
lupt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "lookuptable", transformation_ctx = "lupt_DS")
lupt_regDF = lupt_DS.toDF()
lupt_regDF = lupt_regDF.filter(lupt_regDF['dboperationtype'] !='D')
lupt_grouped = lupt_regDF.groupBy("tableid").agg({"updatetime":'max'})
lupt_agg = lupt_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
lupt_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "lookuptable", transformation_ctx = "lupt_DS2")
lupt_regDF2 = lupt_DS2.toDF()
lupt_regDF2 = lupt_regDF2.filter(lupt_regDF2['dboperationtype'] !='D')
lupt_dist = lupt_regDF2.dropDuplicates()
#Join
lupt_join = lupt_dist.join(lupt_agg, (lupt_dist.tableid == lupt_agg.tableid) & (lupt_dist.updatetime == lupt_agg.updatetime)).drop(lupt_agg.tableid).drop(lupt_agg.updatetime)
lupt_drop = lupt_join.drop('dboperationtype')
lupt_final = lupt_drop.distinct()
lupt_dynDF = DynamicFrame.fromDF(lupt_final,glueContext,"nested")

    ###############

## operationalview connection

#1st DF
opview_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "operationalview", transformation_ctx = "opview_DS")
opview_regDF = opview_DS.toDF()
opview_regDF = opview_regDF.filter(opview_regDF['dboperationtype'] !='D')
opview_grouped = opview_regDF.groupBy("operationalviewid").agg({"updatetime":'max'})
opview_agg = opview_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
opview_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "operationalview", transformation_ctx = "opview_DS2")
opview_regDF2 = opview_DS2.toDF()
opview_regDF2 = opview_regDF2.filter(opview_regDF2['dboperationtype'] !='D')
opview_dist = opview_regDF2.dropDuplicates()
#Join
opview_join = opview_dist.join(opview_agg, (opview_dist.operationalviewid == opview_agg.operationalviewid) & (opview_dist.updatetime == opview_agg.updatetime)).drop(opview_agg.operationalviewid).drop(opview_agg.updatetime)
opview_drop = opview_join.drop('dboperationtype')
opview_final = opview_drop.distinct()
opview_dynDF = DynamicFrame.fromDF(opview_final,glueContext,"nested")

    ###############

## worktype connection
#1st DF
workt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "worktype", transformation_ctx = "workt_DS")
workt_regDF = workt_DS.toDF()
workt_regDF = workt_regDF.filter(workt_regDF['dboperationtype'] !='D')
workt_grouped = workt_regDF.groupBy("worktypeid").agg({"updatetime":'max'})
workt_agg = workt_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
workt_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "worktype", transformation_ctx = "workt_DS2")
workt_regDF2 = workt_DS2.toDF()
workt_regDF2 = workt_regDF2.filter(workt_regDF2['dboperationtype'] !='D')
workt_dist = workt_regDF2.dropDuplicates()
#Join
workt_join = workt_dist.join(workt_agg, (workt_dist.worktypeid == workt_agg.worktypeid) & (workt_dist.updatetime == workt_agg.updatetime)).drop(workt_agg.worktypeid).drop(workt_agg.updatetime)
workt_drop = workt_join.drop('dboperationtype')
workt_final = workt_drop.distinct()
workt_dynDF = DynamicFrame.fromDF(workt_final,glueContext,"nested")

## tableattribute connection
#1st DF
tabatt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "tableattribute", transformation_ctx = "tabatt_DS")
tabbatt_regDF = tabatt_DS.toDF()
tabbatt_regDF = tabbatt_regDF.filter(tabbatt_regDF['dboperationtype'] !='D')
tabbatt_grouped = tabbatt_regDF.groupBy("tableattributeid").agg({"updatetime":'max'})
tabbatt_agg = tabbatt_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
tabbatt_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "tableattribute", transformation_ctx = "tabbatt_DS2")
tabbatt_regDF2 = tabbatt_DS2.toDF()
tabbatt_regDF2 = tabbatt_regDF2.filter(tabbatt_regDF2['dboperationtype'] !='D')
tabbatt_dist = tabbatt_regDF2.dropDuplicates()
#Join
tabbatt_join = tabbatt_dist.join(tabbatt_agg, (tabbatt_dist.tableattributeid == tabbatt_agg.tableattributeid) & (tabbatt_dist.updatetime == tabbatt_agg.updatetime)).drop(tabbatt_agg.tableattributeid).drop(tabbatt_agg.updatetime)
tabbatt_drop = tabbatt_join.drop('dboperationtype')
tabbatt_final = tabbatt_drop.distinct()
tabbatt_dynDF = DynamicFrame.fromDF(tabbatt_final,glueContext,"nested")

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## department mapping
dep_applymapping = ApplyMapping.apply(frame = dep_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("departmentid", "long", "departmentid", "long"), ("departmentname", "string", "departmentname", "string"), ("departmentdescription", "string", "departmentdescription", "string"), ("departmenttype", "string", "departmenttype", "string"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string"), ("expendituretypeid", "long", "expendituretypeid", "long")], transformation_ctx = "dep_applymapping")


## location mapping
loc_applymapping = ApplyMapping.apply(frame = loc_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("locationid", "long", "locationid", "long"), ("locationname", "string", "locationname", "string"), ("locationdescription", "string", "locationdescription", "string"), ("gpslatitude", "double", "gpslatitude", "double"), ("gpslongitude", "double", "gpslongitude", "double"), ("mapreference", "string", "mapreference", "string"), ("rfid", "string", "rfid", "string"), ("locationclass1id", "long", "locationclass1id", "long"), ("locationclass2id", "long", "locationclass2id", "long"), ("locationclass3id", "long", "locationclass3id", "long"), ("locationclasstext1", "string", "locationclasstext1", "string"), ("locationclasstext2", "string", "locationclasstext2", "string"), ("locationclasstext3", "string", "locationclasstext3", "string"), ("locationclassvalue1", "double", "locationclassvalue1", "double"), ("locationclassvalue2", "double", "locationclassvalue2", "double"), ("locationclassdate1", "timestamp", "locationclassdate1", "timestamp"), ("locationclassdate2", "timestamp", "locationclassdate2", "timestamp"), ("comments", "string", "comments", "string"), ("addressid", "long", "addressid", "long"), ("locationtype", "string", "locationtype", "string"), ("alternativelocationid", "long", "alternativelocationid", "long"), ("gisforeignkey", "string", "gisforeignkey", "string"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("operationalviewid", "long", "operationalviewid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("pointlocation", "string", "pointlocation", "string"), ("geolocation", "string", "geolocation", "string"), ("layer", "long", "layer", "long"), ("gridlocationx", "double", "gridlocationx", "double"), ("gridlocationy", "double", "gridlocationy", "double"), ("segment", "string", "segment", "string")], transformation_ctx = "loc_applymapping")


## lookuptable Mapping
lupt_applymapping = ApplyMapping.apply(frame = lupt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("tableid", "long", "tableid", "long"), ("tablekey", "string", "tablekey", "string"), ("tablename", "string", "tablename", "string"), ("tabledescription", "string", "tabledescription", "string"), ("tablevalue", "double", "tablevalue", "double"), ("tablevalue2", "double", "tablevalue2", "double"), ("tabletext", "string", "tabletext", "string"), ("tablecomments", "string", "tablecomments", "string"), ("tableclasstext1", "string", "tableclasstext1", "string"), ("tableclasstext2", "string", "tableclasstext2", "string"), ("tableclasstext3", "string", "tableclasstext3", "string"), ("tableclassvalue1", "double", "tableclassvalue1", "double"), ("tableclassvalue2", "double", "tableclassvalue2", "double"), ("tableclassdate1", "timestamp", "tableclassdate1", "timestamp"), ("tableclassdate2", "timestamp", "tableclassdate2", "timestamp"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string")], transformation_ctx = "lupt_applymapping")


## operationalview mapping
opview_applymapping = ApplyMapping.apply(frame = opview_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("operationalviewid", "long", "operationalviewid", "long"), ("alternatekey", "string", "alternatekey", "string"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("type", "string", "type", "string"), ("currencyid", "long", "currencyid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("companypassword", "string", "companypassword", "string")], transformation_ctx = "opview_applymapping")


## worktype mapping
workt_applymapping = ApplyMapping.apply(frame = workt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("worktypeid", "long", "worktypeid", "long"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("category", "string", "category", "string"), ("iscompliance", "boolean", "iscompliance", "boolean"), ("isshutdown", "boolean", "isshutdown", "boolean"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("segment", "string", "segment", "string")], transformation_ctx = "workt_applymapping")

## tableattribute mapping
tabatt_applymapping = ApplyMapping.apply(frame = tabbatt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("tableattributeid", "long", "tableattributeid", "long"), ("attributekey", "string", "attributekey", "string"), ("keyid", "long", "keyid", "long"), ("sequence", "long", "sequence", "long"), ("attributenameid", "long", "attributenameid", "long"), ("datavalue", "string", "datavalue", "string"), ("ismandatory", "boolean", "ismandatory", "boolean"), ("createtime", "string", "createtime", "string"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "string"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "long", "rowstatusid", "long")], transformation_ctx = "tabatt_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
## department datasink
dep_datasink = glueContext.write_dynamic_frame.from_options(frame = dep_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/department"}, format = "parquet", transformation_ctx = "dep_datasink")

## location datasink
loc_datasink = glueContext.write_dynamic_frame.from_options(frame = loc_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/location"}, format = "parquet", transformation_ctx = "loc_datasink")

## lookuptable Datasink
lupt_datasink = glueContext.write_dynamic_frame.from_options(frame = lupt_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/lookuptable"}, format = "parquet", transformation_ctx = "lupt_datasink")

## operationalview datasink
opview_datasink = glueContext.write_dynamic_frame.from_options(frame = opview_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/operationalview"}, format = "parquet", transformation_ctx = "opview_datasink")

## worktype datasink
workt_datasink = glueContext.write_dynamic_frame.from_options(frame = workt_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/worktype"}, format = "parquet", transformation_ctx = "workt_datasink")

## tableattribute datasink
tabatt_datasink = glueContext.write_dynamic_frame.from_options(frame = tabatt_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/tableattribute"}, format = "parquet", transformation_ctx = "tabatt_datasink")


job.commit()
