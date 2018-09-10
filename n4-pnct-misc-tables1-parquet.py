import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,current_timestamp,expr
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



										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
## argo_cal_event connection
#1st DF
calev_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_cal_event", transformation_ctx = "calev_DS")
calev_regDF = calev_DS.toDF()
calev_grouped = calev_regDF.groupBy("gkey")
calev_agg = calev_grouped.agg({"audtdateadded":'max'})
calev_agg = calev_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
#2nd DF
calev_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_cal_event", transformation_ctx = "calev_DS2")
calev_regDF2 = calev_DS2.toDF()
calev_dist = calev_regDF2.dropDuplicates()
#Join
calev_join = calev_dist.join(calev_agg, (calev_dist.gkey == calev_agg.gkey) & (calev_dist.audtdateadded == calev_agg.audtdateadded)).drop(calev_dist.gkey).drop(calev_dist.audtdateadded)
calev_drop = calev_join.drop('dboperationtype', 'deleted_dt', 'is_deleted')
calev_final = calev_drop.distinct()
calev_dynDF = DynamicFrame.fromDF(calev_final,glueContext,"nested")

                            ##############

## argo_facility connection
#1st DF
fac_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_facility", transformation_ctx = "fac_DS")
fac_regDF = fac_DS.toDF()
fac_grouped = fac_regDF.groupBy("gkey")
fac_agg = fac_grouped.agg({"audtdateadded":'max'})
fac_agg = fac_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
#2nd DF
fac_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_facility", transformation_ctx = "fac_DS2")
fac_regDF2 = fac_DS2.toDF()
fac_dist = fac_regDF2.dropDuplicates()
#Join
fac_join = fac_dist.join(fac_agg, (fac_agg.gkey == fac_dist.gkey) & (fac_agg.audtdateadded == fac_dist.audtdateadded)).drop(fac_dist.gkey).drop(fac_dist.audtdateadded)
fac_drop = fac_join.drop('dboperationtype', 'deleted_dt', 'is_deleted')
fac_final = fac_drop.distinct()
fac_dynDF = DynamicFrame.fromDF(fac_final,glueContext,"nested")

                        ##############

## frm_mapping_predicates Connection
#1st DF
mappred_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "frm_mapping_predicates", transformation_ctx = "mappred_DS")
mappred_regDF = mappred_DS.toDF()
mappred_grouped = mappred_regDF.groupBy("gkey")
mappred_agg = mappred_grouped.agg({"audtdateadded":'max'})
mappred_agg = mappred_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
#2nd DF
mappred_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "frm_mapping_predicates", transformation_ctx = "mappred_DS2")
mappred_regDF2 = mappred_DS2.toDF()
mappred_dist = mappred_regDF2.dropDuplicates()
#Join
mappred_join = mappred_dist.join(mappred_agg, (mappred_agg.gkey == mappred_dist.gkey) & (mappred_agg.audtdateadded == mappred_dist.audtdateadded)).drop(mappred_dist.gkey).drop(mappred_dist.audtdateadded)
mappred_drop = mappred_join.drop('dboperationtype', 'deleted_dt', 'is_deleted')
mappred_final = mappred_drop.distinct()
mappred_dynDF = DynamicFrame.fromDF(mappred_final,glueContext,"nested")

                        ##############

## frm_mapping_values connection
#1st DF
mapval_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "frm_mapping_values", transformation_ctx = "mapval_DS")
mapval_regDF = mapval_DS.toDF()
mapval_grouped = mapval_regDF.groupBy("gkey")
mapval_agg = mapval_grouped.agg({"audtdateadded":'max'})
mapval_agg = mapval_agg.withColumnRenamed('max(audtdateadded)', 'audtdateadded')
#2nd DF
mapval_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "frm_mapping_values", transformation_ctx = "mapval_DS2")
mapval_regDF2 = mapval_DS2.toDF()
mapval_dist = mapval_regDF2.dropDuplicates()
#Join
mapval_join = mapval_dist.join(mapval_agg, (mapval_agg.gkey == mapval_dist.gkey) & (mapval_agg.audtdateadded == mapval_dist.audtdateadded)).drop(mapval_dist.gkey).drop(mapval_dist.audtdateadded)
mapval_drop = mapval_join.drop('dboperationtype', 'deleted_dt', 'is_deleted')
mapval_final = mapval_drop.distinct()
mapval_dynDF = DynamicFrame.fromDF(mapval_final,glueContext,"nested")

                        ##############

## inv_storage_rule connection
#1st DF
storrul_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_storage_rule", transformation_ctx = "storrul_DS")
storrul_regDF = storrul_DS.toDF()
storrul_regDF = storrul_regDF.na.fill({'changed':'1900-01-01'})
storrul_grouped = storrul_regDF.groupBy("gkey")
storrul_agg = storrul_grouped.agg({"changed":'max', "created":'max'})
storrul_agg = storrul_agg.withColumnRenamed('max(created)', 'created').withColumnRenamed('max(changed)', 'changed')

#2nd DF
storrul_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_storage_rule", transformation_ctx = "storrul_DS2")
storrul_regDF2 = storrul_DS2.toDF()
storrul_regDF2 = storrul_regDF2.na.fill({'changed':'1900-01-01'})
storrul_dist = storrul_regDF2.dropDuplicates()

#Join
storrul_join = storrul_dist.join(storrul_agg, (storrul_agg.gkey == storrul_dist.gkey) & (storrul_agg.created == storrul_dist.created) & (storrul_agg.changed == storrul_dist.changed)).drop(storrul_dist.gkey).drop(storrul_dist.created).drop(storrul_dist.changed)
storrul_drop = storrul_join.drop('dboperationtype', 'audtdateadded', 'deleted_dt', 'is_deleted')
storrul_final = storrul_drop.distinct()
storrul_dynDF = DynamicFrame.fromDF(storrul_final,glueContext,"nested")

                        ##############

## road_document_messages connection
#1st DF
docmes_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_document_messages", transformation_ctx = "docmes_DS")
docmes_regDF = docmes_DS.toDF()
docmes_na = docmes_regDF.na.fill(-1, subset=['doc_gkey', 'tran_gkey'])
docmes_grouped = docmes_na.groupBy("gkey")
docmes_agg = docmes_grouped.agg({"created":'max',"tran_gkey":'max',"doc_gkey":'max'})
docmes_agg = docmes_agg.withColumnRenamed('max(created)', 'created').withColumnRenamed('max(tran_gkey)', 'tran_gkey').withColumnRenamed('max(doc_gkey)', 'doc_gkey')

#2nd DF
docmes_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_document_messages", transformation_ctx = "docmes_DS2")
docmes_regDF2 = docmes_DS2.toDF()
docmes_na2 = docmes_regDF2.na.fill(-1, subset=['doc_gkey', 'tran_gkey'])
docmes_dist = docmes_na2.dropDuplicates()

#Join
docmes_join = docmes_dist.join(docmes_agg, (docmes_agg.gkey == docmes_dist.gkey) & (docmes_agg.tran_gkey == docmes_dist.tran_gkey) & (docmes_agg.doc_gkey == docmes_dist.doc_gkey)).drop(docmes_dist.gkey).drop(docmes_dist.created).drop(docmes_dist.tran_gkey).drop(docmes_dist.doc_gkey)
docmes_drop = docmes_join.drop('dboperationtype', 'audtdateadded', 'deleted_dt', 'is_deleted')
docmes_final = docmes_drop.distinct()
docmes_dynDF = DynamicFrame.fromDF(docmes_final,glueContext,"nested")

                                          ####################################
                                          ####        MAPPING BLOCK       ####
                                          ####################################
## argo_cal_event mapping
calev_applymapping = ApplyMapping.apply(frame = calev_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("occ_start", "timestamp", "occ_start", "timestamp"), ("rec_end", "timestamp", "rec_end", "timestamp"), ("repeat_interval", "string", "repeat_interval", "string"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("event_type_gkey", "long", "event_type_gkey", "long"), ("calendar_gkey", "long", "calendar_gkey", "long")], transformation_ctx = "calev_applymapping")

## argo_facility mapping
fac_applymapping = ApplyMapping.apply(frame = fac_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("non_operational", "long", "non_operational", "long"), ("time_zone", "string", "time_zone", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("cpx_gkey", "long", "cpx_gkey", "long"), ("rp_gkey", "long", "rp_gkey", "long"), ("teu_green", "long", "teu_green", "long"), ("teu_yellow", "long", "teu_yellow", "long"), ("teu_red", "long", "teu_red", "long"), ("jms_provider", "string", "jms_provider", "string"), ("jms_provider_url", "string", "jms_provider_url", "string"), ("jms_in_uri", "string", "jms_in_uri", "string"), ("jms_poll_in_uri", "string", "jms_poll_in_uri", "string"), ("jms_poll_frequency", "long", "jms_poll_frequency", "long"), ("jms_out_uri", "string", "jms_out_uri", "string"), ("route_resolver_rules", "long", "route_resolver_rules", "long")], transformation_ctx = "fac_applymapping")

## frm_mapping_predicates Mapping
mappred_applymapping = ApplyMapping.apply(frame = mappred_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("verb", "string", "verb", "string"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string"), ("negated", "long", "negated", "long"), ("next_emapp_gkey", "long", "next_emapp_gkey", "long"), ("sub_emapp_gkey", "long", "sub_emapp_gkey", "long")], transformation_ctx = "mappred_applymapping")

## frm_mapping_values mapping
mapval_applymapping = ApplyMapping.apply(frame = mapval_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("emapp_gkey", "long", "emapp_gkey", "long"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string")], transformation_ctx = "mapval_applymapping")

## inv_storage_rule mapping
storrul_applymapping = ApplyMapping.apply(frame = storrul_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("start_day", "string", "start_day", "string"), ("is_start_day_included", "long", "is_start_day_included", "long"), ("start_day_extension", "long", "start_day_extension", "long"), ("end_daty", "string", "end_daty", "string"), ("is_end_day_included", "long", "is_end_day_included", "long"), ("end_day_extension", "long", "end_day_extension", "long"), ("is_freedays_included", "long", "is_freedays_included", "long"), ("is_gratis_included", "long", "is_gratis_included", "long"), ("round_up_hours", "long", "round_up_hours", "long"), ("round_up_minutes", "long", "round_up_minutes", "long"), ("is_rule_for_power", "long", "is_rule_for_power", "long"), ("start_day_cutoff_hours", "long", "start_day_cutoff_hours", "long"), ("end_day_cutoff_hours", "long", "end_day_cutoff_hours", "long"), ("power_charge_by", "string", "power_charge_by", "string"), ("power_first_tier_rounding", "long", "power_first_tier_rounding", "long"), ("power_other_tier_rounding", "long", "power_other_tier_rounding", "long"), ("is_free_time_chged_if_exceeded", "long", "is_free_time_chged_if_exceeded", "long"), ("calendar_gkey", "long", "calendar_gkey", "long"), ("calculation_extension", "long", "calculation_extension", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "storrul_applymapping")

## road_document_messages mapping
docmes_applymapping = ApplyMapping.apply(frame = docmes_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("msg_id", "string", "msg_id", "string"), ("msg_text", "string", "msg_text", "string"), ("severity", "string", "severity", "string"), ("param1", "string", "param1", "string"), ("param2", "string", "param2", "string"), ("param3", "string", "param3", "string"), ("param4", "string", "param4", "string"), ("param5", "string", "param5", "string"), ("param6", "string", "param6", "string"), ("param7", "string", "param7", "string"), ("param8", "string", "param8", "string"), ("param9", "string", "param9", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("stage_id", "string", "stage_id", "string"), ("tran_gkey", "long", "tran_gkey", "long"), ("doc_gkey", "long", "doc_gkey", "long")], transformation_ctx = "docmes_applymapping")


                                           ####################################
                                           ####        DATASINK BLOCK      ####
                                           ####################################
## argo_cal_event datasink
calev_datasink = glueContext.write_dynamic_frame.from_options(frame = calev_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_cal_event"}, format = "parquet", transformation_ctx = "calev_datasink")

## argo_facility datasink
fac_datasink = glueContext.write_dynamic_frame.from_options(frame = fac_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_facility"}, format = "parquet", transformation_ctx = "fac_datasink")

## frm_mapping_predicates Datasink
mappred_datasink = glueContext.write_dynamic_frame.from_options(frame = mappred_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/frm_mapping_predicates"}, format = "parquet", transformation_ctx = "mappred_datasink")

## frm_mapping_values datasink
mapval_datasink = glueContext.write_dynamic_frame.from_options(frame = mapval_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/frm_mapping_values"}, format = "parquet", transformation_ctx = "mapval_datasink")

## inv_storage_rule datasink
storrul_datasink = glueContext.write_dynamic_frame.from_options(frame = storrul_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_storage_rule"}, format = "parquet", transformation_ctx = "storrul_datasink")

## road_document_messages datasink
docmes_datasink = glueContext.write_dynamic_frame.from_options(frame = docmes_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_document_messages"}, format = "parquet", transformation_ctx = "docmes_datasink")

job.commit()