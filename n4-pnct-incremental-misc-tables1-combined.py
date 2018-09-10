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
glue = boto3.client(service_name='glue', region_name='us-west-2',
              endpoint_url='https://glue.us-west-2.amazonaws.com')

										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
## argo_cal_event connection
calev_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "argo_cal_event", transformation_ctx = "calev_DS")
calev_regDF = calev_DS.toDF()
calev_regDF = calev_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
calev_dynDF = DynamicFrame.fromDF(calev_regDF,glueContext,"nested")

## argo_facility connection
fac_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "argo_facility", transformation_ctx = "fac_DS")
fac_regDF = fac_DS.toDF()
fac_regDF = fac_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
fac_dynDF = DynamicFrame.fromDF(fac_regDF,glueContext,"nested")

## frm_mapping_predicates Connection
mappred_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "frm_mapping_predicates", transformation_ctx = "mappred_DS")
mappred_regDF = mappred_DS.toDF()
mappred_regDF = mappred_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
mappred_dynDF = DynamicFrame.fromDF(mappred_regDF,glueContext,"nested")

## frm_mapping_values connection
mapval_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "frm_mapping_values", transformation_ctx = "mapval_DS")
mapval_regDF = mapval_DS.toDF()
mapval_regDF = mapval_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
mapval_dynDF = DynamicFrame.fromDF(mapval_regDF,glueContext,"nested")

## inv_storage_rule connection
storrul_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "inv_storage_rule", transformation_ctx = "storrul_DS")
storrul_regDF = storrul_DS.toDF()
storrul_regDF = storrul_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
storrul_dynDF = DynamicFrame.fromDF(storrul_regDF,glueContext,"nested")

## road_document_messages connection
docmes_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_incremental", table_name = "road_document_messages", transformation_ctx = "docmes_DS")
docmes_regDF = docmes_DS.toDF()
docmes_regDF = docmes_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("audtdateadded",lit(current_timestamp))
docmes_dynDF = DynamicFrame.fromDF(docmes_regDF,glueContext,"nested")

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## argo_cal_event mapping
calev_applymapping = ApplyMapping.apply(frame = calev_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("occ_start", "string", "occ_start", "timestamp"), ("rec_end", "string", "rec_end", "timestamp"), ("repeat_interval", "string", "repeat_interval", "string"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("event_type_gkey", "long", "event_type_gkey", "long"), ("calendar_gkey", "long", "calendar_gkey", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "calev_applymapping")

## argo_facility mapping
fac_applymapping = ApplyMapping.apply(frame = fac_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("non_operational", "long", "non_operational", "long"), ("time_zone", "string", "time_zone", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("cpx_gkey", "long", "cpx_gkey", "long"), ("rp_gkey", "long", "rp_gkey", "long"), ("teu_green", "long", "teu_green", "long"), ("teu_yellow", "long", "teu_yellow", "long"), ("teu_red", "long", "teu_red", "long"), ("jms_provider", "string", "jms_provider", "string"), ("jms_provider_url", "string", "jms_provider_url", "string"), ("jms_in_uri", "string", "jms_in_uri", "string"), ("jms_poll_in_uri", "string", "jms_poll_in_uri", "string"), ("jms_poll_frequency", "long", "jms_poll_frequency", "long"), ("jms_out_uri", "string", "jms_out_uri", "string"), ("route_resolver_rules", "long", "route_resolver_rules", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "fac_applymapping")

## frm_mapping_predicates Mapping
mappred_applymapping = ApplyMapping.apply(frame = mappred_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),
("gkey", "long", "gkey", "long"), ("verb", "string", "verb", "string"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string"), ("negated", "long", "negated", "long"), ("next_emapp_gkey", "long", "next_emapp_gkey", "long"), ("sub_emapp_gkey", "long", "sub_emapp_gkey", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "mappred_applymapping")

## frm_mapping_values mapping
mapval_applymapping = ApplyMapping.apply(frame = mapval_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("emapp_gkey", "long", "emapp_gkey", "long"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "mapval_applymapping")

## inv_storage_rule mapping
storrul_applymapping = ApplyMapping.apply(frame = storrul_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("start_day", "string", "start_day", "string"), ("is_start_day_included", "long", "is_start_day_included", "long"), ("start_day_extension", "long", "start_day_extension", "long"), ("end_daty", "string", "end_daty", "string"), ("is_end_day_included", "long", "is_end_day_included", "long"), ("end_day_extension", "long", "end_day_extension", "long"), ("is_freedays_included", "long", "is_freedays_included", "long"), ("is_gratis_included", "long", "is_gratis_included", "long"), ("round_up_hours", "long", "round_up_hours", "long"), ("round_up_minutes", "long", "round_up_minutes", "long"), ("is_rule_for_power", "long", "is_rule_for_power", "long"), ("start_day_cutoff_hours", "long", "start_day_cutoff_hours", "long"), ("end_day_cutoff_hours", "long", "end_day_cutoff_hours", "long"), ("power_charge_by", "string", "power_charge_by", "string"), ("power_first_tier_rounding", "long", "power_first_tier_rounding", "long"), ("power_other_tier_rounding", "long", "power_other_tier_rounding", "long"), ("is_free_time_chged_if_exceeded", "long", "is_free_time_chged_if_exceeded", "long"), ("calendar_gkey", "long", "calendar_gkey", "long"), ("calculation_extension", "long", "calculation_extension", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "storrul_applymapping")

## road_document_messages mapping
docmes_applymapping = ApplyMapping.apply(frame = docmes_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("msg_id", "string", "msg_id", "string"), ("msg_text", "string", "msg_text", "string"), ("severity", "string", "severity", "string"), ("param1", "string", "param1", "string"), ("param2", "string", "param2", "string"), ("param3", "string", "param3", "string"), ("param4", "string", "param4", "string"), ("param5", "string", "param5", "string"), ("param6", "string", "param6", "string"), ("param7", "string", "param7", "string"), ("param8", "string", "param8", "string"), ("param9", "string", "param9", "string"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("stage_id", "string", "stage_id", "string"), ("tran_gkey", "long", "tran_gkey", "long"), ("doc_gkey", "long", "doc_gkey", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "docmes_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
## argo_cal_event datasink
calev_datasink = glueContext.write_dynamic_frame.from_options(frame = calev_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_cal_event"}, format = "parquet", transformation_ctx = "calev_datasink")

## argo_facility datasink
fac_datasink = glueContext.write_dynamic_frame.from_options(frame = fac_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_facility"}, format = "parquet", transformation_ctx = "fac_datasink")

## frm_mapping_predicates Datasink
mappred_datasink = glueContext.write_dynamic_frame.from_options(frame = mappred_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/frm_mapping_predicates"}, format = "parquet", transformation_ctx = "mappred_datasink")

## frm_mapping_values datasink
mapval_datasink = glueContext.write_dynamic_frame.from_options(frame = mapval_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/frm_mapping_values"}, format = "parquet", transformation_ctx = "mapval_datasink")

## inv_storage_rule datasink
storrul_datasink = glueContext.write_dynamic_frame.from_options(frame = storrul_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/inv_storage_rule"}, format = "parquet", transformation_ctx = "storrul_datasink")

## road_document_messages datasink
docmes_datasink = glueContext.write_dynamic_frame.from_options(frame = docmes_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/road_document_messages"}, format = "parquet", transformation_ctx = "docmes_datasink")


# start Parquet processing
glue.start_job_run(JobName = 'n4-pnct-misc-tables1-parquet')


job.commit()
