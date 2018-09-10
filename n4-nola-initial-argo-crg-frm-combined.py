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
                                        
## argo_carrier_visit connection
argoCV_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_carrier_visit", transformation_ctx = "argoCV_ds")
argoCV_regDF = argoCV_ds.toDF()
argoCV_regDF = argoCV_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoCV_distDF = argoCV_regDF.distinct() 
argoCV_dynDF = DynamicFrame.fromDF(argoCV_distDF,glueContext,"nested")

## argo_chargeable_unit_events connection
argoCUE_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_chargeable_unit_events", transformation_ctx = "argoCUE_ds")
argoCUE_regDF = argoCUE_ds.toDF()
argoCUE_regDF = argoCUE_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoCUE_distDF=argoCUE_regDF.distinct()
argoCUE_dynDF = DynamicFrame.fromDF(argoCUE_distDF,glueContext,"nested")

## argo_visit_details connection
argoVD_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_visit_details", transformation_ctx = "argoVD_ds")
argoVD_regDF = argoVD_ds.toDF()
argoVD_regDF = argoVD_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoVD_distDF=argoVD_regDF.distinct()
argoVD_dynDF = DynamicFrame.fromDF(argoVD_distDF,glueContext,"nested")

## argo_yard connection
#argoyard_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_yard", transformation_ctx = "argoyard_ds")
#argoyard_regDF = argoyard_ds.toDF()
#argoyard_regDF = argoyard_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
#argoyard_distDF=argoyard_regDF.distinct()
#argoyard_dynDF = DynamicFrame.fromDF(argoyard_distDF,glueContext,"nested")

## argo_cal_event connection
calev_DS = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_cal_event", transformation_ctx = "calev_DS")
calev_regDF = calev_DS.toDF()
calev_regDF = calev_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("audtdateadded",lit(current_timestamp))
calev_distDF=calev_regDF.distinct()
calev_dynDF = DynamicFrame.fromDF(calev_distDF,glueContext,"nested")

## argo_facility connection
fac_DS = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "argo_facility", transformation_ctx = "fac_DS")
fac_regDF = fac_DS.toDF()
fac_regDF = fac_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("audtdateadded",lit(current_timestamp))
fac_distDF=fac_regDF.distinct()
fac_dynDF = DynamicFrame.fromDF(fac_distDF,glueContext,"nested")

## crg_bills_of_lading connection
crgBOL_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "crg_bills_of_lading", transformation_ctx = "crgBOL_ds")
crgBOL_regDF = crgBOL_ds.toDF()
crgBOL_regDF = crgBOL_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
crgBOL_distDF=crgBOL_regDF.distinct()
crgBOL_dynDF = DynamicFrame.fromDF(crgBOL_distDF,glueContext,"nested")

## crg_bl_goods connection
crgBG_ds = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "crg_bl_goods", transformation_ctx = "crgBG_ds")
crgBG_regDF = crgBG_ds.toDF()
crgBG_regDF = crgBG_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
crgBG_distDF=crgBG_regDF.distinct()
crgBG_dynDF = DynamicFrame.fromDF(crgBG_distDF,glueContext,"nested")

## frm_mapping_predicates Connection
mappred_DS = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "frm_mapping_predicates", transformation_ctx = "mappred_DS")
mappred_regDF = mappred_DS.toDF()
mappred_regDF = mappred_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("audtdateadded",lit(current_timestamp))
mappred_distDF=mappred_regDF.distinct()
mappred_dynDF = DynamicFrame.fromDF(mappred_distDF,glueContext,"nested")

## frm_mapping_values connection
mapval_DS = glueContext.create_dynamic_frame.from_catalog(database = "nola_staging_initial", table_name = "frm_mapping_values", transformation_ctx = "mapval_DS")
mapval_regDF = mapval_DS.toDF()
mapval_regDF = mapval_regDF.withColumn("sourcesystem",lit("NOLA")).withColumn("audtdateadded",lit(current_timestamp))
mapval_distDF=mapval_regDF.distinct()
mapval_dynDF = DynamicFrame.fromDF(mapval_distDF,glueContext,"nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
										
## argo_cal_event mapping
calev_applymapping = ApplyMapping.apply(frame = calev_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("name", "string", "name", "string"), ("description", "string", "description", "string"), ("occ_start", "string", "occ_start", "timestamp"), ("rec_end", "string", "rec_end", "timestamp"), ("repeat_interval", "string", "repeat_interval", "string"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("event_type_gkey", "long", "event_type_gkey", "long"), ("calendar_gkey", "long", "calendar_gkey", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "calev_applymapping")

## argo_facility mapping
fac_applymapping = ApplyMapping.apply(frame = fac_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("non_operational", "long", "non_operational", "long"), ("time_zone", "string", "time_zone", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("cpx_gkey", "long", "cpx_gkey", "long"), ("rp_gkey", "long", "rp_gkey", "long"), ("teu_green", "long", "teu_green", "long"), ("teu_yellow", "long", "teu_yellow", "long"), ("teu_red", "long", "teu_red", "long"), ("jms_provider", "string", "jms_provider", "string"), ("jms_provider_url", "string", "jms_provider_url", "string"), ("jms_in_uri", "string", "jms_in_uri", "string"), ("jms_poll_in_uri", "string", "jms_poll_in_uri", "string"), ("jms_poll_frequency", "long", "jms_poll_frequency", "long"), ("jms_out_uri", "string", "jms_out_uri", "string"), ("route_resolver_rules", "long", "route_resolver_rules", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "fac_applymapping")
										
                                        
## argo_carrier_visit mapping                                                       
argoCV_mappingcombined = ApplyMapping.apply(frame = argoCV_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("id", "string", "id", "string"), 
                                                                ("customs_id", "string", "customs_id", "string"), 
                                                                ("carrier_mode", "string", "carrier_mode", "string"), 
                                                                ("visit_nbr", "long", "visit_nbr", "long"), 
                                                                ("phase", "string", "phase", "string"), 
                                                                ("operator_gkey", "long", "operator_gkey", "long"), 
                                                                ("cpx_gkey", "long", "cpx_gkey", "long"), 
                                                                ("fcy_gkey", "long", "fcy_gkey", "long"), 
                                                                ("next_fcy_gkey", "long", "next_fcy_gkey", "long"), 
                                                                ("ata", "string", "ata", "timestamp"), 
                                                                ("atd", "string", "atd", "timestamp"), 
                                                                ("send_on_board_unit_updates", "long", "send_on_board_unit_updates", "long"), 
                                                                ("send_crane_work_list_updates", "long", "send_crane_work_list_updates", "long"), 
                                                                ("cvcvd_gkey", "long", "cvcvd_gkey", "long"), 
                                                                ("created", "string", "created", "timestamp"), 
                                                                ("creator", "string", "creator", "string"), 
                                                                ("changed", "string", "changed", "timestamp"), 
                                                                ("changer", "string", "changer", "string"), 
                                                                ("deleted_dt", "string", "deleted_dt", "timestamp"), 
                                                                ("is_deleted", "boolean", "is_deleted", "boolean")
                                                            ], 
                                                            transformation_ctx = "argoCV_mappingcombined")       

## argo_chargeable_unit_events mapping                                                       
argoCUE_mappingcombined = ApplyMapping.apply(frame = argoCUE_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("batch_id", "long", "batch_id", "long"), 
                                                                ("event_type", "string", "event_type", "string"), 
                                                                ("source_event_gkey", "long", "source_event_gkey", "long"), 
                                                                ("ufv_gkey", "long", "ufv_gkey", "long"), 
                                                                ("facility_id", "string", "facility_id", "string"), 
                                                                ("facility_gkey", "long", "facility_gkey", "long"), 
                                                                ("complex_id", "string", "complex_id", "string"), 
                                                                ("terminal_operator_gkey", "long", "terminal_operator_gkey", "long"), 
                                                                ("equipment_id", "string", "equipment_id", "string"), 
                                                                ("eq_subclass", "string", "eq_subclass", "string"), 
                                                                ("line_operator_id", "string", "line_operator_id", "string"), 
                                                                ("freight_kind", "string", "freight_kind", "string"), 
                                                                ("iso_code", "string", "iso_code", "string"), 
                                                                ("iso_group", "string", "iso_group", "string"), 
                                                                ("eq_length", "string", "eq_length", "string"), 
                                                                ("eq_height", "string", "eq_height", "string"), 
                                                                ("category", "string", "category", "string"), 
                                                                ("paid_thru_day", "string", "paid_thru_day", "timestamp"), 
                                                                ("dray_status", "string", "dray_status", "string"), 
                                                                ("is_oog", "long", "is_oog", "long"), 
                                                                ("is_refrigerated", "long", "is_refrigerated", "long"), 
                                                                ("is_hazardous", "long", "is_hazardous", "long"), 
                                                                ("imdg_class", "string", "imdg_class", "string"), 
                                                                ("fire_code", "string", "fire_code", "string"), 
                                                                ("fire_code_class", "string", "fire_code_class", "string"), 
                                                                ("commodity_id", "string", "commodity_id", "string"), 
                                                                ("special_stow_id", "string", "special_stow_id", "string"), 
                                                                ("pod1", "string", "pod1", "string"), 
                                                                ("pol1", "string", "pol1", "string"), 
                                                                ("bundle_unitid", "string", "bundle_unitid", "string"), 
                                                                ("bundle_ufv_gkey", "long", "bundle_ufv_gkey", "long"), 
                                                                ("bl_nbr", "string", "bl_nbr", "string"), 
                                                                ("guarantee_party", "string", "guarantee_party", "string"), 
                                                                ("guarantee_thru_day", "string", "guarantee_thru_day", "timestamp"), 
                                                                ("booking_nbr", "string", "booking_nbr", "string"), 
                                                                ("restow_type", "string", "restow_type", "string"), 
                                                                ("unit_gkey", "long", "unit_gkey", "long"), 
                                                                ("opl", "string", "opl", "string"), 
                                                                ("final_destination", "string", "final_destination", "string"), 
                                                                ("ufv_time_in", "string", "ufv_time_in", "timestamp"), 
                                                                ("ufv_time_out", "string", "ufv_time_out", "timestamp"), 
                                                                ("temp_reqd_c", "float", "temp_reqd_c", "float"), 
                                                                ("time_load", "string", "time_load", "timestamp"), 
                                                                ("consignee_id", "string", "consignee_id", "string"), 
                                                                ("shipper_id", "string", "shipper_id", "string"), 
                                                                ("time_first_free_day", "string", "time_first_free_day", "string"), 
                                                                ("time_discharge_complete", "string", "time_discharge_complete", "timestamp"), 
                                                                ("cargo_quantity", "float", "cargo_quantity", "float"), 
                                                                ("cargo_quantity_unit", "string", "cargo_quantity_unit", "string"), 
                                                                ("ib_intended_loctype", "string", "ib_intended_loctype", "string"), 
                                                                ("ib_intended_id", "string", "ib_intended_id", "string"), 
                                                                ("ib_intended_visit_id", "string", "ib_intended_visit_id", "string"), 
                                                                ("ib_intended_carrier_name", "string", "ib_intended_carrier_name", "string"), 
                                                                ("ib_intended_call_nbr", "string", "ib_intended_call_nbr", "string"), 
                                                                ("ib_intended_eta", "string", "ib_intended_eta", "timestamp"), 
                                                                ("ib_intended_ata", "string", "ib_intended_ata", "timestamp"), 
                                                                ("ib_intended_atd", "string", "ib_intended_atd", "timestamp"), 
                                                                ("ib_intended_vessel_type", "string", "ib_intended_vessel_type", "string"), 
                                                                ("ib_intended_line_id", "string", "ib_intended_line_id", "string"), 
                                                                ("ib_intended_service_id", "string", "ib_intended_service_id", "string"), 
                                                                ("ib_intended_vessel_class_id", "string", "ib_intended_vessel_class_id", "string"), 
                                                                ("ib_loctype", "string", "ib_loctype", "string"), 
                                                                ("ib_id", "string", "ib_id", "string"), 
                                                                ("ib_visit_id", "string", "ib_visit_id", "string"), 
                                                                ("ib_carrier_name", "string", "ib_carrier_name", "string"), 
                                                                ("ib_call_nbr", "string", "ib_call_nbr", "string"), 
                                                                ("ib_eta", "string", "ib_eta", "string"), 
                                                                ("ib_ata", "string", "ib_ata", "string"), 
                                                                ("ib_atd", "string", "ib_atd", "string"), 
                                                                ("ib_vessel_type", "string", "ib_vessel_type", "string"), 
                                                                ("ib_line_id", "string", "ib_line_id", "string"), 
                                                                ("ib_service_id", "string", "ib_service_id", "string"), 
                                                                ("ib_vessel_class_id", "string", "ib_vessel_class_id", "string"), 
                                                                ("ib_vessel_lloyds_id", "string", "ib_vessel_lloyds_id", "string"), 
                                                                ("ob_intended_loctype", "string", "ob_intended_loctype", "string"), 
                                                                ("ob_intended_id", "string", "ob_intended_id", "string"), 
                                                                ("ob_intended_visit_id", "string", "ob_intended_visit_id", "string"), 
                                                                ("ob_intended_carrier_name", "string", "ob_intended_carrier_name", "string"), 
                                                                ("ob_intended_call_nbr", "string", "ob_intended_call_nbr", "string"), 
                                                                ("ob_intended_eta", "string", "ob_intended_eta", "timestamp"), 
                                                                ("ob_intended_ata", "string", "ob_intended_ata", "timestamp"), 
                                                                ("ob_intended_atd", "string", "ob_intended_atd", "timestamp"), 
                                                                ("ob_intended_vessel_type", "string", "ob_intended_vessel_type", "string"), 
                                                                ("ob_intended_line_id", "string", "ob_intended_line_id", "string"), 
                                                                ("ob_intended_service_id", "string", "ob_intended_service_id", "string"), 
                                                                ("ob_intended_vessel_class_id", "string", "ob_intended_vessel_class_id", "string"), 
                                                                ("ob_loctype", "string", "ob_loctype", "string"), 
                                                                ("ob_id", "string", "ob_id", "string"), 
                                                                ("ob_visit_id", "string", "ob_visit_id", "string"), 
                                                                ("ob_carrier_name", "string", "ob_carrier_name", "string"), 
                                                                ("ob_call_nbr", "string", "ob_call_nbr", "string"), 
                                                                ("ob_eta", "string", "ob_eta", "string"), 
                                                                ("ob_ata", "string", "ob_ata", "string"), 
                                                                ("ob_atd", "string", "ob_atd", "string"), 
                                                                ("ob_vessel_type", "string", "ob_vessel_type", "string"), 
                                                                ("ob_line_id", "string", "ob_line_id", "string"), 
                                                                ("ob_service_id", "string", "ob_service_id", "string"), 
                                                                ("ob_vessel_class_id", "string", "ob_vessel_class_id", "string"), 
                                                                ("ob_vessel_lloyds_id", "string", "ob_vessel_lloyds_id", "string"), 
                                                                ("event_start_time", "string", "event_start_time", "timestamp"), 
                                                                ("event_end_time", "string", "event_end_time", "timestamp"), 
                                                                ("rule_start_day", "string", "rule_start_day", "timestamp"), 
                                                                ("rule_end_day", "string", "rule_end_day", "timestamp"), 
                                                                ("payee_customer_id", "string", "payee_customer_id", "string"), 
                                                                ("payee_role", "string", "payee_role", "string"), 
                                                                ("first_availability_date", "string", "first_availability_date", "timestamp"), 
                                                                ("qc_che_id", "string", "qc_che_id", "string"), 
                                                                ("fm_pos_loctype", "string", "fm_pos_loctype", "string"), 
                                                                ("fm_pos_locid", "string", "fm_pos_locid", "string"), 
                                                                ("to_pos_loctype", "string", "to_pos_loctype", "string"), 
                                                                ("to_pos_locid", "string", "to_pos_locid", "string"), 
                                                                ("service_order", "string", "service_order", "string"), 
                                                                ("restow_reason", "string", "restow_reason", "string"), 
                                                                ("restow_account", "string", "restow_account", "string"), 
                                                                ("rehandle_count", "long", "rehandle_count", "long"), 
                                                                ("quantity", "float", "quantity", "float"), 
                                                                ("quantity_unit", "string", "quantity_unit", "string"), 
                                                                ("fm_pos_slot", "string", "fm_pos_slot", "string"), 
                                                                ("fm_pos_name", "string", "fm_pos_name", "string"), 
                                                                ("to_pos_slot", "string", "to_pos_slot", "string"), 
                                                                ("to_pos_name", "string", "to_pos_name", "string"), 
                                                                ("is_locked", "long", "is_locked", "long"), 
                                                                ("status", "string", "status", "string"), 
                                                                ("last_draft_inv_nbr", "string", "last_draft_inv_nbr", "string"), 
                                                                ("notes", "string", "notes", "string"), 
                                                                ("is_override_value", "long", "is_override_value", "long"), 
                                                                ("override_value_type", "string", "override_value_type", "string"), 
                                                                ("override_value", "float", "override_value", "float"), 
                                                                ("guarantee_id", "string", "guarantee_id", "string"), 
                                                                ("guarantee_gkey", "long", "guarantee_gkey", "long"), 
                                                                ("gnte_inv_processing_status", "string", "gnte_inv_processing_status", "string"), 
                                                                ("flex_string01", "string", "flex_string01", "string"), 
                                                                ("flex_string02", "string", "flex_string02", "string"), 
                                                                ("flex_string03", "string", "flex_string03", "string"), 
                                                                ("flex_string04", "string", "flex_string04", "string"), 
                                                                ("flex_string05", "string", "flex_string05", "string"), 
                                                                ("flex_string06", "string", "flex_string06", "string"), 
                                                                ("flex_string07", "string", "flex_string07", "string"), 
                                                                ("flex_string08", "string", "flex_string08", "string"), 
                                                                ("flex_string09", "string", "flex_string09", "string"), 
                                                                ("flex_string10", "string", "flex_string10", "string"), 
                                                                ("flex_string11", "string", "flex_string11", "string"), 
                                                                ("flex_string12", "string", "flex_string12", "string"), 
                                                                ("flex_string13", "string", "flex_string13", "string"), 
                                                                ("flex_string14", "string", "flex_string14", "string"), 
                                                                ("flex_string15", "string", "flex_string15", "string"), 
                                                                ("flex_string16", "string", "flex_string16", "string"), 
                                                                ("flex_string17", "string", "flex_string17", "string"), 
                                                                ("flex_string18", "string", "flex_string18", "string"), 
                                                                ("flex_string19", "string", "flex_string19", "string"), 
                                                                ("flex_string20", "string", "flex_string20", "string"), 
																("flex_string21", "string", "flex_string21", "string"),
																   ("flex_string22", "string", "flex_string22", "string"),
																   ("flex_string23", "string", "flex_string23", "string"),
																   ("flex_string24", "string", "flex_string24", "string"),
																   ("flex_string25", "string", "flex_string25", "string"),
																   ("flex_string26", "string", "flex_string26", "string"),
																   ("flex_string27", "string", "flex_string27", "string"),
																   ("flex_string28", "string", "flex_string28", "string"),
																   ("flex_string29", "string", "flex_string29", "string"),
																   ("flex_string30", "string", "flex_string30", "string"),
																   ("flex_string31", "string", "flex_string31", "string"),
																   ("flex_string32", "string", "flex_string32", "string"),
																   ("flex_string33", "string", "flex_string33", "string"),
																   ("flex_string34", "string", "flex_string34", "string"),
																   ("flex_string35", "string", "flex_string35", "string"),
																   ("flex_string36", "string", "flex_string36", "string"),
																   ("flex_string37", "string", "flex_string37", "string"),
																   ("flex_string38", "string", "flex_string38", "string"),
																   ("flex_string39", "string", "flex_string39", "string"),
																   ("flex_string40", "string", "flex_string40", "string"),
                                                                ("flex_date01", "string", "flex_date01", "timestamp"), 
                                                                ("flex_date02", "string", "flex_date02", "timestamp"), 
                                                                ("flex_date03", "string", "flex_date03", "timestamp"), 
                                                                ("flex_date04", "string", "flex_date04", "timestamp"), 
                                                                ("flex_date05", "string", "flex_date05", "timestamp"), 
                                                                ("flex_long01", "long", "flex_long01", "long"), 
                                                                ("flex_long02", "long", "flex_long02", "long"), 
                                                                ("flex_long03", "long", "flex_long03", "long"), 
                                                                ("flex_long04", "long", "flex_long04", "long"), 
                                                                ("flex_long05", "long", "flex_long05", "long"), 
                                                                ("flex_double01", "float", "flex_double01", "float"), 
                                                                ("flex_double02", "float", "flex_double02", "float"), 
                                                                ("flex_double03", "float", "flex_double03", "float"), 
                                                                ("flex_double04", "float", "flex_double04", "float"), 
                                                                ("flex_double05", "float", "flex_double05", "float"), 
                                                                ("equipment_owner_id", "string", "equipment_owner_id", "string"), 
                                                                ("equipment_role", "string", "equipment_role", "string"), 
                                                                ("created", "string", "created", "timestamp"), 
                                                                ("creator", "string", "creator", "string"), 
                                                                ("changed", "string", "changed", "timestamp"), 
                                                                ("changer", "string", "changer", "string"), 
                                                                ("verified_gross_mass", "float", "verified_gross_mass", "float"), 
                                                                ("vgm_verifier_entity", "string", "vgm_verifier_entity", "string"), 
																("is_bundle", "bigint", "is_bundle", "bigint" ),
                                                                ("deleted_dt", "string", "deleted_dt", "timestamp"), 
                                                                ("is_deleted", "boolean", "is_deleted", "boolean")
                                                            ], 
                                                            transformation_ctx = "argoCUE_mappingcombined")   	

## argo_visit_details mapping
argoVD_mappingcombined = ApplyMapping.apply(frame = argoVD_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("eta", "string", "eta", "timestamp"), 
                                                                ("etd", "string", "etd", "timestamp"), 
                                                                ("time_discharge_complete", "string", "time_discharge_complete", "timestamp"), 
                                                                ("time_first_availabiltiy", "string", "time_first_availabiltiy", "timestamp"), 
                                                                ("ffd", "string", "ffd", "timestamp"), 
                                                                ("service", "long", "service", "long"), 
                                                                ("itinereray", "long", "itinereray", "long"), 
                                                                ("in_call_number", "string", "in_call_number", "string"), 
                                                                ("out_call_number", "string", "out_call_number", "string"), 
                                                                ("data_source", "string", "data_source", "string"), 
                                                                ("time_periodic_start", "string", "time_periodic_start", "timestamp"), 
                                                                ("time_periodic_end", "string", "time_periodic_end", "timestamp"), 
                                                                ("periodic_interval", "long", "periodic_interval", "long"), 
                                                                ("life_cycle_state", "string", "life_cycle_state", "string"), 
                                                                ("deleted_dt", "string", "deleted_dt", "timestamp"), 
                                                                ("is_deleted", "boolean", "is_deleted", "boolean")
                                                            ], 
                                                            transformation_ctx = "argoVD_mappingcombined") 	

## argo_yard mapping
#argoyard_mappingcombined = ApplyMapping.apply(frame = argoyard_dynDF, mappings = [
 #                                                               ("sourcesystem", "string", "sourcesystem", "string"),
  #                                                              ("dboperationtype", "string", "dboperationtype", "string"),
   #                                                             ("audtdateadded", "string", "audtdateadded", "timestamp"),
    #                                                            ("gkey", "long", "gkey", "long"), 
     #                                                           ("id", "string", "id", "string"), 
      #                                                          ("name", "string", "name", "string"), 
       ##                                                        ("xps_state", "string", "xps_state", "string"), 
         #                                                       #("compiled_yard", "string", "compiled_yard", "string"), 
          #                                                      #("sparcs_settings_file", "string", "sparcs_settings_file", "string"), 
           #                                                     #("berth_text_file", "string", "berth_text_file", "string"), 
            #                                                    ("fcy_gkey", "long", "fcy_gkey", "long"), 
             #                                                   ("abm_gkey", "long", "abm_gkey", "long")
              #                                              ], 
               #                                             transformation_ctx = "argoyard_mappingcombined")                                                            

## crg_bills_of_lading mapping                                                       
crgBOL_mappingcombined = ApplyMapping.apply(frame = crgBOL_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("nbr", "string", "nbr", "string"), 
                                                                ("gid", "long", "gid", "long"), 
                                                                ("category", "string", "category", "string"), 
                                                                ("consignee_name", "string", "consignee_name", "string"), 
                                                                ("shipper_name", "string", "shipper_name", "string"), 
                                                                ("bonded_destination", "string", "bonded_destination", "string"), 
                                                                ("original_bl_gkey", "long", "original_bl_gkey", "long"), 
                                                                ("origin", "string", "origin", "string"), 
                                                                ("destination", "string", "destination", "string"), 
                                                                ("notes", "string", "notes", "string"), 
                                                                ("manifested_qty", "float", "manifested_qty", "float"), 
                                                                ("inbond", "string", "inbond", "string"), 
                                                                ("exam", "string", "exam", "string"), 
                                                                ("mnft_bl_seq_nbr", "string", "mnft_bl_seq_nbr", "string"), 
                                                                ("pin", "string", "pin", "string"), 
                                                                ("notify_pty1_name", "string", "notify_pty1_name", "string"), 
                                                                ("notify_pty2_name", "string", "notify_pty2_name", "string"), 
                                                                ("notify_pty1_add1", "string", "notify_pty1_add1", "string"), 
                                                                ("notify_pty2_add1", "string", "notify_pty2_add1", "string"), 
                                                                ("notify_pty1_ctct_no1", "string", "notify_pty1_ctct_no1", "string"), 
                                                                ("notify_pty2_ctct_no1", "string", "notify_pty2_ctct_no1", "string"), 
                                                                ("complex_gkey", "long", "complex_gkey", "long"), 
                                                                ("line_gkey", "long", "line_gkey", "long"), 
                                                                ("consignee_gkey", "long", "consignee_gkey", "long"), 
                                                                ("shipper_gkey", "long", "shipper_gkey", "long"), 
                                                                ("agent_gkey", "long", "agent_gkey", "long"), 
                                                                ("cv_gkey", "long", "cv_gkey", "long"), 
                                                                ("pol_gkey", "long", "pol_gkey", "long"), 
                                                                ("pod1_gkey", "long", "pod1_gkey", "long"), 
                                                                ("pod2_gkey", "long", "pod2_gkey", "long"), 
                                                                ("bond_trucking_company", "long", "bond_trucking_company", "long"), 
                                                                ("created", "string", "created", "timestamp"), 
                                                                ("creator", "string", "creator", "string"), 
                                                                ("changed", "string", "changed", "timestamp"), 
                                                                ("changer", "string", "changer", "string"), 
																("flex_string01", "string", "flex_string01", "string"),
															   ("flex_string02", "string", "flex_string02", "string"),
															   ("flex_string03", "string", "flex_string03", "string"),
															   ("flex_string04", "string", "flex_string04", "string"),
															   ("flex_string05", "string", "flex_string05", "string"),
															   ("flex_date01", "string", "flex_date01", "timestamp"),
															   ("flex_date02", "string", "flex_date02", "timestamp"),
															   ("flex_date03", "string", "flex_date03", "timestamp"),
															   ("flex_date04", "string", "flex_date04", "timestamp"),
															   ("flex_date05", "string", "flex_date05", "timestamp"),
                                                                ("deleted_dt", "string", "deleted_dt", "timestamp"), 
                                                                ("is_deleted", "boolean", "is_deleted", "boolean")
                                                            ], 
                                                            transformation_ctx = "crgBOL_mappingcombined")  

## crg_bl_goods mapping
crgBG_mappingcombined = ApplyMapping.apply(frame = crgBG_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("bl_gkey", "long", "bl_gkey", "long"), 
                                                                ("gds_gkey", "long", "gds_gkey", "long"), 
                                                                ("created", "string", "created", "timestamp"), 
                                                                ("creator", "string", "creator", "string"), 
                                                                ("changed", "string", "changed", "timestamp"), 
                                                                ("changer", "string", "changer", "string"), 
                                                                ("deleted_dt", "string", "deleted_dt", "timestamp"), 
                                                                ("is_deleted", "boolean", "is_deleted", "boolean")
                                                            ], 
                                                            transformation_ctx = "crgBG_mappingcombined")		

## frm_mapping_predicates Mapping
mappred_applymapping = ApplyMapping.apply(frame = mappred_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),
("gkey", "long", "gkey", "long"), ("verb", "string", "verb", "string"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string"), ("negated", "long", "negated", "long"), ("next_emapp_gkey", "long", "next_emapp_gkey", "long"), ("sub_emapp_gkey", "long", "sub_emapp_gkey", "long"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "mappred_applymapping")

## frm_mapping_values mapping
mapval_applymapping = ApplyMapping.apply(frame = mapval_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), ("emapp_gkey", "long", "emapp_gkey", "long"), ("metafield", "string", "metafield", "string"), ("value", "string", "value", "string"), ("uivalue", "string", "uivalue", "string"), ("deleted_dt", "string",  "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "mapval_applymapping")
															


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
                                        
## argo_cal_event datasink
calev_datasink = glueContext.write_dynamic_frame.from_options(frame = calev_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_cal_event"}, format = "parquet", transformation_ctx = "calev_datasink")

## argo_facility datasink
fac_datasink = glueContext.write_dynamic_frame.from_options(frame = fac_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_facility"}, format = "parquet", transformation_ctx = "fac_datasink")
										
## argo_carrier_visit datasink
argoCV_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoCV_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_carrier_visit"}, format = "parquet", transformation_ctx = "argoCV_datasinkcombined")

## argo_chargeable_unit_events datasink
argoCUE_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoCUE_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_chargeable_unit_events"}, format = "parquet", transformation_ctx = "argoCUE_datasinkcombined")

## argo_visit_details datasink 
argoVD_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoVD_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_visit_details"}, format = "parquet", transformation_ctx = "argoVD_datasinkcombined")

## argo_yard datasink
#argoyard_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoyard_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/argo_yard"}, format = "parquet", transformation_ctx = "argoyard_datasinkcombined")

## crg_bills_of_lading datasink
crgBOL_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = crgBOL_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/crg_bills_of_lading"}, format = "parquet", transformation_ctx = "crgBOL_datasinkcombined")

## crg_bl_goods datasink
crgBG_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = crgBG_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/crg_bl_goods"}, format = "parquet", transformation_ctx = "crgBG_datasinkcombined")

## frm_mapping_predicates Datasink
mappred_datasink = glueContext.write_dynamic_frame.from_options(frame = mappred_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/frm_mapping_predicates"}, format = "parquet", transformation_ctx = "mappred_datasink")

## frm_mapping_values datasink
mapval_datasink = glueContext.write_dynamic_frame.from_options(frame = mapval_applymapping, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/nola/combined/frm_mapping_values"}, format = "parquet", transformation_ctx = "mapval_datasink")


job.commit()