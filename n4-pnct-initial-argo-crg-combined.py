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
argoCV_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "argo_carrier_visit", transformation_ctx = "argoCV_ds")
argoCV_regDF = argoCV_ds.toDF()
argoCV_regDF = argoCV_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoCV_dynDF = DynamicFrame.fromDF(argoCV_regDF,glueContext,"nested")

## argo_chargeable_unit_events connection
argoCUE_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "argo_chargeable_unit_events", transformation_ctx = "argoCUE_ds")
argoCUE_regDF = argoCUE_ds.toDF()
argoCUE_regDF = argoCUE_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoCUE_dynDF = DynamicFrame.fromDF(argoCUE_regDF,glueContext,"nested")

## argo_visit_details connection
argoVD_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "argo_visit_details", transformation_ctx = "argoVD_ds")
argoVD_regDF = argoVD_ds.toDF()
argoVD_regDF = argoVD_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoVD_dynDF = DynamicFrame.fromDF(argoVD_regDF,glueContext,"nested")

## argo_yard connection
argoyard_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "argo_yard", transformation_ctx = "argoyard_ds")
argoyard_regDF = argoyard_ds.toDF()
argoyard_regDF = argoyard_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
argoyard_dynDF = DynamicFrame.fromDF(argoyard_regDF,glueContext,"nested")

## crg_bills_of_lading connection
crgBOL_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "crg_bills_of_lading", transformation_ctx = "crgBOL_ds")
crgBOL_regDF = crgBOL_ds.toDF()
crgBOL_regDF = crgBOL_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
crgBOL_dynDF = DynamicFrame.fromDF(crgBOL_regDF,glueContext,"nested")

## crg_bl_goods connection
crgBG_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "crg_bl_goods", transformation_ctx = "crgBG_ds")
crgBG_regDF = crgBG_ds.toDF()
crgBG_regDF = crgBG_regDF.withColumn("sourcesystem",lit("PNCT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
crgBG_dynDF = DynamicFrame.fromDF(crgBG_regDF,glueContext,"nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
                                        
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
argoyard_mappingcombined = ApplyMapping.apply(frame = argoyard_dynDF, mappings = [
                                                                ("sourcesystem", "string", "sourcesystem", "string"),
                                                                ("dboperationtype", "string", "dboperationtype", "string"),
                                                                ("audtdateadded", "string", "audtdateadded", "timestamp"),
                                                                ("gkey", "long", "gkey", "long"), 
                                                                ("id", "string", "id", "string"), 
                                                                ("name", "string", "name", "string"), 
                                                                ("life_cycle_state", "string", "life_cycle_state", "string"), 
                                                                ("xps_state", "string", "xps_state", "string"), 
                                                                #("compiled_yard", "string", "compiled_yard", "string"), 
                                                                #("sparcs_settings_file", "string", "sparcs_settings_file", "string"), 
                                                                #("berth_text_file", "string", "berth_text_file", "string"), 
                                                                ("fcy_gkey", "long", "fcy_gkey", "long"), 
                                                                ("abm_gkey", "long", "abm_gkey", "long")
                                                            ], 
                                                            transformation_ctx = "argoyard_mappingcombined")                                                            

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


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
                                        
## argo_carrier_visit datasink
argoCV_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoCV_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_carrier_visit"}, format = "parquet", transformation_ctx = "argoCV_datasinkcombined")

## argo_chargeable_unit_events datasink
argoCUE_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoCUE_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_chargeable_unit_events"}, format = "parquet", transformation_ctx = "argoCUE_datasinkcombined")

## argo_visit_details datasink 
argoVD_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoVD_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_visit_details"}, format = "parquet", transformation_ctx = "argoVD_datasinkcombined")

## argo_yard datasink
argoyard_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = argoyard_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/argo_yard"}, format = "parquet", transformation_ctx = "argoyard_datasinkcombined")

## crg_bills_of_lading datasink
crgBOL_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = crgBOL_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/crg_bills_of_lading"}, format = "parquet", transformation_ctx = "crgBOL_datasinkcombined")

## crg_bl_goods datasink
crgBG_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = crgBG_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/crg_bl_goods"}, format = "parquet", transformation_ctx = "crgBG_datasinkcombined")


job.commit()