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
										
## ref_bizunit_scoped connection
refBizScopedCon_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_bizunit_scoped", transformation_ctx = "refBizScopedCon_ds")
refBizScopedCon_regDF = refBizScopedCon_ds.toDF()
refBizScopedCon_regDF = refBizScopedCon_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refBizScopedCon_dynDF = DynamicFrame.fromDF(refBizScopedCon_regDF, glueContext, "nested")

## ref_carrier_itinerary connection
refCarItinCon_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_carrier_itinerary", transformation_ctx = "refCarItinCon_ds")
refCarItinCon_regDF = refCarItinCon_ds.toDF()
refCarItinCon_regDF = refCarItinCon_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refCarItinCon_dynDF = DynamicFrame.fromDF(refCarItinCon_regDF, glueContext, "nested")

## ref_carrier_service connection
refCarServCon_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_carrier_service", transformation_ctx = "refCarServCon_ds")
refCarServCon_regDF = refCarServCon_ds.toDF()
refCarServCon_regDF = refCarServCon_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refCarServCon_dynDF = DynamicFrame.fromDF(refCarServCon_regDF, glueContext, "nested")

## ref_country connection
refCountryCon_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_country", transformation_ctx = "refCountryCon_ds")
refCountryCon_regDF = refCountryCon_ds.toDF()
refCountryCon_regDF = refCountryCon_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refCountryCon_dynDF = DynamicFrame.fromDF(refCountryCon_regDF, glueContext, "nested")

## ref_equipment connection
refEquip_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_equipment", transformation_ctx = "refEquip_ds")
refEquip_regDF = refEquip_ds.toDF()
refEquip_regDF = refEquip_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refEquip_dynDF = DynamicFrame.fromDF(refEquip_regDF, glueContext, "nested")

## ref_equip_type connection
refEquipType_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_equip_type", transformation_ctx = "refEquipType_ds")
refEquipType_regDF = refEquipType_ds.toDF()
refEquipType_regDF = refEquipType_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refEquipType_dynDF = DynamicFrame.fromDF(refEquipType_regDF, glueContext, "nested")

## ref_line_operator connection
refLineOper_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_line_operator", transformation_ctx = "refLineOper_ds")
refLineOper_regDF = refLineOper_ds.toDF()
refLineOper_regDF = refLineOper_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refLineOper_dynDF = DynamicFrame.fromDF(refLineOper_regDF, glueContext, "nested")

## ref_routing_point connection
refRoutingPCon_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_routing_point", transformation_ctx = "refRoutingPCon_ds")
refRoutingPCon_regDF = refRoutingPCon_ds.toDF()
refRoutingPCon_regDF = refRoutingPCon_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refRoutingPCon_dynDF = DynamicFrame.fromDF(refRoutingPCon_regDF, glueContext, "nested")

## ref_unloc_code connection
refUC_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "ref_unloc_code", transformation_ctx = "refUC_ds")
refUC_regDF = refUC_ds.toDF()
refUC_regDF = refUC_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
refUC_dynDF = DynamicFrame.fromDF(refUC_regDF, glueContext, "nested")

## srv_event connection
srvEV_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "srv_event", transformation_ctx = "srvEV_ds")
srvEV_regDF = srvEV_ds.toDF()
srvEV_regDF = srvEV_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
srvEV_dynDF = DynamicFrame.fromDF(srvEV_regDF, glueContext, "nested")

## srv_event_types connection
srvEVType_ds = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "srv_event_types", transformation_ctx = "srvEVType_ds")
srvEVType_regDF = srvEVType_ds.toDF()
srvEVType_regDF = srvEVType_regDF.withColumn("sourcesystem", lit("PNCT")).withColumn("dboperationtype", lit("L")).withColumn("audtdateadded", lit(current_timestamp))
srvEVType_dynDF = DynamicFrame.fromDF(srvEVType_regDF, glueContext, "nested")

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
						
## ref_bizunit_scoped mapping
refBizScopedCon_mappingcombined = ApplyMapping.apply(frame = refBizScopedCon_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("name", "string", "name", "string"), ("role", "string", "role", "string"), ("scac", "string", "scac", "string"), ("bic", "string", "bic", "string"), ("is_eq_owner", "long", "is_eq_owner", "long"), ("is_eq_operator", "long", "is_eq_operator", "long"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("credit_status", "string", "credit_status", "string"), ("per_unit_guarantee_limit", "float", "per_unit_guarantee_limit", "float"), ("notes", "string", "notes", "string"), ("ct_name", "string", "ct_name", "string"), ("address_line1", "string", "address_line1", "string"), ("address_line2", "string", "address_line2", "string"), ("address_line3", "string", "address_line3", "string"), ("city", "string", "city", "string"), ("state_gkey", "long", "state_gkey", "long"), ("country_code", "string", "country_code", "string"), ("mail_code", "string", "mail_code", "string"), ("telephone", "string", "telephone", "string"), ("fax", "string", "fax", "string"), ("email_address", "string", "email_address", "string"), ("sms_number", "string", "sms_number", "string"), ("website_url", "string", "website_url", "string"), ("ftp_address", "string", "ftp_address", "string"), ("bizu_gkey", "long", "bizu_gkey", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refBizScopedCon_mappingcombined")
				
## ref_carrier_itinerary mapping
refCarItinCon_mappingcombined = ApplyMapping.apply(frame = refCarItinCon_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("owner_cv", "long", "owner_cv", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refCarItinCon_mappingcombined")

## ref_carrier_service mapping
refCarServCon_mappingcombined = ApplyMapping.apply(frame = refCarServCon_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("name", "string", "name", "string"), ("carrier_mode", "string", "carrier_mode", "string"), ("itin_gkey", "long", "itin_gkey", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("pkey", "long", "pkey", "long"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refCarServCon_mappingcombined")

## ref_country mapping
refCountryCon_mappingcombined = ApplyMapping.apply(frame = refCountryCon_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("cntry_code", "string", "cntry_code", "string"), ("cntry_alpha3_code", "string", "cntry_alpha3_code", "string"), ("cntry_num3_code", "string", "cntry_num3_code", "string"), ("cntry_name", "string", "cntry_name", "string"), ("cntry_off_name", "string", "cntry_off_name", "string"), ("cntry_life_cycle_state", "string", "cntry_life_cycle_state", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refCountryCon_mappingcombined")

## ref_equipment mapping
refEquip_mappingcombined = ApplyMapping.apply(frame = refEquip_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("eq_subclass", "string", "eq_subclass", "string"), ("id", "string", "id", "string"), ("scope", "long", "scope", "long"), ("data_source", "string", "data_source", "string"), ("id_prefix", "string", "id_prefix", "string"), ("id_nbr_only", "string", "id_nbr_only", "string"), ("id_check_digit", "string", "id_check_digit", "string"), ("id_full", "string", "id_full", "string"), ("class", "string", "class", "string"), ("eqtyp_gkey", "long", "eqtyp_gkey", "long"), ("length_mm", "long", "length_mm", "long"), ("height_mm", "long", "height_mm", "long"), ("width_mm", "long", "width_mm", "long"), ("iso_group", "string", "iso_group", "string"), ("tare_kg", "float", "tare_kg", "float"), ("safe_kg", "float", "safe_kg", "float"), ("owner", "long", "owner", "long"), ("operator", "long", "operator", "long"), ("prior_operator", "long", "prior_operator", "long"), ("lease_expiration", "string", "lease_expiration", "timestamp"), ("build_date", "string", "build_date", "timestamp"), ("no_stow_on_if_empty", "long", "no_stow_on_if_empty", "long"), ("no_stow_on_if_laden", "long", "no_stow_on_if_laden", "long"), ("must_stow_below_deck", "long", "must_stow_below_deck", "long"), ("must_stow_above_deck", "long", "must_stow_above_deck", "long"), ("p_o_s", "long", "p_o_s", "long"), ("insulated", "long", "insulated", "long"), ("material", "string", "material", "string"), ("trnsp_type", "string", "trnsp_type", "string"), ("trnsp_id", "string", "trnsp_id", "string"), ("trnsp_time", "string", "trnsp_time", "timestamp"), ("uses_accessories", "long", "uses_accessories", "long"), ("is_temperature_controlled", "long", "is_temperature_controlled", "long"), ("oog_ok", "long", "oog_ok", "long"), ("unsealable", "long", "unsealable", "long"), ("has_wheels", "long", "has_wheels", "long"), ("is_open", "long", "is_open", "long"), ("strength_code", "string", "strength_code", "string"), ("csc_expiration", "string", "csc_expiration", "string"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("x_cord", "float", "x_cord", "float"), ("y_cord", "float", "y_cord", "float"), ("z_cord", "float", "z_cord", "float"), ("rfr_type", "string", "rfr_type", "string"), ("is_starvent", "long", "is_starvent", "long"), ("is_super_freeze_reefer", "long", "is_super_freeze_reefer", "long"), ("is_controlled_atmo_reefer", "long", "is_controlled_atmo_reefer", "long"), ("is_vented", "long", "is_vented", "long"), ("tank_rails", "string", "tank_rails", "string"), ("license_nbr", "string", "license_nbr", "string"), ("license_state", "string", "license_state", "string"), ("fed_inspect_exp", "string", "fed_inspect_exp", "timestamp"), ("state_inspect_exp", "string", "state_inspect_exp", "timestamp"), ("axle_count", "long", "axle_count", "long"), ("is_triaxle", "long", "is_triaxle", "long"), ("fits_20", "long", "fits_20", "long"), ("fits_24", "long", "fits_24", "long"), ("fits_30", "long", "fits_30", "long"), ("fits_40", "long", "fits_40", "long"), ("fits_45", "long", "fits_45", "long"), ("fits_48", "long", "fits_48", "long"), ("fits_53", "long", "fits_53", "long"), ("door_kind", "string", "door_kind", "string"), ("tows_dolly", "long", "tows_dolly", "long"), ("semi_reefer_kind", "string", "semi_reefer_kind", "string"), ("acry_description", "string", "acry_description", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refEquip_mappingcombined")

## ref_equip_type mapping
refEquipType_mappingcombined = ApplyMapping.apply(frame = refEquipType_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("data_source", "string", "data_source", "string"), ("is_deprecated", "long", "is_deprecated", "long"), ("is_archetype", "long", "is_archetype", "long"), ("archetype", "long", "archetype", "long"), ("clone_source", "long", "clone_source", "long"), ("basic_length", "string", "basic_length", "string"), ("nominal_length", "string", "nominal_length", "string"), ("nominal_height", "string", "nominal_height", "string"), ("rfr_type", "string", "rfr_type", "string"), ("class", "string", "class", "string"), ("iso_group", "string", "iso_group", "string"), ("length_mm", "long", "length_mm", "long"), ("height_mm", "long", "height_mm", "long"), ("width_mm", "long", "width_mm", "long"), ("tare_weight_kg", "float", "tare_weight_kg", "float"), ("safe_weight_kg", "float", "safe_weight_kg", "float"), ("description", "string", "description", "string"), ("pict_id", "long", "pict_id", "long"), ("milliteus", "long", "milliteus", "long"), ("uses_accessories", "long", "uses_accessories", "long"), ("is_temperature_controlled", "long", "is_temperature_controlled", "long"), ("oog_ok", "long", "oog_ok", "long"), ("unsealable", "long", "unsealable", "long"), ("has_wheels", "long", "has_wheels", "long"), ("is_open", "long", "is_open", "long"), ("fits_20", "long", "fits_20", "long"), ("fits_24", "long", "fits_24", "long"), ("fits_30", "long", "fits_30", "long"), ("fits_40", "long", "fits_40", "long"), ("fits_45", "long", "fits_45", "long"), ("fits_48", "long", "fits_48", "long"), ("fits_53", "long", "fits_53", "long"), ("is_bombcart", "long", "is_bombcart", "long"), ("is_cassette", "long", "is_cassette", "long"), ("is_nopick", "long", "is_nopick", "long"), ("is_triaxle", "long", "is_triaxle", "long"), ("is_super_freeze_reefer", "long", "is_super_freeze_reefer", "long"), ("is_controlled_atmo_reefer", "long", "is_controlled_atmo_reefer", "long"), ("no_stow_on_if_empty", "long", "no_stow_on_if_empty", "long"), ("no_stow_on_if_laden", "long", "no_stow_on_if_laden", "long"), ("must_stow_below_deck", "long", "must_stow_below_deck", "long"), ("must_stow_above_deck", "long", "must_stow_above_deck", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("teu_capacity", "long", "teu_capacity", "long"), ("slot_labels", "string", "slot_labels", "string"), ("is_2x20notallowed", "long", "is_2x20notallowed", "long"), ("events_type_to_record", "string", "events_type_to_record", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refEquipType_mappingcombined")

## ref_line_operator mapping
refLineOper_mappingcombined = ApplyMapping.apply(frame = refLineOper_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("lineop_id","long","lineop_id","long"), ("dot_haz_carrier_cert_nbr", "string", "dot_haz_carrier_cert_nbr", "string"), ("bkg_unique", "long", "bkg_unique", "long"), ("stop_bkg_updates", "long", "stop_bkg_updates", "long"), ("stop_rtg_updates", "long", "stop_rtg_updates", "long"), ("bl_unique", "long", "bl_unique", "long"), ("bkg_usage", "string", "bkg_usage", "string"), ("bkg_roll", "string", "bkg_roll", "string"), ("bkg_roll_cutoff", "string", "bkg_roll_cutoff", "string"), ("bkg_adhoc", "long", "bkg_adhoc", "long"), ("bkg_is_not_validated", "long", "bkg_is_not_validated", "long"), ("roll_late_ctr", "long", "roll_late_ctr", "long"), ("demurrage_rules", "long", "demurrage_rules", "long"), ("power_rules", "long", "power_rules", "long"), ("line_demurrage_rules", "long", "line_demurrage_rules", "long"), ("gen_pin_nbr", "long", "gen_pin_nbr", "long"), ("use_pin_nbr", "long", "use_pin_nbr", "long"), ("order_item_not_unique", "long", "order_item_not_unique", "long"), ("is_order_nbr_unique", "long", "is_order_nbr_unique", "long"), ("is_edo_nbr_unique_by_facility", "long", "is_edo_nbr_unique_by_facility", "long"), ("reefer_time_mon1", "string", "reefer_time_mon1", "timestamp"), ("reefer_time_mon2", "string", "reefer_time_mon2", "timestamp"), ("reefer_time_mon3", "string", "reefer_time_mon3", "timestamp"), ("reefer_time_mon4", "string", "reefer_time_mon4", "timestamp"), ("reefer_off_power_time", "long", "reefer_off_power_time", "long"), ("propagate_changes", "long", "propagate_changes", "long"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refLineOper_mappingcombined")

## ref_routing_point mapping
refRoutingPCon_mappingcombined = ApplyMapping.apply(frame = refRoutingPCon_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("transit_to_mode", "string", "transit_to_mode", "string"), ("unloc_gkey", "long", "unloc_gkey", "long"), ("actual_pod_gkey", "long", "actual_pod_gkey", "long"), ("schedule_dcode", "string", "schedule_dcode", "string"), ("schedule_kcode", "string", "schedule_kcode", "string"), ("splc_code", "string", "splc_code", "string"), ("terminal", "string", "terminal", "string"), ("is_placeholder_point", "long", "is_placeholder_point", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("data_source", "string", "data_source", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refRoutingPCon_mappingcombined")

## ref_unloc_code mapping
refUC_mappingcombined = ApplyMapping.apply(frame = refUC_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("cntry_code", "string", "cntry_code", "string"), ("place_code", "string", "place_code", "string"), ("place_name", "string", "place_name", "string"), ("is_port", "long", "is_port", "long"), ("is_rlterminal", "long", "is_rlterminal", "long"), ("is_rdterminal", "long", "is_rdterminal", "long"), ("is_airport", "long", "is_airport", "long"), ("is_multimodal", "long", "is_multimodal", "long"), ("is_fdtrans", "long", "is_fdtrans", "long"), ("is_brcross", "long", "is_brcross", "long"), ("is_funcunknown", "long", "is_funcunknown", "long"), ("unloc_status", "string", "unloc_status", "string"), ("sub_div", "string", "sub_div", "string"), ("latitude", "string", "latitude", "string"), ("latnors", "string", "latnors", "string"), ("longitude", "string", "longitude", "string"), ("lateorw", "string", "lateorw", "string"), ("remarks", "string", "remarks", "string"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "refUC_mappingcombined")

## srv_event mapping
srvEV_mappingcombined = ApplyMapping.apply(frame = srvEV_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("operator_gkey", "long", "operator_gkey", "long"), ("complex_gkey", "long", "complex_gkey", "long"), ("facility_gkey", "long", "facility_gkey", "long"), ("yard_gkey", "long", "yard_gkey", "long"), ("placed_by", "string", "placed_by", "string"), ("placed_time", "string", "placed_time", "timestamp"), ("event_type_gkey", "long", "event_type_gkey", "long"), ("applied_to_class", "string", "applied_to_class", "string"), ("applied_to_gkey", "long", "applied_to_gkey", "long"), ("applied_to_natural_key", "string", "applied_to_natural_key", "string"), ("note", "string", "note", "string"), ("billing_extract_batch_id", "long", "billing_extract_batch_id", "long"), ("quantity", "float", "quantity", "float"), ("quantity_unit", "string", "quantity_unit", "string"), ("responsible_party", "long", "responsible_party", "long"), ("related_entity_gkey", "long", "related_entity_gkey", "long"), ("related_entity_id", "string", "related_entity_id", "string"), ("related_entity_class", "string", "related_entity_class", "string"), ("related_batch_id", "long", "related_batch_id", "long"), ("acknowledged", "string", "acknowledged", "timestamp"), ("acknowledged_by", "string", "acknowledged_by", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_date01", "string", "flex_date01", "timestamp"), ("flex_date02", "string", "flex_date02", "timestamp"), ("flex_date03", "string", "flex_date03", "timestamp"), ("flex_double01", "float", "flex_double01", "long"), ("flex_double02", "float", "flex_double02", "float"), ("flex_double03", "float", "flex_double03", "float"), ("flex_double04", "float", "flex_double04", "float"), ("flex_double05", "float", "flex_double05", "float"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("primary_event_gkey", "long", "primary_event_gkey", "long"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "srvEV_mappingcombined")

## srv_event_types mapping
srvEVType_mappingcombined = ApplyMapping.apply(frame = srvEVType_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "string", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("reference_set", "long", "reference_set", "long"), ("id", "string", "id", "string"), ("description", "string", "description", "string"), ("is_built_in", "long", "is_built_in", "long"), ("is_bulkable", "long", "is_bulkable", "long"), ("is_facility_service", "long", "is_facility_service", "long"), ("is_billable", "long", "is_billable", "long"), ("is_notifiable", "long", "is_notifiable", "long"), ("is_acknowledgeable", "long", "is_acknowledgeable", "long"), ("functional_area", "string", "functional_area", "string"), ("is_event_recorded", "long", "is_event_recorded", "long"), ("applies_to", "string", "applies_to", "string"), ("filter_gkey", "long", "filter_gkey", "long"), ("is_auto_extract", "long", "is_auto_extract", "long"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "srvEVType_mappingcombined")



                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## ref_bizunit_scoped datasink
refBizScopedCon_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refBizScopedCon_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_bizunit_scoped"}, format = "parquet", transformation_ctx = "refBizScopedCon_datasinkcombined")

## ref_carrier_itinerary datasink
refCarItinCon_datasinkparquest = glueContext.write_dynamic_frame.from_options(frame = refCarItinCon_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_carrier_itinerary"}, format = "parquet", transformation_ctx = "refCarItinCon_datasinkparquest")

## ref_carrier_service datasink
refCarServCon_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refCarServCon_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_carrier_service"}, format = "parquet", transformation_ctx = "refCarServCon_datasinkcombined")

## ref_country datasink
refCountryCon_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refCountryCon_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_country"}, format = "parquet", transformation_ctx = "refCountryCon_datasinkcombined")

## ref_equipment datasink
refEquip_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refEquip_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_equipment"}, format = "parquet", transformation_ctx = "refEquip_datasinkcombined")

## ref_equip_type datasink
refEquipType_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refEquipType_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_equip_type"}, format = "parquet", transformation_ctx = "refEquipType_datasinkcombined")

## ref_line_operator datasink
refLineOper_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refLineOper_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_line_operator"}, format = "parquet", transformation_ctx = "refLineOper_datasinkcombined")

## ref_routing_point datasink
refRoutingPCon_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refRoutingPCon_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_routing_point"}, format = "parquet", transformation_ctx = "refRoutingPCon_datasinkcombined")

## ref_unloc_code datasink
refUC_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = refUC_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/ref_unloc_code"}, format = "parquet", transformation_ctx = "refUC_datasinkcombined")

## srv_event datasink
srvEV_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = srvEV_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/srv_event"}, format = "parquet", transformation_ctx = "srvEV_datasinkcombined")

## srv_event_types datasink
srvEVType_datasinkcombined = glueContext.write_dynamic_frame.from_options(frame = srvEVType_mappingcombined, connection_type = "s3", connection_options = {"path": "s3://pa-dms-staging/n4/pnct/combined/srv_event_types"}, format = "parquet", transformation_ctx = "srvEVType_datasinkcombined")



job.commit()