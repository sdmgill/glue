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
										
## equipment_uses datasource
eq_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "equipment_uses", transformation_ctx = "eq_DS")
eq_regDF = eq_DS.toDF()
eq_regDF = eq_regDF.withColumn("sourcesystem",lit("SMT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
eq_dynDF = DynamicFrame.fromDF(eq_regDF,glueContext,"nested")

## truck_transactions datasource
tt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "truck_transactions", transformation_ctx = "tt_DS")
tt_regDF = tt_DS.toDF()
tt_regDF = tt_regDF.withColumn("sourcesystem",lit("SMT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
tt_dynDF = DynamicFrame.fromDF(tt_regDF,glueContext,"nested")

## truck_visits datasource
tv_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_initial", table_name = "truck_visits", transformation_ctx = "tv_DS")
tv_regDF = tv_DS.toDF()
tv_regDF = tv_regDF.withColumn("sourcesystem",lit("SMT")).withColumn("dboperationtype",lit("L")).withColumn("audtdateadded",lit(current_timestamp))
tv_dynDF = DynamicFrame.fromDF(tv_regDF,glueContext,"nested")



                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
										
## equipment_uses mapping
eq_applymapping = ApplyMapping.apply(frame = eq_dynDF, mappings = 
[("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), 
("eq_nbr", "string", "eq_nbr", "string"), ("eq_class", "string", "eq_class", "string"), ("eqsz_id", "string", "eqsz_id", "string"), ("eqtp_id", "string", "eqtp_id", "string"), ("eqht_id", "string", "eqht_id", "string"), 
("iso_code", "string", "iso_code", "string"), ("eq_owner_id", "string", "eq_owner_id", "string"), ("line_id", "string", "line_id", "string"), ("category", "string", "category", "string"), 
("status", "string", "status", "string"), ("release_status", "string", "release_status", "string"), ("line_status", "string", "line_status", "string"), ("port_lfd", "string", "port_lfd", "timestamp"), 
("port_ptd", "string", "port_ptd", "timestamp"), ("line_lfd", "string", "line_lfd", "timestamp"), ("line_ptd", "string", "line_ptd", "timestamp"), ("storage", "string", "storage", "string"), 
("commodity_code", "string", "commodity_code", "string"), ("commodity_description", "string", "commodity_description", "string"), ("haz_class", "string", "haz_class", "string"), ("temp_required", "float", "temp_required", "float"), 
("temp_units", "string", "temp_units", "string"), ("vent_required", "string", "vent_required", "string"), ("vent_units", "string", "vent_units", "string"), ("gross_weight", "float", "gross_weight", "float"), 
("gross_units", "string", "gross_units", "string"), ("oog_back", "float", "oog_back", "float"), ("oog_front", "float", "oog_front", "float"), ("oog_back_units", "string", "oog_back_units", "string"), 
("oog_front_units", "string", "oog_front_units", "string"), ("oog_left", "float", "oog_left", "float"), ("oog_left_units", "string", "oog_left_units", "string"), ("oog_right", "float", "oog_right", "float"), 
("oog_right_units", "string", "oog_right_units", "string"), ("oog_top", "float", "oog_top", "float"), ("oog_top_units", "string", "oog_top_units", "string"), ("consignee", "string", "consignee", "string"), 
("shipper", "string", "shipper", "string"), ("origin", "string", "origin", "string"), ("destination", "string", "destination", "string"), ("load_point_id", "string", "load_point_id", "string"), 
("discharge_point_id1", "string", "discharge_point_id1", "string"), ("discharge_point_id2", "string", "discharge_point_id2", "string"), ("block_id", "string", "block_id", "string"), ("group_id", "string", "group_id", "string"), 
("shand_id", "string", "shand_id", "string"), ("note", "string", "note", "string"), ("eqo_eqsz_id", "string", "eqo_eqsz_id", "string"), ("eqo_eqtp_id", "string", "eqo_eqtp_id", "string"), 
("eqo_eqht_id", "string", "eqo_eqht_id", "string"), ("eqo_gkey", "long", "eqo_gkey", "long"), ("dray_status", "string", "dray_status", "string"), ("seal_nbr1", "string", "seal_nbr1", "string"), 
("seal_nbr2", "string", "seal_nbr2", "string"), ("dray_trkc_id", "string", "dray_trkc_id", "string"), ("arr_loc_type", "string", "arr_loc_type", "string"), ("arr_loc_id", "string", "arr_loc_id", "string"), 
("arr_pos_id", "string", "arr_pos_id", "string"), ("arr_visit_id", "string", "arr_visit_id", "string"), ("arr_call_nbr", "string", "arr_call_nbr", "string"), ("in_loc_type", "string", "in_loc_type", "string"), 
("in_loc_id", "string", "in_loc_id", "string"), ("in_pos_id", "string", "in_pos_id", "string"), ("in_visit_id", "string", "in_visit_id", "string"), ("in_call_nbr", "string", "in_call_nbr", "string"), 
("in_time", "string", "in_time", "timestamp"), ("out_loc_type", "string", "out_loc_type", "string"), ("out_loc_id", "string", "out_loc_id", "string"), ("out_pos_id", "string", "out_pos_id", "string"), 
("out_visit_id", "string", "out_visit_id", "string"), ("out_call_nbr", "string", "out_call_nbr", "string"), ("out_time", "string", "out_time", "timestamp"), ("dep_loc_type", "string", "dep_loc_type", "string"), 
("dep_loc_id", "string", "dep_loc_id", "string"), ("dep_pos_id", "string", "dep_pos_id", "string"), ("dep_visit_id", "string", "dep_visit_id", "string"), ("dep_call_nbr", "string", "dep_call_nbr", "string"), 
("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("shipping_mode", "string", "shipping_mode", "string"), 
("seal_nbr3", "string", "seal_nbr3", "string"), ("order_app_date", "string", "order_app_date", "timestamp"), ("order_app_time", "string", "order_app_time", "string"), ("client_contact", "string", "client_contact", "string"), 
("perdiem_dry_ffd", "string", "perdiem_dry_ffd", "timestamp"), ("perdiem_reefer_ffd", "string", "perdiem_reefer_ffd", "timestamp"), ("perdiem_dry_lfd", "string", "perdiem_dry_lfd", "timestamp"), 
("perdiem_reefer_lfd", "string", "perdiem_reefer_lfd", "timestamp"), ("perdiem_lfd", "string", "perdiem_lfd", "timestamp"), ("perdiem_ptd", "string", "perdiem_ptd", "timestamp"), ("remarks", "string", "remarks", "string"), 
("export_release_nbr", "string", "export_release_nbr", "string"), ("customs_level", "string", "customs_level", "string"), ("rehandle_reason", "string", "rehandle_reason", "string"), ("on_power", "string", "on_power", "string"), 
("agri_status", "string", "agri_status", "string"), ("forestry_status", "string", "forestry_status", "string"), ("military_v_nbr", "string", "military_v_nbr", "string"), ("power_date", "string", "power_date", "timestamp"), 
("temp_last", "float", "temp_last", "float"), ("temp_last_unit", "string", "temp_last_unit", "string"), ("temp_max", "float", "temp_max", "float"), ("temp_max_unit", "string", "temp_max_unit", "string"), 
("temp_min", "float", "temp_min", "float"), ("temp_min_unit", "string", "temp_min_unit", "string"), ("temp_read", "string", "temp_read", "timestamp"), ("temp_received", "float", "temp_received", "float"), 
("temp_received_unit", "string", "temp_received_unit", "string"), ("temp_set", "float", "temp_set", "float"), ("temp_set_unit", "string", "temp_set_unit", "string"), ("customer", "string", "customer", "string"), 
("max_days", "long", "max_days", "long"), ("max_amount", "float", "max_amount", "float"), ("storage_note", "string", "storage_note", "string"), ("storage_created", "string", "storage_created", "timestamp"), 
("storage_creator", "string", "storage_creator", "string"), ("pin_nbr", "string", "pin_nbr", "string"), ("eqo_client_sztp", "string", "eqo_client_sztp", "string"), ("export_port_ptd", "string", "export_port_ptd", "timestamp"), 
("export_line_ptd", "string", "export_line_ptd", "timestamp"), ("is_disc_percent", "float", "is_disc_percent", "float"), ("is_disc_amount", "float", "is_disc_amount", "float"), ("sc_agent", "string", "sc_agent", "string"), 
("customs_group", "string", "customs_group", "string"), ("pmount_ref", "long", "pmount_ref", "long"), ("humidity_required", "long", "humidity_required", "long"), ("crnc_id", "string", "crnc_id", "string"), 
("override_id", "string", "override_id", "string"), ("co2_required", "float", "co2_required", "float"), ("seal_nbr4", "string", "seal_nbr4", "string"), ("carrier_client_id", "string", "carrier_client_id", "string"), 
("inspection_type", "string", "inspection_type", "string"), ("inspection_date", "string", "inspection_date", "timestamp"), ("inspection_time", "string", "inspection_time", "string"), 
("commodity_code_customs", "string", "commodity_code_customs", "string"), ("discharge_point_id3", "string", "discharge_point_id3", "string"), ("optional_point_id1", "string", "optional_point_id1", "string"), 
("optional_point_id2", "string", "optional_point_id2", "string"), ("optional_point_id3", "string", "optional_point_id3", "string"), ("state", "string", "state", "string"), ("vin_nbr", "string", "vin_nbr", "string"), 
("port_in_time", "string", "port_in_time", "timestamp"), ("inter_terminal_xfr", "string", "inter_terminal_xfr", "string"), ("delivery_note", "string", "delivery_note", "string"), 
("mty_return_loc_id", "string", "mty_return_loc_id", "string"), ("o2_required", "float", "o2_required", "float"), ("release_date", "string", "release_date", "timestamp"), ("chs_type", "string", "chs_type", "string"), 
("port_gtd", "string", "port_gtd", "timestamp"), ("billing_terms", "string", "billing_terms", "string"), ("roll_status", "string", "roll_status", "string"), ("priority", "string", "priority", "string"), 
("reef_port_ptd", "string", "reef_port_ptd", "timestamp"), ("changeid", "long", "changeid", "long"), ("sub_category", "string", "sub_category", "string"), ("in_terminal_id", "string", "in_terminal_id", "string"), 
("out_terminal_id", "string", "out_terminal_id", "string"), ("arr_terminal_id", "string", "arr_terminal_id", "string"), ("dep_terminal_id", "string", "dep_terminal_id", "string"), 
("accessory_required", "string", "accessory_required", "string"), ("direct_handling", "string", "direct_handling", "string"), ("release_till_date", "string", "release_till_date", "timestamp"), 
("delivery_instruction", "string", "delivery_instruction", "string"), ("chassis_own", "string", "chassis_own", "string"), ("sgd_number", "string", "sgd_number", "string"), ("scan_mode", "string", "scan_mode", "string"), 
("paccs", "string", "paccs", "string"), ("release_reference", "string", "release_reference", "string"), ("export_manifest_number", "string", "export_manifest_number", "string"), ("otr_rfid_tag", "string", "otr_rfid_tag", "string"), 
("billing_hold_reason", "string", "billing_hold_reason", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean"), 
("train_visit_removed", "string", "train_visit_removed", "string"), ("verified_gross_weight", "float", "verified_gross_weight", "float"), ("verified_gross_units", "string", "verified_gross_units", "string"), 
("next_bill_cycle", "string", "next_bill_cycle", "string")], transformation_ctx = "eq_applymapping")

## truck_transactions mapping
tt_applymapping = ApplyMapping.apply(frame = tt_dynDF, mappings = 
[("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),("audtdateadded", "string", "audtdateadded", "timestamp"),("gkey", "long", "gkey", "long"), 
("nbr", "long", "nbr", "long"), ("status", "string", "status", "string"), ("truck_entered", "string", "truck_entered", "timestamp"), ("truck_license_nbr", "string", "truck_license_nbr", "string"), 
("truck_license_state", "string", "truck_license_state", "string"), ("truck_tare_weight", "float", "truck_tare_weight", "float"), ("trkc_id", "string", "trkc_id", "string"), ("ship_id", "string", "ship_id", "string"), 
("line_id", "string", "line_id", "string"), ("voy_nbr", "string", "voy_nbr", "string"), ("call_nbr", "string", "call_nbr", "string"), ("scale_weight", "float", "scale_weight", "float"), 
("scale_units", "string", "scale_units", "string"), ("ctr_nbr", "string", "ctr_nbr", "string"), ("ctruse_gkey", "long", "ctruse_gkey", "long"), ("ctr_owner_id", "string", "ctr_owner_id", "string"), 
("ctr_eqtp_id", "string", "ctr_eqtp_id", "string"), ("ctr_eqsz_id", "string", "ctr_eqsz_id", "string"), ("ctr_eqht_id", "string", "ctr_eqht_id", "string"), ("ctr_damage", "string", "ctr_damage", "string"), 
("ctr_acc_nbr", "string", "ctr_acc_nbr", "string"), ("ctr_acc_type_id", "string", "ctr_acc_type_id", "string"), ("ctr_safe_units", "string", "ctr_safe_units", "string"), ("ctr_safe_weight", "float", "ctr_safe_weight", "float"), 
("ctr_tare_units", "string", "ctr_tare_units", "string"), ("ctr_tare_weight", "float", "ctr_tare_weight", "float"), ("ctr_net_weight", "float", "ctr_net_weight", "float"), ("ctr_net_units", "string", "ctr_net_units", "string"), 
("ctr_gross_units", "string", "ctr_gross_units", "string"), ("ctr_gross_weight", "float", "ctr_gross_weight", "float"), ("ctr_loc_type", "string", "ctr_loc_type", "string"), ("ctr_loc_id", "string", "ctr_loc_id", "string"), 
("ctr_pos_id", "string", "ctr_pos_id", "string"), ("chs_nbr", "string", "chs_nbr", "string"), ("chsuse_gkey", "long", "chsuse_gkey", "long"), ("chs_owner_id", "string", "chs_owner_id", "string"), 
("chs_eqsz_id", "string", "chs_eqsz_id", "string"), ("chs_eqtp_id", "string", "chs_eqtp_id", "string"), ("chs_eqht_id", "string", "chs_eqht_id", "string"), ("chs_acc_nbr", "string", "chs_acc_nbr", "string"), 
("chs_damage", "string", "chs_damage", "string"), ("chs_acc_type_id", "string", "chs_acc_type_id", "string"), ("chs_tare_weight", "float", "chs_tare_weight", "float"), ("chs_safe_units", "string", "chs_safe_units", "string"), 
("chs_safe_weight", "float", "chs_safe_weight", "float"), ("chs_tare_units", "string", "chs_tare_units", "string"), ("chs_loc_type", "string", "chs_loc_type", "string"), ("chs_loc_id", "string", "chs_loc_id", "string"), 
("chs_pos_id", "string", "chs_pos_id", "string"), ("eqo_nbr", "string", "eqo_nbr", "string"), ("eqo_gkey", "long", "eqo_gkey", "long"), ("eqo_eqtp_id", "string", "eqo_eqtp_id", "string"), 
("eqo_eqsz_id", "string", "eqo_eqsz_id", "string"), ("eqo_eqht_id", "string", "eqo_eqht_id", "string"), ("commodity_code", "string", "commodity_code", "string"), 
("commodity_description", "string", "commodity_description", "string"), ("temp_units", "string", "temp_units", "string"), ("temp_required", "float", "temp_required", "float"), 
("temp_observed", "float", "temp_observed", "float"), ("temp_setting", "float", "temp_setting", "float"), ("vent_units", "string", "vent_units", "string"), ("vent_required", "string", "vent_required", "string"), 
("vent_setting", "string", "vent_setting", "string"), ("haz_flag", "string", "haz_flag", "string"), ("haz_class", "string", "haz_class", "string"), ("oog_flag", "string", "oog_flag", "string"), 
("oog_front", "float", "oog_front", "float"), ("oog_back", "float", "oog_back", "float"), ("oog_left", "float", "oog_left", "float"), ("oog_right", "float", "oog_right", "float"), ("oog_top", "float", "oog_top", "float"), 
("seal_nbr1", "string", "seal_nbr1", "string"), ("seal_nbr2", "string", "seal_nbr2", "string"), ("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), 
("changer", "string", "changer", "string"), ("ctr_status", "string", "ctr_status", "string"), ("sub_type", "string", "sub_type", "string"), ("oog_units", "string", "oog_units", "string"), 
("driver_license_nbr", "string", "driver_license_nbr", "string"), ("driver_name", "string", "driver_name", "string"), ("military_v_nbr", "string", "military_v_nbr", "string"), ("seal_nbr3", "string", "seal_nbr3", "string"), 
("notes", "string", "notes", "string"), ("port_id", "string", "port_id", "string"), ("bl_nbr", "string", "bl_nbr", "string"), ("export_release_nbr", "string", "export_release_nbr", "string"), 
("ctr_truck_position", "string", "ctr_truck_position", "string"), ("ctr_door_direction", "string", "ctr_door_direction", "string"), ("trade_id", "string", "trade_id", "string"), 
("reservation_nbr", "string", "reservation_nbr", "string"), ("military_tcn", "string", "military_tcn", "string"), ("load_point_id", "string", "load_point_id", "string"), 
("discharge_point_id1", "string", "discharge_point_id1", "string"), ("discharge_point_id2", "string", "discharge_point_id2", "string"), ("origin", "string", "origin", "string"), ("destination", "string", "destination", "string"), 
("shipper", "string", "shipper", "string"), ("consignee", "string", "consignee", "string"), ("convoy_gkey", "long", "convoy_gkey", "long"), ("bat_nbr", "string", "bat_nbr", "string"), ("chs_type", "string", "chs_type", "string"), 
("group_id", "string", "group_id", "string"), ("handled", "string", "handled", "timestamp"), ("shand_id", "string", "shand_id", "string"), ("haz_un_nbr", "string", "haz_un_nbr", "string"), 
("haz_un_nbr2", "string", "haz_un_nbr2", "string"), ("haz_un_nbr3", "string", "haz_un_nbr3", "string"), ("haz_un_nbr4", "string", "haz_un_nbr4", "string"), ("haz_class2", "string", "haz_class2", "string"), 
("haz_class3", "string", "haz_class3", "string"), ("haz_class4", "string", "haz_class4", "string"), ("ctr_iso_code", "string", "ctr_iso_code", "string"), ("pin_nbr", "string", "pin_nbr", "string"), 
("sc_agent", "string", "sc_agent", "string"), ("ctr_acc_damage", "string", "ctr_acc_damage", "string"), ("chs_acc_damage", "string", "chs_acc_damage", "string"), ("ctr_fuel_level", "string", "ctr_fuel_level", "string"), 
("chs_fuel_level", "string", "chs_fuel_level", "string"), ("humidity_required", "long", "humidity_required", "long"), ("related_tran_gkey", "long", "related_tran_gkey", "long"), ("eqftr_id", "string", "eqftr_id", "string"), 
("material", "string", "material", "string"), ("acc_tare_weight", "float", "acc_tare_weight", "float"), ("chs_license_nbr", "string", "chs_license_nbr", "string"), ("seal_nbr4", "string", "seal_nbr4", "string"), 
("discharge_point_id3", "string", "discharge_point_id3", "string"), ("optional_point_id1", "string", "optional_point_id1", "string"), ("terminal_id", "string", "terminal_id", "string"), ("grade_id", "string", "grade_id", "string"), 
("ticket_pos_id", "string", "ticket_pos_id", "string"), ("bl_gkey", "long", "bl_gkey", "long"), ("bl_item", "long", "bl_item", "long"), ("bl_item_qty", "long", "bl_item_qty", "long"), 
("sorders_gkey", "long", "sorders_gkey", "long"), ("soit_seq", "long", "soit_seq", "long"), ("eqo_acc_type_id", "string", "eqo_acc_type_id", "string"), ("cancel_code", "string", "cancel_code", "string"), 
("cargo_doc_id", "string", "cargo_doc_id", "string"), ("release_nbr", "string", "release_nbr", "string"), ("own_chs_nbr", "string", "own_chs_nbr", "string"), ("next_stage_id", "string", "next_stage_id", "string"), 
("trouble", "string", "trouble", "string"), ("stage_id", "string", "stage_id", "string"), ("bl_item_weight", "float", "bl_item_weight", "float"), ("bl_item_weight_units", "string", "bl_item_weight_units", "string"), 
("placards_flag", "string", "placards_flag", "string"), ("placards_ok", "string", "placards_ok", "string"), ("chs_road_inspected", "string", "chs_road_inspected", "string"), 
("chs_inspected_by", "string", "chs_inspected_by", "string"), ("trouble_status", "string", "trouble_status", "string"), ("eq_nbr_entered", "string", "eq_nbr_entered", "string"), 
("inspection_status", "string", "inspection_status", "string"), ("tvisit_appt_code", "long", "tvisit_appt_code", "long"), ("direct_handling", "string", "direct_handling", "string"), 
("turn_around", "string", "turn_around", "string"), ("paccs", "string", "paccs", "string"), ("sub_agent", "string", "sub_agent", "string"), ("otr_rfid_tag", "string", "otr_rfid_tag", "string"), 
("otr_rfid_flag", "string", "otr_rfid_flag", "string"), ("truck_exempt_flag", "string", "truck_exempt_flag", "string"), ("placard", "string", "placard", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), 
("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "tt_applymapping")

## truck_visits mapping
tv_applymapping = ApplyMapping.apply(frame = tv_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"),
("audtdateadded", "string", "audtdateadded", "timestamp"),("truck_license_nbr", "string", "truck_license_nbr", "string"), ("entered", "string", "entered", "timestamp"), ("exited", "string", "exited", "timestamp"), 
("bat_nbr", "string", "bat_nbr", "string"), ("status", "string", "status", "string"), ("entry_lane_id", "string", "entry_lane_id", "string"), ("exit_lane_id", "string", "exit_lane_id", "string"), 
("created", "string", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "string", "changed", "timestamp"), ("changer", "string", "changer", "string"), 
("driver_license_id", "string", "driver_license_id", "string"), ("driver_name", "string", "driver_name", "string"), ("call_up_time", "string", "call_up_time", "timestamp"), ("entered_yard", "string", "entered_yard", "timestamp"), 
("gkey", "long", "gkey", "long"), ("card_nbr", "long", "card_nbr", "long"), ("remain_on_eq1", "string", "remain_on_eq1", "string"), ("remain_on_eq2", "string", "remain_on_eq2", "string"), 
("remain_on_eq3", "string", "remain_on_eq3", "string"), ("next_stage_id", "string", "next_stage_id", "string"), ("trkc_id", "string", "trkc_id", "string"), ("otr_rfid_tag", "string", "otr_rfid_tag", "string"), 
("terminal_id", "string", "terminal_id", "string"), ("truck_exempt_flag", "string", "truck_exempt_flag", "string"), ("deleted_dt", "string", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], 
transformation_ctx = "tv_applymapping")



                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
										
## equipment_uses datasink
eq_datasink = glueContext.write_dynamic_frame.from_options(frame = eq_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/express/equipment_uses"}, format = "parquet", transformation_ctx = "eq_datasink")

## truck_transactions datasink
tt_datasink = glueContext.write_dynamic_frame.from_options(frame = tt_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/express/truck_transactions"}, format = "parquet", transformation_ctx = "tt_datasink")

## truck_visits datasink
tv_datasink = glueContext.write_dynamic_frame.from_options(frame = tv_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/express/truck_visits"}, format = "parquet", transformation_ctx = "tv_datasink")


job.commit()