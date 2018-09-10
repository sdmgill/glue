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


										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################

## xps_che connection
xpsche_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_che", transformation_ctx = "xpsche_DS")
xpsche_regDF = xpsche_DS.toDF()
xpsche_regDF.createOrReplaceTempView("distxpsche")
xpsche_distDF = spark.sql("with maxche as \
(SELECT DISTINCT gkey, max(coalesce(last_time,CAST('1900-01-01' as timestamp))) last_time, max(coalesce(time_dispatch,CAST('1900-01-01' as timestamp))) time_dispatch \
FROM distxpsche \
GROUP BY gkey \
) \
SELECT DISTINCT ch.sourcesystem, \
ch.gkey, \
ch.yard, \
ch.life_cycle_state, \
ch.pkey, \
ch.pool, \
ch.pool_gkey, \
ch.pow, \
ch.point_of_work_gkey, \
ch.vessel_code, \
ch.status, \
ch.status_enum, \
ch.talk_status, \
ch.talk_status_enum, \
ch.message_status, \
ch.message_status_enum, \
ch.mts_status, \
ch.itt_dispatch_mode, \
ch.last_time, \
ch.time_complete_last_job, \
ch.time_log_in_out, \
ch.time_available_unavailable, \
ch.time_out_of_service, \
ch.time_lift, \
ch.time_finish, \
ch.time_dispatch, \
ch.job_distance, \
ch.to_job_distance, \
ch.laden_travel, \
ch.empty_travel, \
ch.terminal, \
ch.planned_terminal, \
ch.mts_platform_size, \
ch.last_position, \
ch.last_put, \
ch.plan_position, \
ch.range_sel_block, \
ch.range_first_row, \
ch.range_last_row, \
ch.range_first_column, \
ch.range_last_column, \
ch.login_name, \
ch.allowed_boxes, \
ch.current_position_section_index, \
ch.last_position_section_index, \
ch.nom_chassis_section_idx, \
ch.assigned_che, \
ch.assigned_che_gkey, \
ch.qualifiers, \
ch.alternative_radio_id, \
ch.default_ec_communicator, \
ch.current_ec_communicator, \
ch.maximum_weight, \
ch.full_name, \
ch.last_wi_reference, \
ch.short_name, \
ch.default_program, \
ch.kind, \
ch.kind_enum, \
ch.operating_mode, \
ch.operating_mode_enum, \
ch.has_mdt, \
ch.maximum_height, \
ch.icon, \
ch.id, \
ch.screen_type, \
ch.screen_horizontal, \
ch.screen_vertical, \
ch.in_program, \
ch.num_function_keys, \
ch.job_step_state, \
ch.job_step_state_enum, \
ch.job_step_complete_time, \
ch.last_jobstep_transition, \
ch.currently_toggled_wi_ref, \
ch.accept_job_done_press_time, \
ch.extended_address, \
ch.maximum_teu, \
ch.is_in_job_step_mode, \
ch.clerk_pow_reference, \
ch.clerk_vessel_code, \
ch.clerk_container_key, \
ch.clerk_last_landed_che, \
ch.clerk_last_landed_che_gkey, \
ch.clerk_teu_landed, \
ch.trailer, \
ch.assist_state, \
ch.assist_state_enum, \
ch.scale_on, \
ch.in_comms_fail, \
ch.chassis_fetch_req, \
ch.no_login_required, \
ch.automation_active, \
ch.dispatch_requested, \
ch.manual_dispatch, \
ch.sends_pds_weight, \
ch.suspend_auto_dispatch, \
ch.twin_carry_capable, \
ch.manual_dispatch_pending, \
ch.has_overheight_gear, \
ch.first_lift_took_place, \
ch.twin_lift_capable, \
ch.mts_damaged, \
ch.uses_pds, \
ch.configurable_trailer, \
ch.nominal_length20_capable, \
ch.nominal_length40_capable, \
ch.nominal_length45_capable, \
ch.nominal_length24_capable, \
ch.nominal_length48_capable, \
ch.nominal_length53_capable, \
ch.nominal_length30_capable, \
ch.nominal_length60_capable, \
ch.autoche_technical_status, \
ch.autoche_operational_status, \
ch.autoche_work_status, \
ch.twin_diff_wgt_allowance, \
ch.scale_weight_unit, \
ch.rel_queue_pos_for, \
ch.waiting_for_truck_insert, \
ch.attached_chassis_id, \
ch.tandem_lift_capable, \
ch.max_tandem_weight, \
ch.proximity_radius, \
ch.quad_lift_capable, \
ch.max_quad_weight, \
ch.dispatch_info, \
ch.che_lane, \
ch.has_trailer, \
ch.work_load, \
ch.autoche_running_hours, \
ch.che_energy_level, \
ch.che_energy_state_enum, \
ch.lift_capacity, \
ch.maximum_weight_in_kg, \
ch.max_quad_weight_in_kg, \
ch.max_tandem_weight_in_kg, \
ch.lift_capacity_in_kg, \
ch.lift_operational_status, \
ch.twin_diff_wgt_allowance_in_kg, \
ch.ocr_mode_switch, \
ch.ec_state_flex_string_1, \
ch.ec_state_flex_string_2, \
ch.ec_state_flex_string_3, \
ch.allowed_chassis_kinds, \
ch.last_pos_loctype, \
ch.last_pos_locid, \
ch.last_pos_loc_gkey, \
ch.last_pos_slot, \
ch.last_pos_orientation, \
ch.last_pos_name, \
ch.last_pos_bin, \
ch.last_pos_tier, \
ch.last_pos_anchor, \
ch.last_pos_orientation_degrees, \
ch.last_ops_pos_id, \
ch.last_pos_slot_on_carriage, \
ch.deleted_dt, \
ch.is_deleted \
FROM distxpsche ch \
INNER JOIN maxche mc ON ch.gkey = mc.gkey \
and coalesce(ch.last_time,cast('1900-01-01' as timestamp)) = mc.last_time \
and coalesce(ch.time_dispatch,cast('1900-01-01' as timestamp)) = mc.time_dispatch \
where status = 1")
xpsche_dynDF = DynamicFrame.fromDF(xpsche_distDF, glueContext, "nested")
				
## xps_ecevent connection	
xpsecevent_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_ecevent", transformation_ctx = "xpsecevent_DS")
xpsecevent_regDF = xpsecevent_DS.toDF()
xpsecevent_regDF.createOrReplaceTempView("distxpsecevent")
xpsecevent_distDF = spark.sql("SELECT sourcesystem, \
gkey, \
yard, \
pkey, \
max(timestamp) ectimestamp, \
type, \
che_id, \
che_name, \
operator_name, \
sub_type, \
type_description, \
from_che_id_name, \
to_che_id_name, \
unit_id_name, \
pow_name, \
pool_name, \
work_queue, \
travel_distance, \
move_kind, \
is_twin_move, \
start_distance, \
work_assignment_gkey, \
work_assignment_id, \
unit_reference, \
tran_id, \
loctype, \
locid, \
loc_slot, \
ops_pos_id, \
unladen_loctype, \
unladen_locid, \
unladen_loc_slot, \
laden_loctype, \
laden_locid, \
laden_loc_slot, \
last_est_move_time, \
deleted_dt, \
is_deleted \
FROM distxpsecevent \
GROUP BY sourcesystem, \
gkey, \
yard, \
pkey, \
type, \
che_id, \
che_name, \
operator_name, \
sub_type, \
type_description, \
from_che_id_name, \
to_che_id_name, \
unit_id_name, \
pow_name, \
pool_name, \
work_queue, \
travel_distance, \
move_kind, \
is_twin_move, \
start_distance, \
work_assignment_gkey, \
work_assignment_id, \
unit_reference, \
tran_id, \
loctype, \
locid, \
loc_slot, \
ops_pos_id, \
unladen_loctype, \
unladen_locid, \
unladen_loc_slot, \
laden_loctype, \
laden_locid, \
laden_loc_slot, \
last_est_move_time, \
deleted_dt, \
is_deleted")
xpsecevent_dynDF = DynamicFrame.fromDF(xpsecevent_distDF, glueContext, "nested")				
									
## xps_ecuser connection
xpsecuser_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_ecuser", transformation_ctx = "xpsecuser_DS")
xpsecuser_regDF = xpsecuser_DS.toDF()
xpsecuser_regDF.createOrReplaceTempView("distxpsecuser")
xpsecuser_distDF = spark.sql("SELECT sourcesystem, \
max(audtdateadded) audtdateadded, \
gkey, \
yard, \
pkey, \
name, \
user_id, \
password \
FROM distxpsecuser \
GROUP BY sourcesystem, \
gkey, \
yard, \
pkey, \
name, \
user_id, \
password")
xpsecuser_dynDF = DynamicFrame.fromDF(xpsecuser_distDF, glueContext, "nested")	

										
                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################										

## xps_che mapping
xpsche_applymapping = ApplyMapping.apply(frame = xpsche_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("pkey", "long", "pkey", "long"), ("pool", "long", "pool", "long"), ("pool_gkey", "long", "pool_gkey", "long"), ("pow", "long", "pow", "long"), ("point_of_work_gkey", "long", "point_of_work_gkey", "long"), ("vessel_code", "string", "vessel_code", "string"), ("status", "long", "status", "long"), ("status_enum", "string", "status_enum", "string"), ("talk_status", "long", "talk_status", "long"), ("talk_status_enum", "string", "talk_status_enum", "string"), ("message_status", "long", "message_status", "long"), ("message_status_enum", "string", "message_status_enum", "string"), ("mts_status", "long", "mts_status", "long"), ("itt_dispatch_mode", "long", "itt_dispatch_mode", "long"), ("last_time", "timestamp", "last_time", "timestamp"), ("time_complete_last_job", "timestamp", "time_complete_last_job", "timestamp"), ("time_log_in_out", "timestamp", "time_log_in_out", "timestamp"), ("time_available_unavailable", "timestamp", "time_available_unavailable", "timestamp"), ("time_out_of_service", "timestamp", "time_out_of_service", "timestamp"), ("time_lift", "timestamp", "time_lift", "timestamp"), ("time_finish", "timestamp", "time_finish", "timestamp"), ("time_dispatch", "timestamp", "time_dispatch", "timestamp"), ("job_distance", "long", "job_distance", "long"), ("to_job_distance", "long", "to_job_distance", "long"), ("laden_travel", "long", "laden_travel", "long"), ("empty_travel", "long", "empty_travel", "long"), ("terminal", "string", "terminal", "string"), ("planned_terminal", "string", "planned_terminal", "string"), ("mts_platform_size", "long", "mts_platform_size", "long"), ("last_position", "string", "last_position", "string"), ("last_put", "string", "last_put", "string"), ("plan_position", "string", "plan_position", "string"), ("range_sel_block", "string", "range_sel_block", "string"), ("range_first_row", "long", "range_first_row", "long"), ("range_last_row", "long", "range_last_row", "long"), ("range_first_column", "long", "range_first_column", "long"), ("range_last_column", "long", "range_last_column", "long"), ("login_name", "string", "login_name", "string"), ("allowed_boxes", "long", "allowed_boxes", "long"), ("current_position_section_index", "long", "current_position_section_index", "long"), ("last_position_section_index", "long", "last_position_section_index", "long"), ("nom_chassis_section_idx", "long", "nom_chassis_section_idx", "long"), ("assigned_che", "long", "assigned_che", "long"), ("assigned_che_gkey", "long", "assigned_che_gkey", "long"), ("qualifiers", "string", "qualifiers", "string"), ("alternative_radio_id", "long", "alternative_radio_id", "long"), ("default_ec_communicator", "long", "default_ec_communicator", "long"), ("current_ec_communicator", "long", "current_ec_communicator", "long"), ("maximum_weight", "long", "maximum_weight", "long"), ("full_name", "string", "full_name", "string"), ("last_wi_reference", "long", "last_wi_reference", "long"), ("short_name", "string", "short_name", "string"), ("default_program", "string", "default_program", "string"), ("kind", "long", "kind", "long"), ("kind_enum", "string", "kind_enum", "string"), ("operating_mode", "long", "operating_mode", "long"), ("operating_mode_enum", "string", "operating_mode_enum", "string"), ("has_mdt", "long", "has_mdt", "long"), ("maximum_height", "long", "maximum_height", "long"), ("icon", "long", "icon", "long"), ("id", "long", "id", "long"), ("screen_type", "long", "screen_type", "long"), ("screen_horizontal", "long", "screen_horizontal", "long"), ("screen_vertical", "long", "screen_vertical", "long"), ("in_program", "long", "in_program", "long"), ("num_function_keys", "long", "num_function_keys", "long"), ("job_step_state", "long", "job_step_state", "long"), ("job_step_state_enum", "string", "job_step_state_enum", "string"), ("job_step_complete_time", "timestamp", "job_step_complete_time", "timestamp"), ("last_jobstep_transition", "timestamp", "last_jobstep_transition", "timestamp"), ("currently_toggled_wi_ref", "long", "currently_toggled_wi_ref", "long"), ("accept_job_done_press_time", "timestamp", "accept_job_done_press_time", "timestamp"), ("extended_address", "string", "extended_address", "string"), ("maximum_teu", "long", "maximum_teu", "long"), ("is_in_job_step_mode", "long", "is_in_job_step_mode", "long"), ("clerk_pow_reference", "long", "clerk_pow_reference", "long"), ("clerk_vessel_code", "string", "clerk_vessel_code", "string"), ("clerk_container_key", "long", "clerk_container_key", "long"), ("clerk_last_landed_che", "long", "clerk_last_landed_che", "long"), ("clerk_last_landed_che_gkey", "long", "clerk_last_landed_che_gkey", "long"), ("clerk_teu_landed", "long", "clerk_teu_landed", "long"), ("trailer", "long", "trailer", "long"), ("assist_state", "long", "assist_state", "long"), ("assist_state_enum", "string", "assist_state_enum", "string"), ("scale_on", "long", "scale_on", "long"), ("in_comms_fail", "long", "in_comms_fail", "long"), ("chassis_fetch_req", "long", "chassis_fetch_req", "long"), ("no_login_required", "long", "no_login_required", "long"), ("automation_active", "long", "automation_active", "long"), ("dispatch_requested", "long", "dispatch_requested", "long"), ("manual_dispatch", "long", "manual_dispatch", "long"), ("sends_pds_weight", "long", "sends_pds_weight", "long"), ("suspend_auto_dispatch", "long", "suspend_auto_dispatch", "long"), ("twin_carry_capable", "long", "twin_carry_capable", "long"), ("manual_dispatch_pending", "long", "manual_dispatch_pending", "long"), ("has_overheight_gear", "long", "has_overheight_gear", "long"), ("first_lift_took_place", "long", "first_lift_took_place", "long"), ("twin_lift_capable", "long", "twin_lift_capable", "long"), ("mts_damaged", "long", "mts_damaged", "long"), ("uses_pds", "long", "uses_pds", "long"), ("configurable_trailer", "long", "configurable_trailer", "long"), ("nominal_length20_capable", "long", "nominal_length20_capable", "long"), ("nominal_length40_capable", "long", "nominal_length40_capable", "long"), ("nominal_length45_capable", "long", "nominal_length45_capable", "long"), ("nominal_length24_capable", "long", "nominal_length24_capable", "long"), ("nominal_length48_capable", "long", "nominal_length48_capable", "long"), ("nominal_length53_capable", "long", "nominal_length53_capable", "long"), ("nominal_length30_capable", "long", "nominal_length30_capable", "long"), ("nominal_length60_capable", "long", "nominal_length60_capable", "long"), ("autoche_technical_status", "string", "autoche_technical_status", "string"), ("autoche_operational_status", "string", "autoche_operational_status", "string"), ("autoche_work_status", "string", "autoche_work_status", "string"), ("twin_diff_wgt_allowance", "long", "twin_diff_wgt_allowance", "long"), ("scale_weight_unit", "long", "scale_weight_unit", "long"), ("rel_queue_pos_for", "long", "rel_queue_pos_for", "long"), ("waiting_for_truck_insert", "long", "waiting_for_truck_insert", "long"), ("attached_chassis_id", "string", "attached_chassis_id", "string"), ("tandem_lift_capable", "long", "tandem_lift_capable", "long"), ("max_tandem_weight", "long", "max_tandem_weight", "long"), ("proximity_radius", "long", "proximity_radius", "long"), ("quad_lift_capable", "long", "quad_lift_capable", "long"), ("max_quad_weight", "long", "max_quad_weight", "long"), ("dispatch_info", "string", "dispatch_info", "string"), ("che_lane", "long", "che_lane", "long"), ("has_trailer", "long", "has_trailer", "long"), ("work_load", "long", "work_load", "long"), ("autoche_running_hours", "long", "autoche_running_hours", "long"), ("che_energy_level", "long", "che_energy_level", "long"), ("che_energy_state_enum", "string", "che_energy_state_enum", "string"), ("lift_capacity", "long", "lift_capacity", "long"), ("maximum_weight_in_kg", "long", "maximum_weight_in_kg", "long"), ("max_quad_weight_in_kg", "long", "max_quad_weight_in_kg", "long"), ("max_tandem_weight_in_kg", "long", "max_tandem_weight_in_kg", "long"), ("lift_capacity_in_kg", "long", "lift_capacity_in_kg", "long"), ("lift_operational_status", "string", "lift_operational_status", "string"), ("twin_diff_wgt_allowance_in_kg", "long", "twin_diff_wgt_allowance_in_kg", "long"), ("ocr_mode_switch", "long", "ocr_mode_switch", "long"), ("ec_state_flex_string_1", "string", "ec_state_flex_string_1", "string"), ("ec_state_flex_string_2", "string", "ec_state_flex_string_2", "string"), ("ec_state_flex_string_3", "string", "ec_state_flex_string_3", "string"), ("allowed_chassis_kinds", "long", "allowed_chassis_kinds", "long"), ("last_pos_loctype", "string", "last_pos_loctype", "string"), ("last_pos_locid", "string", "last_pos_locid", "string"), ("last_pos_loc_gkey", "long", "last_pos_loc_gkey", "long"), ("last_pos_slot", "string", "last_pos_slot", "string"), ("last_pos_orientation", "string", "last_pos_orientation", "string"), ("last_pos_name", "string", "last_pos_name", "string"), ("last_pos_bin", "long", "last_pos_bin", "long"), ("last_pos_tier", "long", "last_pos_tier", "long"), ("last_pos_anchor", "string", "last_pos_anchor", "string"), ("last_pos_orientation_degrees", "long", "last_pos_orientation_degrees", "long"), ("last_ops_pos_id", "string", "last_ops_pos_id", "string"), ("last_pos_slot_on_carriage", "string", "last_pos_slot_on_carriage", "string"), ("deleted_dt", "timestamp", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "xpsche_applymapping")
										
## xps_ecevent mapping
xpsecevent_applymapping = ApplyMapping.apply(frame = xpsecevent_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("pkey", "long", "pkey", "long"), ("ectimestamp", "timestamp", "ectimestamp", "timestamp"), ("type", "long", "type", "long"), ("che_id", "long", "che_id", "long"), ("che_name", "string", "che_name", "string"), ("operator_name", "string", "operator_name", "string"), ("sub_type", "long", "sub_type", "long"), ("type_description", "string", "type_description", "string"), ("from_che_id_name", "long", "from_che_id_name", "long"), ("to_che_id_name", "long", "to_che_id_name", "long"), ("unit_id_name", "string", "unit_id_name", "string"), ("pow_name", "string", "pow_name", "string"), ("pool_name", "string", "pool_name", "string"), ("work_queue", "string", "work_queue", "string"), ("travel_distance", "long", "travel_distance", "long"), ("move_kind", "string", "move_kind", "string"), ("is_twin_move", "long", "is_twin_move", "long"), ("start_distance", "long", "start_distance", "long"), ("work_assignment_gkey", "long", "work_assignment_gkey", "long"), ("work_assignment_id", "string", "work_assignment_id", "string"), ("unit_reference", "string", "unit_reference", "string"), ("tran_id", "string", "tran_id", "string"), ("loctype", "string", "loctype", "string"), ("locid", "string", "locid", "string"), ("loc_slot", "string", "loc_slot", "string"), ("ops_pos_id", "string", "ops_pos_id", "string"), ("unladen_loctype", "string", "unladen_loctype", "string"), ("unladen_locid", "string", "unladen_locid", "string"), ("unladen_loc_slot", "string", "unladen_loc_slot", "string"), ("laden_loctype", "string", "laden_loctype", "string"), ("laden_locid", "string", "laden_locid", "string"), ("laden_loc_slot", "string", "laden_loc_slot", "string"), ("last_est_move_time", "timestamp", "last_est_move_time", "timestamp"), ("deleted_dt", "timestamp", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "xpsecevent_applymapping")
						
## xps_ecuser mapping
xpsecuser_applymapping = ApplyMapping.apply(frame = xpsecuser_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("pkey", "long", "pkey", "long"), ("name", "string", "name", "string"), ("user_id", "string", "user_id", "string"), ("password", "string", "password", "string")], transformation_ctx = "xpsecuser_applymapping")
										
										
										####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## xps_che datasink
xpsche_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsche_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_che"}, format = "parquet", transformation_ctx = "xpsche_datasink")

## xps_ecevent datasink
xpsecevent_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsecevent_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_ecevent"}, format = "parquet", transformation_ctx = "xpsecevent_datasink")

## xps_ecuser datasink
xpsecuser_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsecuser_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_ecuser"}, format = "parquet", transformation_ctx = "xpsecuser_datasink")


job.commit()										