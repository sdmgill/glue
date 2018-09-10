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

## road_truck_actions connection
truckact_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_actions", transformation_ctx = "truckact_DS")
truckact_regDF = truckact_DS.toDF()
truckact_regDF.createOrReplaceTempView("disttruckact")
truckact_distDF = spark.sql("with maxrta as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM disttruckact \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT rta.sourcesystem, \
                            		rta.gkey, \
                            		rta.created, \
                            		rta.creator, \
                            		rta.changed, \
                            		rta.changer, \
                            		rta.fcy_gkey, \
                            		rta.opr_gkey, \
                            		rta.life_cycle_state, \
                            		rta.id, \
                            		rta.description, \
                            		rta.is_retry, \
                            		rta.hold_truck, \
                            		rta.is_default_inbound, \
                            		rta.is_default_outbound, \
                            		rta.is_default_yard, \
                            		rta.location \
                            FROM disttruckact rta \
                            INNER JOIN maxrta ma on rta.gkey = ma.gkey \
                            	and coalesce(rta.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and rta.created = ma.created \
                            WHERE rta.is_deleted = false")
truckact_dynDF = DynamicFrame.fromDF(truckact_distDF, glueContext, "nested")

## road_truck_company_drivers connection
truckcodr_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_company_drivers", transformation_ctx = "truckcodr_DS")
truckcodr_regDF = truckcodr_DS.toDF()
truckcodr_regDF.createOrReplaceTempView("disttruckcodr")
truckcodr_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
									max(audtdateadded) audtdateadded, \
									gkey, \
									max(expiration) expiration, \
									trkco_gkey, \
									driver_gkey \
							FROM disttruckcodr \
							WHERE is_deleted = false \
							GROUP BY sourcesystem, \
									gkey, \
									trkco_gkey, \
									driver_gkey")
truckcodr_dynDF = DynamicFrame.fromDF(truckcodr_distDF, glueContext, "nested")

## road_truck_drivers connection
truckdr_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_drivers", transformation_ctx = "truckdr_DS")
truckdr_regDF = truckdr_DS.toDF()
truckdr_regDF.createOrReplaceTempView("disttruckdr")
truckdr_distDF = spark.sql("with maxrtd as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM disttruckdr \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT rtd.sourcesystem, \
                            		rtd.gkey, \
                            		rtd.reference_set, \
                            		rtd.name, \
                            		rtd.card_id, \
                            		rtd.driver_license_nbr, \
                            		rtd.bat_nbr, \
                            		rtd.driver_license_state, \
                            		rtd.email_address, \
                            		rtd.driver_birth_date, \
                            		rtd.haz_lic_exp_date, \
                            		rtd.driver_haz_lic_nbr, \
                            		rtd.is_hazard_licensed, \
                            		rtd.suspended, \
                            		rtd.status, \
                            		rtd.created, \
                            		rtd.creator, \
                            		rtd.changed, \
                            		rtd.changer, \
                            		rtd.life_cycle_state, \
                            		rtd.card_expiration, \
                            		rtd.truck_gkey, \
                            		rtd.trkco_gkey, \
                            		rtd.id, \
                            		rtd.flex_string01, \
                            		rtd.flex_string02, \
                            		rtd.flex_string03, \
                            		rtd.flex_string04, \
                            		rtd.is_internal, \
                            		rtd.che_logon_id \
                            FROM disttruckdr rtd \
                            INNER JOIN maxrtd ma on rtd.gkey = ma.gkey \
                            	and coalesce(rtd.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and rtd.created = ma.created \
                            WHERE rtd.is_deleted = false")
truckdr_dynDF = DynamicFrame.fromDF(truckdr_distDF, glueContext, "nested")

## road_truck_transaction_stages connection
truckstg_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_transaction_stages", transformation_ctx = "truckstg_DS")
truckstg_regDF = truckstg_DS.toDF()
truckstg_regDF.createOrReplaceTempView("disttruckstg")
truckstg_distDF = spark.sql("with maxtts as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM disttruckstg \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT tts.sourcesystem, \
                            		tts.gkey, \
                            		tts.seq, \
                            		tts.id, \
                            		tts.stage_start, \
                            		tts.stage_end, \
                            		tts.status, \
                            		tts.stage_type, \
                            		tts.changed, \
                            		tts.changer, \
                            		tts.created, \
                            		tts.creator, \
                            		tts.stage_order, \
                            		tts.had_trouble, \
                            		tts.queue_time, \
                            		tts.trouble_resolve_time, \
                            		tts.tran_gkey \
                            FROM disttruckstg tts \
                            INNER JOIN maxtts ma on tts.gkey = ma.gkey \
                            	and coalesce(tts.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and tts.created = ma.created \
                            WHERE tts.is_deleted = false")
truckstg_dynDF = DynamicFrame.fromDF(truckstg_distDF, glueContext, "nested")

## road_truck_transactions connection
trucktrn_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_transactions", transformation_ctx = "trucktrn_DS")
trucktrn_regDF = trucktrn_DS.toDF()
trucktrn_regDF.createOrReplaceTempView("disttrucktrn")
trucktrn_distDF = spark.sql("with maxtrn as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM disttrucktrn \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT trn.sourcesystem, \
                            		trn.gkey, \
                            		trn.seq_within_facility, \
                            		trn.nbr, \
                            		trn.sub_type, \
                            		trn.status, \
                            		trn.stage_id, \
                            		trn.next_stage_id, \
                            		trn.trouble, \
                            		trn.trouble_status, \
                            		trn.had_trouble, \
                            		trn.cancel_code, \
                            		trn.notes, \
                            		trn.handled, \
                            		trn.terminal_id, \
                            		trn.export_release_nbr, \
                            		trn.import_release_nbr, \
                            		trn.appointment_nbr, \
                            		trn.pin_nbr, \
                            		trn.trkco_id, \
                            		trn.truck_tare_weight, \
                            		trn.truck_safe_weight, \
                            		trn.scale_weight, \
                            		trn.ctr_truck_position, \
                            		trn.ctr_center_to_chassis_back_mm, \
                            		trn.ctr_door_direction, \
                            		trn.ctr_id, \
                            		trn.ctr_id_data_source, \
                            		trn.ctr_owner_id, \
                            		trn.ctr_freight_kind, \
                            		trn.ctrunit_gkey, \
                            		trn.ctr_type_id, \
                            		trn.ctr_is_damaged, \
                            		trn.ctr_safe_weight, \
                            		trn.ctr_tare_weight, \
                            		trn.ctr_net_weight, \
                            		trn.ctr_gross_weight, \
                            		trn.ctr_id_assigned, \
                            		trn.ctr_id_provided, \
                            		trn.ctr_ticket_pos_id, \
                            		trn.ctr_acc_nbr, \
                            		trn.ctr_acc_type_id, \
                            		trn.ctr_acc_owner_id, \
                            		trn.ctr_acc_fuel_level, \
                            		trn.ctr_acc_tare_weight, \
                            		trn.chs_id, \
                            		trn.chs_id_data_source, \
                            		trn.chs_owner_id, \
                            		trn.chs_is_owners, \
                            		trn.chsunit_gkey, \
                            		trn.chs_type_id, \
                            		trn.chs_is_damaged, \
                            		trn.chs_tare_weight, \
                            		trn.chs_safe_weight, \
                            		trn.chs_license_nbr, \
                            		trn.chs_id_assigned, \
                            		trn.chs_acc_nbr, \
                            		trn.chs_acc_type_id, \
                            		trn.chs_acc_owner_id, \
                            		trn.chs_acc_fuel_level, \
                            		trn.chs_acc_tare_weight, \
                            		trn.unit_id, \
                            		trn.trade_id, \
                            		trn.origin, \
                            		trn.destination, \
                            		trn.line_id, \
                            		trn.shipper, \
                            		trn.consignee, \
                            		trn.sc_agent, \
                            		trn.eqo_acc_type_id, \
                            		trn.eqo_eq_iso_group, \
                            		trn.eqo_eq_length, \
                            		trn.eqo_eq_height, \
                            		trn.eqo_nbr, \
                            		trn.military_v_nbr, \
                            		trn.military_tcn, \
                            		trn.material, \
                            		trn.shand_id, \
                            		trn.shand_id2, \
                            		trn.shand_id3, \
                            		trn.group_id, \
                            		trn.ido_id, \
                            		trn.feature_gkey, \
                            		trn.grade_gkey, \
                            		trn.condition_gkey, \
                            		trn.mnr_gkey, \
                            		trn.csc_date, \
                            		trn.built_date, \
                            		trn.bl_nbr, \
                            		trn.bl_item_qty, \
                            		trn.bl_item_qty_moved, \
                            		trn.commodity_code, \
                            		trn.commodity_description, \
                            		trn.temp_required, \
                            		trn.temp_observed, \
                            		trn.temp_setting, \
                            		trn.reefer_hours, \
                            		trn.reefer_fault_code, \
                            		trn.reefer_fuel_level, \
                            		trn.vent_units, \
                            		trn.vent_required_pct, \
                            		trn.vent_setting_pct, \
                            		trn.o2_required, \
                            		trn.co2_required, \
                            		trn.humidity_required, \
                            		trn.is_hazard, \
                            		trn.is_hazard_checked, \
                            		trn.is_placarded, \
                            		trn.is_placarded_ok, \
                            		trn.is_oog, \
                            		trn.oog_front, \
                            		trn.oog_back, \
                            		trn.oog_left, \
                            		trn.oog_right, \
                            		trn.oog_top, \
                            		trn.seal_nbr1, \
                            		trn.seal_nbr2, \
                            		trn.seal_nbr3, \
                            		trn.seal_nbr4, \
                            		trn.is_ctr_sealed, \
                            		trn.truck_action, \
                            		trn.is_xray_required, \
                            		trn.is_confirmed, \
                            		trn.seq_nbr, \
                            		trn.exchange_area_id, \
                            		trn.has_documents, \
                            		trn.category, \
                            		trn.ctr_pos_loctype, \
                            		trn.ctr_pos_locid, \
                            		trn.ctr_pos_loc_gkey, \
                            		trn.ctr_pos_slot, \
                            		trn.ctr_pos_orientation, \
                            		trn.ctr_pos_name, \
                            		trn.ctr_pos_bin, \
                            		trn.ctr_pos_tier, \
                            		trn.ctr_pos_anchor, \
                            		trn.ctr_pos_orientation_degrees, \
                            		trn.ctr_pos_slot_on_carriage, \
                            		trn.ctr_pos_is_wheeled, \
                            		trn.chs_pos_loctype, \
                            		trn.chs_pos_locid, \
                            		trn.chs_pos_loc_gkey, \
                            		trn.chs_pos_slot, \
                            		trn.chs_pos_orientation, \
                            		trn.chs_pos_name, \
                            		trn.chs_pos_bin, \
                            		trn.chs_pos_tier, \
                            		trn.chs_pos_anchor, \
                            		trn.chs_pos_orientation_degrees, \
                            		trn.chs_pos_slot_on_carriage, \
                            		trn.flex_string01, \
                            		trn.flex_string02, \
                            		trn.flex_string03, \
                            		trn.flex_string04, \
                            		trn.flex_string09, \
                            		trn.flex_string10, \
                            		trn.flex_string11, \
                            		trn.flex_string12, \
                            		trn.flex_string13, \
                            		trn.flex_string14, \
                            		trn.flex_string15, \
                            		trn.flex_string16, \
                            		trn.flex_string17, \
                            		trn.flex_string18, \
                            		trn.flex_string19, \
                            		trn.flex_string05, \
                            		trn.flex_string06, \
                            		trn.flex_string07, \
                            		trn.flex_string08, \
                            		trn.flex_string20, \
                            		trn.flex_string21, \
                            		trn.flex_string22, \
                            		trn.flex_string23, \
                            		trn.flex_string24, \
                            		trn.flex_string25, \
                            		trn.flex_date01, \
                            		trn.flex_date02, \
                            		trn.flex_date03, \
                            		trn.flex_date04, \
                            		trn.tran_flex_string01, \
                            		trn.tran_flex_string02, \
                            		trn.tran_flex_string03, \
                            		trn.tran_flex_string04, \
                            		trn.tran_flex_string05, \
                            		trn.tran_flex_string06, \
                            		trn.tran_flex_string07, \
                            		trn.tran_flex_string08, \
                            		trn.tran_flex_date01, \
                            		trn.tran_flex_date02, \
                            		trn.tran_flex_date03, \
                            		trn.tran_flex_date04, \
                            		trn.created, \
                            		trn.creator, \
                            		trn.changed, \
                            		trn.changer, \
                            		trn.unit_gkey, \
                            		trn.line_gkey, \
                            		trn.chs_gkey, \
                            		trn.ctr_gkey, \
                            		trn.chsacc_gkey, \
                            		trn.ctracc_gkey, \
                            		trn.ctr_owner_gkey, \
                            		trn.chs_owner_gkey, \
                            		trn.ctr_acc_owner_gkey, \
                            		trn.chs_acc_owner_gkey, \
                            		trn.ctr_operator_gkey, \
                            		trn.chs_operator_gkey, \
                            		trn.ctr_acc_operator_gkey, \
                            		trn.chs_acc_operator_gkey, \
                            		trn.eqo_gkey, \
                            		trn.eqoitem_gkey, \
                            		trn.cv_gkey, \
                            		trn.pol_gkey, \
                            		trn.pod1_gkey, \
                            		trn.pod2_gkey, \
                            		trn.pod3_gkey, \
                            		trn.opt_pod1_gkey, \
                            		trn.commodity_gkey, \
                            		trn.bl_gkey, \
                            		trn.bl_item_gkey, \
                            		trn.cargo_service_order_gkey, \
                            		trn.cargo_service_order_nbr, \
                            		trn.cargo_service_order_item_gkey, \
                            		trn.trkco_gkey, \
                            		trn.truck_visit_gkey, \
                            		trn.ctr_damages_gkey, \
                            		trn.chs_damages_gkey, \
                            		trn.haz_gkey, \
                            		trn.ctr_acc_damages_gkey, \
                            		trn.chs_acc_damages_gkey, \
                            		trn.xchln_gkey, \
                            		trn.app_xchln_gkey, \
                            		trn.yard_gkey, \
                            		trn.fcy_gkey, \
                            		trn.complex_gkey, \
                            		trn.operator_gkey, \
                            		trn.gate_gkey, \
                            		trn.next_gate_gkey, \
                            		trn.cancel_reason_gkey, \
                            		trn.tank_rails, \
                            		trn.auto_closed, \
                            		trn.che_id, \
                            		trn.is_tran_bundled, \
                            		trn.ctr_vgm_weight, \
                            		trn.ctr_vgm_entity \
                            FROM disttrucktrn trn \
                            INNER JOIN maxtrn ma on trn.gkey = ma.gkey \
                            	and coalesce(trn.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and trn.created = ma.created \
                            WHERE trn.is_deleted = false")
trucktrn_dynDF = DynamicFrame.fromDF(trucktrn_distDF, glueContext, "nested")

## road_truck_visit_details connection
truckvstdet_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_visit_details", transformation_ctx = "truckvstdet_DS")
truckvstdet_regDF = truckvstdet_DS.toDF()
truckvstdet_regDF.createOrReplaceTempView("disttruckvstdet")
truckvstdet_distDF = spark.sql("with maxtrn as \
                                	( \
                                		SELECT tvdtls_gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                                		FROM disttruckvstdet \
                                		GROUP BY tvdtls_gkey \
                                	) \
                                SELECT DISTINCT tvd.sourcesystem, \
                                		tvd.tvdtls_gkey, \
                                		tvd.bat_nbr, \
                                		tvd.bat_nbr_out, \
                                		tvd.status, \
                                		tvd.truck_status, \
                                		tvd.truck_license_nbr, \
                                		tvd.truck_id, \
                                		tvd.truck_aei_tag_id, \
                                		tvd.driver_license_nbr, \
                                		tvd.driver_card_id, \
                                		tvd.driver_name, \
                                		tvd.driver_is_hazard_licensed, \
                                		tvd.call_up_time, \
                                		tvd.entered_yard, \
                                		tvd.exited_yard, \
                                		tvd.next_stage_id, \
                                		tvd.scale_weight, \
                                		tvd.created, \
                                		tvd.creator, \
                                		tvd.changed, \
                                		tvd.changer, \
                                		tvd.priority, \
                                		tvd.gos_tv_key, \
                                		tvd.wait_status, \
                                		tvd.pos_loctype, \
                                		tvd.pos_locid, \
                                		tvd.pos_loc_gkey, \
                                		tvd.pos_slot, \
                                		tvd.pos_orientation, \
                                		tvd.pos_name, \
                                		tvd.pos_bin, \
                                		tvd.pos_tier, \
                                		tvd.pos_anchor, \
                                		tvd.pos_orientation_degrees, \
                                		tvd.pos_slot_on_carriage, \
                                		tvd.truck_action, \
                                		tvd.pre_stage_status, \
                                		tvd.gate_gkey, \
                                		tvd.entry_lane_gkey, \
                                		tvd.entry_gateln_gkey, \
                                		tvd.exchange_lane_gkey, \
                                		tvd.xchln_gkey, \
                                		tvd.exit_lane_gkey, \
                                		tvd.exit_gateln_gkey, \
                                		tvd.trouble_lane_gkey, \
                                		tvd.trouble_gateln_gkey, \
                                		tvd.trkco_gkey, \
                                		tvd.truck_gkey, \
                                		tvd.driver_gkey, \
                                		tvd.truck_visit_appointment_nbr, \
                                		tvd.flex_string01, \
                                		tvd.flex_string02, \
                                		tvd.flex_string03, \
                                		tvd.flex_date01, \
                                		tvd.trucking_co_id, \
                                		tvd.chpro_gkey, \
                                		tvd.chassis_count, \
                                		tvd.chassis_length_overall, \
                                		tvd.chassis_total_teu, \
                                		tvd.chassis1_teu, \
                                		tvd.chassis2_teu, \
                                		tvd.chassis3_teu, \
                                		tvd.chassis4_teu, \
                                		tvd.chassis5_teu, \
                                		tvd.closure_type, \
                                		tvd.extra_time_override, \
                                		tvd.has_visit_trouble, \
                                		tvd.xcharea_gkey, \
                                		tvd.next_xcharea_gkey, \
                                		tvd.auto_closed \
                                FROM disttruckvstdet tvd \
                                INNER JOIN maxtrn ma on tvd.tvdtls_gkey = ma.tvdtls_gkey \
                                	and coalesce(tvd.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                                	and tvd.created = ma.created \
                                WHERE tvd.is_deleted = false")
truckvstdet_dynDF = DynamicFrame.fromDF(truckvstdet_distDF, glueContext, "nested")

## road_truck_visit_stages connection
trkvststg_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_visit_stages", transformation_ctx = "trkvststg_DS")
trkvststg_regDF = trkvststg_DS.toDF()
trkvststg_regDF.createOrReplaceTempView("disttrkvststg")
trkvststg_distDF = spark.sql("with maxtvs as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created, max(stage_end) stage_end \
                            		FROM disttrkvststg \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT tvs.sourcesystem, \
                            		tvs.gkey, \
                            		tvs.tvstat_gkey, \
                            		tvs.seq, \
                            		tvs.id, \
                            		tvs.stage_start, \
                            		tvs.stage_end, \
                            		tvs.queue_time, \
                            		tvs.had_trouble, \
                            		tvs.trouble_resolve_time, \
                            		tvs.extra_time_mins, \
                            		tvs.changed, \
                            		tvs.changer, \
                            		tvs.created, \
                            		tvs.creator \
                            FROM disttrkvststg tvs \
                            INNER JOIN maxtvs ma on tvs.gkey = ma.gkey \
                            	and coalesce(tvs.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and tvs.created = ma.created \
                            	and tvs.stage_end = ma.stage_end \
                            WHERE tvs.is_deleted = false")
trkvststg_dynDF = DynamicFrame.fromDF(trkvststg_distDF, glueContext, "nested")

## road_truck_visit_stats connection
trkvststat_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "road_truck_visit_stats", transformation_ctx = "trkvststat_DS")
trkvststat_regDF = trkvststat_DS.toDF()
trkvststat_regDF.createOrReplaceTempView("disttrkvststat")
trkvststat_distDF = spark.sql("with maxtstat as \
                                	( \
                                		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                                		FROM disttrkvststat \
                                		GROUP BY gkey \
                                	) \
                                SELECT DISTINCT tstat.sourcesystem, \
                                		tstat.gkey, \
                                		tstat.gate_gkey, \
                                		tstat.tv_gkey, \
                                		tstat.trkc_gkey, \
                                		tstat.driver_gkey, \
                                		tstat.line_rcv_gkey, \
                                		tstat.line_dlv_gkey, \
                                		tstat.had_trouble, \
                                		tstat.started, \
                                		tstat.completed, \
                                		tstat.turn_time, \
                                		tstat.tv_status, \
                                		tstat.tran_count, \
                                		tstat.is_canceled, \
                                		tstat.dlv_full_count, \
                                		tstat.rcv_full_count, \
                                		tstat.dlv_mty_count, \
                                		tstat.rcv_mty_count, \
                                		tstat.dlv_chs_count, \
                                		tstat.rcv_chs_count, \
                                		tstat.changed, \
                                		tstat.changer, \
                                		tstat.created, \
                                		tstat.creator \
                                FROM disttrkvststat tstat \
                                INNER JOIN maxtstat ma on tstat.gkey = ma.gkey \
                                	and coalesce(tstat.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                                	and tstat.created = ma.created \
                                WHERE tstat.is_deleted = false")
trkvststat_dynDF = DynamicFrame.fromDF(trkvststat_distDF, glueContext, "nested")

## vsl_vessel_classes connection
vslclass_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vsl_vessel_classes", transformation_ctx = "vslclass_DS")
vslclass_regDF = vslclass_DS.toDF()
vslclass_regDF.createOrReplaceTempView("distvslclass")
vslclass_distDF = spark.sql("with maxvcl as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distvslclass \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT vcl.sourcesystem, \
                            		vcl.gkey, \
                            		vcl.reference_set, \
                            		vcl.id, \
                            		vcl.name, \
                            		vcl.basic_class, \
                            		vcl.is_active, \
                            		vcl.is_self_sustaining, \
                            		vcl.loa_cm, \
                            		vcl.beam_cm, \
                            		vcl.bays_forward, \
                            		vcl.bays_aft, \
                            		vcl.bow_overhang_cm, \
                            		vcl.stern_overhang_cm, \
                            		vcl.vbridge_to_bow_cm, \
                            		vcl.gross_registered_ton, \
                            		vcl.net_registered_ton, \
                            		vcl.notes, \
                            		vcl.created, \
                            		vcl.creator, \
                            		vcl.changed, \
                            		vcl.changer, \
                            		vcl.life_cycle_state, \
                            		vcl.abm_gkey \
                            FROM distvslclass vcl \
                            INNER JOIN maxvcl ma on vcl.gkey = ma.gkey \
                            	and coalesce(vcl.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and vcl.created = ma.created \
                            WHERE vcl.is_deleted = false")
vslclass_dynDF = DynamicFrame.fromDF(vslclass_distDF, glueContext, "nested")

## vsl_vessel_visit_details connection
vspvstdet_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vsl_vessel_visit_details", transformation_ctx = "vspvstdet_DS")
vspvstdet_regDF = vspvstdet_DS.toDF()
vspvstdet_regDF.createOrReplaceTempView("distvspvstdet")
vspvstdet_distDF = spark.sql("with maxvvd as \
                                	( \
                                		SELECT vvd_gkey, coalesce(max(published_etd),cast('1900-01-01' as timestamp)) published_etd, max(published_eta) published_eta \
                                		FROM distvspvstdet \
                                		GROUP BY vvd_gkey \
                                	) \
                                SELECT DISTINCT vvd.sourcesystem, \
                                		vvd.vvd_gkey, \
                                		vvd.bizu_gkey, \
                                		vvd.vessel_gkey, \
                                		vvd.ib_vyg, \
                                		vvd.ob_vyg, \
                                		vvd.stacking_strategy, \
                                		vvd.is_dray_off, \
                                		vvd.notes, \
                                		vvd.in_customs_voy_nbr, \
                                		vvd.out_customs_voy_nbr, \
                                		vvd.is_no_client_access, \
                                		vvd.is_common_carrier, \
                                		vvd.published_eta, \
                                		vvd.published_etd, \
                                		vvd.begin_receive, \
                                		vvd.empty_pickup, \
                                		vvd.cargo_cutoff, \
                                		vvd.haz_cutoff, \
                                		vvd.reefer_cutoff, \
                                		vvd.labor_on_board, \
                                		vvd.labor_off_board, \
                                		vvd.off_port_arr, \
                                		vvd.off_port_dep, \
                                		vvd.pilot_on_board, \
                                		vvd.pilot_off_board, \
                                		vvd.start_work, \
                                		vvd.end_work, \
                                		vvd.in_ves_captain, \
                                		vvd.out_ves_captain, \
                                		vvd.export_mnft_nbr, \
                                		vvd.import_mnft_nbr, \
                                		vvd.import_mnft_date, \
                                		vvd.export_mnft_date, \
                                		vvd.classification, \
                                		vvd.flex_string01, \
                                		vvd.flex_string02, \
                                		vvd.flex_string03, \
                                		vvd.flex_string04, \
                                		vvd.flex_string05, \
                                		vvd.flex_string06, \
                                		vvd.flex_string07, \
                                		vvd.flex_string08, \
                                		vvd.flex_date01, \
                                		vvd.flex_date02, \
                                		vvd.flex_date03, \
                                		vvd.flex_date04, \
                                		vvd.flex_date05, \
                                		vvd.flex_date06, \
                                		vvd.flex_date07, \
                                		vvd.flex_date08, \
                                		vvd.est_load, \
                                		vvd.est_discharge, \
                                		vvd.est_restow, \
                                		vvd.est_shift, \
                                		vvd.est_bbk_load, \
                                		vvd.est_bbk_discharge \
                                FROM distvspvstdet vvd \
                                INNER JOIN maxvvd ma on vvd.vvd_gkey = ma.vvd_gkey \
                                	and coalesce(vvd.published_etd,cast('1900-01-01' as timestamp)) = ma.published_etd \
                                	and vvd.published_eta = ma.published_eta \
                                WHERE vvd.is_deleted = false")
vspvstdet_dynDF = DynamicFrame.fromDF(vspvstdet_distDF, glueContext, "nested")

## vsl_vessel_visit_lines connection
cslvstlines_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vsl_vessel_visit_lines", transformation_ctx = "cslvstlines_DS")
cslvstlines_regDF = cslvstlines_DS.toDF()
cslvstlines_regDF.createOrReplaceTempView("distcslvstlines")
cslvstlines_distDF = spark.sql("with maxvvl as \
                                	( \
                                		SELECT vvline_gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                                		FROM distcslvstlines \
                                		GROUP BY vvline_gkey \
                                	) \
                                SELECT DISTINCT  vvl.sourcesystem, \
                                		vvl.vvline_gkey, \
                                		vvl.vvd_gkey, \
                                		vvl.line_in_voy_nbr, \
                                		vvl.line_out_voy_nbr, \
                                		vvl.line_gkey, \
                                		vvl.begin_receive, \
                                		vvl.empty_pickup, \
                                		vvl.cargo_cutoff, \
                                		vvl.reefer_cutoff, \
                                		vvl.haz_cutoff, \
                                		vvl.activate_yard, \
                                		vvl.created, \
                                		vvl.creator, \
                                		vvl.changed, \
                                		vvl.changer \
                                FROM distcslvstlines vvl \
                                INNER JOIN maxvvl ma on vvl.vvline_gkey = ma.vvline_gkey \
                                	and coalesce(vvl.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                                	and vvl.created = ma.created \
                                WHERE vvl.is_deleted = false")
cslvstlines_dynDF = DynamicFrame.fromDF(cslvstlines_distDF, glueContext, "nested")

## vsl_vessels connection
vslvsls_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "vsl_vessels", transformation_ctx = "vslvsls_DS")
vslvsls_regDF = vslvsls_DS.toDF()
vslvsls_regDF.createOrReplaceTempView("distvslvsls")
vslvsls_distDF = spark.sql("with maxvsl as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distvslvsls \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT vsl.sourcesystem, \
                            		vsl.gkey, \
                            		vsl.reference_set, \
                            		vsl.id, \
                            		vsl.lloyds_id, \
                            		vsl.name, \
                            		vsl.vesclass_gkey, \
                            		vsl.owner_gkey, \
                            		vsl.ves_captain, \
                            		vsl.radio_call_sign, \
                            		vsl.country_code, \
                            		vsl.is_active, \
                            		vsl.unit_system, \
                            		vsl.temperature_unit, \
                            		vsl.notes, \
                            		vsl.created, \
                            		vsl.creator,  \
                            		vsl.changed,  \
                            		vsl.changer, \
                            		vsl.stowage_scheme, \
                            		vsl.documentation_nbr, \
                            		vsl.service_registry_nbr, \
                            		vsl.life_cycle_state \
                            FROM distvslvsls vsl \
                            INNER JOIN maxvsl ma on vsl.gkey = ma.gkey \
                            	and coalesce(vsl.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and vsl.created = ma.created \
                            WHERE vsl.is_deleted = false")
vslvsls_dynDF = DynamicFrame.fromDF(vslvsls_distDF, glueContext, "nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
										
## road_truck_actions mapping
truckact_applymapping = ApplyMapping.apply(frame = truckact_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("opr_gkey", "long", "opr_gkey", "long"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("id", "string", "id", "string"), ("description", "string", "description", "string"), ("severity", "long", "severity", "long"), ("is_retry", "long", "is_retry", "long"), ("hold_truck", "long", "hold_truck", "long"), ("is_default_inbound", "long", "is_default_inbound", "long"), ("is_default_outbound", "long", "is_default_outbound", "long"), ("is_default_yard", "long", "is_default_yard", "long"), ("location", "string", "location", "string")], transformation_ctx = "truckact_applymapping")

## road_truck_company_drivers mapping
truckcodr_applymapping = ApplyMapping.apply(frame = truckcodr_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("expiration", "timestamp", "expiration", "timestamp"), ("trkco_gkey", "long", "trkco_gkey", "long"), ("driver_gkey", "long", "driver_gkey", "long"), ("deleted_dt", "timestamp", "deleted_dt", "timestamp"), ("is_deleted", "boolean", "is_deleted", "boolean")], transformation_ctx = "truckcodr_applymapping")

## road_truck_drivers mapping
truckdr_applymapping = ApplyMapping.apply(frame = truckdr_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("reference_set", "long", "reference_set", "long"), ("name", "string", "name", "string"), ("card_id", "string", "card_id", "string"), ("driver_license_nbr", "string", "driver_license_nbr", "string"), ("bat_nbr", "string", "bat_nbr", "string"), ("driver_license_state", "string", "driver_license_state", "string"), ("email_address", "string", "email_address", "string"), ("driver_birth_date", "timestamp", "driver_birth_date", "timestamp"), ("haz_lic_exp_date", "timestamp", "haz_lic_exp_date", "timestamp"), ("driver_haz_lic_nbr", "string", "driver_haz_lic_nbr", "string"), ("is_hazard_licensed", "long", "is_hazard_licensed", "long"), ("suspended", "timestamp", "suspended", "timestamp"), ("status", "string", "status", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("card_expiration", "timestamp", "card_expiration", "timestamp"), ("truck_gkey", "long", "truck_gkey", "long"), ("trkco_gkey", "long", "trkco_gkey", "long"), ("id", "string", "id", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("is_internal", "long", "is_internal", "long"), ("che_logon_id", "string", "che_logon_id", "string")], transformation_ctx = "truckdr_applymapping")

## road_truck_transaction_stages mapping
truckstg_applymapping = ApplyMapping.apply(frame = truckstg_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("seq", "long", "seq", "long"), ("id", "string", "id", "string"), ("stage_start", "timestamp", "stage_start", "timestamp"), ("stage_end", "timestamp", "stage_end", "timestamp"), ("status", "string", "status", "string"), ("stage_type", "string", "stage_type", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("stage_order", "long", "stage_order", "long"), ("had_trouble", "long", "had_trouble", "long"), ("queue_time", "long", "queue_time", "long"), ("trouble_resolve_time", "long", "trouble_resolve_time", "long"), ("tran_gkey", "long", "tran_gkey", "long")], transformation_ctx = "truckstg_applymapping")

## road_truck_transactions mapping
trucktrn_applymapping = ApplyMapping.apply(frame = trucktrn_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("seq_within_facility", "long", "seq_within_facility", "long"), ("nbr", "long", "nbr", "long"), ("sub_type", "string", "sub_type", "string"), ("status", "string", "status", "string"), ("stage_id", "string", "stage_id", "string"), ("next_stage_id", "string", "next_stage_id", "string"), ("trouble", "string", "trouble", "string"), ("trouble_status", "string", "trouble_status", "string"), ("had_trouble", "long", "had_trouble", "long"), ("cancel_code", "string", "cancel_code", "string"), ("notes", "string", "notes", "string"), ("handled", "timestamp", "handled", "timestamp"), ("terminal_id", "string", "terminal_id", "string"), ("export_release_nbr", "string", "export_release_nbr", "string"), ("import_release_nbr", "string", "import_release_nbr", "string"), ("appointment_nbr", "string", "appointment_nbr", "string"), ("pin_nbr", "string", "pin_nbr", "string"), ("trkco_id", "string", "trkco_id", "string"), ("truck_tare_weight", "float", "truck_tare_weight", "float"), ("truck_safe_weight", "float", "truck_safe_weight", "float"), ("scale_weight", "float", "scale_weight", "float"), ("ctr_truck_position", "long", "ctr_truck_position", "long"), ("ctr_center_to_chassis_back_mm", "long", "ctr_center_to_chassis_back_mm", "long"), ("ctr_door_direction", "string", "ctr_door_direction", "string"), ("ctr_id", "string", "ctr_id", "string"), ("ctr_id_data_source", "string", "ctr_id_data_source", "string"), ("ctr_owner_id", "string", "ctr_owner_id", "string"), ("ctr_freight_kind", "string", "ctr_freight_kind", "string"), ("ctrunit_gkey", "long", "ctrunit_gkey", "long"), ("ctr_type_id", "string", "ctr_type_id", "string"), ("ctr_is_damaged", "long", "ctr_is_damaged", "long"), ("ctr_safe_weight", "float", "ctr_safe_weight", "float"), ("ctr_tare_weight", "float", "ctr_tare_weight", "float"), ("ctr_net_weight", "float", "ctr_net_weight", "float"), ("ctr_gross_weight", "float", "ctr_gross_weight", "float"), ("ctr_id_assigned", "string", "ctr_id_assigned", "string"), ("ctr_id_provided", "string", "ctr_id_provided", "string"), ("ctr_ticket_pos_id", "string", "ctr_ticket_pos_id", "string"), ("ctr_acc_nbr", "string", "ctr_acc_nbr", "string"), ("ctr_acc_type_id", "string", "ctr_acc_type_id", "string"), ("ctr_acc_owner_id", "string", "ctr_acc_owner_id", "string"), ("ctr_acc_fuel_level", "string", "ctr_acc_fuel_level", "string"), ("ctr_acc_tare_weight", "float", "ctr_acc_tare_weight", "float"), ("chs_id", "string", "chs_id", "string"), ("chs_id_data_source", "string", "chs_id_data_source", "string"), ("chs_owner_id", "string", "chs_owner_id", "string"), ("chs_is_owners", "long", "chs_is_owners", "long"), ("chsunit_gkey", "long", "chsunit_gkey", "long"), ("chs_type_id", "string", "chs_type_id", "string"), ("chs_is_damaged", "long", "chs_is_damaged", "long"), ("chs_tare_weight", "float", "chs_tare_weight", "float"), ("chs_safe_weight", "float", "chs_safe_weight", "float"), ("chs_license_nbr", "string", "chs_license_nbr", "string"), ("chs_id_assigned", "string", "chs_id_assigned", "string"), ("chs_acc_nbr", "string", "chs_acc_nbr", "string"), ("chs_acc_type_id", "string", "chs_acc_type_id", "string"), ("chs_acc_owner_id", "string", "chs_acc_owner_id", "string"), ("chs_acc_fuel_level", "string", "chs_acc_fuel_level", "string"), ("chs_acc_tare_weight", "float", "chs_acc_tare_weight", "float"), ("unit_id", "string", "unit_id", "string"), ("trade_id", "string", "trade_id", "string"), ("origin", "string", "origin", "string"), ("destination", "string", "destination", "string"), ("line_id", "string", "line_id", "string"), ("shipper", "string", "shipper", "string"), ("consignee", "string", "consignee", "string"), ("sc_agent", "string", "sc_agent", "string"), ("eqo_acc_type_id", "string", "eqo_acc_type_id", "string"), ("eqo_eq_iso_group", "string", "eqo_eq_iso_group", "string"), ("eqo_eq_length", "string", "eqo_eq_length", "string"), ("eqo_eq_height", "string", "eqo_eq_height", "string"), ("eqo_nbr", "string", "eqo_nbr", "string"), ("military_v_nbr", "string", "military_v_nbr", "string"), ("military_tcn", "string", "military_tcn", "string"), ("material", "string", "material", "string"), ("shand_id", "string", "shand_id", "string"), ("shand_id2", "string", "shand_id2", "string"), ("shand_id3", "string", "shand_id3", "string"), ("group_id", "string", "group_id", "string"), ("ido_id", "string", "ido_id", "string"), ("feature_gkey", "long", "feature_gkey", "long"), ("grade_gkey", "long", "grade_gkey", "long"), ("condition_gkey", "long", "condition_gkey", "long"), ("mnr_gkey", "long", "mnr_gkey", "long"), ("csc_date", "string", "csc_date", "string"), ("built_date", "timestamp", "built_date", "timestamp"), ("bl_nbr", "string", "bl_nbr", "string"), ("bl_item_qty", "float", "bl_item_qty", "float"), ("bl_item_qty_moved", "float", "bl_item_qty_moved", "float"), ("commodity_code", "string", "commodity_code", "string"), ("commodity_description", "string", "commodity_description", "string"), ("temp_required", "float", "temp_required", "float"), ("temp_observed", "float", "temp_observed", "float"), ("temp_setting", "float", "temp_setting", "float"), ("reefer_hours", "float", "reefer_hours", "float"), ("reefer_fault_code", "string", "reefer_fault_code", "string"), ("reefer_fuel_level", "float", "reefer_fuel_level", "float"), ("vent_units", "string", "vent_units", "string"), ("vent_required_pct", "float", "vent_required_pct", "float"), ("vent_setting_pct", "float", "vent_setting_pct", "float"), ("o2_required", "float", "o2_required", "float"), ("co2_required", "float", "co2_required", "float"), ("humidity_required", "float", "humidity_required", "float"), ("is_hazard", "long", "is_hazard", "long"), ("is_hazard_checked", "long", "is_hazard_checked", "long"), ("is_placarded", "long", "is_placarded", "long"), ("is_placarded_ok", "long", "is_placarded_ok", "long"), ("is_oog", "long", "is_oog", "long"), ("oog_front", "long", "oog_front", "long"), ("oog_back", "long", "oog_back", "long"), ("oog_left", "long", "oog_left", "long"), ("oog_right", "long", "oog_right", "long"), ("oog_top", "long", "oog_top", "long"), ("seal_nbr1", "string", "seal_nbr1", "string"), ("seal_nbr2", "string", "seal_nbr2", "string"), ("seal_nbr3", "string", "seal_nbr3", "string"), ("seal_nbr4", "string", "seal_nbr4", "string"), ("is_ctr_sealed", "long", "is_ctr_sealed", "long"), ("truck_action", "long", "truck_action", "long"), ("is_xray_required", "long", "is_xray_required", "long"), ("is_confirmed", "long", "is_confirmed", "long"), ("seq_nbr", "long", "seq_nbr", "long"), ("exchange_area_id", "string", "exchange_area_id", "string"), ("has_documents", "long", "has_documents", "long"), ("category", "string", "category", "string"), ("ctr_pos_loctype", "string", "ctr_pos_loctype", "string"), ("ctr_pos_locid", "string", "ctr_pos_locid", "string"), ("ctr_pos_loc_gkey", "long", "ctr_pos_loc_gkey", "long"), ("ctr_pos_slot", "string", "ctr_pos_slot", "string"), ("ctr_pos_orientation", "string", "ctr_pos_orientation", "string"), ("ctr_pos_name", "string", "ctr_pos_name", "string"), ("ctr_pos_bin", "long", "ctr_pos_bin", "long"), ("ctr_pos_tier", "long", "ctr_pos_tier", "long"), ("ctr_pos_anchor", "string", "ctr_pos_anchor", "string"), ("ctr_pos_orientation_degrees", "float", "ctr_pos_orientation_degrees", "float"), ("ctr_pos_slot_on_carriage", "string", "ctr_pos_slot_on_carriage", "string"), ("ctr_pos_is_wheeled", "long", "ctr_pos_is_wheeled", "long"), ("chs_pos_loctype", "string", "chs_pos_loctype", "string"), ("chs_pos_locid", "string", "chs_pos_locid", "string"), ("chs_pos_loc_gkey", "long", "chs_pos_loc_gkey", "long"), ("chs_pos_slot", "string", "chs_pos_slot", "string"), ("chs_pos_orientation", "string", "chs_pos_orientation", "string"), ("chs_pos_name", "string", "chs_pos_name", "string"), ("chs_pos_bin", "long", "chs_pos_bin", "long"), ("chs_pos_tier", "long", "chs_pos_tier", "long"), ("chs_pos_anchor", "string", "chs_pos_anchor", "string"), ("chs_pos_orientation_degrees", "float", "chs_pos_orientation_degrees", "float"), ("chs_pos_slot_on_carriage", "string", "chs_pos_slot_on_carriage", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string"), ("flex_string11", "string", "flex_string11", "string"), ("flex_string12", "string", "flex_string12", "string"), ("flex_string13", "string", "flex_string13", "string"), ("flex_string14", "string", "flex_string14", "string"), ("flex_string15", "string", "flex_string15", "string"), ("flex_string16", "string", "flex_string16", "string"), ("flex_string17", "string", "flex_string17", "string"), ("flex_string18", "string", "flex_string18", "string"), ("flex_string19", "string", "flex_string19", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string20", "string", "flex_string20", "string"), ("flex_string21", "string", "flex_string21", "string"), ("flex_string22", "string", "flex_string22", "string"), ("flex_string23", "string", "flex_string23", "string"), ("flex_string24", "string", "flex_string24", "string"), ("flex_string25", "string", "flex_string25", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("flex_date02", "timestamp", "flex_date02", "timestamp"), ("flex_date03", "timestamp", "flex_date03", "timestamp"), ("flex_date04", "timestamp", "flex_date04", "timestamp"), ("tran_flex_string01", "string", "tran_flex_string01", "string"), ("tran_flex_string02", "string", "tran_flex_string02", "string"), ("tran_flex_string03", "string", "tran_flex_string03", "string"), ("tran_flex_string04", "string", "tran_flex_string04", "string"), ("tran_flex_string05", "string", "tran_flex_string05", "string"), ("tran_flex_string06", "string", "tran_flex_string06", "string"), ("tran_flex_string07", "string", "tran_flex_string07", "string"), ("tran_flex_string08", "string", "tran_flex_string08", "string"), ("tran_flex_date01", "timestamp", "tran_flex_date01", "timestamp"), ("tran_flex_date02", "timestamp", "tran_flex_date02", "timestamp"), ("tran_flex_date03", "timestamp", "tran_flex_date03", "timestamp"), ("tran_flex_date04", "timestamp", "tran_flex_date04", "timestamp"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("unit_gkey", "long", "unit_gkey", "long"), ("line_gkey", "long", "line_gkey", "long"), ("chs_gkey", "long", "chs_gkey", "long"), ("ctr_gkey", "long", "ctr_gkey", "long"), ("chsacc_gkey", "long", "chsacc_gkey", "long"), ("ctracc_gkey", "long", "ctracc_gkey", "long"), ("ctr_owner_gkey", "long", "ctr_owner_gkey", "long"), ("chs_owner_gkey", "long", "chs_owner_gkey", "long"), ("ctr_acc_owner_gkey", "long", "ctr_acc_owner_gkey", "long"), ("chs_acc_owner_gkey", "long", "chs_acc_owner_gkey", "long"), ("ctr_operator_gkey", "long", "ctr_operator_gkey", "long"), ("chs_operator_gkey", "long", "chs_operator_gkey", "long"), ("ctr_acc_operator_gkey", "long", "ctr_acc_operator_gkey", "long"), ("chs_acc_operator_gkey", "long", "chs_acc_operator_gkey", "long"), ("eqo_gkey", "long", "eqo_gkey", "long"), ("eqoitem_gkey", "long", "eqoitem_gkey", "long"), ("cv_gkey", "long", "cv_gkey", "long"), ("pol_gkey", "long", "pol_gkey", "long"), ("pod1_gkey", "long", "pod1_gkey", "long"), ("pod2_gkey", "long", "pod2_gkey", "long"), ("pod3_gkey", "long", "pod3_gkey", "long"), ("opt_pod1_gkey", "long", "opt_pod1_gkey", "long"), ("commodity_gkey", "long", "commodity_gkey", "long"), ("bl_gkey", "long", "bl_gkey", "long"), ("bl_item_gkey", "long", "bl_item_gkey", "long"), ("cargo_service_order_gkey", "long", "cargo_service_order_gkey", "long"), ("cargo_service_order_nbr", "string", "cargo_service_order_nbr", "string"), ("cargo_service_order_item_gkey", "long", "cargo_service_order_item_gkey", "long"), ("trkco_gkey", "long", "trkco_gkey", "long"), ("truck_visit_gkey", "long", "truck_visit_gkey", "long"), ("ctr_damages_gkey", "long", "ctr_damages_gkey", "long"), ("chs_damages_gkey", "long", "chs_damages_gkey", "long"), ("haz_gkey", "long", "haz_gkey", "long"), ("ctr_acc_damages_gkey", "long", "ctr_acc_damages_gkey", "long"), ("chs_acc_damages_gkey", "long", "chs_acc_damages_gkey", "long"), ("xchln_gkey", "long", "xchln_gkey", "long"), ("app_xchln_gkey", "long", "app_xchln_gkey", "long"), ("yard_gkey", "long", "yard_gkey", "long"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("complex_gkey", "long", "complex_gkey", "long"), ("operator_gkey", "long", "operator_gkey", "long"), ("gate_gkey", "long", "gate_gkey", "long"), ("next_gate_gkey", "long", "next_gate_gkey", "long"), ("cancel_reason_gkey", "long", "cancel_reason_gkey", "long"), ("tank_rails", "string", "tank_rails", "string"), ("auto_closed", "long", "auto_closed", "long"), ("che_id", "string", "che_id", "string"), ("is_tran_bundled", "long", "is_tran_bundled", "long"), ("ctr_vgm_weight", "float", "ctr_vgm_weight", "float"), ("ctr_vgm_entity", "string", "ctr_vgm_entity", "string")], transformation_ctx = "trucktrn_applymapping")

## road_truck_visit_details mapping
truckvstdet_applymapping = ApplyMapping.apply(frame = truckvstdet_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("tvdtls_gkey", "long", "tvdtls_gkey", "long"), ("bat_nbr", "string", "bat_nbr", "string"), ("bat_nbr_out", "string", "bat_nbr_out", "string"), ("status", "string", "status", "string"), ("truck_status", "string", "truck_status", "string"), ("truck_license_nbr", "string", "truck_license_nbr", "string"), ("truck_id", "string", "truck_id", "string"), ("truck_aei_tag_id", "string", "truck_aei_tag_id", "string"), ("driver_license_nbr", "string", "driver_license_nbr", "string"), ("driver_card_id", "string", "driver_card_id", "string"), ("driver_name", "string", "driver_name", "string"), ("driver_is_hazard_licensed", "long", "driver_is_hazard_licensed", "long"), ("call_up_time", "timestamp", "call_up_time", "timestamp"), ("entered_yard", "timestamp", "entered_yard", "timestamp"), ("exited_yard", "timestamp", "exited_yard", "timestamp"), ("next_stage_id", "string", "next_stage_id", "string"), ("scale_weight", "float", "scale_weight", "float"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("priority", "string", "priority", "string"), ("gos_tv_key", "long", "gos_tv_key", "long"), ("wait_status", "string", "wait_status", "string"), ("pos_loctype", "string", "pos_loctype", "string"), ("pos_locid", "string", "pos_locid", "string"), ("pos_loc_gkey", "long", "pos_loc_gkey", "long"), ("pos_slot", "string", "pos_slot", "string"), ("pos_orientation", "string", "pos_orientation", "string"), ("pos_name", "string", "pos_name", "string"), ("pos_bin", "long", "pos_bin", "long"), ("pos_tier", "long", "pos_tier", "long"), ("pos_anchor", "string", "pos_anchor", "string"), ("pos_orientation_degrees", "float", "pos_orientation_degrees", "float"), ("pos_slot_on_carriage", "string", "pos_slot_on_carriage", "string"), ("truck_action", "long", "truck_action", "long"), ("pre_stage_status", "string", "pre_stage_status", "string"), ("gate_gkey", "long", "gate_gkey", "long"), ("entry_lane_gkey", "long", "entry_lane_gkey", "long"), ("entry_gateln_gkey", "long", "entry_gateln_gkey", "long"), ("exchange_lane_gkey", "long", "exchange_lane_gkey", "long"), ("xchln_gkey", "long", "xchln_gkey", "long"), ("exit_lane_gkey", "long", "exit_lane_gkey", "long"), ("exit_gateln_gkey", "long", "exit_gateln_gkey", "long"), ("trouble_lane_gkey", "long", "trouble_lane_gkey", "long"), ("trouble_gateln_gkey", "long", "trouble_gateln_gkey", "long"), ("trkco_gkey", "long", "trkco_gkey", "long"), ("truck_gkey", "long", "truck_gkey", "long"), ("driver_gkey", "long", "driver_gkey", "long"), ("truck_visit_appointment_nbr", "long", "truck_visit_appointment_nbr", "long"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("trucking_co_id", "string", "trucking_co_id", "string"), ("chpro_gkey", "long", "chpro_gkey", "long"), ("chassis_count", "long", "chassis_count", "long"), ("chassis_length_overall", "float", "chassis_length_overall", "float"), ("chassis_total_teu", "long", "chassis_total_teu", "long"), ("chassis1_teu", "long", "chassis1_teu", "long"), ("chassis2_teu", "long", "chassis2_teu", "long"), ("chassis3_teu", "long", "chassis3_teu", "long"), ("chassis4_teu", "long", "chassis4_teu", "long"), ("chassis5_teu", "long", "chassis5_teu", "long"), ("closure_type", "string", "closure_type", "string"), ("extra_time_override", "long", "extra_time_override", "long"), ("has_visit_trouble", "long", "has_visit_trouble", "long"), ("xcharea_gkey", "long", "xcharea_gkey", "long"), ("next_xcharea_gkey", "long", "next_xcharea_gkey", "long"), ("auto_closed", "long", "auto_closed", "long")], transformation_ctx = "truckvstdet_applymapping")

## road_truck_visit_stages mapping
trkvststg_applymapping = ApplyMapping.apply(frame = trkvststg_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("tvstat_gkey", "long", "tvstat_gkey", "long"), ("seq", "long", "seq", "long"), ("id", "string", "id", "string"), ("stage_start", "timestamp", "stage_start", "timestamp"), ("stage_end", "timestamp", "stage_end", "timestamp"), ("queue_time", "long", "queue_time", "long"), ("had_trouble", "long", "had_trouble", "long"), ("trouble_resolve_time", "long", "trouble_resolve_time", "long"), ("extra_time_mins", "long", "extra_time_mins", "long"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string")], transformation_ctx = "trkvststg_applymapping")

## road_truck_visit_stats mapping
trkvststat_applymapping = ApplyMapping.apply(frame = trkvststat_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("dboperationtype", "string", "dboperationtype", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("gate_gkey", "long", "gate_gkey", "long"), ("tv_gkey", "long", "tv_gkey", "long"), ("trkc_gkey", "long", "trkc_gkey", "long"), ("driver_gkey", "long", "driver_gkey", "long"), ("line_rcv_gkey", "long", "line_rcv_gkey", "long"), ("line_dlv_gkey", "long", "line_dlv_gkey", "long"), ("had_trouble", "long", "had_trouble", "long"), ("started", "timestamp", "started", "timestamp"), ("completed", "timestamp", "completed", "timestamp"), ("turn_time", "long", "turn_time", "long"), ("tv_status", "string", "tv_status", "string"), ("tran_count", "long", "tran_count", "long"), ("is_canceled", "long", "is_canceled", "long"), ("dlv_full_count", "long", "dlv_full_count", "long"), ("rcv_full_count", "long", "rcv_full_count", "long"), ("dlv_mty_count", "long", "dlv_mty_count", "long"), ("rcv_mty_count", "long", "rcv_mty_count", "long"), ("dlv_chs_count", "long", "dlv_chs_count", "long"), ("rcv_chs_count", "long", "rcv_chs_count", "long"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string")], transformation_ctx = "trkvststat_applymapping")

## vsl_vessel_classes mapping
vslclass_applymapping = ApplyMapping.apply(frame = vslclass_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("reference_set", "long", "reference_set", "long"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("basic_class", "string", "basic_class", "string"), ("is_active", "long", "is_active", "long"), ("is_self_sustaining", "long", "is_self_sustaining", "long"), ("loa_cm", "long", "loa_cm", "long"), ("beam_cm", "long", "beam_cm", "long"), ("bays_forward", "long", "bays_forward", "long"), ("bays_aft", "long", "bays_aft", "long"), ("bow_overhang_cm", "long", "bow_overhang_cm", "long"), ("stern_overhang_cm", "long", "stern_overhang_cm", "long"), ("vbridge_to_bow_cm", "long", "vbridge_to_bow_cm", "long"), ("gross_registered_ton", "float", "gross_registered_ton", "float"), ("net_registered_ton", "float", "net_registered_ton", "float"), ("notes", "string", "notes", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("abm_gkey", "long", "abm_gkey", "long")], transformation_ctx = "vslclass_applymapping")

## vsl_vessel_visit_details mapping
vspvstdet_applymapping = ApplyMapping.apply(frame = vspvstdet_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("vvd_gkey", "long", "vvd_gkey", "long"), ("bizu_gkey", "long", "bizu_gkey", "long"), ("vessel_gkey", "long", "vessel_gkey", "long"), ("ib_vyg", "string", "ib_vyg", "string"), ("ob_vyg", "string", "ob_vyg", "string"), ("stacking_strategy", "string", "stacking_strategy", "string"), ("is_dray_off", "long", "is_dray_off", "long"), ("notes", "string", "notes", "string"), ("in_customs_voy_nbr", "string", "in_customs_voy_nbr", "string"), ("out_customs_voy_nbr", "string", "out_customs_voy_nbr", "string"), ("is_no_client_access", "long", "is_no_client_access", "long"), ("is_common_carrier", "long", "is_common_carrier", "long"), ("published_eta", "timestamp", "published_eta", "timestamp"), ("published_etd", "timestamp", "published_etd", "timestamp"), ("begin_receive", "timestamp", "begin_receive", "timestamp"), ("empty_pickup", "timestamp", "empty_pickup", "timestamp"), ("cargo_cutoff", "timestamp", "cargo_cutoff", "timestamp"), ("haz_cutoff", "timestamp", "haz_cutoff", "timestamp"), ("reefer_cutoff", "timestamp", "reefer_cutoff", "timestamp"), ("labor_on_board", "timestamp", "labor_on_board", "timestamp"), ("labor_off_board", "timestamp", "labor_off_board", "timestamp"), ("off_port_arr", "timestamp", "off_port_arr", "timestamp"), ("off_port_dep", "timestamp", "off_port_dep", "timestamp"), ("pilot_on_board", "timestamp", "pilot_on_board", "timestamp"), ("pilot_off_board", "timestamp", "pilot_off_board", "timestamp"), ("start_work", "timestamp", "start_work", "timestamp"), ("end_work", "timestamp", "end_work", "timestamp"), ("in_ves_captain", "string", "in_ves_captain", "string"), ("out_ves_captain", "string", "out_ves_captain", "string"), ("export_mnft_nbr", "string", "export_mnft_nbr", "string"), ("import_mnft_nbr", "string", "import_mnft_nbr", "string"), ("import_mnft_date", "timestamp", "import_mnft_date", "timestamp"), ("export_mnft_date", "timestamp", "export_mnft_date", "timestamp"), ("classification", "string", "classification", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("flex_date02", "timestamp", "flex_date02", "timestamp"), ("flex_date03", "timestamp", "flex_date03", "timestamp"), ("flex_date04", "timestamp", "flex_date04", "timestamp"), ("flex_date05", "timestamp", "flex_date05", "timestamp"), ("flex_date06", "timestamp", "flex_date06", "timestamp"), ("flex_date07", "timestamp", "flex_date07", "timestamp"), ("flex_date08", "timestamp", "flex_date08", "timestamp"), ("est_load", "long", "est_load", "long"), ("est_discharge", "long", "est_discharge", "long"), ("est_restow", "long", "est_restow", "long"), ("est_shift", "long", "est_shift", "long"), ("est_bbk_load", "long", "est_bbk_load", "long"), ("est_bbk_discharge", "long", "est_bbk_discharge", "long")], transformation_ctx = "vspvstdet_applymapping")

## vsl_vessel_visit_lines mapping
cslvstlines_applymapping = ApplyMapping.apply(frame = cslvstlines_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("vvline_gkey", "long", "vvline_gkey", "long"), ("vvd_gkey", "long", "vvd_gkey", "long"), ("line_in_voy_nbr", "string", "line_in_voy_nbr", "string"), ("line_out_voy_nbr", "string", "line_out_voy_nbr", "string"), ("line_gkey", "long", "line_gkey", "long"), ("begin_receive", "timestamp", "begin_receive", "timestamp"), ("empty_pickup", "timestamp", "empty_pickup", "timestamp"), ("cargo_cutoff", "timestamp", "cargo_cutoff", "timestamp"), ("reefer_cutoff", "timestamp", "reefer_cutoff", "timestamp"), ("haz_cutoff", "timestamp", "haz_cutoff", "timestamp"), ("activate_yard", "timestamp", "activate_yard", "timestamp"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "cslvstlines_applymapping")

## vsl_vessels mapping
vslvsls_applymapping = ApplyMapping.apply(frame = vslvsls_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("reference_set", "long", "reference_set", "long"), ("id", "string", "id", "string"), ("lloyds_id", "string", "lloyds_id", "string"), ("name", "string", "name", "string"), ("vesclass_gkey", "long", "vesclass_gkey", "long"), ("owner_gkey", "long", "owner_gkey", "long"), ("ves_captain", "string", "ves_captain", "string"), ("radio_call_sign", "string", "radio_call_sign", "string"), ("country_code", "string", "country_code", "string"), ("is_active", "long", "is_active", "long"), ("unit_system", "string", "unit_system", "string"), ("temperature_unit", "string", "temperature_unit", "string"), ("notes", "string", "notes", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("stowage_scheme", "string", "stowage_scheme", "string"), ("documentation_nbr", "string", "documentation_nbr", "string"), ("service_registry_nbr", "string", "service_registry_nbr", "string"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "vslvsls_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## road_truck_actions datasink
truckact_datasink = glueContext.write_dynamic_frame.from_options(frame = truckact_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_actions"}, format = "parquet", transformation_ctx = "truckact_datasink")

## road_truck_company_drivers datasink
truckcodr_datasink = glueContext.write_dynamic_frame.from_options(frame = truckcodr_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_company_drivers"}, format = "parquet", transformation_ctx = "truckcodr_datasink")

## road_truck_drivers datasink
truckdr_datasink = glueContext.write_dynamic_frame.from_options(frame = truckdr_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_drivers"}, format = "parquet", transformation_ctx = "truckdr_datasink")

## road_truck_transaction_stages datasink
truckstg_datasink = glueContext.write_dynamic_frame.from_options(frame = truckstg_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_transaction_stages"}, format = "parquet", transformation_ctx = "truckstg_datasink")

## road_truck_transactions datasink
trucktrn_datasink = glueContext.write_dynamic_frame.from_options(frame = trucktrn_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_transactions"}, format = "parquet", transformation_ctx = "trucktrn_datasink")

## road_truck_visit_details datasink
truckvstdet_datasink = glueContext.write_dynamic_frame.from_options(frame = truckvstdet_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_visit_details"}, format = "parquet", transformation_ctx = "truckvstdet_datasink")

## road_truck_visit_stages datasink
trkvststg_datasink = glueContext.write_dynamic_frame.from_options(frame = trkvststg_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_visit_stages"}, format = "parquet", transformation_ctx = "trkvststg_datasink")

## road_truck_visit_stats datasink
trkvststat_datasink = glueContext.write_dynamic_frame.from_options(frame = trkvststat_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/road_truck_visit_stats"}, format = "parquet", transformation_ctx = "trkvststat_datasink")

## vsl_vessel_classes datasink
vslclass_datasink = glueContext.write_dynamic_frame.from_options(frame = vslclass_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/vsl_vessel_classes"}, format = "parquet", transformation_ctx = "vslclass_datasink")

## vsl_vessel_visit_details datasink
vspvstdet_datasink = glueContext.write_dynamic_frame.from_options(frame = vspvstdet_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/vsl_vessel_visit_details"}, format = "parquet", transformation_ctx = "vspvstdet_datasink")

## vsl_vessel_visit_lines datasink
cslvstlines_datasink = glueContext.write_dynamic_frame.from_options(frame = cslvstlines_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/vsl_vessel_visit_lines"}, format = "parquet", transformation_ctx = "cslvstlines_datasink")

## vsl_vessels datasink
vslvsls_datasink = glueContext.write_dynamic_frame.from_options(frame = vslvsls_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/vsl_vessels"}, format = "parquet", transformation_ctx = "vslvsls_datasink")



job.commit()