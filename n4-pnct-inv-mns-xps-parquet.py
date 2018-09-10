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

glue = boto3.client(service_name='glue', region_name='us-west-2',endpoint_url='https://glue.us-west-2.amazonaws.com')
              

										######################################
                                        ####        CONNECTION BLOCK      ####
                                        ######################################
										
## inv_goods connection
ig_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_goods", transformation_ctx = "ig_DS")
ig_regDF = ig_DS.toDF()
ig_regDF.createOrReplaceTempView("distig")
ig_distDF = spark.sql("with maxig as \
						( \
						  SELECT gkey, max(audtdateadded) audtdateadded \
						  FROM distig \
						  GROUP BY gkey \
						) \
						SELECT DISTINCT ig.sourcesystem, \
								ig.audtdateadded, \
								ig.gkey, \
								ig.consignee, \
								ig.shipper, \
								ig.consignee_bzu, \
								ig.shipper_bzu, \
								ig.commodity_gkey, \
								ig.origin, \
								ig.destination, \
								ig.hazardous, \
								ig.imdg_types, \
								ig.hazard_un_nums, \
								ig.bl_nbr, hazards_gkey, \
								ig.temp_reqd_c, \
								ig.temp_max_c, \
								ig.temp_min_c, \
								ig.temp_show_fahrenheit, \
								ig.vent_required_pct, \
								ig.vent_unit, \
								ig.humidity_required_pct, \
								ig.o2_pct, \
								ig.co2_pct, \
								ig.on_power, \
								ig.off_power_time, \
								ig.time_mon1, \
								ig.time_mon2, \
								ig.time_mon3, \
								ig.time_mon4, \
								ig.unplug_warn_min, \
								ig.ext_time_monitors \
						FROM distig ig \
						INNER JOIN maxig ma ON ig.gkey = ma.gkey \
						 and ig.audtdateadded = ma.audtdateadded \
						where ig.is_deleted = false")
ig_dynDF = DynamicFrame.fromDF(ig_distDF, glueContext, "nested")

## inv_move_event connection
me_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_move_event", transformation_ctx = "me_DS")
me_regDF = me_DS.toDF()
me_regDF.createOrReplaceTempView("distme")
me_distDF = spark.sql("with maxme as \
                        	( \
                        		SELECT mve_gkey, coalesce(max(t_put),cast('1900-01-01' as timestamp)) t_put, coalesce(max(processed),0) processed \
                        		FROM distme \
                        		GROUP BY mve_gkey \
                        	) \
                        SELECT DISTINCT me.sourcesystem, \
                        	me.mve_gkey, \
                        	me.move_kind, \
                        	me.ufv_gkey, \
                        	me.line_op, \
                        	me.carrier_gkey, \
                        	me.exclude, \
                        	me.fm_pos_loctype, \
                        	me.fm_pos_locid, \
                        	me.fm_pos_loc_gkey, \
                        	me.fm_pos_slot, \
                        	me.fm_pos_orientation, \
                        	me.fm_pos_name, \
                        	me.fm_pos_bin, \
                        	me.fm_pos_tier, \
                        	me.fm_pos_anchor, \
                        	me.fm_pos_orientation_degrees, \
                        	me.to_pos_loctype, \
                        	me.to_pos_locid, \
                        	me.to_pos_loc_gkey, \
                        	me.to_pos_slot, \
                        	me.to_pos_orientation, \
                        	me.to_pos_name, \
                        	me.to_pos_bin, \
                        	me.to_pos_tier, \
                        	me.to_pos_anchor, \
                        	me.to_pos_orientation_degrees, \
                        	me.che_fetch, \
                        	me.che_carry, \
                        	me.che_put, \
                        	me.che_qc, \
                        	me.dist_start, \
                        	me.dist_carry, \
                        	me.t_carry_complete, \
                        	me.t_dispatch, \
                        	me.t_fetch, \
                        	me.t_discharge, \
                        	me.t_put, \
                        	me.t_carry_fetch_ready, \
                        	me.t_carry_put_ready, \
                        	me.t_carry_dispatch, \
                        	me.t_tz_arrival, \
                        	me.rehandle_count, \
                        	me.twin_fetch, \
                        	me.twin_carry, \
                        	me.twin_put, \
                        	me.restow_account, \
                        	me.service_order, \
                        	me.restow_reason, \
                        	coalesce(me.processed,0) processed, \
                        	me.pow, \
                        	me.che_carry_login_name, \
                        	me.che_put_login_name, \
                        	me.che_fetch_login_name, \
                        	me.berth, \
                        	me.category, \
                        	me.freight_kind \
                        FROM distme me \
                        INNER JOIN maxme ma on me.mve_gkey = ma.mve_gkey  \
                        		and coalesce(me.t_put,cast('1900-01-01' as timestamp)) = ma.t_put \
                        		and coalesce(me.processed,0) = ma.processed \
                        WHERE me.is_deleted = false")
me_dynDF = DynamicFrame.fromDF(me_distDF, glueContext, "nested")

## inv_unit connection
iu_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_unit", transformation_ctx = "iu_DS")
iu_regDF = iu_DS.toDF()
iu_regDF.createOrReplaceTempView("distiu")
iu_distDF = spark.sql("with maxiu as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(create_time) create_time,coalesce(max(time_denorm_calc),cast('1900-01-01' as timestamp)) time_denorm_calc \
                        		FROM distiu \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT iu.sourcesystem, \
                        		iu.gkey, \
                        		iu.id, \
                        		iu.foreignhost_key, \
                        		iu.visit_state, \
                        		iu.needs_review, \
                        		iu.placards_mismatched, \
                        		iu.create_time, \
                        		iu.declrd_ib_cv, \
                        		iu.category, \
                        		iu.freight_kind, \
                        		iu.dray_status, \
                        		iu.complex_gkey, \
                        		iu.goods, \
                        		iu.special_stow_gkey, \
                        		iu.special_stow2_gkey, \
                        		iu.special_stow3_gkey, \
                        		iu.deck_rqmnt, \
                        		iu.requires_power, \
                        		iu.is_powered, \
                        		iu.want_powered, \
                        		iu.power_rqst_time, \
                        		iu.is_alarm_on, \
                        		iu.is_oog, \
                        		iu.oog_back_cm, \
                        		iu.oog_front_cm, \
                        		iu.oog_left_cm, \
                        		iu.oog_right_cm, \
                        		iu.oog_top_cm, \
                        		iu.vldt_rlgp, \
                        		iu.xfer_rlgp, \
                        		iu.line_op, \
                        		iu.goods_and_ctr_wt_kg, \
                        		iu.goods_ctr_wt_kg_advised, \
                        		iu.goods_ctr_wt_kg_gate_measured, \
                        		iu.goods_ctr_wt_kg_yard_measured, \
                        		iu.goods_ctr_wt_kg_qc_measured, \
                        		iu.ign_pyld_wghts, \
                        		iu.ign_pyld_hghts, \
                        		iu.is_stowplan_posted, \
                        		iu.seal_nbr1, \
                        		iu.seal_nbr2, \
                        		iu.seal_nbr3, \
                        		iu.seal_nbr4, \
                        		iu.is_ctr_sealed, \
                        		iu.is_bundle, \
                        		iu.active_ufv, \
                        		iu.opl_gkey, \
                        		iu.pol_gkey, \
                        		iu.cv_gkey, \
                        		iu.service_gkey, \
                        		iu.pod1_gkey, \
                        		iu.pod2_gkey, \
                        		iu.opt1_gkey, \
                        		iu.opt2_gkey, \
                        		iu.opt3_gkey, \
                        		iu.group_gkey, \
                        		iu.description, \
                        		iu.export_clearance_nbr, \
                        		iu.return_to_location, \
                        		iu.trucking_company, \
                        		iu.pin_nbr, \
                        		iu.bond_trucking_company, \
                        		iu.bonded_destination, \
                        		iu.projected_pod_gkey, \
                        		iu.ido_gkey, \
                        		iu.ido_expiry_date, \
                        		iu.time_denorm_calc, \
                        		iu.time_state_change, \
                        		iu.stopped_vessel, \
                        		iu.stopped_rail, \
                        		iu.stopped_road, \
                        		iu.imped_vessel, \
                        		iu.imped_rail, \
                        		iu.imped_road, \
                        		iu.remark, \
                        		iu.way_bill_nbr, \
                        		iu.way_bill_date, \
                        		iu.export_release_nbr, \
                        		iu.export_release_date, \
                        		iu.flex_string01, \
                        		iu.flex_string02, \
                        		iu.flex_string03, \
                        		iu.flex_string04, \
                        		iu.flex_string05, \
                        		iu.flex_string06, \
                        		iu.flex_string07, \
                        		iu.flex_string08, \
                        		iu.flex_string09, \
                        		iu.flex_string10, \
                        		iu.flex_string12, \
                        		iu.flex_string13, \
                        		iu.flex_string14, \
                        		iu.flex_string15, \
                        		iu.touch_ctr, \
                        		iu.inbond, \
                        		iu.exam, \
                        		iu.acry_equip_ids, \
                        		iu.customs_id, \
                        		iu.agent1, \
                        		iu.agent2, \
                        		iu.changed, \
                        		iu.cargo_quantity, \
                        		iu.cargo_quantity_unit, \
                        		iu.related_unit, \
                        		iu.relationship_role, \
                        		iu.unit_combo, \
                        		iu.eq_gkey, \
                        		iu.carriage_unit, \
                        		iu.eqs_gkey, \
                        		iu.damage, \
                        		iu.sparcs_damage_code, \
                        		iu.dmgs_gkey, \
                        		iu.condition_gkey, \
                        		iu.bad_nbr, \
                        		iu.is_folded, \
                        		iu.arrive_order_item_gkey, \
                        		iu.depart_order_item_gkey, \
                        		iu.is_reserved, \
                        		iu.mnr_status_gkey, \
                        		iu.placarded, \
                        		iu.grade_gkey, \
                        		iu.goods_ctr_wt_kg_vgm, \
                        		iu.unit_gross_weight_source, \
                        		iu.unit_vgm_entity, \
                        		iu.unit_vgm_verified_date \
                        FROM distiu iu \
                        INNER JOIN maxiu ma on iu.gkey = ma.gkey \
                        	and coalesce(iu.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and iu.create_time = ma.create_time \
                        	and coalesce(iu.time_denorm_calc,cast('1900-01-01' as timestamp)) = ma.time_denorm_calc \
                        WHERE iu.is_deleted = false")
iu_dynDF = DynamicFrame.fromDF(iu_distDF, glueContext, "nested")

## inv_unit_fcy_visit connection
fcy_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_unit_fcy_visit", transformation_ctx = "fcy_DS")
fcy_regDF = fcy_DS.toDF()
fcy_regDF.createOrReplaceTempView("distfcy")
fcy_distDF = spark.sql("with maxfv as \
                        	( \
                        		SELECT gkey, coalesce(max(time_move),cast('1900-01-01' as timestamp)) time_move, max(create_time) create_time \
                        		FROM distfcy \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT fv.sourcesystem, \
                        		fv.gkey, \
                        		fv.horizon, \
                        		fv.visit_state, \
                        		fv.transit_state, \
                        		fv.create_time, \
                        		fv.arrive_pos_loctype, \
                        		fv.arrive_pos_locid, \
                        		fv.arrive_pos_loc_gkey, \
                        		fv.arrive_pos_slot, \
                        		fv.arrive_pos_orientation, \
                        		fv.arrive_pos_name, \
                        		fv.arrive_ops_pos_id, \
                        		fv.arrive_pos_bin, \
                        		fv.arrive_pos_tier, \
                        		fv.arrive_pos_anchor, \
                        		fv.arrive_pos_orientation_degrees, \
                        		fv.arrive_pos_slot_on_carriage, \
                        		fv.last_pos_loctype, \
                        		fv.last_pos_locid, \
                        		fv.last_pos_loc_gkey, \
                        		fv.last_pos_slot, \
                        		fv.last_pos_orientation, \
                        		fv.last_pos_name, \
                        		fv.last_ops_pos_id, \
                        		fv.last_pos_bin, \
                        		fv.last_pos_tier, \
                        		fv.last_pos_anchor, \
                        		fv.last_pos_orientation_degrees, \
                        		fv.last_pos_slot_on_carriage, \
                        		fv.time_move, \
                        		fv.time_load, \
                        		fv.time_rnd, \
                        		fv.time_ecout, \
                        		fv.time_in, \
                        		fv.time_out, \
                        		fv.time_dlv_appmnt, \
                        		fv.appt_nbr, \
                        		fv.time_inv, \
                        		fv.time_complete, \
                        		fv.unit_gkey, \
                        		fv.fcy_gkey, \
                        		fv.sequence, \
                        		fv.intend_ob_cv, \
                        		fv.restow_typ, \
                        		fv.hndlg_rsn, \
                        		fv.yrd_stwge, \
                        		fv.sparcs_stopped, \
                        		fv.sparcs_note, \
                        		fv.has_changed, \
                        		fv.vrfd_load, \
                        		fv.vrfd_yard, \
                        		fv.ed_anomaly, \
                        		fv.actual_ib_cv, \
                        		fv.actual_ob_cv, \
                        		fv.last_free_day, \
                        		fv.paid_thru_day, \
                        		fv.guarantee_thru_day, \
                        		fv.guarantee_party_gkey, \
                        		fv.power_last_free_day, \
                        		fv.power_paid_thru_day, \
                        		fv.power_guarantee_thru_day, \
                        		fv.power_guarantee_party_gkey, \
                        		fv.line_last_free_day, \
                        		fv.line_paid_thru_day, \
                        		fv.line_guarantee_thru_day, \
                        		fv.line_guarantee_party_gkey, \
                        		fv.visible_sparcs, \
                        		fv.flex_string01, \
                        		fv.flex_string02, \
                        		fv.flex_string03, \
                        		fv.flex_string04, \
                        		fv.flex_string05, \
                        		fv.flex_string06, \
                        		fv.flex_string07, \
                        		fv.flex_string08, \
                        		fv.flex_string09, \
                        		fv.flex_string10, \
                        		fv.flex_date01, \
                        		fv.flex_date02, \
                        		fv.flex_date03, \
                        		fv.flex_date04, \
                        		fv.flex_date05, \
                        		fv.flex_date06, \
                        		fv.flex_date07, \
                        		fv.flex_date08, \
                        		fv.is_direct_ib_to_ob_move, \
                        		fv.validate_marry_errors, \
                        		fv.stow_factor, \
                        		fv.return_to_yard, \
                        		fv.move_count, \
                        		fv.ras_priority, \
                        		fv.housekp_cur_slot, \
                        		fv.housekp_ftr_slot, \
                        		fv.housekp_cur_score, \
                        		fv.housekp_ftr_score, \
                        		fv.housekp_timestamp, \
                        		fv.stacking_factor, \
                        		fv.section_factor, \
                        		fv.cas_unit_reference, \
                        		fv.cas_tran_reference, \
                        		fv.preferred_transfer_location, \
                        		fv.door_direction, \
                        		fv.optimal_rail_tz_slot, \
                        		fv.carrier_incompatible_reason, \
                        		fv.rail_cone_status, \
                        		fv.segregation_factor \
                        FROM distfcy fv \
                        INNER JOIN maxfv ma on fv.gkey = ma.gkey \
                        	and coalesce(fv.time_move,cast('1900-01-01' as timestamp)) = ma.time_move \
                        	and fv.create_time = ma.create_time \
                        WHERE fv.is_deleted = false")
fcy_dynDF = DynamicFrame.fromDF(fcy_distDF, glueContext, "nested")

## inv_unit_yrd_visit connection
yrd_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_unit_yrd_visit", transformation_ctx = "yrd_DS")
yrd_regDF = yrd_DS.toDF()
yrd_regDF.createOrReplaceTempView("distyrd")
yrd_distDF = spark.sql("with maxyrd as \
						( \
						  SELECT gkey,max(audtdateadded) audtdateadded \
						  FROM distyrd \
						  GROUP BY gkey \
						 ) \
						SELECT DISTINCT \
								yr.sourcesystem,  \
								yr.audtdateadded,  \
								yr.gkey,  \
								yr.ufv_gkey,  \
								yr.yrd_gkey,  \
								yr.pkey \
						FROM distyrd yr \
						INNER JOIN maxyrd ma on yr.gkey = ma.gkey \
						 and yr.audtdateadded = ma.audtdateadded \
						WHERE yr.is_deleted = false")
yrd_dynDF = DynamicFrame.fromDF(yrd_distDF, glueContext, "nested")

## inv_wi connection
wi_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_wi", transformation_ctx = "wi_DS")
wi_regDF = wi_DS.toDF()
wi_regDF.createOrReplaceTempView("distwi")
wi_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
							gkey, \
							pkey, \
							facility_gkey, \
							uyv_gkey, \
							tbdu_gkey, \
							wq_pkey, \
							work_queue_gkey, \
							ar_pkey, \
							ar_gkey, \
							shift_target_pkey, \
							shift_target_gkey, \
							che_id, \
							che_gkey, \
							itv_id, \
							itv_gkey, \
							che_work_assignment_pkey, \
							che_work_assignment_gkey, \
							itv_work_assignment_pkey, \
							itv_work_assignment_gkey, \
							intended_che_id, \
							intended_che_gkey, \
							nominated_che_gkey, \
							move_number, \
							sequence, \
							crane_lane, \
							set_aside, \
							door_direction, \
							decking_restriction, \
							twin_with, \
							twin_int_fetch, \
							twin_int_carry, \
							twin_int_put, \
							ec_state_fetch, \
							ec_state_dispatch_request, \
							ec_state_itv_dispatch_request, \
							ec_state_dispatch_b, \
							ec_state_itv_dispatch_b, \
							suspend_state, \
							locked, \
							confirmed, \
							definite, \
							executable, \
							asc_aborted, \
							increment_crane_lane, \
							being_rehandled, \
							being_deleted, \
							should_be_sent, \
							sequenced_pwp, \
							host_request_priority, \
							skip_host_update, \
							move_stage, \
							conf_move_stage, \
							ec_hold, \
							eq_moves_key, \
							eq_uses_key, \
							move_kind, \
							carrier_loctype, \
							carrier_locid, \
							mvhs_fetch_che_id, \
							mvhs_fetch_che_name, \
							mvhs_carry_che_id, \
							mvhs_carry_che_name, \
							mvhs_put_che_id, \
							mvhs_put_che_name, \
							mvhs_dist_start, \
							mvhs_dist_carry, \
							mvhs_t_carry_complete, \
							mvhs_t_dispatch, \
							mvhs_t_fetch, \
							mvhs_t_put, \
							mvhs_t_carry_fetch_ready, \
							mvhs_t_carry_put_ready, \
							mvhs_t_carry_dispatch, \
							mvhs_t_discharge, \
							mvhs_rehandle_count, \
							mvhs_pow_pkey, \
							mvhs_pool_pkey, \
							mvhs_twin_fetch, \
							mvhs_twin_carry, \
							mvhs_twin_put, \
							mvhs_tandem_fetch, \
							mvhs_tandem_put, \
							mvhs_tz_arrival_time, \
							mvhs_fetch_che_distance, \
							mvhs_carry_che_distance, \
							mvhs_put_che_distance, \
							mvhs_t_fetch_dispatch, \
							mvhs_t_put_dispatch, \
							est_move_time, \
							t_orig_est_start, \
							restow_account, \
							service_order, \
							restow_reason, \
							road_truck, \
							che_dispatch_order, \
							itv_dispatch_order, \
							pos_loctype, \
							pos_locid, \
							pos_loc_gkey, \
							pos_slot, \
							pos_orientation, \
							pos_name, \
							pos_bin, \
							pos_tier, \
							pos_anchor, \
							pos_orientation_degrees, \
							pos_slot_on_carriage, \
							truck_visit_ref, \
							pool_level, \
							msg_ref, \
							planner_create, \
							planner_modify, \
							t_create, \
							t_modified, \
							create_tool, \
							imminent_move, \
							almost_imminent, \
							almost_current, \
							being_carried, \
							carry_by_straddle, \
							dispatch_hung, \
							ignore_gate_hold, \
							tv_gkey, \
							tran_gkey, \
							origin_from_qual, \
							assigned_chassid, \
							swappable_delivery, \
							n4_specified_queue, \
							gate_transaction_sequence, \
							crane_lane_tier, \
							yard_move_filter, \
							actual_sequence, \
							actual_vessel_twin_pkey, \
							actual_vessel_twin_gkey, \
							strict_load, \
							preferred_transfer_location, \
							is_tandem_with_next, \
							is_tandem_with_previous, \
							xps_object_version, \
							emt_time_horizon_min, \
							pair_with \
					FROM distwi")
wi_dynDF = DynamicFrame.fromDF(wi_distDF, glueContext, "nested")

## inv_wi_tracking connection
wit_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_wi_tracking", transformation_ctx = "wit_DS")
wit_regDF = wit_DS.toDF()
wit_regDF.createOrReplaceTempView("distwit")
wit_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
							wi_tracking_id, \
							date_added, \
							gkey, \
							pkey, \
							facility_gkey, \
							uyv_gkey, \
							tbdu_gkey, \
							wq_pkey, \
							work_queue_gkey, \
							ar_pkey, \
							ar_gkey, \
							shift_target_pkey, \
							shift_target_gkey, \
							che_id, \
							che_gkey, \
							itv_id, \
							itv_gkey, \
							che_work_assignment_pkey, \
							che_work_assignment_gkey, \
							itv_work_assignment_pkey, \
							itv_work_assignment_gkey, \
							intended_che_id, \
							intended_che_gkey, \
							nominated_che_gkey, \
							move_number, \
							sequence, \
							crane_lane, \
							set_aside, \
							door_direction, \
							decking_restriction, \
							twin_with, \
							twin_int_fetch, \
							twin_int_carry, \
							twin_int_put, \
							ec_state_fetch, \
							ec_state_dispatch_request, \
							ec_state_itv_dispatch_request, \
							ec_state_dispatch_b, \
							ec_state_itv_dispatch_b, \
							suspend_state, \
							locked, \
							confirmed, \
							definite, \
							executable, \
							asc_aborted, \
							increment_crane_lane, \
							being_rehandled, \
							being_deleted, \
							should_be_sent, \
							sequenced_pwp, \
							host_request_priority, \
							skip_host_update, \
							move_stage, \
							conf_move_stage, \
							ec_hold, \
							eq_moves_key, \
							eq_uses_key, \
							move_kind, \
							carrier_loctype, \
							carrier_locid, \
							mvhs_fetch_che_id, \
							mvhs_fetch_che_name, \
							mvhs_carry_che_id, \
							mvhs_carry_che_name, \
							mvhs_put_che_id, \
							mvhs_put_che_name, \
							mvhs_dist_start, \
							mvhs_dist_carry, \
							mvhs_t_carry_complete, \
							mvhs_t_dispatch, \
							mvhs_t_fetch, \
							mvhs_t_put, \
							mvhs_t_carry_fetch_ready, \
							mvhs_t_carry_put_ready, \
							mvhs_t_carry_dispatch, \
							mvhs_t_discharge, \
							mvhs_rehandle_count, \
							mvhs_pow_pkey, \
							mvhs_pool_pkey, \
							mvhs_twin_fetch, \
							mvhs_twin_carry, \
							mvhs_twin_put, \
							mvhs_tandem_fetch, \
							mvhs_tandem_put, \
							mvhs_tz_arrival_time, \
							mvhs_fetch_che_distance, \
							mvhs_carry_che_distance, \
							mvhs_put_che_distance, \
							mvhs_t_fetch_dispatch, \
							mvhs_t_put_dispatch, \
							est_move_time, \
							t_orig_est_start, \
							restow_account, \
							service_order, \
							restow_reason, \
							road_truck, \
							che_dispatch_order, \
							itv_dispatch_order, \
							pos_loctype, \
							pos_locid, \
							pos_loc_gkey, \
							pos_slot, \
							pos_orientation,pos_name, \
							pos_bin, \
							pos_tier, \
							pos_anchor, \
							pos_orientation_degrees, \
							pos_slot_on_carriage, \
							truck_visit_ref, \
							pool_level, \
							msg_ref, \
							planner_create, \
							planner_modify, \
							t_create, \
							t_modified, \
							create_tool, \
							imminent_move, \
							almost_imminent, \
							almost_current, \
							being_carried, \
							carry_by_straddle, \
							dispatch_hung, \
							ignore_gate_hold, \
							tv_gkey, \
							tran_gkey, \
							origin_from_qual, \
							assigned_chassid, \
							swappable_delivery, \
							n4_specified_queue, \
							gate_transaction_sequence, \
							crane_lane_tier, \
							yard_move_filter, \
							actual_sequence, \
							actual_vessel_twin_pkey, \
							actual_vessel_twin_gkey, \
							strict_load, \
							preferred_transfer_location, \
							is_tandem_with_next, \
							is_tandem_with_previous, \
							xps_object_version, \
							emt_time_horizon_min, \
							pair_with \
					FROM distwit")
wit_dynDF = DynamicFrame.fromDF(wit_distDF, glueContext, "nested")

## inv_wq connection
wq_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "inv_wq", transformation_ctx = "wq_DS")
wq_regDF = wq_DS.toDF()
wq_regDF.createOrReplaceTempView("distwq")
wq_distDF = spark.sql("with maxwq as \
							 ( \
								select gkey, \
										max(t_last_activation_chg) t_last_activation_chg, \
										max(audtdateadded) audtdateadded, \
										max(t_delete_ready) t_delete_ready \
								from distwq \
								group by gkey \
							  ) \
						SELECT DISTINCT  wq.sourcesystem, \
								wq.audtdateadded, \
								wq.gkey, \
								wq.pkey, \
								wq.pow_pkey, \
								wq.pow_gkey, \
								wq.yrd_gkey, \
								wq.cycle_companion_pkey, \
								wq.cycle_companion_gkey, \
								wq.first_shift_pkey, \
								wq.first_shift_gkey, \
								wq.qorder, \
								wq.qtype, \
								wq.qfission, \
								wq.wis_sent_tls, \
								wq.save_completed_moves, \
								wq.is_blue, \
								wq.permanent, \
								wq.service_order_queue, \
								wq.manual_sequence_mode, \
								wq.yard_loadback_queue, \
								wq.allow_40, \
								wq.allow_20, \
								wq.pos_loctype, \
								wq.pos_locid, \
								wq.qcode, \
								wq.qdeck, \
								wq.qrow, \
								wq.name, \
								wq.t_delete_ready, \
								wq.doublecycle_from_sequence, \
								wq.doublecycle_to_sequence, \
								wq.note, \
								wq.computed_20_projection, \
								wq.computed_40_projection, \
								wq.vessel_lcg, \
								wq.use_wq_prod, \
								wq.productivity_std, \
								wq.productivity_dual, \
								wq.productivity_twin, \
								wq.max_teu_cap, \
								wq.pct_cap_start_prty, \
								wq.pnlty_exceed_cap, \
								wq.tls_sort_key, \
								wq.productivity_tandem, \
								wq.productivity_quad, \
								wq.berth_call_slave_id, \
								wq.t_last_activation_chg \
						FROM distwq wq \
						INNER JOIN maxwq ma on wq.gkey = ma.gkey \
							and wq.t_last_activation_chg = ma.t_last_activation_chg \
							and wq.audtdateadded = ma.audtdateadded \
							and wq.t_delete_ready = ma.t_delete_ready \
						WHERE is_deleted = false")
wq_dynDF = DynamicFrame.fromDF(wq_distDF, glueContext, "nested")

## mns_che_move_statistics connection
mvstat_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_move_statistics", transformation_ctx = "mvstat_DS")
mvstat_regDF = mvstat_DS.toDF()
mvstat_regDF.createOrReplaceTempView("distmvstat")
mvstat_distDF = spark.sql("with maxms as \
								( \
									SELECT gkey, \
											max(audtdateadded) audtdateadded, \
											coalesce(max(t_pick_dispatch),cast('1900-01-01' as timestamp)) t_pick_dispatch \
									FROM distmvstat \
									GROUP BY gkey \
								) \
							SELECT DISTINCT ms.sourcesystem, \
									ms.audtdateadded, \
									ms.gkey, \
									ms.che_trip_gkey, \
									ms.che_session_period_gkey, \
									ms.move_event_gkey, \
									ms.move_gkey, \
									ms.carrier_gkey, \
									ms.declrd_ib_cv_gkey, \
									ms.declrd_ob_cv_gkey, \
									ms.che_qc, \
									ms.unit_id, \
									ms.category, \
									ms.freight_kind, \
									ms.is_live_reefer, \
									ms.basic_length, \
									ms.eq_type_iso, \
									ms.nominal_length, \
									ms.work_queue, \
									ms.pow, \
									ms.pool, \
									ms.fm_pos_loctype, \
									ms.fm_pos_locid, \
									ms.fm_pos_loc_gkey, \
									ms.fm_pos_slot, \
									ms.fm_pos_orientation, \
									ms.fm_pos_name, \
									ms.fm_pos_bin, \
									ms.fm_pos_tier, \
									ms.fm_pos_orientation_degrees, \
									ms.fm_pos_slot_on_carriage, \
									ms.to_pos_loctype, \
									ms.to_pos_locid, \
									ms.to_pos_loc_gkey, \
									ms.to_pos_slot, \
									ms.to_pos_orientation, \
									ms.to_pos_name, \
									ms.to_pos_bin, \
									ms.to_pos_tier, \
									ms.to_pos_orientation_degrees, \
									ms.to_pos_slot_on_carriage, \
									ms.t_pick_dispatch, \
									ms.t_pick_arrive, \
									ms.t_lift_dispatch, \
									ms.t_cntr_on, \
									ms.t_drop_dispatch, \
									ms.t_drop_arrive, \
									ms.t_cntr_off, \
									ms.pick_idle_time, \
									ms.pick_wait_time, \
									ms.drop_idle_time, \
									ms.drop_wait_time, \
									ms.rehandle_count, \
									ms.twin_move, \
									ms.lane_change, \
									ms.pick_distance, \
									ms.drop_distance, \
									ms.move_direction, \
									ms.move_category, \
									ms.from_block, \
									ms.from_block_type, \
									ms.to_block, \
									ms.to_block_type, \
									ms.wait_time_qc, \
									ms.wait_time_block, \
									ms.move_purpose, \
									ms.is_tandem, \
									ms.is_quad, \
									ms.spreader_id, \
									ms.cycle_time, \
									ms.t_buffer_enter, \
									ms.t_buffer_exit \
							FROM distmvstat ms \
							INNER JOIN maxms ma ON ms.gkey = ma.gkey \
							 and ms.audtdateadded = ma.audtdateadded \
							 and ms.t_pick_dispatch = ma.t_pick_dispatch \
							WHERE ms.is_deleted = false")
mvstat_dynDF = DynamicFrame.fromDF(mvstat_distDF, glueContext, "nested")

## mns_che_operator_statistics connection
operstat_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_operator_statistics", transformation_ctx = "operstat_DS")
operstat_regDF = operstat_DS.toDF()
operstat_regDF.createOrReplaceTempView("distoperstat")
operstat_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
								max(audtdateadded) audtdateadded, \
								gkey, \
								che_operator, \
								che, \
								che_op_action, \
								t_action_time  \
							FROM distoperstat \
							WHERE is_deleted = false \
							GROUP BY sourcesystem, \
								gkey, \
								che_operator, \
								che, \
								che_op_action, \
								t_action_time")
operstat_dynDF = DynamicFrame.fromDF(operstat_distDF, glueContext, "nested")

## mns_che_session connection
chesess_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_session", transformation_ctx = "chesess_DS")
chesess_regDF = chesess_DS.toDF()
chesess_regDF.createOrReplaceTempView("distchesess")
chesess_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
									max(audtdateadded) audtdateadded, \
									gkey, \
									che_operator, \
									che, \
									max(t_login) t_login, \
									coalesce(max(t_logout),cast('1900-01-01' as timestamp)) t_logout \
							FROM distchesess \
							where is_deleted = false \
							GROUP BY sourcesystem, \
								gkey, \
								che_operator, \
								che")
chesess_dynDF = DynamicFrame.fromDF(chesess_distDF, glueContext, "nested")

## mns_che_session_period connection
sessper_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_session_period", transformation_ctx = "sessper_DS")
sessper_regDF = sessper_DS.toDF()
sessper_regDF.createOrReplaceTempView("distsessper")
sessper_distDF = spark.sql("with maxcs as \
                            	( \
                            		SELECT gkey, coalesce(max(t_end),cast('1900-01-01' as timestamp)) t_end, max(t_start) t_start \
                            		FROM distsessper \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT cs.sourcesystem, \
                            		cs.gkey, \
                            		cs.che_session_gkey, \
                            		cs.t_start, \
                            		cs.t_end, \
                            		cs.duration, \
                            		cs.csp_period_type \
                            FROM distsessper cs \
                            INNER JOIN maxcs ma on cs.gkey = ma.gkey \
                            	and coalesce(cs.t_end,cast('1900-01-01' as timestamp)) = ma.t_end \
                            	and cs.t_start = ma.t_start \
                            WHERE cs.is_deleted = false")
sessper_dynDF = DynamicFrame.fromDF(sessper_distDF, glueContext, "nested")

## mns_che_status connection -- done
status_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_status", transformation_ctx = "status_DS")
status_regDF = status_DS.toDF()
status_regDF.createOrReplaceTempView("diststatus")
status_distDF = spark.sql("with maxsta as \
                            	( \
                            		SELECT gkey, coalesce(max(end_time),cast('1900-01-01' as timestamp)) end_time, max(start_time) start_time \
                            		FROM diststatus \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT sta.sourcesystem, \
                            		sta.gkey, \
                            		sta.che, \
                            		sta.che_name, \
                            		sta.start_time, \
                            		sta.end_time, \
                            		sta.duration, \
                            		sta.status \
                            FROM diststatus sta \
                            INNER JOIN maxsta ma on sta.gkey = ma.gkey \
                            	and coalesce(sta.end_time,cast('1900-01-01' as timestamp)) = ma.end_time \
                            	and sta.start_time = ma.start_time \
                            WHERE sta.is_deleted = false")
status_dynDF = DynamicFrame.fromDF(status_distDF, glueContext, "nested")

## mns_che_trip_statistics connection -- need to work on dupes
tripstat_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "mns_che_trip_statistics", transformation_ctx = "tripstat_DS")
tripstat_regDF = tripstat_DS.toDF()
tripstat_regDF.createOrReplaceTempView("disttripstat")
tripstat_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
									max(audtdateadded) audtdateadded, \
									gkey, \
									yard_gkey, \
									id, \
									che, \
									che_type, \
									move_kind, \
									che_phase, \
									che_operator, \
									dist_laden, \
									dist_unladen, \
									coalesce(max(t_first_cntr_on),cast('1900-01-01' as timestamp)) t_first_cntr_on, \
									coalesce(max(t_last_cntr_off),cast('1900-01-01' as timestamp)) t_last_cntr_off, \
									coalesce(max(t_first_dispatch),cast('1900-01-01' as timestamp)) t_first_dispatch, \
									t_unladen_travel, \
									t_laden_travel \
							FROM disttripstat \
							WHERE is_deleted = false \
							GROUP BY sourcesystem, \
									gkey, \
									yard_gkey, \
									id, \
									che, \
									che_type, \
									move_kind, \
									che_phase, \
									che_operator, \
									dist_laden, \
									dist_unladen, \
									t_unladen_travel, \
									t_laden_travel")
tripstat_dynDF = DynamicFrame.fromDF(tripstat_distDF, glueContext, "nested")

## xps_che connection
xpsche_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_che", transformation_ctx = "xpsche_DS")
xpsche_regDF = xpsche_DS.toDF()
xpsche_regDF.createOrReplaceTempView("distxpsche")
xpsche_distDF = spark.sql("with maxche as \
                        	( \
                        		SELECT DISTINCT gkey, \
                        			max(pool_gkey) pool_gkey, \
                        			max(coalesce(last_time,CAST('1900-01-01' as timestamp))) last_time, \
                        			max(coalesce(time_dispatch,CAST('1900-01-01' as timestamp))) time_dispatch, \
                        			max(coalesce(time_available_unavailable,CAST('1900-01-01' as timestamp))) time_available_unavailable, \
                        			max(coalesce(last_jobstep_transition,CAST('1900-01-01' as timestamp))) last_jobstep_transition \
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
                        		and ch.pool_gkey = mc.pool_gkey \
                        		and coalesce(ch.last_time,cast('1900-01-01' as timestamp)) = mc.last_time \
                        		and coalesce(ch.time_dispatch,cast('1900-01-01' as timestamp)) = mc.time_dispatch \
                        		and coalesce(ch.time_available_unavailable,CAST('1900-01-01' as timestamp)) = mc.time_available_unavailable \
                        		and coalesce(ch.last_jobstep_transition,CAST('1900-01-01' as timestamp)) = mc.last_jobstep_transition \
                        	WHERE ch.last_time > cast('2000-01-01' as timestamp) \
                        	    and ch.is_deleted = false")
xpsche_dynDF = DynamicFrame.fromDF(xpsche_distDF, glueContext, "nested")
				
## xps_ecevent connection	
xpsecevent_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_ecevent", transformation_ctx = "xpsecevent_DS")
xpsecevent_regDF = xpsecevent_DS.toDF()
xpsecevent_regDF.createOrReplaceTempView("distxpsecevent")
xpsecevent_distDF = spark.sql("with maxevt as \
                                	( \
                                		SELECT gkey, max(timestamp) timestamp \
                                		FROM distxpsecevent \
                                		GROUP BY gkey \
                                	) \
                                SELECT DISTINCT evt.sourcesystem, \
                                	evt.gkey, \
                                	evt.yard, \
                                	evt.pkey, \
                                	evt.timestamp ectimestamp, \
                                	evt.type, \
                                	evt.che_id, \
                                	evt.che_name, \
                                	evt.operator_name, \
                                	evt.sub_type, \
                                	evt.type_description, \
                                	evt.from_che_id_name, \
                                	evt.to_che_id_name, \
                                	evt.unit_id_name, \
                                	evt.pow_name, \
                                	evt.pool_name, \
                                	evt.work_queue, \
                                	evt.travel_distance, \
                                	evt.move_kind, \
                                	evt.is_twin_move, \
                                	evt.start_distance, \
                                	evt.work_assignment_gkey, \
                                	evt.work_assignment_id, \
                                	evt.unit_reference, \
                                	evt.tran_id, \
                                	evt.loctype, \
                                	evt.locid, \
                                	evt.loc_slot, \
                                	evt.ops_pos_id, \
                                	evt.unladen_loctype, \
                                	evt.unladen_locid, \
                                	evt.unladen_loc_slot, \
                                	evt.laden_loctype, \
                                	evt.laden_locid, \
                                	evt.laden_loc_slot, \
                                	evt.last_est_move_time \
                                FROM distxpsecevent evt \
                                INNER JOIN maxevt ma on evt.gkey = ma.gkey \
                                	and evt.timestamp = ma.timestamp")
xpsecevent_dynDF = DynamicFrame.fromDF(xpsecevent_distDF, glueContext, "nested")				
									
## xps_ecuser connection
xpsecuser_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "xps_ecuser", transformation_ctx = "xpsecuser_DS")
xpsecuser_regDF = xpsecuser_DS.toDF()
xpsecuser_regDF.createOrReplaceTempView("distxpsecuser")
xpsecuser_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
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
										
## inv_goods mapping
ig_applymapping = ApplyMapping.apply(frame = ig_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("consignee", "string", "consignee", "string"), ("shipper", "string", "shipper", "string"), ("consignee_bzu", "long", "consignee_bzu", "long"), ("shipper_bzu", "long", "shipper_bzu", "long"), ("commodity_gkey", "long", "commodity_gkey", "long"), ("origin", "string", "origin", "string"), ("destination", "string", "destination", "string"), ("hazardous", "long", "hazardous", "long"), ("imdg_types", "string", "imdg_types", "string"), ("hazard_un_nums", "string", "hazard_un_nums", "string"), ("bl_nbr", "string", "bl_nbr", "string"), ("hazards_gkey", "long", "hazards_gkey", "long"), ("temp_reqd_c", "float", "temp_reqd_c", "float"), ("temp_max_c", "float", "temp_max_c", "float"), ("temp_min_c", "float", "temp_min_c", "float"), ("temp_show_fahrenheit", "long", "temp_show_fahrenheit", "long"), ("vent_required_pct", "float", "vent_required_pct", "float"), ("vent_unit", "string", "vent_unit", "string"), ("humidity_required_pct", "float", "humidity_required_pct", "float"), ("o2_pct", "float", "o2_pct", "float"), ("co2_pct", "float", "co2_pct", "float"), ("on_power", "timestamp", "on_power", "timestamp"), ("off_power_time", "timestamp", "off_power_time", "timestamp"), ("time_mon1", "timestamp", "time_mon1", "timestamp"), ("time_mon2", "timestamp", "time_mon2", "timestamp"), ("time_mon3", "timestamp", "time_mon3", "timestamp"), ("time_mon4", "timestamp", "time_mon4", "timestamp"), ("unplug_warn_min", "long", "unplug_warn_min", "long"), ("ext_time_monitors", "long", "ext_time_monitors", "long")], transformation_ctx = "ig_applymapping")

## inv_move_event mapping
me_applymapping = ApplyMapping.apply(frame = me_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("mve_gkey", "long", "mve_gkey", "long"), ("move_kind", "string", "move_kind", "string"), ("ufv_gkey", "long", "ufv_gkey", "long"), ("line_op", "long", "line_op", "long"), ("carrier_gkey", "long", "carrier_gkey", "long"), ("exclude", "long", "exclude", "long"), ("fm_pos_loctype", "string", "fm_pos_loctype", "string"), ("fm_pos_locid", "string", "fm_pos_locid", "string"), ("fm_pos_loc_gkey", "long", "fm_pos_loc_gkey", "long"), ("fm_pos_slot", "string", "fm_pos_slot", "string"), ("fm_pos_orientation", "string", "fm_pos_orientation", "string"), ("fm_pos_name", "string", "fm_pos_name", "string"), ("fm_pos_bin", "long", "fm_pos_bin", "long"), ("fm_pos_tier", "long", "fm_pos_tier", "long"), ("fm_pos_anchor", "string", "fm_pos_anchor", "string"), ("fm_pos_orientation_degrees", "float", "fm_pos_orientation_degrees", "float"), ("to_pos_loctype", "string", "to_pos_loctype", "string"), ("to_pos_locid", "string", "to_pos_locid", "string"), ("to_pos_loc_gkey", "long", "to_pos_loc_gkey", "long"), ("to_pos_slot", "string", "to_pos_slot", "string"), ("to_pos_orientation", "string", "to_pos_orientation", "string"), ("to_pos_name", "string", "to_pos_name", "string"), ("to_pos_bin", "long", "to_pos_bin", "long"), ("to_pos_tier", "long", "to_pos_tier", "long"), ("to_pos_anchor", "string", "to_pos_anchor", "string"), ("to_pos_orientation_degrees", "float", "to_pos_orientation_degrees", "float"), ("che_fetch", "long", "che_fetch", "long"), ("che_carry", "long", "che_carry", "long"), ("che_put", "long", "che_put", "long"), ("che_qc", "long", "che_qc", "long"), ("dist_start", "long", "dist_start", "long"), ("dist_carry", "long", "dist_carry", "long"), ("t_carry_complete", "timestamp", "t_carry_complete", "timestamp"), ("t_dispatch", "timestamp", "t_dispatch", "timestamp"), ("t_fetch", "timestamp", "t_fetch", "timestamp"), ("t_discharge", "timestamp", "t_discharge", "timestamp"), ("t_put", "timestamp", "t_put", "timestamp"), ("t_carry_fetch_ready", "timestamp", "t_carry_fetch_ready", "timestamp"), ("t_carry_put_ready", "timestamp", "t_carry_put_ready", "timestamp"), ("t_carry_dispatch", "timestamp", "t_carry_dispatch", "timestamp"), ("t_tz_arrival", "timestamp", "t_tz_arrival", "timestamp"), ("rehandle_count", "long", "rehandle_count", "long"), ("twin_fetch", "long", "twin_fetch", "long"), ("twin_carry", "long", "twin_carry", "long"), ("twin_put", "long", "twin_put", "long"), ("restow_account", "string", "restow_account", "string"), ("service_order", "string", "service_order", "string"), ("restow_reason", "string", "restow_reason", "string"), ("processed", "long", "processed", "long"), ("pow", "string", "pow", "string"), ("che_carry_login_name", "string", "che_carry_login_name", "string"), ("che_put_login_name", "string", "che_put_login_name", "string"), ("che_fetch_login_name", "string", "che_fetch_login_name", "string"), ("berth", "string", "berth", "string"), ("category", "string", "category", "string"), ("freight_kind", "string", "freight_kind", "string")], transformation_ctx = "me_applymapping")

## inv_unit mapping
iu_applymapping = ApplyMapping.apply(frame = iu_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("foreignhost_key", "string", "foreignhost_key", "string"), ("visit_state", "string", "visit_state", "string"), ("needs_review", "long", "needs_review", "long"), ("placards_mismatched", "long", "placards_mismatched", "long"), ("create_time", "timestamp", "create_time", "timestamp"), ("declrd_ib_cv", "long", "declrd_ib_cv", "long"), ("category", "string", "category", "string"), ("freight_kind", "string", "freight_kind", "string"), ("dray_status", "string", "dray_status", "string"), ("complex_gkey", "long", "complex_gkey", "long"), ("goods", "long", "goods", "long"), ("special_stow_gkey", "long", "special_stow_gkey", "long"), ("special_stow2_gkey", "long", "special_stow2_gkey", "long"), ("special_stow3_gkey", "long", "special_stow3_gkey", "long"), ("deck_rqmnt", "string", "deck_rqmnt", "string"), ("requires_power", "long", "requires_power", "long"), ("is_powered", "long", "is_powered", "long"), ("want_powered", "long", "want_powered", "long"), ("power_rqst_time", "timestamp", "power_rqst_time", "timestamp"), ("is_alarm_on", "long", "is_alarm_on", "long"), ("is_oog", "long", "is_oog", "long"), ("oog_back_cm", "long", "oog_back_cm", "long"), ("oog_front_cm", "long", "oog_front_cm", "long"), ("oog_left_cm", "long", "oog_left_cm", "long"), ("oog_right_cm", "long", "oog_right_cm", "long"), ("oog_top_cm", "long", "oog_top_cm", "long"), ("vldt_rlgp", "long", "vldt_rlgp", "long"), ("xfer_rlgp", "long", "xfer_rlgp", "long"), ("line_op", "long", "line_op", "long"), ("goods_and_ctr_wt_kg", "float", "goods_and_ctr_wt_kg", "float"), ("goods_ctr_wt_kg_advised", "float", "goods_ctr_wt_kg_advised", "float"), ("goods_ctr_wt_kg_gate_measured", "float", "goods_ctr_wt_kg_gate_measured", "float"), ("goods_ctr_wt_kg_yard_measured", "float", "goods_ctr_wt_kg_yard_measured", "float"), ("goods_ctr_wt_kg_qc_measured", "float", "goods_ctr_wt_kg_qc_measured", "float"), ("ign_pyld_wghts", "long", "ign_pyld_wghts", "long"), ("ign_pyld_hghts", "long", "ign_pyld_hghts", "long"), ("is_stowplan_posted", "long", "is_stowplan_posted", "long"), ("seal_nbr1", "string", "seal_nbr1", "string"), ("seal_nbr2", "string", "seal_nbr2", "string"), ("seal_nbr3", "string", "seal_nbr3", "string"), ("seal_nbr4", "string", "seal_nbr4", "string"), ("is_ctr_sealed", "long", "is_ctr_sealed", "long"), ("is_bundle", "long", "is_bundle", "long"), ("active_ufv", "long", "active_ufv", "long"), ("opl_gkey", "long", "opl_gkey", "long"), ("pol_gkey", "long", "pol_gkey", "long"), ("cv_gkey", "long", "cv_gkey", "long"), ("service_gkey", "long", "service_gkey", "long"), ("pod1_gkey", "long", "pod1_gkey", "long"), ("pod2_gkey", "long", "pod2_gkey", "long"), ("opt1_gkey", "long", "opt1_gkey", "long"), ("opt2_gkey", "long", "opt2_gkey", "long"), ("opt3_gkey", "long", "opt3_gkey", "long"), ("group_gkey", "long", "group_gkey", "long"), ("description", "string", "description", "string"), ("export_clearance_nbr", "string", "export_clearance_nbr", "string"), ("return_to_location", "string", "return_to_location", "string"), ("trucking_company", "long", "trucking_company", "long"), ("pin_nbr", "string", "pin_nbr", "string"), ("bond_trucking_company", "long", "bond_trucking_company", "long"), ("bonded_destination", "string", "bonded_destination", "string"), ("projected_pod_gkey", "long", "projected_pod_gkey", "long"), ("ido_gkey", "long", "ido_gkey", "long"), ("ido_expiry_date", "timestamp", "ido_expiry_date", "timestamp"), ("time_denorm_calc", "timestamp", "time_denorm_calc", "timestamp"), ("time_state_change", "timestamp", "time_state_change", "timestamp"), ("stopped_vessel", "long", "stopped_vessel", "long"), ("stopped_rail", "long", "stopped_rail", "long"), ("stopped_road", "long", "stopped_road", "long"), ("imped_vessel", "string", "imped_vessel", "string"), ("imped_rail", "string", "imped_rail", "string"), ("imped_road", "string", "imped_road", "string"), ("remark", "string", "remark", "string"), ("way_bill_nbr", "string", "way_bill_nbr", "string"), ("way_bill_date", "timestamp", "way_bill_date", "timestamp"), ("export_release_nbr", "string", "export_release_nbr", "string"), ("export_release_date", "timestamp", "export_release_date", "timestamp"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string"), ("flex_string11", "string", "flex_string11", "string"), ("flex_string12", "string", "flex_string12", "string"), ("flex_string13", "string", "flex_string13", "string"), ("flex_string14", "string", "flex_string14", "string"), ("flex_string15", "string", "flex_string15", "string"), ("touch_ctr", "long", "touch_ctr", "long"), ("inbond", "string", "inbond", "string"), ("exam", "string", "exam", "string"), ("acry_equip_ids", "string", "acry_equip_ids", "string"), ("customs_id", "string", "customs_id", "string"), ("agent1", "long", "agent1", "long"), ("agent2", "long", "agent2", "long"), ("changed", "timestamp", "changed", "timestamp"), ("cargo_quantity", "float", "cargo_quantity", "float"), ("cargo_quantity_unit", "string", "cargo_quantity_unit", "string"), ("related_unit", "long", "related_unit", "long"), ("relationship_role", "string", "relationship_role", "string"), ("unit_combo", "long", "unit_combo", "long"), ("eq_gkey", "long", "eq_gkey", "long"), ("carriage_unit", "long", "carriage_unit", "long"), ("eqs_gkey", "long", "eqs_gkey", "long"), ("damage", "string", "damage", "string"), ("sparcs_damage_code", "string", "sparcs_damage_code", "string"), ("dmgs_gkey", "long", "dmgs_gkey", "long"), ("condition_gkey", "long", "condition_gkey", "long"), ("bad_nbr", "string", "bad_nbr", "string"), ("is_folded", "long", "is_folded", "long"), ("arrive_order_item_gkey", "long", "arrive_order_item_gkey", "long"), ("depart_order_item_gkey", "long", "depart_order_item_gkey", "long"), ("is_reserved", "long", "is_reserved", "long"), ("mnr_status_gkey", "long", "mnr_status_gkey", "long"), ("placarded", "string", "placarded", "string"), ("grade_gkey", "long", "grade_gkey", "long"), ("goods_ctr_wt_kg_vgm", "float", "goods_ctr_wt_kg_vgm", "float"), ("unit_gross_weight_source", "string", "unit_gross_weight_source", "string"), ("unit_vgm_entity", "string", "unit_vgm_entity", "string"), ("unit_vgm_verified_date", "timestamp", "unit_vgm_verified_date", "timestamp")], transformation_ctx = "iu_applymapping")

## inv_unit_fcy_visit mapping
fcy_applymapping = ApplyMapping.apply(frame = fcy_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("horizon", "long", "horizon", "long"), ("visit_state", "string", "visit_state", "string"), ("transit_state", "string", "transit_state", "string"), ("create_time", "timestamp", "create_time", "timestamp"), ("arrive_pos_loctype", "string", "arrive_pos_loctype", "string"), ("arrive_pos_locid", "string", "arrive_pos_locid", "string"), ("arrive_pos_loc_gkey", "long", "arrive_pos_loc_gkey", "long"), ("arrive_pos_slot", "string", "arrive_pos_slot", "string"), ("arrive_pos_orientation", "string", "arrive_pos_orientation", "string"), ("arrive_pos_name", "string", "arrive_pos_name", "string"), ("arrive_ops_pos_id", "string", "arrive_ops_pos_id", "string"), ("arrive_pos_bin", "long", "arrive_pos_bin", "long"), ("arrive_pos_tier", "long", "arrive_pos_tier", "long"), ("arrive_pos_anchor", "string", "arrive_pos_anchor", "string"), ("arrive_pos_orientation_degrees", "float", "arrive_pos_orientation_degrees", "float"), ("arrive_pos_slot_on_carriage", "string", "arrive_pos_slot_on_carriage", "string"), ("last_pos_loctype", "string", "last_pos_loctype", "string"), ("last_pos_locid", "string", "last_pos_locid", "string"), ("last_pos_loc_gkey", "long", "last_pos_loc_gkey", "long"), ("last_pos_slot", "string", "last_pos_slot", "string"), ("last_pos_orientation", "string", "last_pos_orientation", "string"), ("last_pos_name", "string", "last_pos_name", "string"), ("last_ops_pos_id", "string", "last_ops_pos_id", "string"), ("last_pos_bin", "long", "last_pos_bin", "long"), ("last_pos_tier", "long", "last_pos_tier", "long"), ("last_pos_anchor", "string", "last_pos_anchor", "string"), ("last_pos_orientation_degrees", "float", "last_pos_orientation_degrees", "float"), ("last_pos_slot_on_carriage", "string", "last_pos_slot_on_carriage", "string"), ("time_move", "timestamp", "time_move", "timestamp"), ("time_load", "timestamp", "time_load", "timestamp"), ("time_rnd", "timestamp", "time_rnd", "timestamp"), ("time_ecout", "timestamp", "time_ecout", "timestamp"), ("time_in", "timestamp", "time_in", "timestamp"), ("time_out", "timestamp", "time_out", "timestamp"), ("time_dlv_appmnt", "timestamp", "time_dlv_appmnt", "timestamp"), ("appt_nbr", "long", "appt_nbr", "long"), ("time_inv", "timestamp", "time_inv", "timestamp"), ("time_complete", "timestamp", "time_complete", "timestamp"), ("unit_gkey", "long", "unit_gkey", "long"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("sequence", "long", "sequence", "long"), ("intend_ob_cv", "long", "intend_ob_cv", "long"), ("restow_typ", "string", "restow_typ", "string"), ("hndlg_rsn", "string", "hndlg_rsn", "string"), ("yrd_stwge", "string", "yrd_stwge", "string"), ("sparcs_stopped", "long", "sparcs_stopped", "long"), ("sparcs_note", "string", "sparcs_note", "string"), ("has_changed", "long", "has_changed", "long"), ("vrfd_load", "long", "vrfd_load", "long"), ("vrfd_yard", "long", "vrfd_yard", "long"), ("ed_anomaly", "long", "ed_anomaly", "long"), ("actual_ib_cv", "long", "actual_ib_cv", "long"), ("actual_ob_cv", "long", "actual_ob_cv", "long"), ("last_free_day", "timestamp", "last_free_day", "timestamp"), ("paid_thru_day", "timestamp", "paid_thru_day", "timestamp"), ("guarantee_thru_day", "timestamp", "guarantee_thru_day", "timestamp"), ("guarantee_party_gkey", "long", "guarantee_party_gkey", "long"), ("power_last_free_day", "timestamp", "power_last_free_day", "timestamp"), ("power_paid_thru_day", "timestamp", "power_paid_thru_day", "timestamp"), ("power_guarantee_thru_day", "timestamp", "power_guarantee_thru_day", "timestamp"), ("power_guarantee_party_gkey", "long", "power_guarantee_party_gkey", "long"), ("line_last_free_day", "timestamp", "line_last_free_day", "timestamp"), ("line_paid_thru_day", "timestamp", "line_paid_thru_day", "timestamp"), ("line_guarantee_thru_day", "timestamp", "line_guarantee_thru_day", "timestamp"), ("line_guarantee_party_gkey", "long", "line_guarantee_party_gkey", "long"), ("visible_sparcs", "long", "visible_sparcs", "long"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("flex_date02", "timestamp", "flex_date02", "timestamp"), ("flex_date03", "timestamp", "flex_date03", "timestamp"), ("flex_date04", "timestamp", "flex_date04", "timestamp"), ("flex_date05", "timestamp", "flex_date05", "timestamp"), ("flex_date06", "timestamp", "flex_date06", "timestamp"), ("flex_date07", "timestamp", "flex_date07", "timestamp"), ("flex_date08", "timestamp", "flex_date08", "timestamp"), ("is_direct_ib_to_ob_move", "long", "is_direct_ib_to_ob_move", "long"), ("validate_marry_errors", "string", "validate_marry_errors", "string"), ("stow_factor", "string", "stow_factor", "string"), ("return_to_yard", "long", "return_to_yard", "long"), ("move_count", "long", "move_count", "long"), ("ras_priority", "long", "ras_priority", "long"), ("housekp_cur_slot", "string", "housekp_cur_slot", "string"), ("housekp_ftr_slot", "string", "housekp_ftr_slot", "string"), ("housekp_cur_score", "long", "housekp_cur_score", "long"), ("housekp_ftr_score", "long", "housekp_ftr_score", "long"), ("housekp_timestamp", "timestamp", "housekp_timestamp", "timestamp"), ("stacking_factor", "string", "stacking_factor", "string"), ("section_factor", "string", "section_factor", "string"), ("cas_unit_reference", "string", "cas_unit_reference", "string"), ("cas_tran_reference", "string", "cas_tran_reference", "string"), ("preferred_transfer_location", "string", "preferred_transfer_location", "string"), ("door_direction", "string", "door_direction", "string"), ("optimal_rail_tz_slot", "string", "optimal_rail_tz_slot", "string"), ("carrier_incompatible_reason", "string", "carrier_incompatible_reason", "string"), ("rail_cone_status", "string", "rail_cone_status", "string"), ("segregation_factor", "string", "segregation_factor", "string")], transformation_ctx = "fcy_applymapping")

## inv_unit_yrd_visit mapping
yrd_applymapping = ApplyMapping.apply(frame = yrd_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("ufv_gkey", "long", "ufv_gkey", "long"), ("yrd_gkey", "long", "yrd_gkey", "long"), ("pkey", "long", "pkey", "long")], transformation_ctx = "yrd_applymapping")

## inv_wi mapping
wi_applymapping = ApplyMapping.apply(frame = wi_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("pkey", "long", "pkey", "long"), ("facility_gkey", "long", "facility_gkey", "long"), ("uyv_gkey", "long", "uyv_gkey", "long"), ("tbdu_gkey", "long", "tbdu_gkey", "long"), ("wq_pkey", "long", "wq_pkey", "long"), ("work_queue_gkey", "long", "work_queue_gkey", "long"), ("ar_pkey", "long", "ar_pkey", "long"), ("ar_gkey", "long", "ar_gkey", "long"), ("shift_target_pkey", "long", "shift_target_pkey", "long"), ("shift_target_gkey", "long", "shift_target_gkey", "long"), ("che_id", "long", "che_id", "long"), ("che_gkey", "long", "che_gkey", "long"), ("itv_id", "long", "itv_id", "long"), ("itv_gkey", "long", "itv_gkey", "long"), ("che_work_assignment_pkey", "long", "che_work_assignment_pkey", "long"), ("che_work_assignment_gkey", "long", "che_work_assignment_gkey", "long"), ("itv_work_assignment_pkey", "long", "itv_work_assignment_pkey", "long"), ("itv_work_assignment_gkey", "long", "itv_work_assignment_gkey", "long"), ("intended_che_id", "long", "intended_che_id", "long"), ("intended_che_gkey", "long", "intended_che_gkey", "long"), ("nominated_che_gkey", "long", "nominated_che_gkey", "long"), ("move_number", "float", "move_number", "float"), ("sequence", "long", "sequence", "long"), ("crane_lane", "long", "crane_lane", "long"), ("set_aside", "long", "set_aside", "long"), ("door_direction", "string", "door_direction", "string"), ("decking_restriction", "string", "decking_restriction", "string"), ("twin_with", "string", "twin_with", "string"), ("twin_int_fetch", "long", "twin_int_fetch", "long"), ("twin_int_carry", "long", "twin_int_carry", "long"), ("twin_int_put", "long", "twin_int_put", "long"), ("ec_state_fetch", "string", "ec_state_fetch", "string"), ("ec_state_dispatch_request", "long", "ec_state_dispatch_request", "long"), ("ec_state_itv_dispatch_request", "long", "ec_state_itv_dispatch_request", "long"), ("ec_state_dispatch_b", "long", "ec_state_dispatch_b", "long"), ("ec_state_itv_dispatch_b", "long", "ec_state_itv_dispatch_b", "long"), ("suspend_state", "string", "suspend_state", "string"), ("locked", "long", "locked", "long"), ("confirmed", "long", "confirmed", "long"), ("definite", "long", "definite", "long"), ("executable", "long", "executable", "long"), ("asc_aborted", "long", "asc_aborted", "long"), ("increment_crane_lane", "long", "increment_crane_lane", "long"), ("being_rehandled", "long", "being_rehandled", "long"), ("being_deleted", "long", "being_deleted", "long"), ("should_be_sent", "long", "should_be_sent", "long"), ("sequenced_pwp", "long", "sequenced_pwp", "long"), ("host_request_priority", "long", "host_request_priority", "long"), ("skip_host_update", "long", "skip_host_update", "long"), ("move_stage", "string", "move_stage", "string"), ("conf_move_stage", "string", "conf_move_stage", "string"), ("ec_hold", "string", "ec_hold", "string"), ("eq_moves_key", "string", "eq_moves_key", "string"), ("eq_uses_key", "string", "eq_uses_key", "string"), ("move_kind", "string", "move_kind", "string"), ("carrier_loctype", "string", "carrier_loctype", "string"), ("carrier_locid", "string", "carrier_locid", "string"), ("mvhs_fetch_che_id", "long", "mvhs_fetch_che_id", "long"), ("mvhs_fetch_che_name", "string", "mvhs_fetch_che_name", "string"), ("mvhs_carry_che_id", "long", "mvhs_carry_che_id", "long"), ("mvhs_carry_che_name", "string", "mvhs_carry_che_name", "string"), ("mvhs_put_che_id", "long", "mvhs_put_che_id", "long"), ("mvhs_put_che_name", "string", "mvhs_put_che_name", "string"), ("mvhs_dist_start", "long", "mvhs_dist_start", "long"), ("mvhs_dist_carry", "long", "mvhs_dist_carry", "long"), ("mvhs_t_carry_complete", "timestamp", "mvhs_t_carry_complete", "timestamp"), ("mvhs_t_dispatch", "timestamp", "mvhs_t_dispatch", "timestamp"), ("mvhs_t_fetch", "timestamp", "mvhs_t_fetch", "timestamp"), ("mvhs_t_put", "timestamp", "mvhs_t_put", "timestamp"), ("mvhs_t_carry_fetch_ready", "timestamp", "mvhs_t_carry_fetch_ready", "timestamp"), ("mvhs_t_carry_put_ready", "timestamp", "mvhs_t_carry_put_ready", "timestamp"), ("mvhs_t_carry_dispatch", "timestamp", "mvhs_t_carry_dispatch", "timestamp"), ("mvhs_t_discharge", "timestamp", "mvhs_t_discharge", "timestamp"), ("mvhs_rehandle_count", "long", "mvhs_rehandle_count", "long"), ("mvhs_pow_pkey", "long", "mvhs_pow_pkey", "long"), ("mvhs_pool_pkey", "long", "mvhs_pool_pkey", "long"), ("mvhs_twin_fetch", "long", "mvhs_twin_fetch", "long"), ("mvhs_twin_carry", "long", "mvhs_twin_carry", "long"), ("mvhs_twin_put", "long", "mvhs_twin_put", "long"), ("mvhs_tandem_fetch", "long", "mvhs_tandem_fetch", "long"), ("mvhs_tandem_put", "long", "mvhs_tandem_put", "long"), ("mvhs_tz_arrival_time", "timestamp", "mvhs_tz_arrival_time", "timestamp"), ("mvhs_fetch_che_distance", "long", "mvhs_fetch_che_distance", "long"), ("mvhs_carry_che_distance", "long", "mvhs_carry_che_distance", "long"), ("mvhs_put_che_distance", "long", "mvhs_put_che_distance", "long"), ("mvhs_t_fetch_dispatch", "timestamp", "mvhs_t_fetch_dispatch", "timestamp"), ("mvhs_t_put_dispatch", "timestamp", "mvhs_t_put_dispatch", "timestamp"), ("est_move_time", "timestamp", "est_move_time", "timestamp"), ("t_orig_est_start", "timestamp", "t_orig_est_start", "timestamp"), ("restow_account", "string", "restow_account", "string"), ("service_order", "string", "service_order", "string"), ("restow_reason", "string", "restow_reason", "string"), ("road_truck", "long", "road_truck", "long"), ("che_dispatch_order", "long", "che_dispatch_order", "long"), ("itv_dispatch_order", "long", "itv_dispatch_order", "long"), ("pos_loctype", "string", "pos_loctype", "string"), ("pos_locid", "string", "pos_locid", "string"), ("pos_loc_gkey", "long", "pos_loc_gkey", "long"), ("pos_slot", "string", "pos_slot", "string"), ("pos_orientation", "string", "pos_orientation", "string"), ("pos_name", "string", "pos_name", "string"), ("pos_bin", "long", "pos_bin", "long"), ("pos_tier", "long", "pos_tier", "long"), ("pos_anchor", "string", "pos_anchor", "string"), ("pos_orientation_degrees", "float", "pos_orientation_degrees", "float"), ("pos_slot_on_carriage", "string", "pos_slot_on_carriage", "string"), ("truck_visit_ref", "long", "truck_visit_ref", "long"), ("pool_level", "long", "pool_level", "long"), ("msg_ref", "long", "msg_ref", "long"), ("planner_create", "long", "planner_create", "long"), ("planner_modify", "long", "planner_modify", "long"), ("t_create", "timestamp", "t_create", "timestamp"), ("t_modified", "timestamp", "t_modified", "timestamp"), ("create_tool", "string", "create_tool", "string"), ("imminent_move", "long", "imminent_move", "long"), ("almost_imminent", "long", "almost_imminent", "long"), ("almost_current", "long", "almost_current", "long"), ("being_carried", "long", "being_carried", "long"), ("carry_by_straddle", "long", "carry_by_straddle", "long"), ("dispatch_hung", "long", "dispatch_hung", "long"), ("ignore_gate_hold", "long", "ignore_gate_hold", "long"), ("tv_gkey", "long", "tv_gkey", "long"), ("tran_gkey", "long", "tran_gkey", "long"), ("origin_from_qual", "string", "origin_from_qual", "string"), ("assigned_chassid", "string", "assigned_chassid", "string"), ("swappable_delivery", "long", "swappable_delivery", "long"), ("n4_specified_queue", "long", "n4_specified_queue", "long"), ("gate_transaction_sequence", "long", "gate_transaction_sequence", "long"), ("crane_lane_tier", "long", "crane_lane_tier", "long"), ("yard_move_filter", "string", "yard_move_filter", "string"), ("actual_sequence", "long", "actual_sequence", "long"), ("actual_vessel_twin_pkey", "long", "actual_vessel_twin_pkey", "long"), ("actual_vessel_twin_gkey", "long", "actual_vessel_twin_gkey", "long"), ("strict_load", "long", "strict_load", "long"), ("preferred_transfer_location", "string", "preferred_transfer_location", "string"), ("is_tandem_with_next", "long", "is_tandem_with_next", "long"), ("is_tandem_with_previous", "long", "is_tandem_with_previous", "long"), ("xps_object_version", "long", "xps_object_version", "long"), ("emt_time_horizon_min", "long", "emt_time_horizon_min", "long"), ("pair_with", "string", "pair_with", "string")], transformation_ctx = "wi_applymapping")

## inv_wi_tracking mapping
wit_applymapping = ApplyMapping.apply(frame = wit_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("wi_tracking_id", "long", "wi_tracking_id", "long"), ("date_added", "timestamp", "date_added", "timestamp"), ("gkey", "long", "gkey", "long"), ("pkey", "long", "pkey", "long"), ("facility_gkey", "long", "facility_gkey", "long"), ("uyv_gkey", "long", "uyv_gkey", "long"), ("tbdu_gkey", "long", "tbdu_gkey", "long"), ("wq_pkey", "long", "wq_pkey", "long"), ("work_queue_gkey", "long", "work_queue_gkey", "long"), ("ar_pkey", "long", "ar_pkey", "long"), ("ar_gkey", "long", "ar_gkey", "long"), ("shift_target_pkey", "long", "shift_target_pkey", "long"), ("shift_target_gkey", "long", "shift_target_gkey", "long"), ("che_id", "long", "che_id", "long"), ("che_gkey", "long", "che_gkey", "long"), ("itv_id", "long", "itv_id", "long"), ("itv_gkey", "long", "itv_gkey", "long"), ("che_work_assignment_pkey", "long", "che_work_assignment_pkey", "long"), ("che_work_assignment_gkey", "long", "che_work_assignment_gkey", "long"), ("itv_work_assignment_pkey", "long", "itv_work_assignment_pkey", "long"), ("itv_work_assignment_gkey", "long", "itv_work_assignment_gkey", "long"), ("intended_che_id", "long", "intended_che_id", "long"), ("intended_che_gkey", "long", "intended_che_gkey", "long"), ("nominated_che_gkey", "long", "nominated_che_gkey", "long"), ("move_number", "float", "move_number", "float"), ("sequence", "long", "sequence", "long"), ("crane_lane", "long", "crane_lane", "long"), ("set_aside", "long", "set_aside", "long"), ("door_direction", "string", "door_direction", "string"), ("decking_restriction", "string", "decking_restriction", "string"), ("twin_with", "string", "twin_with", "string"), ("twin_int_fetch", "long", "twin_int_fetch", "long"), ("twin_int_carry", "long", "twin_int_carry", "long"), ("twin_int_put", "long", "twin_int_put", "long"), ("ec_state_fetch", "string", "ec_state_fetch", "string"), ("ec_state_dispatch_request", "long", "ec_state_dispatch_request", "long"), ("ec_state_itv_dispatch_request", "long", "ec_state_itv_dispatch_request", "long"), ("ec_state_dispatch_b", "long", "ec_state_dispatch_b", "long"), ("ec_state_itv_dispatch_b", "long", "ec_state_itv_dispatch_b", "long"), ("suspend_state", "string", "suspend_state", "string"), ("locked", "long", "locked", "long"), ("confirmed", "long", "confirmed", "long"), ("definite", "long", "definite", "long"), ("executable", "long", "executable", "long"), ("asc_aborted", "long", "asc_aborted", "long"), ("increment_crane_lane", "long", "increment_crane_lane", "long"), ("being_rehandled", "long", "being_rehandled", "long"), ("being_deleted", "long", "being_deleted", "long"), ("should_be_sent", "long", "should_be_sent", "long"), ("sequenced_pwp", "long", "sequenced_pwp", "long"), ("host_request_priority", "long", "host_request_priority", "long"), ("skip_host_update", "long", "skip_host_update", "long"), ("move_stage", "string", "move_stage", "string"), ("conf_move_stage", "string", "conf_move_stage", "string"), ("ec_hold", "string", "ec_hold", "string"), ("eq_moves_key", "string", "eq_moves_key", "string"), ("eq_uses_key", "string", "eq_uses_key", "string"), ("move_kind", "string", "move_kind", "string"), ("carrier_loctype", "string", "carrier_loctype", "string"), ("carrier_locid", "string", "carrier_locid", "string"), ("mvhs_fetch_che_id", "long", "mvhs_fetch_che_id", "long"), ("mvhs_fetch_che_name", "string", "mvhs_fetch_che_name", "string"), ("mvhs_carry_che_id", "long", "mvhs_carry_che_id", "long"), ("mvhs_carry_che_name", "string", "mvhs_carry_che_name", "string"), ("mvhs_put_che_id", "long", "mvhs_put_che_id", "long"), ("mvhs_put_che_name", "string", "mvhs_put_che_name", "string"), ("mvhs_dist_start", "long", "mvhs_dist_start", "long"), ("mvhs_dist_carry", "long", "mvhs_dist_carry", "long"), ("mvhs_t_carry_complete", "timestamp", "mvhs_t_carry_complete", "timestamp"), ("mvhs_t_dispatch", "timestamp", "mvhs_t_dispatch", "timestamp"), ("mvhs_t_fetch", "timestamp", "mvhs_t_fetch", "timestamp"), ("mvhs_t_put", "timestamp", "mvhs_t_put", "timestamp"), ("mvhs_t_carry_fetch_ready", "timestamp", "mvhs_t_carry_fetch_ready", "timestamp"), ("mvhs_t_carry_put_ready", "timestamp", "mvhs_t_carry_put_ready", "timestamp"), ("mvhs_t_carry_dispatch", "timestamp", "mvhs_t_carry_dispatch", "timestamp"), ("mvhs_t_discharge", "timestamp", "mvhs_t_discharge", "timestamp"), ("mvhs_rehandle_count", "long", "mvhs_rehandle_count", "long"), ("mvhs_pow_pkey", "long", "mvhs_pow_pkey", "long"), ("mvhs_pool_pkey", "long", "mvhs_pool_pkey", "long"), ("mvhs_twin_fetch", "long", "mvhs_twin_fetch", "long"), ("mvhs_twin_carry", "long", "mvhs_twin_carry", "long"), ("mvhs_twin_put", "long", "mvhs_twin_put", "long"), ("mvhs_tandem_fetch", "long", "mvhs_tandem_fetch", "long"), ("mvhs_tandem_put", "long", "mvhs_tandem_put", "long"), ("mvhs_tz_arrival_time", "timestamp", "mvhs_tz_arrival_time", "timestamp"), ("mvhs_fetch_che_distance", "long", "mvhs_fetch_che_distance", "long"), ("mvhs_carry_che_distance", "long", "mvhs_carry_che_distance", "long"), ("mvhs_put_che_distance", "long", "mvhs_put_che_distance", "long"), ("mvhs_t_fetch_dispatch", "timestamp", "mvhs_t_fetch_dispatch", "timestamp"), ("mvhs_t_put_dispatch", "timestamp", "mvhs_t_put_dispatch", "timestamp"), ("est_move_time", "timestamp", "est_move_time", "timestamp"), ("t_orig_est_start", "timestamp", "t_orig_est_start", "timestamp"), ("restow_account", "string", "restow_account", "string"), ("service_order", "string", "service_order", "string"), ("restow_reason", "string", "restow_reason", "string"), ("road_truck", "long", "road_truck", "long"), ("che_dispatch_order", "long", "che_dispatch_order", "long"), ("itv_dispatch_order", "long", "itv_dispatch_order", "long"), ("pos_loctype", "string", "pos_loctype", "string"), ("pos_locid", "string", "pos_locid", "string"), ("pos_loc_gkey", "long", "pos_loc_gkey", "long"), ("pos_slot", "string", "pos_slot", "string"), ("pos_orientation", "string", "pos_orientation", "string"), ("pos_name", "string", "pos_name", "string"), ("pos_bin", "long", "pos_bin", "long"), ("pos_tier", "long", "pos_tier", "long"), ("pos_anchor", "string", "pos_anchor", "string"), ("pos_orientation_degrees", "float", "pos_orientation_degrees", "float"), ("pos_slot_on_carriage", "string", "pos_slot_on_carriage", "string"), ("truck_visit_ref", "long", "truck_visit_ref", "long"), ("pool_level", "long", "pool_level", "long"), ("msg_ref", "long", "msg_ref", "long"), ("planner_create", "long", "planner_create", "long"), ("planner_modify", "long", "planner_modify", "long"), ("t_create", "timestamp", "t_create", "timestamp"), ("t_modified", "timestamp", "t_modified", "timestamp"), ("create_tool", "string", "create_tool", "string"), ("imminent_move", "long", "imminent_move", "long"), ("almost_imminent", "long", "almost_imminent", "long"), ("almost_current", "long", "almost_current", "long"), ("being_carried", "long", "being_carried", "long"), ("carry_by_straddle", "long", "carry_by_straddle", "long"), ("dispatch_hung", "long", "dispatch_hung", "long"), ("ignore_gate_hold", "long", "ignore_gate_hold", "long"), ("tv_gkey", "long", "tv_gkey", "long"), ("tran_gkey", "long", "tran_gkey", "long"), ("origin_from_qual", "string", "origin_from_qual", "string"), ("assigned_chassid", "string", "assigned_chassid", "string"), ("swappable_delivery", "long", "swappable_delivery", "long"), ("n4_specified_queue", "long", "n4_specified_queue", "long"), ("gate_transaction_sequence", "long", "gate_transaction_sequence", "long"), ("crane_lane_tier", "long", "crane_lane_tier", "long"), ("yard_move_filter", "string", "yard_move_filter", "string"), ("actual_sequence", "long", "actual_sequence", "long"), ("actual_vessel_twin_pkey", "long", "actual_vessel_twin_pkey", "long"), ("actual_vessel_twin_gkey", "long", "actual_vessel_twin_gkey", "long"), ("strict_load", "long", "strict_load", "long"), ("preferred_transfer_location", "string", "preferred_transfer_location", "string"), ("is_tandem_with_next", "long", "is_tandem_with_next", "long"), ("is_tandem_with_previous", "long", "is_tandem_with_previous", "long"), ("xps_object_version", "long", "xps_object_version", "long"), ("emt_time_horizon_min", "long", "emt_time_horizon_min", "long"), ("pair_with", "string", "pair_with", "string")], transformation_ctx = "wit_applymapping")

## inv_wq mapping
wq_applymapping = ApplyMapping.apply(frame = wq_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("pkey", "long", "pkey", "long"), ("pow_pkey", "long", "pow_pkey", "long"), ("pow_gkey", "long", "pow_gkey", "long"), ("yrd_gkey", "long", "yrd_gkey", "long"), ("cycle_companion_pkey", "long", "cycle_companion_pkey", "long"), ("cycle_companion_gkey", "long", "cycle_companion_gkey", "long"), ("first_shift_pkey", "long", "first_shift_pkey", "long"), ("first_shift_gkey", "long", "first_shift_gkey", "long"), ("qorder", "float", "qorder", "float"), ("qtype", "string", "qtype", "string"), ("qfission", "long", "qfission", "long"), ("wis_sent_tls", "long", "wis_sent_tls", "long"), ("save_completed_moves", "long", "save_completed_moves", "long"), ("is_blue", "long", "is_blue", "long"), ("permanent", "long", "permanent", "long"), ("service_order_queue", "long", "service_order_queue", "long"), ("manual_sequence_mode", "long", "manual_sequence_mode", "long"), ("yard_loadback_queue", "long", "yard_loadback_queue", "long"), ("allow_40", "long", "allow_40", "long"), ("allow_20", "long", "allow_20", "long"), ("pos_loctype", "string", "pos_loctype", "string"), ("pos_locid", "string", "pos_locid", "string"), ("qcode", "string", "qcode", "string"), ("qdeck", "string", "qdeck", "string"), ("qrow", "string", "qrow", "string"), ("name", "string", "name", "string"), ("t_delete_ready", "timestamp", "t_delete_ready", "timestamp"), ("doublecycle_from_sequence", "long", "doublecycle_from_sequence", "long"), ("doublecycle_to_sequence", "long", "doublecycle_to_sequence", "long"), ("note", "string", "note", "string"), ("computed_20_projection", "long", "computed_20_projection", "long"), ("computed_40_projection", "long", "computed_40_projection", "long"), ("vessel_lcg", "long", "vessel_lcg", "long"), ("use_wq_prod", "long", "use_wq_prod", "long"), ("productivity_std", "long", "productivity_std", "long"), ("productivity_dual", "long", "productivity_dual", "long"), ("productivity_twin", "long", "productivity_twin", "long"), ("max_teu_cap", "long", "max_teu_cap", "long"), ("pct_cap_start_prty", "long", "pct_cap_start_prty", "long"), ("pnlty_exceed_cap", "long", "pnlty_exceed_cap", "long"), ("tls_sort_key", "long", "tls_sort_key", "long"), ("productivity_tandem", "long", "productivity_tandem", "long"), ("productivity_quad", "long", "productivity_quad", "long"), ("berth_call_slave_id", "long", "berth_call_slave_id", "long"), ("t_last_activation_chg", "timestamp", "t_last_activation_chg", "timestamp")], transformation_ctx = "wq_applymapping")

## mns_che_move_statistics mapping
mvstat_applymapping = ApplyMapping.apply(frame = mvstat_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("che_trip_gkey", "long", "che_trip_gkey", "long"), ("che_session_period_gkey", "long", "che_session_period_gkey", "long"), ("move_event_gkey", "long", "move_event_gkey", "long"), ("move_gkey", "long", "move_gkey", "long"), ("carrier_gkey", "long", "carrier_gkey", "long"), ("declrd_ib_cv_gkey", "long", "declrd_ib_cv_gkey", "long"), ("declrd_ob_cv_gkey", "long", "declrd_ob_cv_gkey", "long"), ("che_qc", "long", "che_qc", "long"), ("unit_id", "string", "unit_id", "string"), ("category", "string", "category", "string"), ("freight_kind", "string", "freight_kind", "string"), ("is_live_reefer", "long", "is_live_reefer", "long"), ("basic_length", "string", "basic_length", "string"), ("eq_type_iso", "string", "eq_type_iso", "string"), ("nominal_length", "string", "nominal_length", "string"), ("work_queue", "string", "work_queue", "string"), ("pow", "string", "pow", "string"), ("pool", "string", "pool", "string"), ("fm_pos_loctype", "string", "fm_pos_loctype", "string"), ("fm_pos_locid", "string", "fm_pos_locid", "string"), ("fm_pos_loc_gkey", "long", "fm_pos_loc_gkey", "long"), ("fm_pos_slot", "string", "fm_pos_slot", "string"), ("fm_pos_orientation", "string", "fm_pos_orientation", "string"), ("fm_pos_name", "string", "fm_pos_name", "string"), ("fm_pos_bin", "long", "fm_pos_bin", "long"), ("fm_pos_tier", "long", "fm_pos_tier", "long"), ("fm_pos_orientation_degrees", "float", "fm_pos_orientation_degrees", "float"), ("fm_pos_slot_on_carriage", "string", "fm_pos_slot_on_carriage", "string"), ("to_pos_loctype", "string", "to_pos_loctype", "string"), ("to_pos_locid", "string", "to_pos_locid", "string"), ("to_pos_loc_gkey", "long", "to_pos_loc_gkey", "long"), ("to_pos_slot", "string", "to_pos_slot", "string"), ("to_pos_orientation", "string", "to_pos_orientation", "string"), ("to_pos_name", "string", "to_pos_name", "string"), ("to_pos_bin", "long", "to_pos_bin", "long"), ("to_pos_tier", "long", "to_pos_tier", "long"), ("to_pos_orientation_degrees", "float", "to_pos_orientation_degrees", "float"), ("to_pos_slot_on_carriage", "string", "to_pos_slot_on_carriage", "string"), ("t_pick_dispatch", "timestamp", "t_pick_dispatch", "timestamp"), ("t_pick_arrive", "timestamp", "t_pick_arrive", "timestamp"), ("t_lift_dispatch", "timestamp", "t_lift_dispatch", "timestamp"), ("t_cntr_on", "timestamp", "t_cntr_on", "timestamp"), ("t_drop_dispatch", "timestamp", "t_drop_dispatch", "timestamp"), ("t_drop_arrive", "timestamp", "t_drop_arrive", "timestamp"), ("t_cntr_off", "timestamp", "t_cntr_off", "timestamp"), ("pick_idle_time", "long", "pick_idle_time", "long"), ("pick_wait_time", "long", "pick_wait_time", "long"), ("drop_idle_time", "long", "drop_idle_time", "long"), ("drop_wait_time", "long", "drop_wait_time", "long"), ("rehandle_count", "long", "rehandle_count", "long"), ("twin_move", "long", "twin_move", "long"), ("lane_change", "long", "lane_change", "long"), ("pick_distance", "long", "pick_distance", "long"), ("drop_distance", "long", "drop_distance", "long"), ("move_direction", "string", "move_direction", "string"), ("move_category", "string", "move_category", "string"), ("from_block", "long", "from_block", "long"), ("from_block_type", "string", "from_block_type", "string"), ("to_block", "long", "to_block", "long"), ("to_block_type", "string", "to_block_type", "string"), ("wait_time_qc", "long", "wait_time_qc", "long"), ("wait_time_block", "long", "wait_time_block", "long"), ("move_purpose", "string", "move_purpose", "string"), ("is_tandem", "long", "is_tandem", "long"), ("is_quad", "long", "is_quad", "long"), ("spreader_id", "string", "spreader_id", "string"), ("cycle_time", "long", "cycle_time", "long"), ("t_buffer_enter", "timestamp", "t_buffer_enter", "timestamp"), ("t_buffer_exit", "timestamp", "t_buffer_exit", "timestamp")], transformation_ctx = "mvstat_applymapping")

## mns_che_operator_statistics mapping
operstat_applymapping = ApplyMapping.apply(frame = operstat_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("che_operator", "long", "che_operator", "long"), ("che", "long", "che", "long"), ("che_op_action", "string", "che_op_action", "string"), ("t_action_time", "string", "t_action_time", "string")], transformation_ctx = "operstat_applymapping")

## mns_che_session mapping
chesess_applymapping = ApplyMapping.apply(frame = chesess_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("che_operator", "long", "che_operator", "long"), ("che", "long", "che", "long"), ("t_login", "timestamp", "t_login", "timestamp"), ("t_logout", "timestamp", "t_logout", "timestamp")], transformation_ctx = "chesess_applymapping")

## mns_che_session_period mapping
sessper_applymapping = ApplyMapping.apply(frame = sessper_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("che_session_gkey", "long", "che_session_gkey", "long"), ("t_start", "timestamp", "t_start", "timestamp"), ("t_end", "timestamp", "t_end", "timestamp"), ("duration", "long", "duration", "long"), ("csp_period_type", "string", "csp_period_type", "string")], transformation_ctx = "sessper_applymapping")

## mns_che_status mapping
status_applymapping = ApplyMapping.apply(frame = status_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("che", "long", "che", "long"), ("che_name", "string", "che_name", "string"), ("start_time", "timestamp", "start_time", "timestamp"), ("end_time", "timestamp", "end_time", "timestamp"), ("duration", "long", "duration", "long"), ("status", "string", "status", "string")], transformation_ctx = "status_applymapping")

## mns_che_trip_statistics mapping
tripstat_applymapping = ApplyMapping.apply(frame = tripstat_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("yard_gkey", "long", "yard_gkey", "long"), ("id", "string", "id", "string"), ("che", "long", "che", "long"), ("che_type", "long", "che_type", "long"), ("move_kind", "string", "move_kind", "string"), ("che_phase", "string", "che_phase", "string"), ("che_operator", "long", "che_operator", "long"), ("dist_laden", "long", "dist_laden", "long"), ("dist_unladen", "long", "dist_unladen", "long"), ("t_first_cntr_on", "timestamp", "t_first_cntr_on", "timestamp"), ("t_last_cntr_off", "timestamp", "t_last_cntr_off", "timestamp"), ("t_first_dispatch", "timestamp", "t_first_dispatch", "timestamp"), ("t_unladen_travel", "long", "t_unladen_travel", "long"), ("t_laden_travel", "long", "t_laden_travel", "long")], transformation_ctx = "tripstat_applymapping")

## xps_che mapping
xpsche_applymapping = ApplyMapping.apply(frame = xpsche_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("pkey", "long", "pkey", "long"), ("pool", "long", "pool", "long"), ("pool_gkey", "long", "pool_gkey", "long"), ("pow", "long", "pow", "long"), ("point_of_work_gkey", "long", "point_of_work_gkey", "long"), ("vessel_code", "string", "vessel_code", "string"), ("status", "long", "status", "long"), ("status_enum", "string", "status_enum", "string"), ("talk_status", "long", "talk_status", "long"), ("talk_status_enum", "string", "talk_status_enum", "string"), ("message_status", "long", "message_status", "long"), ("message_status_enum", "string", "message_status_enum", "string"), ("mts_status", "long", "mts_status", "long"), ("itt_dispatch_mode", "long", "itt_dispatch_mode", "long"), ("last_time", "timestamp", "last_time", "timestamp"), ("time_complete_last_job", "timestamp", "time_complete_last_job", "timestamp"), ("time_log_in_out", "timestamp", "time_log_in_out", "timestamp"), ("time_available_unavailable", "timestamp", "time_available_unavailable", "timestamp"), ("time_out_of_service", "timestamp", "time_out_of_service", "timestamp"), ("time_lift", "timestamp", "time_lift", "timestamp"), ("time_finish", "timestamp", "time_finish", "timestamp"), ("time_dispatch", "timestamp", "time_dispatch", "timestamp"), ("job_distance", "long", "job_distance", "long"), ("to_job_distance", "long", "to_job_distance", "long"), ("laden_travel", "long", "laden_travel", "long"), ("empty_travel", "long", "empty_travel", "long"), ("terminal", "string", "terminal", "string"), ("planned_terminal", "string", "planned_terminal", "string"), ("mts_platform_size", "long", "mts_platform_size", "long"), ("last_position", "string", "last_position", "string"), ("last_put", "string", "last_put", "string"), ("plan_position", "string", "plan_position", "string"), ("range_sel_block", "string", "range_sel_block", "string"), ("range_first_row", "long", "range_first_row", "long"), ("range_last_row", "long", "range_last_row", "long"), ("range_first_column", "long", "range_first_column", "long"), ("range_last_column", "long", "range_last_column", "long"), ("login_name", "string", "login_name", "string"), ("allowed_boxes", "long", "allowed_boxes", "long"), ("current_position_section_index", "long", "current_position_section_index", "long"), ("last_position_section_index", "long", "last_position_section_index", "long"), ("nom_chassis_section_idx", "long", "nom_chassis_section_idx", "long"), ("assigned_che", "long", "assigned_che", "long"), ("assigned_che_gkey", "long", "assigned_che_gkey", "long"), ("qualifiers", "string", "qualifiers", "string"), ("alternative_radio_id", "long", "alternative_radio_id", "long"), ("default_ec_communicator", "long", "default_ec_communicator", "long"), ("current_ec_communicator", "long", "current_ec_communicator", "long"), ("maximum_weight", "long", "maximum_weight", "long"), ("full_name", "string", "full_name", "string"), ("last_wi_reference", "long", "last_wi_reference", "long"), ("short_name", "string", "short_name", "string"), ("default_program", "string", "default_program", "string"), ("kind", "long", "kind", "long"), ("kind_enum", "string", "kind_enum", "string"), ("operating_mode", "long", "operating_mode", "long"), ("operating_mode_enum", "string", "operating_mode_enum", "string"), ("has_mdt", "long", "has_mdt", "long"), ("maximum_height", "long", "maximum_height", "long"), ("icon", "long", "icon", "long"), ("id", "long", "id", "long"), ("screen_type", "long", "screen_type", "long"), ("screen_horizontal", "long", "screen_horizontal", "long"), ("screen_vertical", "long", "screen_vertical", "long"), ("in_program", "long", "in_program", "long"), ("num_function_keys", "long", "num_function_keys", "long"), ("job_step_state", "long", "job_step_state", "long"), ("job_step_state_enum", "string", "job_step_state_enum", "string"), ("job_step_complete_time", "timestamp", "job_step_complete_time", "timestamp"), ("last_jobstep_transition", "timestamp", "last_jobstep_transition", "timestamp"), ("currently_toggled_wi_ref", "long", "currently_toggled_wi_ref", "long"), ("accept_job_done_press_time", "timestamp", "accept_job_done_press_time", "timestamp"), ("extended_address", "string", "extended_address", "string"), ("maximum_teu", "long", "maximum_teu", "long"), ("is_in_job_step_mode", "long", "is_in_job_step_mode", "long"), ("clerk_pow_reference", "long", "clerk_pow_reference", "long"), ("clerk_vessel_code", "string", "clerk_vessel_code", "string"), ("clerk_container_key", "long", "clerk_container_key", "long"), ("clerk_last_landed_che", "long", "clerk_last_landed_che", "long"), ("clerk_last_landed_che_gkey", "long", "clerk_last_landed_che_gkey", "long"), ("clerk_teu_landed", "long", "clerk_teu_landed", "long"), ("trailer", "long", "trailer", "long"), ("assist_state", "long", "assist_state", "long"), ("assist_state_enum", "string", "assist_state_enum", "string"), ("scale_on", "long", "scale_on", "long"), ("in_comms_fail", "long", "in_comms_fail", "long"), ("chassis_fetch_req", "long", "chassis_fetch_req", "long"), ("no_login_required", "long", "no_login_required", "long"), ("automation_active", "long", "automation_active", "long"), ("dispatch_requested", "long", "dispatch_requested", "long"), ("manual_dispatch", "long", "manual_dispatch", "long"), ("sends_pds_weight", "long", "sends_pds_weight", "long"), ("suspend_auto_dispatch", "long", "suspend_auto_dispatch", "long"), ("twin_carry_capable", "long", "twin_carry_capable", "long"), ("manual_dispatch_pending", "long", "manual_dispatch_pending", "long"), ("has_overheight_gear", "long", "has_overheight_gear", "long"), ("first_lift_took_place", "long", "first_lift_took_place", "long"), ("twin_lift_capable", "long", "twin_lift_capable", "long"), ("mts_damaged", "long", "mts_damaged", "long"), ("uses_pds", "long", "uses_pds", "long"), ("configurable_trailer", "long", "configurable_trailer", "long"), ("nominal_length20_capable", "long", "nominal_length20_capable", "long"), ("nominal_length40_capable", "long", "nominal_length40_capable", "long"), ("nominal_length45_capable", "long", "nominal_length45_capable", "long"), ("nominal_length24_capable", "long", "nominal_length24_capable", "long"), ("nominal_length48_capable", "long", "nominal_length48_capable", "long"), ("nominal_length53_capable", "long", "nominal_length53_capable", "long"), ("nominal_length30_capable", "long", "nominal_length30_capable", "long"), ("nominal_length60_capable", "long", "nominal_length60_capable", "long"), ("autoche_technical_status", "string", "autoche_technical_status", "string"), ("autoche_operational_status", "string", "autoche_operational_status", "string"), ("autoche_work_status", "string", "autoche_work_status", "string"), ("twin_diff_wgt_allowance", "long", "twin_diff_wgt_allowance", "long"), ("scale_weight_unit", "long", "scale_weight_unit", "long"), ("rel_queue_pos_for", "float", "rel_queue_pos_for", "float"), ("waiting_for_truck_insert", "long", "waiting_for_truck_insert", "long"), ("attached_chassis_id", "string", "attached_chassis_id", "string"), ("tandem_lift_capable", "long", "tandem_lift_capable", "long"), ("max_tandem_weight", "long", "max_tandem_weight", "long"), ("proximity_radius", "long", "proximity_radius", "long"), ("quad_lift_capable", "long", "quad_lift_capable", "long"), ("max_quad_weight", "long", "max_quad_weight", "long"), ("dispatch_info", "string", "dispatch_info", "string"), ("che_lane", "long", "che_lane", "long"), ("has_trailer", "long", "has_trailer", "long"), ("work_load", "long", "work_load", "long"), ("autoche_running_hours", "long", "autoche_running_hours", "long"), ("che_energy_level", "long", "che_energy_level", "long"), ("che_energy_state_enum", "string", "che_energy_state_enum", "string"), ("lift_capacity", "long", "lift_capacity", "long"), ("maximum_weight_in_kg", "float", "maximum_weight_in_kg", "float"), ("max_quad_weight_in_kg", "float", "max_quad_weight_in_kg", "float"), ("max_tandem_weight_in_kg", "float", "max_tandem_weight_in_kg", "float"), ("lift_capacity_in_kg", "float", "lift_capacity_in_kg", "float"), ("lift_operational_status", "string", "lift_operational_status", "string"), ("twin_diff_wgt_allowance_in_kg", "float", "twin_diff_wgt_allowance_in_kg", "float"), ("ocr_mode_switch", "long", "ocr_mode_switch", "long"), ("ec_state_flex_string_1", "string", "ec_state_flex_string_1", "string"), ("ec_state_flex_string_2", "string", "ec_state_flex_string_2", "string"), ("ec_state_flex_string_3", "string", "ec_state_flex_string_3", "string"), ("allowed_chassis_kinds", "long", "allowed_chassis_kinds", "long"), ("last_pos_loctype", "string", "last_pos_loctype", "string"), ("last_pos_locid", "string", "last_pos_locid", "string"), ("last_pos_loc_gkey", "long", "last_pos_loc_gkey", "long"), ("last_pos_slot", "string", "last_pos_slot", "string"), ("last_pos_orientation", "string", "last_pos_orientation", "string"), ("last_pos_name", "string", "last_pos_name", "string"), ("last_pos_bin", "long", "last_pos_bin", "long"), ("last_pos_tier", "long", "last_pos_tier", "long"), ("last_pos_anchor", "string", "last_pos_anchor", "string"), ("last_pos_orientation_degrees", "float", "last_pos_orientation_degrees", "float"), ("last_ops_pos_id", "string", "last_ops_pos_id", "string"), ("last_pos_slot_on_carriage", "string", "last_pos_slot_on_carriage", "string")], transformation_ctx = "xpsche_applymapping")
										
## xps_ecevent mapping
xpsecevent_applymapping = ApplyMapping.apply(frame = xpsecevent_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("pkey", "long", "pkey", "long"), ("ectimestamp", "timestamp", "ectimestamp", "timestamp"), ("type", "long", "type", "long"), ("che_id", "long", "che_id", "long"), ("che_name", "string", "che_name", "string"), ("operator_name", "string", "operator_name", "string"), ("sub_type", "long", "sub_type", "long"), ("type_description", "string", "type_description", "string"), ("from_che_id_name", "long", "from_che_id_name", "long"), ("to_che_id_name", "long", "to_che_id_name", "long"), ("unit_id_name", "string", "unit_id_name", "string"), ("pow_name", "string", "pow_name", "string"), ("pool_name", "string", "pool_name", "string"), ("work_queue", "string", "work_queue", "string"), ("travel_distance", "long", "travel_distance", "long"), ("move_kind", "string", "move_kind", "string"), ("is_twin_move", "long", "is_twin_move", "long"), ("start_distance", "long", "start_distance", "long"), ("work_assignment_gkey", "long", "work_assignment_gkey", "long"), ("work_assignment_id", "string", "work_assignment_id", "string"), ("unit_reference", "string", "unit_reference", "string"), ("tran_id", "string", "tran_id", "string"), ("loctype", "string", "loctype", "string"), ("locid", "string", "locid", "string"), ("loc_slot", "string", "loc_slot", "string"), ("ops_pos_id", "string", "ops_pos_id", "string"), ("unladen_loctype", "string", "unladen_loctype", "string"), ("unladen_locid", "string", "unladen_locid", "string"), ("unladen_loc_slot", "string", "unladen_loc_slot", "string"), ("laden_loctype", "string", "laden_loctype", "string"), ("laden_locid", "string", "laden_locid", "string"), ("laden_loc_slot", "string", "laden_loc_slot", "string"), ("last_est_move_time", "timestamp", "last_est_move_time", "timestamp")], transformation_ctx = "xpsecevent_applymapping")
						
## xps_ecuser mapping
xpsecuser_applymapping = ApplyMapping.apply(frame = xpsecuser_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("yard", "long", "yard", "long"), ("pkey", "long", "pkey", "long"), ("name", "string", "name", "string"), ("user_id", "string", "user_id", "string"), ("password", "string", "password", "string")], transformation_ctx = "xpsecuser_applymapping")
										
			

                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## inv_goods datasink
ig_datasink = glueContext.write_dynamic_frame.from_options(frame = ig_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_goods"}, format = "parquet", transformation_ctx = "ig_datasink")

## inv_move_event datasink
me_datasink = glueContext.write_dynamic_frame.from_options(frame = me_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_move_event"}, format = "parquet", transformation_ctx = "me_datasink")

## inv_unit datasink
iu_datasink = glueContext.write_dynamic_frame.from_options(frame = iu_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_unit"}, format = "parquet", transformation_ctx = "iu_datasink")

## inv_unit_fcy_visit datasink
fcy_datasink = glueContext.write_dynamic_frame.from_options(frame = fcy_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_unit_fcy_visit"}, format = "parquet", transformation_ctx = "fcy_datasink")

## inv_unit_yrd_visit datasink
yrd_datasink = glueContext.write_dynamic_frame.from_options(frame = yrd_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_unit_yrd_visit"}, format = "parquet", transformation_ctx = "yrd_datasink")

## inv_wi datasink
wi_datasink = glueContext.write_dynamic_frame.from_options(frame = wi_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_wi"}, format = "parquet", transformation_ctx = "wi_datasink")

## inv_wi_tracking datasink
wit_datasink = glueContext.write_dynamic_frame.from_options(frame = wit_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_wi_tracking"}, format = "parquet", transformation_ctx = "wit_datasink")

## inv_wq datasink
wq_datasink = glueContext.write_dynamic_frame.from_options(frame = wq_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/inv_wq"}, format = "parquet", transformation_ctx = "wq_datasink")

## mns_che_move_statistics datasink
mvstat_datasink = glueContext.write_dynamic_frame.from_options(frame = mvstat_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_move_statistics"}, format = "parquet", transformation_ctx = "mvstat_datasink")

## mns_che_operator_statistics datasink
operstat_datasink = glueContext.write_dynamic_frame.from_options(frame = operstat_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_operator_statistics"}, format = "parquet", transformation_ctx = "operstat_datasink")

## mns_che_session datasink
chesess_datasink = glueContext.write_dynamic_frame.from_options(frame = chesess_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_session"}, format = "parquet", transformation_ctx = "chesess_datasink")

## mns_che_session_period datasink
sessper_datasink = glueContext.write_dynamic_frame.from_options(frame = sessper_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_session_period"}, format = "parquet", transformation_ctx = "sessper_datasink")

## mns_che_status datasink
status_datasink = glueContext.write_dynamic_frame.from_options(frame = status_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_status"}, format = "parquet", transformation_ctx = "status_datasink")

## mns_che_trip_statistics datasink
tripstat_datasink = glueContext.write_dynamic_frame.from_options(frame = tripstat_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/mns_che_trip_statistics"}, format = "parquet", transformation_ctx = "tripstat_datasink")

## xps_che datasink
xpsche_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsche_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_che"}, format = "parquet", transformation_ctx = "xpsche_datasink")

## xps_ecevent datasink
xpsecevent_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsecevent_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_ecevent"}, format = "parquet", transformation_ctx = "xpsecevent_datasink")

## xps_ecuser datasink
xpsecuser_datasink = glueContext.write_dynamic_frame.from_options(frame = xpsecuser_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/xps_ecuser"}, format = "parquet", transformation_ctx = "xpsecuser_datasink")

# start Glue job
glue.start_job_run(JobName = 'n4-pnct-custom-logon-parquet')


job.commit()