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
										
## argo_carrier_visit connection
carvst_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_carrier_visit", transformation_ctx = "carvst_DS")
carvst_regDF = carvst_DS.toDF()
carvst_regDF.createOrReplaceTempView("distcarvst")
carvst_distDF = spark.sql("with maxcv as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distcarvst \
                            		GROUP BY gkey \
                            	) \
                            	SELECT DISTINCT dc.sourcesystem, \
                            			 dc.gkey, \
                            			 dc.id, \
                            			 dc.customs_id, \
                            			 dc.carrier_mode, \
                            			 dc.visit_nbr, \
                            			 dc.phase, \
                            			 dc.operator_gkey, \
                            			 dc.cpx_gkey, \
                            			 dc.fcy_gkey, \
                            			 dc.next_fcy_gkey, \
                            			 dc.ata, \
                            			 dc.atd, \
                            			 dc.send_on_board_unit_updates, \
                            			 dc.send_crane_work_list_updates, \
                            			 dc.cvcvd_gkey, \
                            			 dc.created, \
                            			 dc.creator, \
                            			 dc.changed, \
                            			 dc.changer \
                            	FROM distcarvst dc \
                            	INNER JOIN maxcv ma on dc.gkey = ma.gkey \
                            		and coalesce(dc.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                                    and dc.created = ma.created \
                            	WHERE dc.is_deleted = false")
carvst_dynDF = DynamicFrame.fromDF(carvst_distDF, glueContext, "nested")

## argo_chargeable_unit_events connection
charunev_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_chargeable_unit_events", transformation_ctx = "charunev_DS")
charunev_regDF = charunev_DS.toDF()
charunev_regDF.createOrReplaceTempView("distcharunev")
charunev_distDF = spark.sql("with maxcu as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distcharunev \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT cue.sourcesystem, \
                            		cue.gkey, \
                            		cue.batch_id, \
                            		cue.event_type, \
                            		cue.source_event_gkey, \
                            		cue.ufv_gkey, \
                            		cue.facility_id, \
                            		cue.facility_gkey, \
                            		cue.complex_id, \
                            		cue.terminal_operator_gkey, \
                            		cue.equipment_id, \
                            		cue.eq_subclass, \
                            		cue.line_operator_id, \
                            		cue.freight_kind, \
                            		cue.iso_code, \
                            		cue.iso_group, \
                            		cue.eq_length, \
                            		cue.eq_height, \
                            		cue.category, \
                            		cue.paid_thru_day, \
                            		cue.dray_status, \
                            		cue.is_oog, \
                            		cue.is_refrigerated, \
                            		cue.is_hazardous, \
                            		cue.imdg_class, \
                            		cue.fire_code, \
                            		cue.fire_code_class, \
                            		cue.commodity_id, \
                            		cue.special_stow_id, \
                            		cue.pod1, \
                            		cue.pol1, \
                            		cue.bundle_unitid, \
                            		cue.bundle_ufv_gkey, \
                            		cue.bl_nbr, \
                            		cue.guarantee_party, \
                            		cue.guarantee_thru_day, \
                            		cue.booking_nbr, \
                            		cue.restow_type, \
                            		cue.unit_gkey, \
                            		cue.opl, \
                            		cue.final_destination, \
                            		cue.ufv_time_in, \
                            		cue.ufv_time_out, \
                            		cue.temp_reqd_c, \
                            		cue.time_load, \
                            		cue.consignee_id, \
                            		cue.shipper_id, \
                            		cue.time_first_free_day, \
                            		cue.time_discharge_complete, \
                            		cue.cargo_quantity, \
                            		cue.cargo_quantity_unit, \
                            		cue.ib_intended_loctype, \
                            		cue.ib_intended_id, \
                            		cue.ib_intended_visit_id, \
                            		cue.ib_intended_carrier_name, \
                            		cue.ib_intended_call_nbr, \
                            		cue.ib_intended_eta, \
                            		cue.ib_intended_ata, \
                            		cue.ib_intended_atd, \
                            		cue.ib_intended_vessel_type, \
                            		cue.ib_intended_line_id, \
                            		cue.ib_intended_service_id, \
                            		cue.ib_intended_vessel_class_id, \
                            		cue.ib_loctype, \
                            		cue.ib_id, \
                            		cue.ib_visit_id, \
                            		cue.ib_carrier_name, \
                            		cue.ib_call_nbr, \
                            		cue.ib_eta, \
                            		cue.ib_ata, \
                            		cue.ib_atd, \
                            		cue.ib_vessel_type, \
                            		cue.ib_line_id, \
                            		cue.ib_service_id, \
                            		cue.ib_vessel_class_id, \
                            		cue.ib_vessel_lloyds_id, \
                            		cue.ob_intended_loctype, \
                            		cue.ob_intended_id, \
                            		cue.ob_intended_visit_id, \
                            		cue.ob_intended_carrier_name, \
                            		cue.ob_intended_call_nbr, \
                            		cue.ob_intended_eta, \
                            		cue.ob_intended_ata, \
                            		cue.ob_intended_atd, \
                            		cue.ob_intended_vessel_type, \
                            		cue.ob_intended_line_id, \
                            		cue.ob_intended_service_id, \
                            		cue.ob_intended_vessel_class_id, \
                            		cue.ob_loctype, \
                            		cue.ob_id, \
                            		cue.ob_visit_id, \
                            		cue.ob_carrier_name, \
                            		cue.ob_call_nbr, \
                            		cue.ob_eta, \
                            		cue.ob_ata, \
                            		cue.ob_atd, \
                            		cue.ob_vessel_type, \
                            		cue.ob_line_id, \
                            		cue.ob_service_id, \
                            		cue.ob_vessel_class_id, \
                            		cue.ob_vessel_lloyds_id, \
                            		cue.event_start_time, \
                            		cue.event_end_time, \
                            		cue.rule_start_day, \
                            		cue.rule_end_day, \
                            		cue.payee_customer_id, \
                            		cue.payee_role, \
                            		cue.first_availability_date, \
                            		cue.qc_che_id, \
                            		cue.fm_pos_loctype, \
                            		cue.fm_pos_locid, \
                            		cue.to_pos_loctype, \
                            		cue.to_pos_locid, \
                            		cue.service_order, \
                            		cue.restow_reason, \
                            		cue.restow_account, \
                            		cue.rehandle_count, \
                            		cue.quantity, \
                            		cue.quantity_unit, \
                            		cue.fm_pos_slot, \
                            		cue.fm_pos_name, \
                            		cue.to_pos_slot, \
                            		cue.to_pos_name, \
                            		cue.is_locked, \
                            		cue.status, \
                            		cue.last_draft_inv_nbr, \
                            		cue.notes, \
                            		cue.is_override_value, \
                            		cue.override_value_type, \
                            		cue.override_value, \
                            		cue.guarantee_id, \
                            		cue.guarantee_gkey, \
                            		cue.gnte_inv_processing_status, \
                            		cue.flex_string01, \
                            		cue.flex_string02, \
                            		cue.flex_string03, \
                            		cue.flex_string04, \
                            		cue.flex_string05, \
                            		cue.flex_string06, \
                            		cue.flex_string07, \
                            		cue.flex_string08, \
                            		cue.flex_string09, \
                            		cue.flex_string10, \
                            		cue.flex_string11, \
                            		cue.flex_string12, \
                            		cue.flex_string13, \
                            		cue.flex_string14, \
                            		cue.flex_string15, \
                            		cue.flex_string16, \
                            		cue.flex_string17, \
                            		cue.flex_string18, \
                            		cue.flex_string19, \
                            		cue.flex_string20, \
                            		cue.flex_date01, \
                            		cue.flex_date02, \
                            		cue.flex_date03, \
                            		cue.flex_date04, \
                            		cue.flex_date05, \
                            		cue.flex_long01, \
                            		cue.flex_long02, \
                            		cue.flex_long03, \
                            		cue.flex_long04, \
                            		cue.flex_long05, \
                            		cue.flex_double01, \
                            		cue.flex_double02, \
                            		cue.flex_double03, \
                            		cue.flex_double04, \
                            		cue.flex_double05, \
                            		cue.equipment_owner_id, \
                            		cue.equipment_role, \
                            		cue.created, \
                            		cue.creator, \
                            		cue.changed, \
                            		cue.changer, \
                            		cue.verified_gross_mass, \
                            		cue.vgm_verifier_entity \
                            FROM distcharunev cue \
                            INNER JOIN maxcu ma on cue.gkey = ma.gkey \
                            		and coalesce(cue.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                                    and cue.created = ma.created \
                            WHERE cue.is_deleted = false")
charunev_dynDF = DynamicFrame.fromDF(charunev_distDF, glueContext, "nested")

## argo_visit_details connection
visdet_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_visit_details", transformation_ctx = "visdet_DS")
visdet_regDF = visdet_DS.toDF()
visdet_regDF.createOrReplaceTempView("distvisdet")
visdet_distDF = spark.sql("with maxvd as \
                            	( \
                            		SELECT gkey, max(audtdateadded) audtdateadded, coalesce(max(eta),cast('1900-01-01' as timestamp)) eta, coalesce(max(etd),cast('1900-01-01' as timestamp)) etd \
                            		FROM distvisdet \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT vd.sourcesystem, \
                            		vd.audtdateadded, \
                            		vd.gkey, \
                            		vd.eta, \
                            		vd.etd, \
                            		vd.time_discharge_complete, \
                            		vd.time_first_availabiltiy, \
                            		vd.ffd, \
                            		vd.service, \
                            		vd.itinereray, \
                            		vd.in_call_number, \
                            		vd.out_call_number, \
                            		vd.data_source, \
                            		vd.time_periodic_start, \
                            		vd.time_periodic_end, \
                            		vd.periodic_interval, \
                            		vd.life_cycle_state \
                            FROM distvisdet vd \
                            INNER JOIN maxvd ma on vd.gkey = ma.gkey \
                            		and vd.audtdateadded = ma.audtdateadded \
                            		and coalesce(vd.eta,cast('1900-01-01' as timestamp)) = ma.eta \
                                    and coalesce(vd.etd,cast('1900-01-01' as timestamp)) = ma.etd \
                            WHERE vd.is_deleted = false")
visdet_dynDF = DynamicFrame.fromDF(visdet_distDF, glueContext, "nested")

## argo_yard connection
yard_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "argo_yard", transformation_ctx = "yard_DS")
yard_regDF = yard_DS.toDF()
yard_regDF.createOrReplaceTempView("distyard")
yard_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
								max(audtdateadded) audtdateadded, \
								gkey, \
								id, \
								name, \
								life_cycle_state, \
								xps_state, \
								fcy_gkey, \
								abm_gkey \
						FROM distyard \
						GROUP BY sourcesystem, \
							gkey, \
							id, \
							name, \
							life_cycle_state, \
							xps_state, \
							fcy_gkey, \
							abm_gkey")
yard_dynDF = DynamicFrame.fromDF(yard_distDF, glueContext, "nested")

## crg_bills_of_lading connection
bol_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "crg_bills_of_lading", transformation_ctx = "bol_DS")
bol_regDF = bol_DS.toDF()
bol_regDF.createOrReplaceTempView("distbol")
bol_distDF = spark.sql("with maxbl as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM distbol \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT bl.sourcesystem, \
                        		bl.gkey, \
                        		bl.nbr, \
                        		bl.gid, \
                        		bl.category, \
                        		bl.consignee_name, \
                        		bl.shipper_name, \
                        		bl.bonded_destination, \
                        		bl.original_bl_gkey, \
                        		bl.origin, \
                        		bl.destination, \
                        		bl.notes, \
                        		bl.manifested_qty, \
                        		bl.inbond, \
                        		bl.exam, \
                        		bl.mnft_bl_seq_nbr, \
                        		bl.pin, \
                        		bl.notify_pty1_name, \
                        		bl.notify_pty2_name, \
                        		bl.notify_pty1_add1, \
                        		bl.notify_pty2_add1, \
                        		bl.notify_pty1_ctct_no1, \
                        		bl.notify_pty2_ctct_no1, \
                        		bl.complex_gkey, \
                        		bl.line_gkey, \
                        		bl.consignee_gkey, \
                        		bl.shipper_gkey, \
                        		bl.agent_gkey, \
                        		bl.cv_gkey, \
                        		bl.pol_gkey, \
                        		bl.pod1_gkey, \
                        		bl.pod2_gkey, \
                        		bl.bond_trucking_company, \
                        		bl.created, \
                        		bl.creator, \
                        		bl.changed, \
                        		bl.changer \
                        FROM distbol bl \
                        INNER JOIN maxbl ma on bl.gkey = ma.gkey \
                        	and coalesce(bl.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and bl.created = ma.created \
                        WHERE bl.is_deleted = false")
bol_dynDF = DynamicFrame.fromDF(bol_distDF, glueContext, "nested")

## crg_bl_goods connection
blg_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "crg_bl_goods", transformation_ctx = "blg_DS")
blg_regDF = blg_DS.toDF()
blg_regDF.createOrReplaceTempView("distblg")
blg_distDF = spark.sql("with maxbg as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM distblg \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT bg.sourcesystem, \
                        	bg.gkey, \
                        	bg.bl_gkey, \
                        	bg.gds_gkey, \
                        	bg.created, \
                        	bg.creator, \
                        	bg.changed, \
                        	bg.changer  \
                        FROM distblg bg \
                        INNER JOIN maxbg ma on bg.gkey = ma.gkey \
                        	and coalesce(bg.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and bg.created = ma.created \
                        WHERE bg.is_deleted = false")
blg_dynDF = DynamicFrame.fromDF(blg_distDF, glueContext, "nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
										
## argo_carrier_visit mapping
carvst_applymapping = ApplyMapping.apply(frame = carvst_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("customs_id", "string", "customs_id", "string"), ("carrier_mode", "string", "carrier_mode", "string"), ("visit_nbr", "long", "visit_nbr", "long"), ("phase", "string", "phase", "string"), ("operator_gkey", "long", "operator_gkey", "long"), ("cpx_gkey", "long", "cpx_gkey", "long"), ("fcy_gkey", "long", "fcy_gkey", "long"), ("next_fcy_gkey", "long", "next_fcy_gkey", "long"), ("ata", "timestamp", "ata", "timestamp"), ("atd", "timestamp", "atd", "timestamp"), ("send_on_board_unit_updates", "long", "send_on_board_unit_updates", "long"), ("send_crane_work_list_updates", "long", "send_crane_work_list_updates", "long"), ("cvcvd_gkey", "long", "cvcvd_gkey", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "carvst_applymapping")

## argo_chargeable_unit_events mapping
charunev_applymapping = ApplyMapping.apply(frame = charunev_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("batch_id", "long", "batch_id", "long"), ("event_type", "string", "event_type", "string"), ("source_event_gkey", "long", "source_event_gkey", "long"), ("ufv_gkey", "long", "ufv_gkey", "long"), ("facility_id", "string", "facility_id", "string"), ("facility_gkey", "long", "facility_gkey", "long"), ("complex_id", "string", "complex_id", "string"), ("terminal_operator_gkey", "long", "terminal_operator_gkey", "long"), ("equipment_id", "string", "equipment_id", "string"), ("eq_subclass", "string", "eq_subclass", "string"), ("line_operator_id", "string", "line_operator_id", "string"), ("freight_kind", "string", "freight_kind", "string"), ("iso_code", "string", "iso_code", "string"), ("iso_group", "string", "iso_group", "string"), ("eq_length", "string", "eq_length", "string"), ("eq_height", "string", "eq_height", "string"), ("category", "string", "category", "string"), ("paid_thru_day", "timestamp", "paid_thru_day", "timestamp"), ("dray_status", "string", "dray_status", "string"), ("is_oog", "long", "is_oog", "long"), ("is_refrigerated", "long", "is_refrigerated", "long"), ("is_hazardous", "long", "is_hazardous", "long"), ("imdg_class", "string", "imdg_class", "string"), ("fire_code", "string", "fire_code", "string"), ("fire_code_class", "string", "fire_code_class", "string"), ("commodity_id", "string", "commodity_id", "string"), ("special_stow_id", "string", "special_stow_id", "string"), ("pod1", "string", "pod1", "string"), ("pol1", "string", "pol1", "string"), ("bundle_unitid", "string", "bundle_unitid", "string"), ("bundle_ufv_gkey", "long", "bundle_ufv_gkey", "long"), ("bl_nbr", "string", "bl_nbr", "string"), ("guarantee_party", "string", "guarantee_party", "string"), ("guarantee_thru_day", "timestamp", "guarantee_thru_day", "timestamp"), ("booking_nbr", "string", "booking_nbr", "string"), ("restow_type", "string", "restow_type", "string"), ("unit_gkey", "long", "unit_gkey", "long"), ("opl", "string", "opl", "string"), ("final_destination", "string", "final_destination", "string"), ("ufv_time_in", "timestamp", "ufv_time_in", "timestamp"), ("ufv_time_out", "timestamp", "ufv_time_out", "timestamp"), ("temp_reqd_c", "float", "temp_reqd_c", "float"), ("time_load", "timestamp", "time_load", "timestamp"), ("consignee_id", "string", "consignee_id", "string"), ("shipper_id", "string", "shipper_id", "string"), ("time_first_free_day", "string", "time_first_free_day", "string"), ("time_discharge_complete", "timestamp", "time_discharge_complete", "timestamp"), ("cargo_quantity", "float", "cargo_quantity", "float"), ("cargo_quantity_unit", "string", "cargo_quantity_unit", "string"), ("ib_intended_loctype", "string", "ib_intended_loctype", "string"), ("ib_intended_id", "string", "ib_intended_id", "string"), ("ib_intended_visit_id", "string", "ib_intended_visit_id", "string"), ("ib_intended_carrier_name", "string", "ib_intended_carrier_name", "string"), ("ib_intended_call_nbr", "string", "ib_intended_call_nbr", "string"), ("ib_intended_eta", "timestamp", "ib_intended_eta", "timestamp"), ("ib_intended_ata", "timestamp", "ib_intended_ata", "timestamp"), ("ib_intended_atd", "timestamp", "ib_intended_atd", "timestamp"), ("ib_intended_vessel_type", "string", "ib_intended_vessel_type", "string"), ("ib_intended_line_id", "string", "ib_intended_line_id", "string"), ("ib_intended_service_id", "string", "ib_intended_service_id", "string"), ("ib_intended_vessel_class_id", "string", "ib_intended_vessel_class_id", "string"), ("ib_loctype", "string", "ib_loctype", "string"), ("ib_id", "string", "ib_id", "string"), ("ib_visit_id", "string", "ib_visit_id", "string"), ("ib_carrier_name", "string", "ib_carrier_name", "string"), ("ib_call_nbr", "string", "ib_call_nbr", "string"), ("ib_eta", "string", "ib_eta", "string"), ("ib_ata", "string", "ib_ata", "string"), ("ib_atd", "string", "ib_atd", "string"), ("ib_vessel_type", "string", "ib_vessel_type", "string"), ("ib_line_id", "string", "ib_line_id", "string"), ("ib_service_id", "string", "ib_service_id", "string"), ("ib_vessel_class_id", "string", "ib_vessel_class_id", "string"), ("ib_vessel_lloyds_id", "string", "ib_vessel_lloyds_id", "string"), ("ob_intended_loctype", "string", "ob_intended_loctype", "string"), ("ob_intended_id", "string", "ob_intended_id", "string"), ("ob_intended_visit_id", "string", "ob_intended_visit_id", "string"), ("ob_intended_carrier_name", "string", "ob_intended_carrier_name", "string"), ("ob_intended_call_nbr", "string", "ob_intended_call_nbr", "string"), ("ob_intended_eta", "timestamp", "ob_intended_eta", "timestamp"), ("ob_intended_ata", "timestamp", "ob_intended_ata", "timestamp"), ("ob_intended_atd", "timestamp", "ob_intended_atd", "timestamp"), ("ob_intended_vessel_type", "string", "ob_intended_vessel_type", "string"), ("ob_intended_line_id", "string", "ob_intended_line_id", "string"), ("ob_intended_service_id", "string", "ob_intended_service_id", "string"), ("ob_intended_vessel_class_id", "string", "ob_intended_vessel_class_id", "string"), ("ob_loctype", "string", "ob_loctype", "string"), ("ob_id", "string", "ob_id", "string"), ("ob_visit_id", "string", "ob_visit_id", "string"), ("ob_carrier_name", "string", "ob_carrier_name", "string"), ("ob_call_nbr", "string", "ob_call_nbr", "string"), ("ob_eta", "string", "ob_eta", "string"), ("ob_ata", "string", "ob_ata", "string"), ("ob_atd", "string", "ob_atd", "string"), ("ob_vessel_type", "string", "ob_vessel_type", "string"), ("ob_line_id", "string", "ob_line_id", "string"), ("ob_service_id", "string", "ob_service_id", "string"), ("ob_vessel_class_id", "string", "ob_vessel_class_id", "string"), ("ob_vessel_lloyds_id", "string", "ob_vessel_lloyds_id", "string"), ("event_start_time", "timestamp", "event_start_time", "timestamp"), ("event_end_time", "timestamp", "event_end_time", "timestamp"), ("rule_start_day", "timestamp", "rule_start_day", "timestamp"), ("rule_end_day", "timestamp", "rule_end_day", "timestamp"), ("payee_customer_id", "string", "payee_customer_id", "string"), ("payee_role", "string", "payee_role", "string"), ("first_availability_date", "timestamp", "first_availability_date", "timestamp"), ("qc_che_id", "string", "qc_che_id", "string"), ("fm_pos_loctype", "string", "fm_pos_loctype", "string"), ("fm_pos_locid", "string", "fm_pos_locid", "string"), ("to_pos_loctype", "string", "to_pos_loctype", "string"), ("to_pos_locid", "string", "to_pos_locid", "string"), ("service_order", "string", "service_order", "string"), ("restow_reason", "string", "restow_reason", "string"), ("restow_account", "string", "restow_account", "string"), ("rehandle_count", "long", "rehandle_count", "long"), ("quantity", "float", "quantity", "float"), ("quantity_unit", "string", "quantity_unit", "string"), ("fm_pos_slot", "string", "fm_pos_slot", "string"), ("fm_pos_name", "string", "fm_pos_name", "string"), ("to_pos_slot", "string", "to_pos_slot", "string"), ("to_pos_name", "string", "to_pos_name", "string"), ("is_locked", "long", "is_locked", "long"), ("status", "string", "status", "string"), ("last_draft_inv_nbr", "string", "last_draft_inv_nbr", "string"), ("notes", "string", "notes", "string"), ("is_override_value", "long", "is_override_value", "long"), ("override_value_type", "string", "override_value_type", "string"), ("override_value", "float", "override_value", "float"), ("guarantee_id", "string", "guarantee_id", "string"), ("guarantee_gkey", "long", "guarantee_gkey", "long"), ("gnte_inv_processing_status", "string", "gnte_inv_processing_status", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string"), ("flex_string11", "string", "flex_string11", "string"), ("flex_string12", "string", "flex_string12", "string"), ("flex_string13", "string", "flex_string13", "string"), ("flex_string14", "string", "flex_string14", "string"), ("flex_string15", "string", "flex_string15", "string"), ("flex_string16", "string", "flex_string16", "string"), ("flex_string17", "string", "flex_string17", "string"), ("flex_string18", "string", "flex_string18", "string"), ("flex_string19", "string", "flex_string19", "string"), ("flex_string20", "string", "flex_string20", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("flex_date02", "timestamp", "flex_date02", "timestamp"), ("flex_date03", "timestamp", "flex_date03", "timestamp"), ("flex_date04", "timestamp", "flex_date04", "timestamp"), ("flex_date05", "timestamp", "flex_date05", "timestamp"), ("flex_long01", "long", "flex_long01", "long"), ("flex_long02", "long", "flex_long02", "long"), ("flex_long03", "long", "flex_long03", "long"), ("flex_long04", "long", "flex_long04", "long"), ("flex_long05", "long", "flex_long05", "long"), ("flex_double01", "float", "flex_double01", "float"), ("flex_double02", "float", "flex_double02", "float"), ("flex_double03", "float", "flex_double03", "float"), ("flex_double04", "float", "flex_double04", "float"), ("flex_double05", "float", "flex_double05", "float"), ("equipment_owner_id", "string", "equipment_owner_id", "string"), ("equipment_role", "string", "equipment_role", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("verified_gross_mass", "float", "verified_gross_mass", "float"), ("vgm_verifier_entity", "string", "vgm_verifier_entity", "string")], transformation_ctx = "charunev_applymapping")

## argo_visit_details mapping
visdet_applymapping = ApplyMapping.apply(frame = visdet_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("gkey", "long", "gkey", "long"), ("eta", "timestamp", "eta", "timestamp"), ("etd", "timestamp", "etd", "timestamp"), ("time_discharge_complete", "timestamp", "time_discharge_complete", "timestamp"), ("time_first_availabiltiy", "timestamp", "time_first_availabiltiy", "timestamp"), ("ffd", "timestamp", "ffd", "timestamp"), ("service", "long", "service", "long"), ("itinereray", "long", "itinereray", "long"), ("in_call_number", "string", "in_call_number", "string"), ("out_call_number", "string", "out_call_number", "string"), ("data_source", "string", "data_source", "string"), ("time_periodic_start", "timestamp", "time_periodic_start", "timestamp"), ("time_periodic_end", "timestamp", "time_periodic_end", "timestamp"), ("periodic_interval", "long", "periodic_interval", "long"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "visdet_applymapping")

## argo_yard mapping
yard_applymapping = ApplyMapping.apply(frame = yard_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), 
("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), 
("xps_state", "string", "xps_state", "string"), 
#("compiled_yard", "string", "compiled_yard", "string"), ("sparcs_settings_file", "string", "sparcs_settings_file", "string"), ("berth_text_file", "string", "berth_text_file", "string"), 
("fcy_gkey", "long", "fcy_gkey", "long"), ("abm_gkey", "long", "abm_gkey", "long")], transformation_ctx = "yard_applymapping")

## crg_bills_of_lading mapping
bol_applymapping = ApplyMapping.apply(frame = bol_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("nbr", "string", "nbr", "string"), ("gid", "long", "gid", "long"), ("category", "string", "category", "string"), ("consignee_name", "string", "consignee_name", "string"), ("shipper_name", "string", "shipper_name", "string"), ("bonded_destination", "string", "bonded_destination", "string"), ("original_bl_gkey", "long", "original_bl_gkey", "long"), ("origin", "string", "origin", "string"), ("destination", "string", "destination", "string"), ("notes", "string", "notes", "string"), ("manifested_qty", "float", "manifested_qty", "float"), ("inbond", "string", "inbond", "string"), ("exam", "string", "exam", "string"), ("mnft_bl_seq_nbr", "string", "mnft_bl_seq_nbr", "string"), ("pin", "string", "pin", "string"), ("notify_pty1_name", "string", "notify_pty1_name", "string"), ("notify_pty2_name", "string", "notify_pty2_name", "string"), ("notify_pty1_add1", "string", "notify_pty1_add1", "string"), ("notify_pty2_add1", "string", "notify_pty2_add1", "string"), ("notify_pty1_ctct_no1", "string", "notify_pty1_ctct_no1", "string"), ("notify_pty2_ctct_no1", "string", "notify_pty2_ctct_no1", "string"), ("complex_gkey", "long", "complex_gkey", "long"), ("line_gkey", "long", "line_gkey", "long"), ("consignee_gkey", "long", "consignee_gkey", "long"), ("shipper_gkey", "long", "shipper_gkey", "long"), ("agent_gkey", "long", "agent_gkey", "long"), ("cv_gkey", "long", "cv_gkey", "long"), ("pol_gkey", "long", "pol_gkey", "long"), ("pod1_gkey", "long", "pod1_gkey", "long"), ("pod2_gkey", "long", "pod2_gkey", "long"), ("bond_trucking_company", "long", "bond_trucking_company", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "bol_applymapping")

## crg_bl_goods mapping
blg_applymapping = ApplyMapping.apply(frame = blg_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("bl_gkey", "long", "bl_gkey", "long"), ("gds_gkey", "long", "gds_gkey", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "blg_applymapping")



                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## argo_carrier_visit datasink
carvst_datasink = glueContext.write_dynamic_frame.from_options(frame = carvst_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_carrier_visit"}, format = "parquet", transformation_ctx = "carvst_datasink")

## argo_chargeable_unit_events datasink
charunev_datasink = glueContext.write_dynamic_frame.from_options(frame = charunev_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_chargeable_unit_events"}, format = "parquet", transformation_ctx = "charunev_datasink")

## argo_visit_details datasink
visdet_datasink = glueContext.write_dynamic_frame.from_options(frame = visdet_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_visit_details"}, format = "parquet", transformation_ctx = "visdet_datasink")

## argo_yard datasink
yard_datasink = glueContext.write_dynamic_frame.from_options(frame = yard_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/argo_yard"}, format = "parquet", transformation_ctx = "yard_datasink")

## crg_bills_of_lading datasink
bol_datasink = glueContext.write_dynamic_frame.from_options(frame = bol_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/crg_bills_of_lading"}, format = "parquet", transformation_ctx = "bol_datasink")

## crg_bl_goods datasink
blg_datasink = glueContext.write_dynamic_frame.from_options(frame = blg_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/crg_bl_goods"}, format = "parquet", transformation_ctx = "blg_datasink")




job.commit()