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

## ref_bizunit_scoped connection
scoped_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_bizunit_scoped", transformation_ctx = "scoped_DS")
scoped_regDF = scoped_DS.toDF()
scoped_regDF.createOrReplaceTempView("distscoped")
scoped_distDF = spark.sql("with maxscp as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distscoped \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT ds.sourcesystem, \
                            	ds.gkey, \
                            	ds.id, \
                            	ds.reference_set, \
                            	ds.name, \
                            	ds.role, \
                            	ds.scac, \
                            	ds.bic, \
                            	ds.is_eq_owner, \
                            	ds.is_eq_operator, \
                            	ds.life_cycle_state, \
                            	ds.credit_status, \
                            	ds.per_unit_guarantee_limit, \
                            	ds.notes, \
                            	ds.ct_name, \
                            	ds.address_line1, \
                            	ds.address_line2, \
                            	ds.address_line3, \
                            	ds.city, \
                            	ds.state_gkey, \
                            	ds.country_code, \
                            	ds.mail_code, \
                            	ds.telephone, \
                            	ds.fax, \
                            	ds.email_address, \
                            	ds.sms_number, \
                            	ds.website_url, \
                            	ds.ftp_address, \
                            	ds.bizu_gkey, \
                            	ds.created, \
                            	ds.creator, \
                            	ds.changed, \
                            	ds.changer \
                            FROM distscoped ds \
                            INNER JOIN maxscp ma on ds.gkey = ma.gkey \
                            	and coalesce(ds.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and ds.created = ma.created \
                            WHERE ds.is_deleted = false")
scoped_dynDF = DynamicFrame.fromDF(scoped_distDF, glueContext, "nested")

## ref_carrier_itinerary connection
carit_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_carrier_itinerary", transformation_ctx = "carit_DS")
carit_regDF = carit_DS.toDF()
carit_regDF.createOrReplaceTempView("distcarit")
carit_distDF = spark.sql("with maxci as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distcarit \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT ci.sourcesystem, \
                            	ci.gkey, \
                            	ci.id, \
                            	ci.owner_cv, \
                            	ci.created, \
                            	ci.creator, \
                            	ci.changed, \
                            	ci.changer, \
                            	ci.life_cycle_state \
                            FROM distcarit ci \
                            INNER JOIN maxci ma on ci.gkey = ma.gkey \
                            	and coalesce(ci.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and ci.created = ma.created \
                            WHERE ci.is_deleted = false")
carit_dynDF = DynamicFrame.fromDF(carit_distDF, glueContext, "nested")

## ref_carrier_service connection
carserv_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_carrier_service", transformation_ctx = "carserv_DS")
carserv_regDF = carserv_DS.toDF()
carserv_regDF.createOrReplaceTempView("distcarserv")
carserv_distDF = spark.sql("with maxcs as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distcarserv \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT cs.sourcesystem, \
                            	cs.gkey, \
                            	cs.id, \
                            	cs.reference_set, \
                            	cs.name, \
                            	cs.carrier_mode, \
                            	cs.itin_gkey, \
                            	cs.created, \
                            	cs.creator, \
                            	cs.changed, \
                            	cs.changer, \
                            	cs.life_cycle_state, \
                            	cs.pkey \
                            FROM distcarserv cs \
                            INNER JOIN maxcs ma on cs.gkey = ma.gkey \
                            	and coalesce(cs.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and cs.created = ma.created \
                            WHERE cs.is_deleted = false")
carserv_dynDF = DynamicFrame.fromDF(carserv_distDF, glueContext, "nested")

## ref_country connection
country_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_country", transformation_ctx = "country_DS")
country_regDF = country_DS.toDF()
country_regDF.createOrReplaceTempView("distcountry")
country_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
								max(audtdateadded) audtdateadded, \
								cntry_code, \
								cntry_alpha3_code, \
								cntry_num3_code, \
								cntry_name, \
								cntry_off_name, \
								cntry_life_cycle_state \
							FROM distcountry \
							WHERE is_deleted = false \
							GROUP BY sourcesystem, \
								cntry_code, \
								cntry_alpha3_code, \
								cntry_num3_code, \
								cntry_name, \
								cntry_off_name, \
								cntry_life_cycle_state")
country_dynDF = DynamicFrame.fromDF(country_distDF, glueContext, "nested")

## ref_equip_type connection
eqtype_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_equip_type", transformation_ctx = "eqtype_DS")
eqtype_regDF = eqtype_DS.toDF()
eqtype_regDF.createOrReplaceTempView("disteqtype")
eqtype_distDF = spark.sql("with maxet as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM disteqtype \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT et.sourcesystem, \
                        	et.gkey, \
                        	et.id, \
                        	et.reference_set, \
                        	et.data_source, \
                        	et.is_deprecated, \
                        	et.is_archetype, \
                        	et.archetype, \
                        	et.clone_source, \
                        	et.basic_length, \
                        	et.nominal_length, \
                        	et.nominal_height, \
                        	et.rfr_type, \
                        	et.class, \
                        	et.iso_group, \
                        	et.length_mm, \
                        	et.height_mm, \
                        	et.width_mm, \
                        	et.tare_weight_kg, \
                        	et.safe_weight_kg, \
                        	et.description, \
                        	et.pict_id, \
                        	et.milliteus, \
                        	et.uses_accessories, \
                        	et.is_temperature_controlled, \
                        	et.oog_ok, \
                        	et.unsealable, \
                        	et.has_wheels, \
                        	et.is_open, \
                        	et.fits_20, \
                        	et.fits_24, \
                        	et.fits_30, \
                        	et.fits_40, \
                        	et.fits_45, \
                        	et.fits_48, \
                        	et.fits_53, \
                        	et.is_bombcart, \
                        	et.is_cassette, \
                        	et.is_nopick, \
                        	et.is_triaxle, \
                        	et.is_super_freeze_reefer, \
                        	et.is_controlled_atmo_reefer, \
                        	et.no_stow_on_if_empty, \
                        	et.no_stow_on_if_laden, \
                        	et.must_stow_below_deck, \
                        	et.must_stow_above_deck, \
                        	et.created, \
                        	et.creator, \
                        	et.changed, \
                        	et.changer, \
                        	et.life_cycle_state, \
                        	et.teu_capacity, \
                        	et.slot_labels, \
                        	et.is_2x20notallowed, \
                        	et.events_type_to_record \
                        FROM disteqtype et \
                        INNER JOIN maxet ma on et.gkey = ma.gkey \
                        	and coalesce(et.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and et.created = ma.created \
                        WHERE et.is_deleted = false")
eqtype_dynDF = DynamicFrame.fromDF(eqtype_distDF, glueContext, "nested")

## ref_equipment connection
equip_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_equipment", transformation_ctx = "equip_DS")
equip_regDF = equip_DS.toDF()
equip_regDF.createOrReplaceTempView("distequip")
equip_distDF = spark.sql("with maxeq as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distequip \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT eq.sourcesystem, \
                            	eq.gkey, \
                            	eq.eq_subclass, \
                            	eq.id, \
                            	eq.scope, \
                            	eq.data_source, \
                            	eq.id_prefix, \
                            	eq.id_nbr_only, \
                            	eq.id_check_digit, \
                            	eq.id_full, \
                            	eq.class, \
                            	eq.eqtyp_gkey, \
                            	eq.length_mm, \
                            	eq.height_mm, \
                            	eq.width_mm, \
                            	eq.iso_group, \
                            	eq.tare_kg, \
                            	eq.safe_kg, \
                            	eq.owner, \
                            	eq.operator, \
                            	eq.prior_operator, \
                            	eq.lease_expiration, \
                            	eq.build_date, \
                            	eq.no_stow_on_if_empty, \
                            	eq.no_stow_on_if_laden, \
                            	eq.must_stow_below_deck, \
                            	eq.must_stow_above_deck, \
                            	eq.p_o_s, \
                            	eq.insulated, \
                            	eq.material, \
                            	eq.trnsp_type, \
                            	eq.trnsp_id, \
                            	eq.trnsp_time, \
                            	eq.uses_accessories, \
                            	eq.is_temperature_controlled, \
                            	eq.oog_ok, \
                            	eq.unsealable, \
                            	eq.has_wheels, \
                            	eq.is_open, \
                            	eq.strength_code, \
                            	eq.csc_expiration, \
                            	eq.created, \
                            	eq.creator, \
                            	eq.changed, \
                            	eq.changer, \
                            	eq.life_cycle_state, \
                            	eq.x_cord, \
                            	eq.y_cord, \
                            	eq.z_cord, \
                            	eq.rfr_type, \
                            	eq.is_starvent, \
                            	eq.is_super_freeze_reefer, \
                            	eq.is_controlled_atmo_reefer, \
                            	eq.is_vented, \
                            	eq.tank_rails, \
                            	eq.license_nbr, \
                            	eq.license_state, \
                            	eq.fed_inspect_exp, \
                            	eq.state_inspect_exp, \
                            	eq.axle_count, \
                            	eq.is_triaxle, \
                            	eq.fits_20, \
                            	eq.fits_24, \
                            	eq.fits_30, \
                            	eq.fits_40, \
                            	eq.fits_45, \
                            	eq.fits_48, \
                            	eq.fits_53, \
                            	eq.door_kind, \
                            	eq.tows_dolly, \
                            	eq.semi_reefer_kind, \
                            	eq.acry_description \
                            FROM distequip eq \
                            INNER JOIN maxeq ma on eq.gkey = ma.gkey \
                            	and coalesce(eq.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and eq.created = ma.created \
                            WHERE eq.is_deleted = false")
equip_dynDF = DynamicFrame.fromDF(equip_distDF, glueContext, "nested")

## ref_routing_point connection
routep_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_routing_point", transformation_ctx = "routep_DS")
routep_regDF = routep_DS.toDF()
routep_regDF.createOrReplaceTempView("distroutep")
routep_distDF = spark.sql("with maxrp as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM distroutep \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT rp.sourcesystem, \
                        	rp.gkey, \
                        	rp.id, \
                        	rp.reference_set, \
                        	rp.transit_to_mode, \
                        	rp.unloc_gkey, \
                        	rp.actual_pod_gkey, \
                        	rp.schedule_dcode, \
                        	rp.schedule_kcode, \
                        	rp.splc_code, \
                        	rp.terminal, \
                        	rp.is_placeholder_point, \
                        	rp.created, \
                        	rp.creator, \
                        	rp.changed, \
                        	rp.changer, \
                        	rp.data_source, \
                        	rp.life_cycle_state \
                        FROM distroutep rp \
                        INNER JOIN maxrp ma on rp.gkey = ma.gkey \
                        	and coalesce(rp.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and rp.created = ma.created \
                        WHERE rp.is_deleted = false")
routep_dynDF = DynamicFrame.fromDF(routep_distDF, glueContext, "nested")

## ref_line_operator connection
lineop_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_line_operator", transformation_ctx = "lineop_DS")
lineop_regDF = lineop_DS.toDF()
lineop_regDF.createOrReplaceTempView("distlineop")
lineop_distDF = spark.sql("SELECT DISTINCT sourcesystem, \
								max(audtdateadded) audtdateadded, \
								lineop_id, \
								dot_haz_carrier_cert_nbr, \
								bkg_unique, \
								stop_bkg_updates, \
								stop_rtg_updates, \
								bl_unique, \
								bkg_usage, \
								bkg_roll, \
								bkg_roll_cutoff, \
								bkg_adhoc, \
								bkg_is_not_validated, \
								roll_late_ctr, \
								demurrage_rules, \
								power_rules, \
								line_demurrage_rules, \
								gen_pin_nbr, \
								use_pin_nbr, \
								order_item_not_unique, \
								is_order_nbr_unique, \
								is_edo_nbr_unique_by_facility, \
								reefer_time_mon1, \
								reefer_time_mon2, \
								reefer_time_mon3, \
								reefer_time_mon4, \
								reefer_off_power_time, \
								propagate_changes, \
								flex_string01, \
								flex_string02, \
								flex_string03, \
								flex_string04, \
								flex_string05, \
								flex_string06, \
								flex_string07, \
								flex_string08, \
								flex_string09, \
								flex_string10 \
						FROM distlineop \
						WHERE is_deleted = false \
						GROUP BY sourcesystem, \
								lineop_id, \
								dot_haz_carrier_cert_nbr, \
								bkg_unique, \
								stop_bkg_updates, \
								stop_rtg_updates, \
								bl_unique, \
								bkg_usage, \
								bkg_roll, \
								bkg_roll_cutoff, \
								bkg_adhoc, \
								bkg_is_not_validated, \
								roll_late_ctr, \
								demurrage_rules, \
								power_rules, \
								line_demurrage_rules, \
								gen_pin_nbr, \
								use_pin_nbr, \
								order_item_not_unique, \
								is_order_nbr_unique, \
								is_edo_nbr_unique_by_facility, \
								reefer_time_mon1, \
								reefer_time_mon2, \
								reefer_time_mon3, \
								reefer_time_mon4, \
								reefer_off_power_time, \
								propagate_changes, \
								flex_string01, \
								flex_string02, \
								flex_string03, \
								flex_string04, \
								flex_string05, \
								flex_string06, \
								flex_string07, \
								flex_string08, \
								flex_string09, \
								flex_string10")
lineop_dynDF = DynamicFrame.fromDF(lineop_distDF, glueContext, "nested")

## ref_unloc_code connection
unloc_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "ref_unloc_code", transformation_ctx = "unloc_DS")
unloc_regDF = unloc_DS.toDF()
unloc_regDF.createOrReplaceTempView("distunloc")
unloc_distDF = spark.sql("with maxun as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM distunloc \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT un.sourcesystem, \
                        		un.gkey, \
                        		un.id, \
                        		un.reference_set, \
                        		un.cntry_code, \
                        		un.place_code, \
                        		un.place_name, \
                        		un.is_port, \
                        		un.is_rlterminal, \
                        		un.is_rdterminal, \
                        		un.is_airport, \
                        		un.is_multimodal, \
                        		un.is_fdtrans, \
                        		un.is_brcross, is_funcunknown, \
                        		un.unloc_status, \
                        		un.sub_div, \
                        		un.latitude, \
                        		un.latnors, \
                        		un.longitude, \
                        		un.lateorw, \
                        		un.remarks, \
                        		un.created, \
                        		un.creator, \
                        		un.changed,  \
                        		un.changer, \
                        		un.life_cycle_state \
                        FROM distunloc un \
                        INNER JOIN maxun ma on un.gkey = ma.gkey \
                        	and coalesce(un.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and un.created = ma.created \
                        WHERE un.is_deleted = false")
unloc_dynDF = DynamicFrame.fromDF(unloc_distDF, glueContext, "nested")

## srv_event connection
srvevt_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "srv_event", transformation_ctx = "srvevt_DS")
srvevt_regDF = srvevt_DS.toDF()
srvevt_regDF.createOrReplaceTempView("distsrvevt")
srvevt_distDF = spark.sql("with maxsrv as \
                        	( \
                        		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                        		FROM distsrvevt \
                        		GROUP BY gkey \
                        	) \
                        SELECT DISTINCT srv.sourcesystem, \
                        		srv.gkey, \
                        		srv.operator_gkey, \
                        		srv.complex_gkey, \
                        		srv.facility_gkey, \
                        		srv.yard_gkey, \
                        		srv.placed_by, \
                        		srv.placed_time, \
                        		srv.event_type_gkey, \
                        		srv.applied_to_class, \
                        		srv.applied_to_gkey, \
                        		srv.applied_to_natural_key, \
                        		srv.note, \
                        		srv.billing_extract_batch_id, \
                        		srv.quantity, \
                        		srv.quantity_unit, \
                        		srv.responsible_party, \
                        		srv.related_entity_gkey, \
                        		srv.related_entity_id, \
                        		srv.related_entity_class, \
                        		srv.related_batch_id, \
                        		srv.acknowledged, \
                        		srv.acknowledged_by, \
                        		srv.flex_string01, \
                        		srv.flex_string02, \
                        		srv.flex_string03, \
                        		srv.flex_string04, \
                        		srv.flex_string05, \
                        		srv.flex_date01, \
                        		srv.flex_date02, \
                        		srv.flex_date03, \
                        		srv.flex_double01, \
                        		srv.flex_double02, \
                        		srv.flex_double03, \
                        		srv.flex_double04, \
                        		srv.flex_double05, \
                        		srv.created, \
                        		srv.creator, \
                        		srv.changed, \
                        		srv.changer, \
                        		srv.primary_event_gkey \
                        FROM distsrvevt srv \
                        INNER JOIN maxsrv ma on srv.gkey = ma.gkey \
                        	and coalesce(srv.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                        	and srv.created = ma.created \
                        WHERE srv.is_deleted = false")
srvevt_dynDF = DynamicFrame.fromDF(srvevt_distDF, glueContext, "nested")

## srv_event_types connection
evtype_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "srv_event_types", transformation_ctx = "evtype_DS")
evtype_regDF = evtype_DS.toDF()
evtype_regDF.createOrReplaceTempView("distevtype")
evtype_distDF = spark.sql("with maxsrvet as \
                            	( \
                            		SELECT gkey, coalesce(max(changed),cast('1900-01-01' as timestamp)) changed, max(created) created \
                            		FROM distevtype \
                            		GROUP BY gkey \
                            	) \
                            SELECT DISTINCT srvet.sourcesystem, \
                            		srvet.gkey, \
                            		srvet.reference_set, \
                            		srvet.id, \
                            		srvet.description, \
                            		srvet.is_built_in, \
                            		srvet.is_bulkable, \
                            		srvet.is_facility_service, \
                            		srvet.is_billable, \
                            		srvet.is_notifiable, \
                            		srvet.is_acknowledgeable, \
                            		srvet.functional_area, \
                            		srvet.is_event_recorded, \
                            		srvet.applies_to, \
                            		srvet.filter_gkey, \
                            		srvet.is_auto_extract, \
                            		srvet.created, \
                            		srvet.creator, \
                            		srvet.changed,  \
                            		srvet.changer, \
                            		srvet.life_cycle_state \
                            FROM distevtype srvet \
                            INNER JOIN maxsrvet ma on srvet.gkey = ma.gkey \
                            	and coalesce(srvet.changed,cast('1900-01-01' as timestamp)) = ma.changed \
                            	and srvet.created = ma.created \
                            WHERE srvet.is_deleted = false")
evtype_dynDF = DynamicFrame.fromDF(evtype_distDF, glueContext, "nested")


                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################

## ref_bizunit_scoped mapping
scoped_applymapping = ApplyMapping.apply(frame = scoped_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("name", "string", "name", "string"), ("role", "string", "role", "string"), ("scac", "string", "scac", "string"), ("bic", "string", "bic", "string"), ("is_eq_owner", "long", "is_eq_owner", "long"), ("is_eq_operator", "long", "is_eq_operator", "long"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("credit_status", "string", "credit_status", "string"), ("per_unit_guarantee_limit", "float", "per_unit_guarantee_limit", "float"), ("notes", "string", "notes", "string"), ("ct_name", "string", "ct_name", "string"), ("address_line1", "string", "address_line1", "string"), ("address_line2", "string", "address_line2", "string"), ("address_line3", "string", "address_line3", "string"), ("city", "string", "city", "string"), ("state_gkey", "long", "state_gkey", "long"), ("country_code", "string", "country_code", "string"), ("mail_code", "string", "mail_code", "string"), ("telephone", "string", "telephone", "string"), ("fax", "string", "fax", "string"), ("email_address", "string", "email_address", "string"), ("sms_number", "string", "sms_number", "string"), ("website_url", "string", "website_url", "string"), ("ftp_address", "string", "ftp_address", "string"), ("bizu_gkey", "long", "bizu_gkey", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string")], transformation_ctx = "scoped_applymapping")

## ref_carrier_itinerary mapping
carit_applymapping = ApplyMapping.apply(frame = carit_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("owner_cv", "long", "owner_cv", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "carit_applymapping")

## ref_carrier_service mapping
carserv_applymapping = ApplyMapping.apply(frame = carserv_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("name", "string", "name", "string"), ("carrier_mode", "string", "carrier_mode", "string"), ("itin_gkey", "long", "itin_gkey", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("pkey", "long", "pkey", "long")], transformation_ctx = "carserv_applymapping")

## ref_country mapping
country_applymapping = ApplyMapping.apply(frame = country_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("cntry_code", "string", "cntry_code", "string"), ("cntry_alpha3_code", "string", "cntry_alpha3_code", "string"), ("cntry_num3_code", "string", "cntry_num3_code", "string"), ("cntry_name", "string", "cntry_name", "string"), ("cntry_off_name", "string", "cntry_off_name", "string"), ("cntry_life_cycle_state", "string", "cntry_life_cycle_state", "string")], transformation_ctx = "country_applymapping")

## ref_equip_type mapping
eqtype_applymapping = ApplyMapping.apply(frame = eqtype_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("data_source", "string", "data_source", "string"), ("is_deprecated", "long", "is_deprecated", "long"), ("is_archetype", "long", "is_archetype", "long"), ("archetype", "long", "archetype", "long"), ("clone_source", "long", "clone_source", "long"), ("basic_length", "string", "basic_length", "string"), ("nominal_length", "string", "nominal_length", "string"), ("nominal_height", "string", "nominal_height", "string"), ("rfr_type", "string", "rfr_type", "string"), ("class", "string", "class", "string"), ("iso_group", "string", "iso_group", "string"), ("length_mm", "long", "length_mm", "long"), ("height_mm", "long", "height_mm", "long"), ("width_mm", "long", "width_mm", "long"), ("tare_weight_kg", "float", "tare_weight_kg", "float"), ("safe_weight_kg", "float", "safe_weight_kg", "float"), ("description", "string", "description", "string"), ("pict_id", "long", "pict_id", "long"), ("milliteus", "long", "milliteus", "long"), ("uses_accessories", "long", "uses_accessories", "long"), ("is_temperature_controlled", "long", "is_temperature_controlled", "long"), ("oog_ok", "long", "oog_ok", "long"), ("unsealable", "long", "unsealable", "long"), ("has_wheels", "long", "has_wheels", "long"), ("is_open", "long", "is_open", "long"), ("fits_20", "long", "fits_20", "long"), ("fits_24", "long", "fits_24", "long"), ("fits_30", "long", "fits_30", "long"), ("fits_40", "long", "fits_40", "long"), ("fits_45", "long", "fits_45", "long"), ("fits_48", "long", "fits_48", "long"), ("fits_53", "long", "fits_53", "long"), ("is_bombcart", "long", "is_bombcart", "long"), ("is_cassette", "long", "is_cassette", "long"), ("is_nopick", "long", "is_nopick", "long"), ("is_triaxle", "long", "is_triaxle", "long"), ("is_super_freeze_reefer", "long", "is_super_freeze_reefer", "long"), ("is_controlled_atmo_reefer", "long", "is_controlled_atmo_reefer", "long"), ("no_stow_on_if_empty", "long", "no_stow_on_if_empty", "long"), ("no_stow_on_if_laden", "long", "no_stow_on_if_laden", "long"), ("must_stow_below_deck", "long", "must_stow_below_deck", "long"), ("must_stow_above_deck", "long", "must_stow_above_deck", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("teu_capacity", "long", "teu_capacity", "long"), ("slot_labels", "string", "slot_labels", "string"), ("is_2x20notallowed", "long", "is_2x20notallowed", "long"), ("events_type_to_record", "string", "events_type_to_record", "string")], transformation_ctx = "eqtype_applymapping")

## ref_equipment mapping
equip_applymapping = ApplyMapping.apply(frame = equip_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("eq_subclass", "string", "eq_subclass", "string"), ("id", "string", "id", "string"), ("scope", "long", "scope", "long"), ("data_source", "string", "data_source", "string"), ("id_prefix", "string", "id_prefix", "string"), ("id_nbr_only", "string", "id_nbr_only", "string"), ("id_check_digit", "string", "id_check_digit", "string"), ("id_full", "string", "id_full", "string"), ("class", "string", "class", "string"), ("eqtyp_gkey", "long", "eqtyp_gkey", "long"), ("length_mm", "long", "length_mm", "long"), ("height_mm", "long", "height_mm", "long"), ("width_mm", "long", "width_mm", "long"), ("iso_group", "string", "iso_group", "string"), ("tare_kg", "float", "tare_kg", "float"), ("safe_kg", "float", "safe_kg", "float"), ("owner", "long", "owner", "long"), ("operator", "long", "operator", "long"), ("prior_operator", "long", "prior_operator", "long"), ("lease_expiration", "timestamp", "lease_expiration", "timestamp"), ("build_date", "timestamp", "build_date", "timestamp"), ("no_stow_on_if_empty", "long", "no_stow_on_if_empty", "long"), ("no_stow_on_if_laden", "long", "no_stow_on_if_laden", "long"), ("must_stow_below_deck", "long", "must_stow_below_deck", "long"), ("must_stow_above_deck", "long", "must_stow_above_deck", "long"), ("p_o_s", "long", "p_o_s", "long"), ("insulated", "long", "insulated", "long"), ("material", "string", "material", "string"), ("trnsp_type", "string", "trnsp_type", "string"), ("trnsp_id", "string", "trnsp_id", "string"), ("trnsp_time", "timestamp", "trnsp_time", "timestamp"), ("uses_accessories", "long", "uses_accessories", "long"), ("is_temperature_controlled", "long", "is_temperature_controlled", "long"), ("oog_ok", "long", "oog_ok", "long"), ("unsealable", "long", "unsealable", "long"), ("has_wheels", "long", "has_wheels", "long"), ("is_open", "long", "is_open", "long"), ("strength_code", "string", "strength_code", "string"), ("csc_expiration", "string", "csc_expiration", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string"), ("x_cord", "float", "x_cord", "float"), ("y_cord", "float", "y_cord", "float"), ("z_cord", "float", "z_cord", "float"), ("rfr_type", "string", "rfr_type", "string"), ("is_starvent", "long", "is_starvent", "long"), ("is_super_freeze_reefer", "long", "is_super_freeze_reefer", "long"), ("is_controlled_atmo_reefer", "long", "is_controlled_atmo_reefer", "long"), ("is_vented", "long", "is_vented", "long"), ("tank_rails", "string", "tank_rails", "string"), ("license_nbr", "string", "license_nbr", "string"), ("license_state", "string", "license_state", "string"), ("fed_inspect_exp", "timestamp", "fed_inspect_exp", "timestamp"), ("state_inspect_exp", "timestamp", "state_inspect_exp", "timestamp"), ("axle_count", "long", "axle_count", "long"), ("is_triaxle", "long", "is_triaxle", "long"), ("fits_20", "long", "fits_20", "long"), ("fits_24", "long", "fits_24", "long"), ("fits_30", "long", "fits_30", "long"), ("fits_40", "long", "fits_40", "long"), ("fits_45", "long", "fits_45", "long"), ("fits_48", "long", "fits_48", "long"), ("fits_53", "long", "fits_53", "long"), ("door_kind", "string", "door_kind", "string"), ("tows_dolly", "long", "tows_dolly", "long"), ("semi_reefer_kind", "string", "semi_reefer_kind", "string"), ("acry_description", "string", "acry_description", "string")], transformation_ctx = "equip_applymapping")

## ref_routing_point mapping
routep_applymapping = ApplyMapping.apply(frame = routep_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("transit_to_mode", "string", "transit_to_mode", "string"), ("unloc_gkey", "long", "unloc_gkey", "long"), ("actual_pod_gkey", "long", "actual_pod_gkey", "long"), ("schedule_dcode", "string", "schedule_dcode", "string"), ("schedule_kcode", "string", "schedule_kcode", "string"), ("splc_code", "string", "splc_code", "string"), ("terminal", "string", "terminal", "string"), ("is_placeholder_point", "long", "is_placeholder_point", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("data_source", "string", "data_source", "string"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "routep_applymapping")

## ref_line_operator mapping
lineop_applymapping = ApplyMapping.apply(frame = lineop_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("audtdateadded", "timestamp", "audtdateadded", "timestamp"), ("lineop_id", "long", "lineop_id", "long"), ("dot_haz_carrier_cert_nbr", "string", "dot_haz_carrier_cert_nbr", "string"), ("bkg_unique", "long", "bkg_unique", "long"), ("stop_bkg_updates", "long", "stop_bkg_updates", "long"), ("stop_rtg_updates", "long", "stop_rtg_updates", "long"), ("bl_unique", "long", "bl_unique", "long"), ("bkg_usage", "string", "bkg_usage", "string"), ("bkg_roll", "string", "bkg_roll", "string"), ("bkg_roll_cutoff", "string", "bkg_roll_cutoff", "string"), ("bkg_adhoc", "long", "bkg_adhoc", "long"), ("bkg_is_not_validated", "long", "bkg_is_not_validated", "long"), ("roll_late_ctr", "long", "roll_late_ctr", "long"), ("demurrage_rules", "long", "demurrage_rules", "long"), ("power_rules", "long", "power_rules", "long"), ("line_demurrage_rules", "long", "line_demurrage_rules", "long"), ("gen_pin_nbr", "long", "gen_pin_nbr", "long"), ("use_pin_nbr", "long", "use_pin_nbr", "long"), ("order_item_not_unique", "long", "order_item_not_unique", "long"), ("is_order_nbr_unique", "long", "is_order_nbr_unique", "long"), ("is_edo_nbr_unique_by_facility", "long", "is_edo_nbr_unique_by_facility", "long"), ("reefer_time_mon1", "timestamp", "reefer_time_mon1", "timestamp"), ("reefer_time_mon2", "timestamp", "reefer_time_mon2", "timestamp"), ("reefer_time_mon3", "timestamp", "reefer_time_mon3", "timestamp"), ("reefer_time_mon4", "timestamp", "reefer_time_mon4", "timestamp"), ("reefer_off_power_time", "long", "reefer_off_power_time", "long"), ("propagate_changes", "long", "propagate_changes", "long"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_string06", "string", "flex_string06", "string"), ("flex_string07", "string", "flex_string07", "string"), ("flex_string08", "string", "flex_string08", "string"), ("flex_string09", "string", "flex_string09", "string"), ("flex_string10", "string", "flex_string10", "string")], transformation_ctx = "lineop_applymapping")

## ref_unloc_code mapping
unloc_applymapping = ApplyMapping.apply(frame = unloc_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("id", "string", "id", "string"), ("reference_set", "long", "reference_set", "long"), ("cntry_code", "string", "cntry_code", "string"), ("place_code", "string", "place_code", "string"), ("place_name", "string", "place_name", "string"), ("is_port", "long", "is_port", "long"), ("is_rlterminal", "long", "is_rlterminal", "long"), ("is_rdterminal", "long", "is_rdterminal", "long"), ("is_airport", "long", "is_airport", "long"), ("is_multimodal", "long", "is_multimodal", "long"), ("is_fdtrans", "long", "is_fdtrans", "long"), ("is_brcross", "long", "is_brcross", "long"), ("is_funcunknown", "long", "is_funcunknown", "long"), ("unloc_status", "string", "unloc_status", "string"), ("sub_div", "string", "sub_div", "string"), ("latitude", "string", "latitude", "string"), ("latnors", "string", "latnors", "string"), ("longitude", "string", "longitude", "string"), ("lateorw", "string", "lateorw", "string"), ("remarks", "string", "remarks", "string"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "unloc_applymapping")

## srv_event mapping
#srvevt_applymapping = ApplyMapping.apply(frame = srvevt_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("operator_gkey", "long", "operator_gkey", "long"), ("complex_gkey", "long", "complex_gkey", "long"), ("facility_gkey", "long", "facility_gkey", "long"), ("yard_gkey", "long", "yard_gkey", "long"), ("placed_by", "string", "placed_by", "string"), ("placed_time", "timestamp", "placed_time", "timestamp"), ("event_type_gkey", "long", "event_type_gkey", "long"), ("applied_to_class", "string", "applied_to_class", "string"), ("applied_to_gkey", "long", "applied_to_gkey", "long"), ("applied_to_natural_key", "string", "applied_to_natural_key", "string"), ("note", "string", "note", "string"), ("billing_extract_batch_id", "long", "billing_extract_batch_id", "long"), ("quantity", "float", "quantity", "float"), ("quantity_unit", "string", "quantity_unit", "string"), ("responsible_party", "long", "responsible_party", "long"), ("related_entity_gkey", "long", "related_entity_gkey", "long"), ("related_entity_id", "string", "related_entity_id", "string"), ("related_entity_class", "string", "related_entity_class", "string"), ("related_batch_id", "long", "related_batch_id", "long"), ("acknowledged", "timestamp", "acknowledged", "timestamp"), ("acknowledged_by", "string", "acknowledged_by", "string"), ("flex_string01", "string", "flex_string01", "string"), ("flex_string02", "string", "flex_string02", "string"), ("flex_string03", "string", "flex_string03", "string"), ("flex_string04", "string", "flex_string04", "string"), ("flex_string05", "string", "flex_string05", "string"), ("flex_date01", "timestamp", "flex_date01", "timestamp"), ("flex_date02", "timestamp", "flex_date02", "timestamp"), ("flex_date03", "timestamp", "flex_date03", "timestamp"), ("flex_double01", "float", "flex_double01", "float"), ("flex_double02", "float", "flex_double02", "float"), ("flex_double03", "float", "flex_double03", "float"), ("flex_double04", "float", "flex_double04", "float"), ("flex_double05", "float", "flex_double05", "float"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("primary_event_gkey", "long", "primary_event_gkey", "long")], transformation_ctx = "srvevt_applymapping")

## srv_event_types mapping
evtype_applymapping = ApplyMapping.apply(frame = evtype_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("gkey", "long", "gkey", "long"), ("reference_set", "long", "reference_set", "long"), ("id", "string", "id", "string"), ("description", "string", "description", "string"), ("is_built_in", "long", "is_built_in", "long"), ("is_bulkable", "long", "is_bulkable", "long"), ("is_facility_service", "long", "is_facility_service", "long"), ("is_billable", "long", "is_billable", "long"), ("is_notifiable", "long", "is_notifiable", "long"), ("is_acknowledgeable", "long", "is_acknowledgeable", "long"), ("functional_area", "string", "functional_area", "string"), ("is_event_recorded", "long", "is_event_recorded", "long"), ("applies_to", "string", "applies_to", "string"), ("filter_gkey", "long", "filter_gkey", "long"), ("is_auto_extract", "long", "is_auto_extract", "long"), ("created", "timestamp", "created", "timestamp"), ("creator", "string", "creator", "string"), ("changed", "timestamp", "changed", "timestamp"), ("changer", "string", "changer", "string"), ("life_cycle_state", "string", "life_cycle_state", "string")], transformation_ctx = "evtype_applymapping")




                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
										
## ref_bizunit_scoped datasink
scoped_datasink = glueContext.write_dynamic_frame.from_options(frame = scoped_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_bizunit_scoped"}, format = "parquet", transformation_ctx = "scoped_datasink")

## ref_carrier_itinerary datasink
carit_datasink = glueContext.write_dynamic_frame.from_options(frame = carit_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_carrier_itinerary"}, format = "parquet", transformation_ctx = "carit_datasink")

## ref_carrier_service datasink
carserv_datasink = glueContext.write_dynamic_frame.from_options(frame = carserv_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_carrier_service"}, format = "parquet", transformation_ctx = "carserv_datasink")

## ref_country datasink
country_datasink = glueContext.write_dynamic_frame.from_options(frame = country_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_country"}, format = "parquet", transformation_ctx = "country_datasink")

## ref_equip_type datasink
eqtype_datasink = glueContext.write_dynamic_frame.from_options(frame = eqtype_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_equip_type"}, format = "parquet", transformation_ctx = "eqtype_datasink")

## ref_equipment datasink
equip_datasink = glueContext.write_dynamic_frame.from_options(frame = equip_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_equipment"}, format = "parquet", transformation_ctx = "equip_datasink")

## ref_routing_point datasink
routep_datasink = glueContext.write_dynamic_frame.from_options(frame = routep_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_routing_point"}, format = "parquet", transformation_ctx = "routep_datasink")

## ref_line_operator datasink
lineop_datasink = glueContext.write_dynamic_frame.from_options(frame = lineop_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_line_operator"}, format = "parquet", transformation_ctx = "lineop_datasink")

## ref_unloc_code datasink
unloc_datasink = glueContext.write_dynamic_frame.from_options(frame = unloc_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/ref_unloc_code"}, format = "parquet", transformation_ctx = "unloc_datasink")

## srv_event datasink
#srvevt_datasink = glueContext.write_dynamic_frame.from_options(frame = srvevt_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/srv_event"}, format = "parquet", transformation_ctx = "srvevt_datasink")

## srv_event_types datasink
evtype_datasink = glueContext.write_dynamic_frame.from_options(frame = evtype_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/n4/srv_event_types"}, format = "parquet", transformation_ctx = "evtype_datasink")





job.commit()