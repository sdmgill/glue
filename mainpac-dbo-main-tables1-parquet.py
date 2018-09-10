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
## meter connection
#1st DF
meter_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "meter", transformation_ctx = "meter_DS")
meter_regDF = meter_DS.toDF()
meter_regDF = meter_regDF.filter(meter_regDF['dboperationtype'] !='D')
meter_grouped = meter_regDF.groupBy("meterid").agg({"updatetime":'max'})
meter_agg = meter_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
meter_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "meter", transformation_ctx = "meter_DS2")
meter_regDF2 = meter_DS2.toDF()
meter_regDF2 = meter_regDF2.filter(meter_regDF2['dboperationtype'] !='D')
meter_dist = meter_regDF2.dropDuplicates()
#Join
meter_join = meter_dist.join(meter_agg, (meter_dist.meterid == meter_agg.meterid) & (meter_dist.updatetime == meter_agg.updatetime)).drop(meter_agg.meterid).drop(meter_agg.updatetime)
meter_drop = meter_join.drop('dboperationtype').drop('audtdateadded')
meter_final = meter_drop.distinct()
meter_dynDF = DynamicFrame.fromDF(meter_final,glueContext,"nested")

    ###############

## asset connection
#1st DF
opass_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "operationalasset", transformation_ctx = "opass_DS")
opass_regDF = opass_DS.toDF()
opass_regDF = opass_regDF.filter(opass_regDF['dboperationtype'] !='D')
opass_grouped = opass_regDF.groupBy("operationalassetid").agg({"updatetime":'max'})
opass_agg = opass_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
opass_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "operationalasset", transformation_ctx = "opass_DS2")
opass_regDF2 = opass_DS2.toDF()
opass_regDF2 = opass_regDF2.filter(opass_regDF2['dboperationtype'] !='D')
opass_dist = opass_regDF2.dropDuplicates()
#Join
opass_join = opass_dist.join(opass_agg, (opass_dist.operationalassetid == opass_agg.operationalassetid) & (opass_dist.updatetime == opass_agg.updatetime)).drop(opass_agg.operationalassetid).drop(opass_agg.updatetime)
opass_drop = opass_join.drop('dboperationtype').drop('audtdateadded')
opass_final = opass_drop.distinct()
opass_dynDF = DynamicFrame.fromDF(opass_final,glueContext,"nested")

    ###############

## task Connection
#1st DF
task_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "task", transformation_ctx = "task_DS")
task_regDF = task_DS.toDF()
task_regDF = task_regDF.filter(task_regDF['dboperationtype'] !='D')
task_grouped = task_regDF.groupBy("taskid").agg({"updatetime":'max',"syncdatelastdoneutc":"max"})
task_agg = task_grouped.withColumnRenamed('max(updatetime)', 'updatetime').withColumnRenamed('max(syncdatelastdoneutc)', 'syncdatelastdoneutc')

#2nd DF
task_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "task", transformation_ctx = "task_DS2")
task_regDF2 = task_DS2.toDF()
task_regDF2 = task_regDF2.filter(task_regDF2['dboperationtype'] !='D')
task_dist = task_regDF2.dropDuplicates()

#Join
task_join = task_dist.join(task_agg, (task_dist.taskid == task_agg.taskid) & (task_dist.updatetime == task_agg.updatetime) & (task_dist.syncdatelastdoneutc == task_agg.syncdatelastdoneutc)).drop(task_agg.taskid).drop(task_agg.updatetime).drop(task_agg.syncdatelastdoneutc)
task_drop = task_join.drop('dboperationtype').drop('audtdateadded')
task_final = task_drop.distinct()
task_dynDF = DynamicFrame.fromDF(task_final,glueContext,"nested")

    ###############

## usage connection
#1st DF
usage_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "usage", transformation_ctx = "usage_DS")
usage_regDF = usage_DS.toDF()
usage_regDF = usage_regDF.filter(usage_regDF['dboperationtype'] !='D')
usage_grouped = usage_regDF.groupBy("usageid").agg({"updatetime":'max'})
usage_agg = usage_grouped.withColumnRenamed('max(updatetime)', 'updatetime')
#2nd DF
usage_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "usage", transformation_ctx = "usage_DS2")
usage_regDF2 = usage_DS2.toDF()
usage_regDF2 = usage_regDF2.filter(usage_regDF2['dboperationtype'] !='D')
usage_dist = usage_regDF2.dropDuplicates()
#Join
usage_join = usage_dist.join(usage_agg, (usage_dist.usageid == usage_agg.usageid) & (usage_dist.updatetime == usage_agg.updatetime)).drop(usage_agg.usageid).drop(usage_agg.updatetime)
usage_drop = usage_join.drop('dboperationtype').drop('audtdateadded')
usage_final = usage_drop.distinct()
usage_dynDF = DynamicFrame.fromDF(usage_final,glueContext,"nested")

    ###############

## workorder connection
#1st DF
worder_DS = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "workorder", transformation_ctx = "worder_DS")
worder_regDF = worder_DS.toDF()
worder_regDF = worder_regDF.filter(worder_regDF['dboperationtype'] !='D')
worder_grouped = worder_regDF.groupBy("workorderid").agg({"updatetime":'max',"estimatedstartdate":'max',"audtdateadded":'max'})
worder_agg = worder_grouped.withColumnRenamed('max(updatetime)', 'updatetime').withColumnRenamed('max(estimatedstartdate)', 'estimatedstartdate').withColumnRenamed('max(audtdateadded)', 'audtdateadded')

#2nd DF
worder_DS2 = glueContext.create_dynamic_frame.from_catalog(database = "staging_combined", table_name = "workorder", transformation_ctx = "worder_DS2")
worder_regDF2 = worder_DS2.toDF()
worder_regDF2 = worder_regDF2.filter(worder_regDF2['dboperationtype'] !='D')
worder_dist = worder_regDF2.dropDuplicates()

#Join
worder_join = worder_dist.join(worder_agg, (worder_dist.workorderid == worder_agg.workorderid) & (worder_dist.updatetime == worder_agg.updatetime) & (worder_dist.estimatedstartdate == worder_agg.estimatedstartdate) & (worder_dist.audtdateadded == worder_agg.audtdateadded)).drop(worder_agg.workorderid).drop(worder_agg.updatetime).drop(worder_agg.estimatedstartdate).drop(worder_agg.audtdateadded)
worder_drop = worder_join.drop('dboperationtype').drop('audtdateadded')
worder_final = worder_drop.distinct()
worder_dynDF = DynamicFrame.fromDF(worder_final,glueContext,"nested")

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## meter mapping
meter_applymapping = ApplyMapping.apply(frame = meter_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),  ("meterid", "long", "meterid", "long"), ("metername", "string", "metername", "string"), ("meterdescription", "string", "meterdescription", "string"), ("operationalassetid", "long", "operationalassetid", "long"), ("sequence", "long", "sequence", "long"), ("usageunitid", "long", "usageunitid", "long"), ("maximumusage", "double", "maximumusage", "double"), ("averageperiod", "double", "averageperiod", "double"), ("averageusage", "double", "averageusage", "double"), ("activeaverageusage", "double", "activeaverageusage", "double"), ("createid", "long", "createid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("updateid", "long", "updateid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("rowstatusid", "byte", "rowstatusid", "byte")], transformation_ctx = "meter_applymapping")

## operationalasset mapping
opass_applymapping = ApplyMapping.apply(frame = opass_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),  ("operationalassetid", "long", "operationalassetid", "long"), ("alternatekey", "string", "alternatekey", "string"), ("operationalassetname", "string", "operationalassetname", "string"), ("operationalassetgroup", "string", "operationalassetgroup", "string"), ("operationalassetstatus", "string", "operationalassetstatus", "string"), ("sortkeyid", "long", "sortkeyid", "long"), ("workorderid", "long", "workorderid", "long"), ("locationid", "long", "locationid", "long"), ("supplierid", "long", "supplierid", "long"), ("manufacturerid", "long", "manufacturerid", "long"), ("type1codeid", "long", "type1codeid", "long"), ("type2codeid", "long", "type2codeid", "long"), ("type3codeid", "long", "type3codeid", "long"), ("serialno", "string", "serialno", "string"), ("modelno", "string", "modelno", "string"), ("approvalid", "long", "approvalid", "long"), ("approvalno", "string", "approvalno", "string"), ("purchasecost", "double", "purchasecost", "double"), ("installcost", "double", "installcost", "double"), ("annualbudget", "double", "annualbudget", "double"), ("targetmtbf", "double", "targetmtbf", "double"), ("actualmtbf", "double", "actualmtbf", "double"), ("failures", "double", "failures", "double"), ("classvalue1", "double", "classvalue1", "double"), ("classvalue2", "double", "classvalue2", "double"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rotable", "string", "rotable", "string"), ("operationalassetcomments", "string", "operationalassetcomments", "string"), ("usageperiod", "string", "usageperiod", "string"), ("operationalassetclass1id", "long", "operationalassetclass1id", "long"), ("operationalassetclass2id", "long", "operationalassetclass2id", "long"), ("operationalassetclass3id", "long", "operationalassetclass3id", "long"), ("usagetype", "string", "usagetype", "string"), ("warrantyusage", "long", "warrantyusage", "long"), ("classtext1", "string", "classtext1", "string"), ("classtext2", "string", "classtext2", "string"), ("classtext3", "string", "classtext3", "string"), ("expirydate", "timestamp", "expirydate", "timestamp"), ("installdate", "timestamp", "installdate", "timestamp"), ("warrantydate", "timestamp", "warrantydate", "timestamp"), ("faildate", "timestamp", "faildate", "timestamp"), ("classdate1", "timestamp", "classdate1", "timestamp"), ("classdate2", "timestamp", "classdate2", "timestamp"), ("conditionrating", "double", "conditionrating", "double"), ("conditionid", "long", "conditionid", "long"), ("assessmentdate", "timestamp", "assessmentdate", "timestamp"), ("workingenvironmentid", "long", "workingenvironmentid", "long"), ("lifeestimate", "double", "lifeestimate", "double"), ("operationalassetdescription", "string", "operationalassetdescription", "string"), ("assetid", "long", "assetid", "long"), ("relatedworkorderclassid", "long", "relatedworkorderclassid", "long"), ("activeaverageusage", "double", "activeaverageusage", "double"), ("criticalityid", "long", "criticalityid", "long"), ("workmethodid", "long", "workmethodid", "long"), ("operationalviewid", "long", "operationalviewid", "long"), ("safetygroupid", "long", "safetygroupid", "long"), ("classificationid", "long", "classificationid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("customersiteid", "long", "customersiteid", "long"), ("customersitelocationid", "long", "customersitelocationid", "long"), ("serialnumberid", "long", "serialnumberid", "long"), ("labourmtd", "double", "labourmtd", "double"), ("labourytd", "double", "labourytd", "double"), ("labourtotal", "double", "labourtotal", "double"), ("materialmtd", "double", "materialmtd", "double"), ("materialytd", "double", "materialytd", "double"), ("materialtotal", "double", "materialtotal", "double"), ("othermtd", "double", "othermtd", "double"), ("otherytd", "double", "otherytd", "double"), ("othertotal", "double", "othertotal", "double"), ("outsidemtd", "double", "outsidemtd", "double"), ("outsideytd", "double", "outsideytd", "double"), ("outsidetotal", "double", "outsidetotal", "double"), ("lostprodmtd", "double", "lostprodmtd", "double"), ("lostprodytd", "double", "lostprodytd", "double"), ("lostprodtotal", "double", "lostprodtotal", "double"), ("prodmtd", "double", "prodmtd", "double"), ("prodytd", "double", "prodytd", "double"), ("prodtotal", "double", "prodtotal", "double"), ("usagedate", "timestamp", "usagedate", "timestamp"), ("allowbatchcriticalityassessment", "boolean", "allowbatchcriticalityassessment", "boolean"), ("allowbatchmaintenancestrategy", "boolean", "allowbatchmaintenancestrategy", "boolean"), ("accountid", "long", "accountid", "long"), ("photoid", "long", "photoid", "long"), ("operationalassetclass4id", "long", "operationalassetclass4id", "long"), ("operationalassetclass5id", "long", "operationalassetclass5id", "long"), ("operationalassetclass6id", "long", "operationalassetclass6id", "long"), ("operationalassetclass7id", "long", "operationalassetclass7id", "long"), ("operationalassetclass8id", "long", "operationalassetclass8id", "long"), ("operationalassetclass9id", "long", "operationalassetclass9id", "long"), ("operationalassetclass10id", "long", "operationalassetclass10id", "long"), ("gpslatitude", "double", "gpslatitude", "double"), ("gpslongitude", "double", "gpslongitude", "double"), ("mapreference", "string", "mapreference", "string"), ("mobilityreference", "string", "mobilityreference", "string"), ("barcode", "string", "barcode", "string"), ("assetclassificationstructureid", "long", "assetclassificationstructureid", "long"), ("classificationcategoryid", "long", "classificationcategoryid", "long")], transformation_ctx = "opass_applymapping")

## task Mapping
task_applymapping = ApplyMapping.apply(frame = task_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),  ("taskid", "long", "taskid", "long"), ("taskname", "string", "taskname", "string"), ("taskdescription", "string", "taskdescription", "string"), ("tasktype", "string", "tasktype", "string"), ("taskactive", "string", "taskactive", "string"), ("taskgroup", "string", "taskgroup", "string"), ("taskstatus", "string", "taskstatus", "string"), ("templateworkOrderid", "long", "templateworkOrderid", "long"), ("operationalassetid", "long", "operationalassetid", "long"), ("tolerance", "long", "tolerance", "long"), ("frequency", "long", "frequency", "long"), ("frequencytype", "string", "frequencytype", "string"), ("isolationsetid", "long", "isolationsetid", "long"), ("workmethodid", "long", "workmethodid", "long"), ("meterid", "long", "meterid", "long"), ("workprogramid", "long", "workprogramid", "long"), ("responsibilityid", "long", "responsibilityid", "long"), ("tasksynclevel", "long", "tasksynclevel", "long"), ("datelastdone", "timestamp", "datelastdone", "timestamp"), ("datelastplanned", "timestamp", "datelastplanned", "timestamp"), ("forecaststartdate", "timestamp", "forecaststartdate", "timestamp"), ("rescheduleviaoperationalasset", "string", "rescheduleviaoperationalasset", "string"), ("syncdatelastdoneutc", "timestamp", "syncdatelastdoneutc", "timestamp"), ("syncdatelastplannedutc", "timestamp", "syncdatelastplannedutc", "timestamp"), ("taskclass1id", "long", "taskclass1id", "long"), ("taskclass2id", "long", "taskclass2id", "long"), ("taskclass3id", "long", "taskclass3id", "long"), ("taskclasstext1", "string", "taskclasstext1", "string"), ("taskclasstext2", "string", "taskclasstext2", "string"), ("taskclasstext3", "string", "taskclasstext3", "string"), ("taskclassvalue1", "double", "taskclassvalue1", "double"), ("taskclassvalue2", "double", "taskclassvalue2", "double"), ("taskclassdate1", "timestamp", "taskclassdate1", "timestamp"), ("taskclassdate2", "timestamp", "taskclassdate2", "timestamp"), ("operationalviewid", "long", "operationalviewid", "long"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("validateddescriptionid", "long", "validateddescriptionid", "long"), ("accountid", "long", "accountid", "long")], transformation_ctx = "task_applymapping")

## usage mapping
usage_applymapping = ApplyMapping.apply(frame = usage_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"),  ("usageid", "long", "usageid", "long"), ("operationalassetid", "long", "operationalassetid", "long"), ("usagereading", "double", "usagereading", "double"), ("usagevalue", "double", "usagevalue", "double"), ("createtime", "timestamp", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "timestamp", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("usagetype", "string", "usagetype", "string"), ("status", "string", "status", "string"), ("usagedate", "timestamp", "usagedate", "timestamp"), ("usagedescription", "string", "usagedescription", "string"), ("meterid", "long", "meterid", "long"), ("usageltd", "double", "usageltd", "double"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("alternatekey", "string", "alternatekey", "string")], transformation_ctx = "usage_applymapping")

## workorder mapping
worder_applymapping = ApplyMapping.apply(frame = worder_dynDF, mappings = [("sourcesystem", "string", "sourcesystem", "string"), ("workorderid", "long", "workorderid", "long"), ("workordernumber", "string", "workordernumber", "string"), ("workorderstatus", "string", "workorderstatus", "string"), ("workorderdescription", "string", "workorderdescription", "string"), ("operationalassetid", "long", "operationalassetid", "long"), ("departmentid", "long", "departmentid", "long"), ("locationid", "long", "locationid", "long"), ("taskid", "long", "taskid", "long"), ("templateworkorderid", "long", "templateworkorderid", "long"), ("workorderrequestid", "long", "workorderrequestid", "long"), ("responsibilityid", "long", "responsibilityid", "long"), ("priorityid", "long", "priorityid", "long"), ("worktypeid", "long", "worktypeid", "long"), ("fault1id", "long", "fault1id", "long"), ("fault2id", "long", "fault2id", "long"), ("fault3id", "long", "fault3id", "long"), ("fault4id", "long", "fault4id", "long"), ("progressid", "long", "progressid", "long"), ("relatedworkorderclassid", "long", "relatedworkorderclassid", "long"), ("impactareaid", "long", "impactareaid", "long"), ("triggerid", "long", "triggerid", "long"), ("workprogramid", "long", "workprogramid", "long"), ("workorderreferenceid", "long", "workorderreferenceid", "long"), ("workordersourcetype", "string", "workordersourcetype", "string"), ("classificationid", "long", "classificationid", "long"), ("pegid", "long", "pegid", "long"), ("customersiteid", "long", "customersiteid", "long"), ("customersitelocationid", "long", "customersitelocationid", "long"), ("workmethodid", "long", "workmethodid", "long"), ("workorderworkmethodstatus", "string", "workorderworkmethodstatus", "string"), ("requestedid", "long", "requestedid", "long"), ("cancelledid", "long", "cancelledid", "long"), ("assignedtoid", "long", "assignedtoid", "long"), ("workordergroup", "string", "workordergroup", "string"), ("workgroupid", "long", "workgroupid", "long"), ("safetygroupid", "long", "safetygroupid", "long"), ("planninggroupid", "long", "planninggroupid", "long"), ("plannedbyid", "long", "plannedbyid", "long"), ("planningstatusid", "long", "planningstatusid", "long"), ("plannedid", "long", "plannedid", "long"), ("planneddate", "string", "planneddate", "timestamp"), ("raisedate", "string", "raisedate", "timestamp"), ("raisetime", "string", "raisetime", "string"), ("estimatedstartdate", "string", "estimatedstartdate", "timestamp"), ("estimatedstarttime", "string", "estimatedstarttime", "string"), ("requireddate", "string", "requireddate", "timestamp"), ("workplan", "string", "workplan", "string"), ("phone", "string", "phone", "string"), ("startdate", "string", "startdate", "timestamp"), ("starttime", "string", "starttime", "string"), ("issuedid", "long", "issuedid", "long"),
("issuedate", "string", "issuedate", "timestamp"), ("issuetime", "string", "issuetime", "timestamp"), ("issueemail", "string", "issueemail", "string"), ("issuemessage", "string", "issuemessage", "string"), ("closeid", "long", "closeid", "long"), ("closedate", "string", "closedate", "timestamp"), ("closeemail", "string", "closeemail", "string"), ("closemessage", "string", "closemessage", "string"), ("completeid", "long", "completeid", "long"), ("percentcomplete", "long", "percentcomplete", "long"), ("donebyid", "long", "donebyid", "long"), ("finishdate", "string", "finishdate", "timestamp"), ("finishtime", "string", "finishtime", "timestamp"), ("workdone", "string", "workdone", "string"), ("estdowntime", "double", "estdowntime", "double"), ("actdowntime", "double", "actdowntime", "double"), ("estduration", "double", "estduration", "double"), ("actduration", "double", "actduration", "double"), ("estremainingduration", "double", "estremainingduration", "double"), ("estnonworking", "double", "estnonworking", "double"), ("actnonworking", "double", "actnonworking", "double"), ("estotherhours", "double", "estotherhours", "double"), ("actotherhours", "double", "actotherhours", "double"), ("labourcost", "double", "labourcost", "double"), ("materialcost", "double", "materialcost", "double"), ("outsidecost", "double", "outsidecost", "double"), ("othercost", "double", "othercost", "double"), ("labourestimate", "double", "labourestimate", "double"), ("materialestimate", "double", "materialestimate", "double"), ("outsideestimate", "double", "outsideestimate", "double"), ("otherestimate", "double", "otherestimate", "double"), ("sundryestimate", "double", "sundryestimate", "double"), ("quotedcost", "double", "quotedcost", "double"), ("totalsales", "double", "totalsales", "double"), ("roundwo", "boolean", "roundwo", "boolean"), ("workshoponly", "boolean", "workshoponly", "boolean"), ("interfaceflag", "string", "interfaceflag", "string"), ("workorderclass1id", "long", "workorderclass1id", "long"), ("workorderclass2id", "long", "workorderclass2id", "long"), ("workorderclass3id", "long", "workorderclass3id", "long"), ("workorderclasstext1", "string", "workorderclasstext1", "string"), ("workorderclasstext2", "string", "workorderclasstext2", "string"), ("workorderclasstext3", "string", "workorderclasstext3", "string"), ("workorderclassvalue1", "double", "workorderclassvalue1", "double"), ("workorderclassvalue2", "double", "workorderclassvalue2", "double"), ("workorderclassdate1", "string", "workorderclassdate1", "timestamp"), ("workorderclassdate2", "string", "workorderclassdate2", "timestamp"),
("workorderclasstime1", "string", "workorderclasstime1", "timestamp"), ("workorderclasstime2", "string", "workorderclasstime2", "timestamp"), ("createtime", "string", "createtime", "timestamp"), ("createid", "long", "createid", "long"), ("updatetime", "string", "updatetime", "timestamp"), ("updateid", "long", "updateid", "long"), ("operationalviewid", "long", "operationalviewid", "long"), ("rowstatusid", "byte", "rowstatusid", "byte"), ("alternatekey", "string", "alternatekey", "string"), ("accountid", "long", "accountid", "long"), ("functionalfailureid", "long", "functionalfailureid", "long"), ("failuremodeid", "long", "failuremodeid", "long"), ("mobilityreference", "string", "mobilityreference", "string"), ("workorderstatuseventid", "long", "workorderstatuseventid", "long"), ("assetclassificationstructureid", "long", "assetclassificationstructureid", "long"), ("classificationcategoryid", "long", "classificationcategoryid", "long"),("plannedtime","string","plannedtime","timestamp"),("requiredtime","string","requiredtime","timestamp")], transformation_ctx = "worder_applymapping")


                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################
## meter datasink
meter_datasink = glueContext.write_dynamic_frame.from_options(frame = meter_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/meter"}, format = "parquet", transformation_ctx = "meter_datasink")

## operationalasset datasink
opass_datasink = glueContext.write_dynamic_frame.from_options(frame = opass_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/operationalasset"}, format = "parquet", transformation_ctx = "opass_datasink")

## task Datasink
task_datasink = glueContext.write_dynamic_frame.from_options(frame = task_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/task"}, format = "parquet", transformation_ctx = "task_datasink")

## usage datasink
usage_datasink = glueContext.write_dynamic_frame.from_options(frame = usage_applymapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/usage"}, format = "parquet", transformation_ctx = "usage_datasink")

## workorder datasink
worder_datasink = glueContext.write_dynamic_frame.from_options(frame = worder_dynDF, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/mainpac/workorder"}, format = "parquet", transformation_ctx = "worder_datasink")


job.commit()
