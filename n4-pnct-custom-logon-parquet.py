import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,current_timestamp,expr
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
					
## latch connection		
## get max
event_DS = glueContext.create_dynamic_frame.from_catalog(database = "parquet", table_name = "xps_ecevent", transformation_ctx = "event_DS")
event_regDF = event_DS.toDF()
event_regDF.createOrReplaceTempView("distevent")

user_DS = glueContext.create_dynamic_frame.from_catalog(database = "parquet", table_name = "xps_ecuser", transformation_ctx = "user_DS")
user_regDF = user_DS.toDF()
user_regDF.createOrReplaceTempView("distuser")

logon_DS = glueContext.create_dynamic_frame.from_catalog(database = "parquet", table_name = "logon", transformation_ctx = "logon_DS")
logon_regDF = logon_DS.toDF()
logon_regDF.createOrReplaceTempView("distlogon")

logon_distDF = spark.sql("with logs as \
                                    ( \
                                    select cast(ev.ectimestamp as date) dt, \
            								ev.che_name, \
            								us.name, \
            								case when ev.type_description = 'LGON' then ev.ectimestamp \
            								end logontime, \
            								case when ev.type_description = 'LGOF' then ev.ectimestamp \
            								end logofftime \
            						from distevent ev \
            						inner join distuser us on ev.operator_name = us.user_id \
            						where ev.ectimestamp > (select coalesce(max(logon),cast('2018-03-03'as date)) from distlogon) \
            							and ev.ectimestamp < (select cast(current_date as date)) \
            							and ev.type_description in ('LGON','LGOF') \
            							and ev.che_name not between 'A' and 'Z' \
            						), \
            				lon as \
            				        ( \
            				        select dt, \
            							che_name, \
            							name, \
            							logontime \
            						from logs \
            						where logontime is not null \
            				        ), \
            				lof as \
                					( \
                						select dt, \
                							che_name, \
                							name, \
                							logofftime \
                						from logs \
                						where logofftime is not null \
                					), \
                			closer as \
                			        ( \
                			        select lo.dt, \
                    					lo.che_name, \
                    					lo.name, \
                    					lo.logontime, \
                    					lf.logofftime, \
                    					row_number() over(partition by lo.dt,lo.che_name,lo.name,lo.logontime order by cast(lf.logofftime as long) - cast(lo.logontime as long)) wiggle \
                    				from lon lo \
                    				left join lof lf on lo.dt = lf.dt \
                    				and lo.che_name = lf.che_name \
                    				and lo.name = lf.name \
                    				and lo.logontime < lf.logofftime \
                			        ), \
                			 alltimes as \
                			        ( \
                			         select dt, \
                    					che_name, \
                    					name, \
                    					logontime, \
                    					logofftime, \
                    					row_number() over (partition by che_name order by logontime) asc_rownum, \
                    					row_number() over (partition by che_name order by logontime desc) desc_rownum \
                    				from closer \
                    				where wiggle = 1 \
            				        ), \
            				handle_boundaries as \
            				        ( \
            				        select che_name, \
                    					name, \
                    					(case when asc_rownum=1 and logontime is null then concat(cast(to_date(dt) as string),' 00:00:00') \
                    							else logontime \
                    					end) logontime, \
                    					(case when desc_rownum=1 and logofftime is null then concat(cast(to_date(date_add(dt,1)) as string),' 00:00:00') \
                    							else logofftime \
                    					end) logofftime \
                    				from alltimes \
                    				), \
                    		handle_nulls as \
                    		        ( \
                    		        select che_name, \
                    					name, \
                    					logontime, \
                    					logofftime, \
                    					coalesce(logontime,lag(cast(logofftime as timestamp) + INTERVAL -1 SECONDS)  over (partition by che_name order by logofftime)) logon, \
                    					coalesce(logofftime,lead(cast(logontime as timestamp) + INTERVAL -1 SECONDS) over (partition by che_name order by logontime)) logoff \
                    				from handle_boundaries \
            				        ) \
            				select che_name, \
            					name, \
            					logon, \
            					case when logoff < logon then (cast(logon as date) + INTERVAL 1 DAYS) \
            					  else logoff \
            					 end logoff \
            				from handle_nulls")
logon_dynDF = DynamicFrame.fromDF(logon_distDF, glueContext, "nested")							

                                        ####################################
                                        ####        MAPPING BLOCK       ####
                                        ####################################
## logon mapping										
logon_mapping = ApplyMapping.apply(frame=logon_dynDF, mappings=[("che_name", "string", "che_name", "string"),("name", "string", "name", "string"),("logon", "string", "logon", "timestamp"),("logoff", "string", "logoff", "timestamp")], transformation_ctx="logon_mapping")

                                        ####################################
                                        ####        DATASINK BLOCK      ####
                                        ####################################

## latch datasink										
logon_datasink = glueContext.write_dynamic_frame.from_options(frame = logon_mapping, connection_type = "s3", connection_options = {"path": "s3://data-lake-us-west-2-062519970039/parquet/lut/logon"}, format = "parquet", transformation_ctx = "logon_datasink")

job.commit()