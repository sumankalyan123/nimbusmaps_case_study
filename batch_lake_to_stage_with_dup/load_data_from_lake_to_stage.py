from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType
import pytz
import os
import logging
import time
import json
from pyspark.sql.types import *
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')
datetime_ist = datetime.now(IST).strftime('%Y:%m:%d %H:%M:%S %Z %z')
directory = "/logs/"
file_name = os.path.basename(sys.argv[0])
log_file_name=file_name + datetime_ist + ".log"
log_file_name="logs/" + log_file_name.replace(' ',"_")
log_file = os.path.join('/home/suman/PycharmProjects/', log_file_name)

logging.basicConfig(filename=log_file, 
					format='%(asctime)s %(message)s', 
					filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


logger.info('spark session started ')

spark = (SparkSession
.builder
.master('local')
.appName('decisionapp')
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2")
.getOrCreate())
sc = spark.sparkContext



sfOptions = {
 "sfURL" : "zu02863.ap-south-1.aws.snowflakecomputing.com",
 "sfAccount" : "zu02863.ap-south-1.aws",
 "sfUser" : "coolkeonjhar",
 "sfPassword" : "Amrita@123",
 "sfDatabase" : "NIMBUS_MAPS",
 "sfSchema" : "RAW_LAYER",
 "sfWarehouse" : "SNOW_LARGE_VWH",
 "sfRole" : "ACCOUNTADMIN"
}
 
    
df = spark.read.format('snowflake') \
  .options(**sfOptions) \
  .option("query",  "select UID,ADDRESS,CITY,ZIPCODE,DECISION,CURRENT_TIMESTAMP() AS CREATE_TS from NIMBUS_MAPS.STAGING_DATA_LAKE.DECISION_DATA_STG where create_ts >(select nvl(max(CREATE_TS),to_timestamp('2001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) FROM NIMBUS_MAPS.RAW_LAYER.DECISION_DATA_RAW)") \
  .load()

print('printing count')
print(df.count())

if df.count()==0:
    print('No data to process')
    sc.stop()
    quit()
    

df.printSchema()

df_dupli = df.groupby(['UID','ADDRESS','CITY','ZIPCODE','DECISION','CREATE_TS']).count().where('count > 1').sort('count', ascending=False)

duplicates_count=df_dupli.count()

if duplicates_count>0:
    print(f'it has {duplicates_count} duplicates ,Logging duplicate Ids in log_duplicates table')
    df_dupli.select('UID','CREATE_TS').write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","NIMBUS_MAPS.RAW_LAYER.log_duplicates").mode("append").options(header=True).save()
    
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "NIMBUS_MAPS.RAW_LAYER.DECISION_DATA_RAW").mode("append").options(header=True).save()

sc.stop()
