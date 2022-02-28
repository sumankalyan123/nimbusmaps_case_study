from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
import pyspark.sql.functions as F
from datetime import datetime
import pytz
import os
import logging
import time
import json

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
.appName('wiki-changes-event-consumer')
# Add kafka package
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2")
.getOrCreate())
sc = spark.sparkContext

logger.info('setting up snowflake connections')

sfOptions = {
 "sfURL" : "zu02863.ap-south-1.aws.snowflakecomputing.com",
 "sfAccount" : "zu02863.ap-south-1.aws",
 "sfUser" : "coolkeonjhar",
 "sfPassword" : "Amrita@123",
 "sfDatabase" : "NIMBUS_MAPS",
 "sfSchema" : "PRESENTATION",
 "sfWarehouse" : "SNOW_LARGE_VWH",
 "sfRole" : "ACCOUNTADMIN"
}
 
logger.info('fetching latest records from RAW table')
    
df = spark.read.format('snowflake') \
  .options(**sfOptions) \
  .option("query",  "select UID,ADDRESS,CITY,ZIPCODE,DECISION,CURRENT_TIMESTAMP() AS CREATE_TS from NIMBUS_MAPS.RAW_LAYER.DECISION_DATA_RAW where create_ts >(select nvl(max(CREATE_TS),to_timestamp('2001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) FROM nimbus_maps.presentation.decision_data_pres)") \
  .load()

logger.info('fetched data from RAW Layer')

fetched_count=df.count()
logger.info(f'{fetched_count} records fetched to be processed')

logger.info('checking if record count is o')
if df.count()==0:
    print('No data to process')
    logger.info('No data to process')
    sc.stop()
    quit()
    
def isGoodDecision(decision):
    verdict = ''
    if decision.upper()=='A':
        verdict=True
    else:
        verdict=False
    return verdict

udfsomefunc = F.udf(isGoodDecision, StringType())
df3 = df.withColumn("isGoodDecision", udfsomefunc("DECISION"))
df4 = df3.select('UID','DECISION','isGoodDecision','create_ts')
cnt=df4.count()
logger.info(f'writing data to snowflake, {cnt} records inserted to resentation table')    
df4.write.format('snowflake').options(**sfOptions).option("dbtable", "NIMBUS_MAPS.PRESENTATION.DECISION_DATA_PRES").mode("append").options(header=True).save()

logger.info(f'Data processed successfully Exiting')    
sc.stop()
exit()
