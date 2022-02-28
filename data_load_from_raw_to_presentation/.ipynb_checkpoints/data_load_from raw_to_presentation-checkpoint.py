from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext


import time
import json
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

spark = (SparkSession
.builder
.master('local')
.appName('wiki-changes-event-consumer')
# Add kafka package
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
 "sfWarehouse" : "compute_wh",
 "sfRole" : "ACCOUNTADMIN"
}
 
    
df = spark.read.format('snowflake') \
  .options(**sfOptions) \
  .option("query",  "select UID,ADDRESS,CITY,ZIPCODE,DECISION,CURRENT_TIMESTAMP() AS CREATE_TS from NIMBUS_MAPS.RAW_LAYER.DECISION_DATA_RAW where create_ts >(select nvl(max(CREATE_TS),to_timestamp('2001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) FROM nimbus_maps.presentation.decision_data_pres)") \
  .load()

print('printing count')
print(df.count())

if df.count()==0:
    print('No data to process')
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
    
df4.write.format('snowflake').options(**sfOptions).option("dbtable", "NIMBUS_MAPS.PRESENTATION.DECISION_DATA_PRES").mode("append").options(header=True).save()

sc.stop()
