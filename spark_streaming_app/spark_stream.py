from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType

from pyspark.sql.types import *
import time
import json

kafka_topic_name='DecisionTopic'

kafka_bootstrap_servers = 'localhost:9092'

# Spark session & context
spark = (SparkSession
.builder
.master('local')
.appName('wiki-changes-event-consumer')
# Add kafka package
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2")
.getOrCreate())
sc = spark.sparkContext

print('completed')

df = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", kafka_bootstrap_servers) # kafka server
.option("subscribe", kafka_topic_name) # topic
.option("startingOffsets", "latest") # start from beginning
.load())

print('completed')
df.printSchema()
df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")

decision_data_schema = StructType().add("uid",StringType()) \
                        .add("address",StringType()) \
                        .add("city",StringType())  \
                        .add("zipcode",StringType()) \
                        .add("decision",StringType())


sfOptions = {
 "sfURL" : "zu02863.ap-south-1.aws.snowflakecomputing.com",
 "sfAccount" : "zu02863.ap-south-1.aws",
 "sfUser" : "coolkeonjhar",
 "sfPassword" : "Amrita@123",
 "sfDatabase" : "NIMBUS_MAPS",
 "sfSchema" : "STAGING_DATA_LAKE",
 "sfWarehouse" : "SNOW_LARGE_VWH",
 "sfRole" : "ACCOUNTADMIN"
}
 
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df2 = df1\
        .select(from_json(col("value"), decision_data_schema)\
        .alias("decisiondata"), "timestamp")

df3 = df2.select("decisiondata.*", "timestamp")


def writeToSnowflake(df_in,epochId):
    try:
        df_in.write.format("snowflake").options(**sfOptions).option("dbtable", "NIMBUS_MAPS.STAGING_DATA_LAKE.DECISION_DATA_STG").mode("append").options(header=True).save()
        print("Stream Data Processing Application Completed.")
    except:
        logger.info(' record could not be processed df_in[UID][0]}')
        
snowflake_write_stream = ( df3.writeStream.trigger(processingTime='6 seconds').foreachBatch(writeToSnowflake).start())
    

print("Stream Data Processing Application Completed.")


snowflake_write_stream.awaitTermination()
sc.stop()