# update
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when,unix_timestamp, lit
# from pyspark.sql import functions as f
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config(
    'spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

yellow_tripdata_schema = StructType(
    [StructField("vendorid", IntegerType(), True),
     StructField("tpep_pickup_datetime", TimestampType(), True),
     StructField("tpep_dropoff_datetime", TimestampType(), True),
     StructField("passenger_count", IntegerType(), True),
     StructField("trip_distance", DoubleType(), True),
     StructField("ratecodeid", IntegerType(), True),
     StructField("store_and_fwd_flag", StringType(), True),
     StructField("pulocationid", IntegerType(), True),
     StructField("dolocationid", IntegerType(), True),
     StructField("payment_type", IntegerType(), True),
     StructField("fare_amount", DoubleType(), True),
     StructField("extra", DoubleType(), True),
     StructField("mta_tax", DoubleType(), True),
     StructField("tip_amount", DoubleType(), True),
     StructField("tolls_amount", DoubleType(), True),
     StructField("improvement_surcharge", DoubleType(), True),
     StructField("total_amount", DoubleType(), True),
     StructField("congestion_surcharge", DoubleType(), True),
     StructField("pk_col", LongType(), True)])

inputDf = spark.read.schema(yellow_tripdata_schema)\
    .option("header", "true")\
    .csv("s3://nyc-tlc/trip data/yellow_tripdata_{2018}*.csv")\
    .withColumn("pk_col", monotonically_increasing_id() + 1)\
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd"))\
    .withColumn("YYYY", to_date(col("tpep_pickup_datetime"), "yyyy"))\
    .withColumn("MM", to_date(col("tpep_pickup_datetime"), "MM"))\
    .withColumn("DD", to_date(col("tpep_pickup_datetime"), "dd"))

inputDf = inputDf.filter(col("tpep_pickup_datetime") < unix_timestamp(lit('2018-02-01 00:00:00')).cast('timestamp'))


# inputDf = inputDf.filter((col("pickup_date") >= "2018-01-01") && (col("pickup_date") < "2018-02-01"))
# inputDf = inputDf.filter(col("tpep_pickup_datetime") < unix_timestamp(lit('2018-02-01 00:00:00')).cast('timestamp'))


# Specify common DataSourceWriteOptions in the single hudiOptions variable
hudiOptions = {
    "hoodie.table.name": "ny_yellow_trip_data",
    "hoodie.datasource.write.recordkey.field": "pk_col",
    "hoodie.datasource.write.precombine.field": "tpep_pickup_datetime",
    "hoodie.datasource.write.partitionpath.field": "pickup_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.table": "nyc_hudi_tripdata_table",
    "hoodie.datasource.hive_sync.partition_fields": "pickup_date",
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    "hoodie.bulkinsert.shuffle.parallelism": 10,
    "hoodie.datasource.write.operation": "bulk_insert"
}

# Write a DataFrame as a Hudi dataset
inputDf.write.format("org.apache.hudi")\
    .option("hoodie.datasource.write.operation", "insert")\
    .options(**hudiOptions)\
    .mode("overwrite")\
    .save(f"s3://olympus-dev-data-nyc-hudi-tripdata-table/hudidataset/")

glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(inputDf, glueContext, "inputDf"),
                                             connection_type="marketplace.spark", connection_options=combinedConf)
job.commit()