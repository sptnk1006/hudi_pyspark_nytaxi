# update
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, unix_timestamp, lit, \
    udf
# from pyspark.sql import functions as f
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
import time
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

random_udf = udf(lambda: str(int(time.time() * 1000000)), StringType())
end_date_str = '2999-12-31'
end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

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

inputDf = spark.read.schema(yellow_tripdata_schema) \
    .option("header", "true") \
    .csv("s3://nyc-tlc/trip data/yellow_tripdata_{2018}*.csv") \
    .withColumn("pk_col", monotonically_increasing_id() + 1) \
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd")) \
    .withColumn("YYYY", to_date(col("tpep_pickup_datetime"), "yyyy")) \
    .withColumn("MM", to_date(col("tpep_pickup_datetime"), "MM")) \
    .withColumn("DD", to_date(col("tpep_pickup_datetime"), "dd")) \
    .withColumn("nyc_trip_data_dim_key", random_udf()) \
    .withColumn("is_current", lit(True)) \
    .withColumn("eff_start_time", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd")) \
    .withColumn("eff_end_time", lit('2999-12-31').cast(DateType()))

inputDf = inputDf.filter(col("tpep_pickup_datetime") < unix_timestamp(lit('2018-02-01 00:00:00')).cast('timestamp'))

hudiOptions = {
    "hoodie.table.name": "nyc_hudi_tripdata_table",
    "hoodie.datasource.write.recordkey.field": "pk_col",
    "hoodie.datasource.write.precombine.field": "tpep_pickup_datetime",
    "hoodie.datasource.write.partitionpath.field": "pickup_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    'hoodie.consistency.check.enabled': 'true',
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "nyc_hudi_tripdata_table",
    "hoodie.datasource.hive_sync.partition_fields": "pickup_date",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.bulkinsert.shuffle.parallelism": 10,
    "hoodie.datasource.write.operation": "insert"
}

# Write a DataFrame as a Hudi dataset
inputDf.write.format("org.apache.hudi") \
    .options(**hudiOptions) \
    .mode("overwrite") \
    .save(f"s3://olympus-dev-data-nyc-hudi-tripdata-table/hudidataset/")

job.commit()