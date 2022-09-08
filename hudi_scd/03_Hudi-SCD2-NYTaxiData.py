# update
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, lit, udf, current_date
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime
import logging

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Logging Setup
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.INFO)

random_udf = udf(lambda: str(int(time.time() * 1000000)), StringType())

# def compare_spark_schemas(left_schema, right_schema):
#     """
#     Compares two schemas and returns columns that are only in the left or right based on their name, datatype and nullability
#     """
#     left_columns = set(list([(f.name, f.dataType, f.nullable) for f in left_schema]))
#     right_columns = set(list([(f.name, f.dataType, f.nullable) for f in right_schema]))
#
#     left_only = left_columns - right_columns
#     right_only = right_columns - left_columns
#
#     return left_only, right_only


hudiOptions = {
    "hoodie.table.name": "nyc_hudi_tripdata_table",
    "hoodie.datasource.write.recordkey.field": "pk_col",
    "hoodie.datasource.write.precombine.field": "tpep_pickup_datetime",
    "hoodie.datasource.write.partitionpath.field": "pickup_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    'hoodie.consistency.check.enabled': 'true',
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": 'default',
    "hoodie.datasource.hive_sync.table": "nyc_hudi_tripdata_table",
    "hoodie.datasource.hive_sync.partition_fields": "pickup_date",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.bulkinsert.shuffle.parallelism": 10,
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": 10,
    "hoodie.index.type": "GLOBAL_BLOOM",
    "hoodie.bloom.index.update.partition.path": "true"
}

# Records to be changed fetch from source
initDf = spark.sql(
    "select pk_col, vendorid, nyc_trip_data_dim_key, eff_start_time, eff_end_time, is_current from default.nyc_hudi_tripdata_table where vendorid = 1 and pickup_date = CAST('2018-01-05' AS DATE) and passenger_count = 5 and ratecodeid = 2")

logging.warning("initDf")

initDf.printSchema()

# new records - Set payment type to 1
newDf = initDf.withColumn("vendorid", when(initDf.payment_type > 1, 1)) \
    .withColumn("eff_start_time", current_date()) \
    .withColumn("eff_end_time", lit('2999-12-31').cast(DateType())) \
    .withColumn("is_current", lit(True)) \
    .withColumn("nyc_trip_data_dim_key", random_udf())

logging.warning("newDf")
newDf.printSchema()

join_cond = [newDf.pk_col == initDf.pk_col,
             initDf.is_current == True]

## records to update
updatedDf = (initDf
             .join(newDf, join_cond)
             .select(initDf.pk_col,
                     initDf.eff_start_time,
                     newDf.eff_start_time.alias('eff_end_time'),
                     initDf.nyc_trip_data_dim_key)
             .withColumn('is_current', lit(False))
             )

# Expire existing source records
# updatedDf = initDf.withColumn("eff_end_time", when(initDf.payment_type > 1, datetime.today())) \
#     .withColumn("is_current", lit(False))

logging.warning("updatedDf")
updatedDf.printSchema()

# compare_spark_schemas(newDf.schema(), updatedDf.schema())
# logging.warning(compare_spark_schemas)

merged_df = newDf.unionByName(updatedDf)

# Write the updated DataFrame to Hudi table
# merged_df.write.format("org.apache.hudi") \
#     .option("hoodie.datasource.write.operation", "upsert") \
#     .options(**hudiOptions) \
#     .mode("append") \
#     .save(f"s3://olympus-dev-data-nyc-hudi-tripdata-table/hudidataset/")

logging.warning("updatedDf")
merged_df.printSchema()

job.commit()
