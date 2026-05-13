import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType,
    BooleanType, IntegerType,
    DoubleType
)

# ---- Setup logging ----
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- Get job arguments ----
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_database',
    'bronze_table',
    'bronze_path',        # ← add this
    'silver_bucket',
    'silver_database',
    'silver_table',
    'silver_path',
])

# ---- Initialize Glue and Spark context ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_bronze_data():
    """
    Read raw statistics data directly from S3
    Using spark.read for better encoding handling
    Handles KR, JP, RU files with special characters
    """
    logger.info(f"Reading bronze data from: {args['bronze_path']}")

    try:
        # ---- Read CSV files with encoding options ----
        df = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .option('multiLine', 'true') \
            .option('escape', '"') \
            .option('encoding', 'UTF-8') \
            .option('mode', 'PERMISSIVE') \
            .csv(args['bronze_path'])

        logger.info(f"Bronze data loaded - rows: {df.count()}")
        df.printSchema()
        return df

    except Exception as e:
        logger.error(f"Error reading bronze data: {str(e)}")
        raise

def parse_and_standardize(df):
    """
    Parse and standardize all columns to correct data types
    This is the core ETL transformation step
    Handles different formats from CSV and API JSON sources
    """
    logger.info("Parsing and standardizing data types...")

    # ---- Parse trending_date ----
    # Kaggle CSV format: YY.DD.MM (e.g. 17.14.11)
    # API JSON format:   YYYY-MM-DD (e.g. 2024-01-15)
    df = df.withColumn(
        'trending_date',
        F.when(
            # Detect Kaggle CSV format YY.DD.MM
            F.col('trending_date').rlike(r'^\d{2}\.\d{2}\.\d{2}$'),
            F.to_date(
                F.concat(
                    F.lit('20'),
                    F.split(F.col('trending_date'), r'\.')[0], F.lit('-'),
                    F.split(F.col('trending_date'), r'\.')[2], F.lit('-'),
                    F.split(F.col('trending_date'), r'\.')[1]
                ),
                'yyyy-MM-dd'
            )
        ).when(
            # Detect API JSON format YYYY-MM-DD
            F.col('trending_date').rlike(r'^\d{4}-\d{2}-\d{2}$'),
            F.to_date(F.col('trending_date'), 'yyyy-MM-dd')
        ).otherwise(None)
    )
    logger.info("Parsed: trending_date → DateType")

    # ---- Parse publish_time ----
    # Format: 2017-11-10T17:00:03.000Z (ISO 8601)
    df = df.withColumn(
        'publish_time',
        F.to_timestamp(F.col('publish_time'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    logger.info("Parsed: publish_time → TimestampType")

    # ---- Parse category_id ----
    # Should be integer not string
    df = df.withColumn(
        'category_id',
        F.col('category_id').cast(IntegerType())
    )
    logger.info("Parsed: category_id → IntegerType")

    # ---- Parse numeric fields ----
    # Views, likes, dislikes and comment_count should be Long (big numbers)
    df = df \
        .withColumn('views',         F.col('views').cast(LongType())) \
        .withColumn('likes',         F.col('likes').cast(LongType())) \
        .withColumn('dislikes',      F.col('dislikes').cast(LongType())) \
        .withColumn('comment_count', F.col('comment_count').cast(LongType()))
    logger.info("Parsed: views, likes, dislikes, comment_count → LongType")

    # ---- Parse boolean fields ----
    # Handle both string 'True'/'False' and actual booleans from CSV and API
    df = df \
        .withColumn(
            'comments_disabled',
            F.when(F.lower(F.col('comments_disabled').cast(StringType())) == 'true', True)
             .when(F.lower(F.col('comments_disabled').cast(StringType())) == 'false', False)
             .otherwise(None).cast(BooleanType())
        ) \
        .withColumn(
            'ratings_disabled',
            F.when(F.lower(F.col('ratings_disabled').cast(StringType())) == 'true', True)
             .when(F.lower(F.col('ratings_disabled').cast(StringType())) == 'false', False)
             .otherwise(None).cast(BooleanType())
        ) \
        .withColumn(
            'video_error_or_removed',
            F.when(F.lower(F.col('video_error_or_removed').cast(StringType())) == 'true', True)
             .when(F.lower(F.col('video_error_or_removed').cast(StringType())) == 'false', False)
             .otherwise(None).cast(BooleanType())
        )
    logger.info("Parsed: comments_disabled, ratings_disabled, video_error_or_removed → BooleanType")

    # ---- Keep string fields as clean strings ----
    df = df \
        .withColumn('video_id',       F.col('video_id').cast(StringType())) \
        .withColumn('title',          F.col('title').cast(StringType())) \
        .withColumn('channel_title',  F.col('channel_title').cast(StringType())) \
        .withColumn('tags',           F.col('tags').cast(StringType())) \
        .withColumn('thumbnail_link', F.col('thumbnail_link').cast(StringType())) \
        .withColumn('description',    F.col('description').cast(StringType()))
    logger.info("Parsed: string fields → StringType")

    # ---- Derive extra useful columns ----
    # Engagement rate = (likes + dislikes + comments) / views * 100
    df = df.withColumn(
        'engagement_rate',
        F.when(
            F.col('views') > 0,
            F.round(
                (F.col('likes') + F.col('dislikes') + F.col('comment_count')) /
                F.col('views') * 100,
                4
            )
        ).otherwise(0.0).cast(DoubleType())
    )
    logger.info("Derived: engagement_rate column added")

    # ---- Like to dislike ratio ----
    df = df.withColumn(
        'like_dislike_ratio',
        F.when(
            F.col('dislikes') > 0,
            F.round(F.col('likes') / F.col('dislikes'), 4)
        ).otherwise(None).cast(DoubleType())
    )
    logger.info("Derived: like_dislike_ratio column added")

    # ---- Extract year and month from trending_date ----
    df = df \
        .withColumn('trending_year',  F.year(F.col('trending_date'))) \
        .withColumn('trending_month', F.month(F.col('trending_date')))
    logger.info("Derived: trending_year and trending_month columns added")

    logger.info("Parsing and standardization complete")
    return df

def apply_data_quality_checks(df):
    """
    Apply data quality checks with row level flagging
    Flags bad rows first then removes them
    Logs how many rows passed and failed per check
    """
    logger.info("Applying data quality checks...")

    # ---- Flag 1: Missing critical fields ----
    df = df.withColumn(
        'dq_missing_critical_fields',
        F.when(
            F.col('video_id').isNull() |
            F.col('title').isNull() |
            F.col('category_id').isNull(),
            True
        ).otherwise(False)
    )

    # ---- Flag 2: Invalid numeric values ----
    # Views, likes, dislikes and comments should never be negative
    df = df.withColumn(
        'dq_invalid_numeric_values',
        F.when(
            (F.col('views') < 0) |
            (F.col('likes') < 0) |
            (F.col('dislikes') < 0) |
            (F.col('comment_count') < 0),
            True
        ).otherwise(False)
    )

    # ---- Flag 3: Video error or removed ----
    df = df.withColumn(
        'dq_video_unavailable',
        F.when(
            F.col('video_error_or_removed') == True,
            True
        ).otherwise(False)
    )

    # ---- Flag 4: Empty title ----
    df = df.withColumn(
        'dq_empty_title',
        F.when(
            (F.col('title').isNull()) |
            (F.trim(F.col('title')) == ''),
            True
        ).otherwise(False)
    )

    # ---- Flag 5: Invalid date ----
    # trending_date should not be null after parsing
    df = df.withColumn(
        'dq_invalid_date',
        F.when(
            F.col('trending_date').isNull(),
            True
        ).otherwise(False)
    )

    # ---- Flag 6: Invalid category_id ----
    # category_id should be a positive integer
    df = df.withColumn(
        'dq_invalid_category',
        F.when(
            F.col('category_id').isNull() |
            (F.col('category_id') <= 0),
            True
        ).otherwise(False)
    )

    # ---- Overall quality flag ----
    # True means row passed all quality checks
    df = df.withColumn(
        'dq_passed',
        F.when(
            (F.col('dq_missing_critical_fields') == False) &
            (F.col('dq_invalid_numeric_values') == False) &
            (F.col('dq_video_unavailable') == False) &
            (F.col('dq_empty_title') == False) &
            (F.col('dq_invalid_date') == False) &
            (F.col('dq_invalid_category') == False),
            True
        ).otherwise(False)
    )

    # ---- Log quality check results ----
    total_rows = df.count()
    passed_rows = df.filter(F.col('dq_passed') == True).count()
    failed_rows = total_rows - passed_rows

    logger.info(f"Quality Check Results:")
    logger.info(f"  Total rows:              {total_rows}")
    logger.info(f"  Passed rows:             {passed_rows}")
    logger.info(f"  Failed rows:             {failed_rows}")
    logger.info(f"  Missing critical fields: {df.filter(F.col('dq_missing_critical_fields') == True).count()}")
    logger.info(f"  Invalid numeric values:  {df.filter(F.col('dq_invalid_numeric_values') == True).count()}")
    logger.info(f"  Video unavailable:       {df.filter(F.col('dq_video_unavailable') == True).count()}")
    logger.info(f"  Empty titles:            {df.filter(F.col('dq_empty_title') == True).count()}")
    logger.info(f"  Invalid dates:           {df.filter(F.col('dq_invalid_date') == True).count()}")
    logger.info(f"  Invalid categories:      {df.filter(F.col('dq_invalid_category') == True).count()}")

    # ---- Keep only rows that passed quality checks ----
    df_clean = df.filter(F.col('dq_passed') == True)

    # ---- Drop quality flag columns before saving ----
    df_clean = df_clean.drop(
        'dq_missing_critical_fields',
        'dq_invalid_numeric_values',
        'dq_video_unavailable',
        'dq_empty_title',
        'dq_invalid_date',
        'dq_invalid_category',
        'dq_passed'
    )

    logger.info("Data quality checks complete")
    return df_clean

def apply_deduplication(df):
    """
    Remove duplicate videos
    Same video can appear multiple times across different ingestions
    Keep the most recent record based on trending_date
    """
    logger.info("Applying deduplication...")

    before_count = df.count()

    # ---- Deduplicate keeping latest trending date per video ----
    df = df.orderBy(F.col('trending_date').desc()) \
           .dropDuplicates(['video_id'])

    after_count = df.count()
    removed = before_count - after_count

    logger.info(f"Deduplication complete:")
    logger.info(f"  Before:  {before_count} rows")
    logger.info(f"  After:   {after_count} rows")
    logger.info(f"  Removed: {removed} duplicate rows")

    return df

def apply_transformations(df):
    """
    Apply data cleaning and standardization transformations
    """
    logger.info("Applying transformations...")

    # ---- Transformation 1: Clean text fields ----
    # Remove leading/trailing spaces
    df = df \
        .withColumn('title',         F.trim(F.col('title'))) \
        .withColumn('channel_title', F.trim(F.col('channel_title'))) \
        .withColumn('description',   F.trim(F.col('description')))
    logger.info("Transformation 1 Passed: Text fields cleaned")

    # ---- Transformation 2: Standardize tags format ----
    # Remove quotes from tags field
    df = df.withColumn(
        'tags',
        F.regexp_replace(F.col('tags'), '"', '')
    )
    logger.info("Transformation 2 Passed: Tags standardized")

    # ---- Transformation 3: Fill null numeric values with 0 ----
    df = df.fillna(0, subset=['views', 'likes', 'dislikes', 'comment_count'])
    logger.info("Transformation 3 Passed: Null numeric values filled")

    # ---- Transformation 4: Fill null text values with empty string ----
    df = df.fillna('', subset=['tags', 'description', 'thumbnail_link'])
    logger.info("Transformation 4 Passed: Null text values filled")

    # ---- Transformation 5: Standardize region to lowercase ----
    # Ensures consistent format like region=ca not region=CA
    if 'region' in df.columns:
        df = df.withColumn('region', F.lower(F.col('region')))
    logger.info("Transformation 5 Passed: Region standardized to lowercase")

    # ---- Transformation 6: Add metadata columns ----
    # Add processing timestamp for tracking
    df = df.withColumn('processed_at', F.current_timestamp())
    logger.info("Transformation 6 Passed: Metadata columns added")

    logger.info("All transformations complete")
    return df

def write_to_silver(df):
    """
    Write clean data to silver bucket as Parquet
    Partitioned by category_id for better query performance
    """
    logger.info(f"Writing to silver path: {args['silver_path']}")

    # ---- Convert back to Glue DynamicFrame for writing ----
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, 'silver_output')

    # ---- Write as Parquet partitioned by category_id ----
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type='s3',
        connection_options={
            'path': args['silver_path'],
            'partitionKeys': ['category_id']
        },
        format='parquet',
        transformation_ctx='write_silver'
    )

    logger.info(f"Successfully written to: {args['silver_path']}")
    logger.info(f"Total records written: {df.count()}")

# ======================================
# ---- Main ETL Pipeline ----
# ======================================
try:
    logger.info("=" * 50)
    logger.info("Starting Glue ETL Job: Bronze >> Silver (Statistics)")
    logger.info("=" * 50)

    # ---- Step 1: Read raw data from bronze ----
    logger.info("Step 1: Reading bronze data...")
    df = read_bronze_data()

    # ---- Step 2: Parse and standardize data types ----
    logger.info("Step 2: Parsing and standardizing data types...")
    df = parse_and_standardize(df)

    # ---- Step 3: Apply data quality checks ----
    logger.info("Step 3: Applying data quality checks...")
    df = apply_data_quality_checks(df)

    # ---- Step 4: Remove duplicates ----
    logger.info("Step 4: Applying deduplication...")
    df = apply_deduplication(df)

    # ---- Step 5: Apply transformations ----
    logger.info("Step 5: Applying transformations...")
    df = apply_transformations(df)

    # ---- Step 6: Write to silver ----
    logger.info("Step 6: Writing to silver bucket...")
    write_to_silver(df)

    logger.info("=" * 50)
    logger.info("Glue ETL Job completed successfully!")
    logger.info("=" * 50)

except Exception as e:
    logger.error(f"Glue ETL Job failed: {str(e)}")
    raise e

finally:
    # ---- Always commit the job at the end ----
    job.commit()