import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

# ---- Setup logging ----
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- Get job arguments ----
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'silver_database',    # Silver Glue catalog database
    'gold_bucket',        # Gold S3 bucket
    'gold_database',      # Gold Glue catalog database
])

# ---- Initialize Glue and Spark context ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_silver_statistics():
    """
    Read clean statistics data from Silver Glue catalog
    This is the main video data cleaned by bronze to silver ETL
    Drops region column if exists to prevent join conflict
    """
    logger.info(f"Reading silver statistics from: {args['silver_database']}")

    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=args['silver_database'],
            table_name='clean_statistics',
            transformation_ctx='read_silver_statistics'
        )

        df = dynamic_frame.toDF()
        logger.info("Silver statistics loaded successfully")
        logger.info(df._jdf.schema().treeString())

        # ---- Drop region if exists to prevent join conflict ----
        # region is a partition column not needed for analytics
        if 'region' in df.columns:
            df = df.drop('region')
            logger.info("Dropped region column from statistics")

        return df

    except Exception as e:
        logger.error(f"Error reading silver statistics: {str(e)}")
        raise

def read_silver_reference():
    """
    Read clean reference data from Silver Glue catalog
    This is the category lookup data cleaned by Lambda
    Table name matches EXACTLY what exists in silver catalog
    Drops region column if exists to prevent join conflict
    """
    logger.info(f"Reading silver reference data from: {args['silver_database']}")

    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=args['silver_database'],
            # ---- Must match EXACTLY the table name in Glue catalog ----
            table_name='raw_statistics_reference_data',
            transformation_ctx='read_silver_reference'
        )

        df = dynamic_frame.toDF()
        logger.info("Silver reference data loaded successfully")

        # ---- Drop region if exists to prevent join conflict ----
        # region is a partition column not needed for category lookup
        if 'region' in df.columns:
            df = df.drop('region')
            logger.info("Dropped region column from reference data")

        return df

    except Exception as e:
        logger.error(f"Error reading silver reference data: {str(e)}")
        raise

def join_with_categories(df_stats, df_ref):
    """
    Join statistics with category names
    Enriches video data with human readable category names
    Instead of category_id=10 we get category_name=Music
    region already dropped from both dataframes before this step
    """
    logger.info("Joining statistics with category reference data...")

    # ---- Prepare reference data for join ----
    # Select only needed columns to avoid any remaining conflicts
    df_ref_clean = df_ref \
        .select(
            F.col('id').cast(IntegerType()).alias('ref_category_id'),
            F.col('title').alias('category_name'),
            F.col('assignable')
        ) \
        .dropDuplicates(['ref_category_id'])

    # ---- Join on category_id ----
    df_joined = df_stats.join(
        df_ref_clean,
        df_stats['category_id'] == df_ref_clean['ref_category_id'],
        how='left'
    ).drop('ref_category_id')

    logger.info("Join with categories complete")
    return df_joined

def build_trending_analytics(df):
    """
    Table 1: trending_analytics
    Daily trending summaries per category

    Answers questions like:
    - How many videos were trending per category per day?
    - What were the total views per category per day?
    - What was the average engagement per day?
    """
    logger.info("Building trending_analytics table...")

    trending = df.groupBy(
        'trending_date',
        'category_id',
        'category_name'
    ).agg(
        # ---- Video counts ----
        F.count('video_id').alias('total_videos_trending'),

        # ---- View metrics ----
        F.sum('views').alias('total_views'),
        F.avg('views').cast(DoubleType()).alias('avg_views'),
        F.max('views').alias('max_views'),

        # ---- Like metrics ----
        F.sum('likes').alias('total_likes'),
        F.avg('likes').cast(DoubleType()).alias('avg_likes'),

        # ---- Dislike metrics ----
        F.sum('dislikes').alias('total_dislikes'),

        # ---- Comment metrics ----
        F.sum('comment_count').alias('total_comments'),

        # ---- Engagement metrics ----
        F.avg('engagement_rate').cast(DoubleType()).alias('avg_engagement_rate'),

        # ---- Metadata ----
        F.current_timestamp().alias('processed_at')
    )

    logger.info("trending_analytics built successfully")
    return trending

def build_channel_analytics(df):
    """
    Table 2: channel_analytics
    Channel performance metrics

    Answers questions like:
    - Which channels trend the most?
    - Which channels get the most views overall?
    - What is the average engagement per channel?
    - Which channels are most liked?

    Using window ranking to get MOST FREQUENT category per channel
    row_number() used instead of rank() to avoid ties
    """
    logger.info("Building channel_analytics table...")

    # ---- Get most frequent category per channel ----
    # Count how many times each channel appeared in each category
    channel_category_counts = df.groupBy(
        'channel_title',
        'category_name'
    ).agg(
        F.count('*').alias('category_count')
    )

    # ---- Rank categories per channel by frequency ----
    # Window partitioned by channel ordered by count descending
    # row_number() ensures exactly one result per channel no ties
    window = Window.partitionBy('channel_title').orderBy(
        F.col('category_count').desc()
    )

    # ---- Get only rank 1 (most frequent category) ----
    primary_category = channel_category_counts \
        .withColumn('rank', F.row_number().over(window)) \
        .filter(F.col('rank') == 1) \
        .select(
            'channel_title',
            F.col('category_name').alias('primary_category')
        )

    # ---- Build main channel aggregations ----
    channel = df.groupBy(
        'channel_title'
    ).agg(
        # ---- Trending counts ----
        # How many times this channel appeared in trending
        F.count('video_id').alias('total_trending_appearances'),
        F.countDistinct('video_id').alias('unique_videos_trending'),

        # ---- View metrics ----
        F.sum('views').alias('total_views'),
        F.avg('views').cast(DoubleType()).alias('avg_views_per_video'),
        F.max('views').alias('max_views_single_video'),

        # ---- Like metrics ----
        F.sum('likes').alias('total_likes'),
        F.avg('likes').cast(DoubleType()).alias('avg_likes_per_video'),

        # ---- Dislike metrics ----
        F.sum('dislikes').alias('total_dislikes'),

        # ---- Comment metrics ----
        F.sum('comment_count').alias('total_comments'),

        # ---- Engagement metrics ----
        F.avg('engagement_rate').cast(DoubleType()).alias('avg_engagement_rate'),
        F.avg('like_dislike_ratio').cast(DoubleType()).alias('avg_like_dislike_ratio'),

        # ---- Date range ----
        F.min('trending_date').alias('first_trending_date'),
        F.max('trending_date').alias('last_trending_date'),

        # ---- Metadata ----
        F.current_timestamp().alias('processed_at')
    )

    # ---- Join with most frequent category ----
    channel = channel.join(
        primary_category,
        on='channel_title',
        how='left'
    )

    logger.info("channel_analytics built successfully")
    return channel

def build_category_analytics(df):
    """
    Table 3: category_analytics
    Category level trends over time

    Answers questions like:
    - Which categories are most popular?
    - How has category popularity changed over time?
    - Which categories get the highest engagement?
    - Which categories have the most trending videos?
    """
    logger.info("Building category_analytics table...")

    category = df.groupBy(
        'category_id',
        'category_name',
        'trending_year',
        'trending_month'
    ).agg(
        # ---- Video counts ----
        F.count('video_id').alias('total_videos_trending'),
        F.countDistinct('video_id').alias('unique_videos'),
        F.countDistinct('channel_title').alias('unique_channels'),

        # ---- View metrics ----
        F.sum('views').alias('total_views'),
        F.avg('views').cast(DoubleType()).alias('avg_views'),
        F.max('views').alias('max_views'),

        # ---- Like metrics ----
        F.sum('likes').alias('total_likes'),
        F.avg('likes').cast(DoubleType()).alias('avg_likes'),

        # ---- Dislike metrics ----
        F.sum('dislikes').alias('total_dislikes'),

        # ---- Comment metrics ----
        F.sum('comment_count').alias('total_comments'),

        # ---- Engagement metrics ----
        F.avg('engagement_rate').cast(DoubleType()).alias('avg_engagement_rate'),
        F.avg('like_dislike_ratio').cast(DoubleType()).alias('avg_like_dislike_ratio'),

        # ---- Metadata ----
        F.current_timestamp().alias('processed_at')
    )

    logger.info("category_analytics built successfully")
    return category

def write_to_gold(df, table_name, partition_cols=None):
    """
    Write analytics table to Gold bucket as Parquet
    Updates Glue catalog automatically
    Each table goes to its own folder in Gold bucket
    """
    output_path = f"s3://{args['gold_bucket']}/youtube/{table_name}/"

    logger.info(f"Writing {table_name} to: {output_path}")

    # ---- Convert to Glue DynamicFrame ----
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)

    # ---- Write with catalog update ----
    sink = glueContext.getSink(
        connection_type='s3',
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior='UPDATE_IN_DATABASE',
        partitionKeys=partition_cols if partition_cols else []
    )

    # ---- Set catalog info ----
    sink.setCatalogInfo(
        catalogDatabase=args['gold_database'],
        catalogTableName=table_name
    )

    # ---- Set format to Parquet ----
    sink.setFormat('glueparquet')

    # ---- Write data ----
    sink.writeFrame(dynamic_frame)

    logger.info(f"Successfully written {table_name} to gold")
    logger.info(f"Catalog updated: {args['gold_database']}.{table_name}")

# ======================================
# ---- Main ETL Pipeline ----
# ======================================
try:
    logger.info("=" * 50)
    logger.info("Starting Glue ETL Job: Silver >> Gold (Analytics)")
    logger.info("=" * 50)

    # ---- Step 1: Read silver data ----
    logger.info("Step 1: Reading silver statistics...")
    df_stats = read_silver_statistics()

    logger.info("Step 1: Reading silver reference data...")
    df_ref = read_silver_reference()

    # ---- Step 2: Join statistics with categories ----
    logger.info("Step 2: Joining with category names...")
    df_enriched = join_with_categories(df_stats, df_ref)

    # ---- Step 3: Build trending analytics ----
    logger.info("Step 3: Building trending analytics...")
    df_trending = build_trending_analytics(df_enriched)

    # ---- Step 4: Write trending analytics to Gold ----
    logger.info("Step 4: Writing trending analytics to Gold...")
    write_to_gold(
        df=df_trending,
        table_name='trending_analytics',
        partition_cols=['trending_date']
    )

    # ---- Step 5: Build channel analytics ----
    logger.info("Step 5: Building channel analytics...")
    df_channel = build_channel_analytics(df_enriched)

    # ---- Step 6: Write channel analytics to Gold ----
    logger.info("Step 6: Writing channel analytics to Gold...")
    write_to_gold(
        df=df_channel,
        table_name='channel_analytics',
        partition_cols=[]
    )

    # ---- Step 7: Build category analytics ----
    logger.info("Step 7: Building category analytics...")
    df_category = build_category_analytics(df_enriched)

    # ---- Step 8: Write category analytics to Gold ----
    logger.info("Step 8: Writing category analytics to Gold...")
    write_to_gold(
        df=df_category,
        table_name='category_analytics',
        partition_cols=['trending_year', 'trending_month']
    )

    logger.info("=" * 50)
    logger.info("Glue ETL Job: Silver >> Gold completed successfully!")
    logger.info("Tables created in Gold:")
    logger.info("  1. trending_analytics  - Daily trending summaries")
    logger.info("  2. channel_analytics   - Channel performance metrics")
    logger.info("  3. category_analytics  - Category level trends")
    logger.info("=" * 50)

    # ---- job.commit() only on success ----
    # Only signals success when everything completes correctly
    # Prevents incorrect job bookmarking on failure
    job.commit()

except Exception as e:
    logger.error(f"Glue ETL Job failed: {str(e)}")
    raise e