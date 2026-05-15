"""
Lambda: Data Quality Checks

Called by Step Functions after the Silver layer is built.
Validates data quality before allowing the Gold aggregation to proceed.

Checks performed:
    1. Row count - is there enough data?
    2. Null percentage - are critical columns populated?
    3. Schema validation - do expected columns exist?
    4. Value range checks - are numeric values reasonable?
    5. Freshness - is the data recent enough?

Environment Variables:
    S3_BUCKET_SILVER     - Silver bucket to check
    SNS_ALERT_TOPIC_ARN  - SNS for alerts
    DQ_MIN_ROW_COUNT     - Minimum acceptable row count
    DQ_MAX_NULL_PERCENT  - Maximum acceptable null percentage
    DQ_FRESHNESS_HOURS   - Maximum acceptable data age in hours
"""

import os
import json
import logging
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime, timezone, timedelta

# ---- Setup logging ----
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- AWS clients ----
sns_client = boto3.client('sns')

# ---- Environment Variables ----
# Silver bucket to check
S3_BUCKET_SILVER = os.environ.get('S3_BUCKET_SILVER', '')
# SNS topic for alerts
SNS_TOPIC = os.environ.get('SNS_ALERT_TOPIC_ARN', '')

# ---- Thresholds ----
# Minimum acceptable row count per table
MIN_ROW_COUNT = int(os.environ.get('DQ_MIN_ROW_COUNT', '10'))
# Maximum acceptable null percentage per critical column
MAX_NULL_PCT = float(os.environ.get('DQ_MAX_NULL_PERCENT', '5.0'))
# Sanity check for view counts - YouTube videos can have billions of views
MAX_VIEWS = 500_000_000
# Data should be no older than this - now configurable via environment variable
FRESHNESS_HOURS = int(os.environ.get('DQ_FRESHNESS_HOURS', '48'))

# ---- Critical columns per table ----
# These columns must not have nulls above threshold
# region removed from clean_statistics as Glue ETL does not add it
# region removed from clean_reference_data as it is a partition not a data column
CRITICAL_COLUMNS = {
    'clean_statistics': [
        'video_id',
        'title',
        'channel_title',
        'views'
    ],
    'clean_reference_data': [
        'id'
    ]
}

# ---- Expected columns per table ----
# Schema validation - these columns must exist
# region not included as it is a partition column not stored inside Parquet
EXPECTED_COLUMNS = {
    'clean_statistics': [
        'video_id',
        'trending_date',
        'title',
        'channel_title',
        'category_id',
        'views',
        'likes',
        'dislikes',
        'comment_count',
        'processed_at'
    ],
    'clean_reference_data': [
        'title',
        'processed_at'
    ]
}

def send_sns_alert(subject, message):
    """
    Send SNS alert notification on quality pass or failure
    Does not raise error if SNS fails to avoid stopping pipeline
    """
    if not SNS_TOPIC:
        logger.warning("SNS_ALERT_TOPIC_ARN not set - skipping notification")
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject,
            Message=message
        )
        logger.info(f"SNS alert sent: {subject}")

    except Exception as e:
        # Do not raise - SNS failure should not stop quality checks
        logger.error(f"Failed to send SNS alert: {str(e)}")

def check_row_count(df, table_name):
    """
    Check 1: Row count
    Make sure table has enough data to be meaningful
    """
    row_count = len(df)
    passed = row_count >= MIN_ROW_COUNT

    logger.info(f"Check 1 - Row Count [{table_name}]:")
    logger.info(f"  Count:   {row_count}")
    logger.info(f"  Minimum: {MIN_ROW_COUNT}")
    logger.info(f"  Result:  {'PASSED' if passed else 'FAILED'}")

    return {
        'check': 'row_count',
        'table': table_name,
        'value': row_count,
        'threshold': MIN_ROW_COUNT,
        'passed': passed
    }

def check_null_percentage(df, table_name):
    """
    Check 2: Null percentage
    Critical columns should not have too many nulls
    """
    results = []
    columns = CRITICAL_COLUMNS.get(table_name, [])

    logger.info(f"Check 2 - Null Percentage [{table_name}]:")

    for col in columns:
        if col not in df.columns:
            # Column not found - log warning and skip
            # This happens if schema changes unexpectedly
            logger.warning(f"  Column {col} not found - skipping")
            continue

        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
        passed = null_pct <= MAX_NULL_PCT

        logger.info(f"  Column: {col}")
        logger.info(f"    Null count: {null_count}")
        logger.info(f"    Null pct:   {null_pct:.2f}%")
        logger.info(f"    Threshold:  {MAX_NULL_PCT}%")
        logger.info(f"    Result:     {'PASSED' if passed else 'FAILED'}")

        results.append({
            'check': 'null_percentage',
            'table': table_name,
            'column': col,
            'null_count': int(null_count),
            'null_pct': round(null_pct, 2),
            'threshold': MAX_NULL_PCT,
            'passed': passed
        })

    return results

def check_schema_validation(df, table_name):
    """
    Check 3: Schema validation
    Make sure all expected columns exist in the table
    """
    expected = EXPECTED_COLUMNS.get(table_name, [])
    actual_columns = list(df.columns)
    missing_columns = [col for col in expected if col not in actual_columns]
    passed = len(missing_columns) == 0

    logger.info(f"Check 3 - Schema Validation [{table_name}]:")
    logger.info(f"  Expected columns: {len(expected)}")
    logger.info(f"  Actual columns:   {len(actual_columns)}")
    logger.info(f"  Missing columns:  {missing_columns}")
    logger.info(f"  Result:           {'PASSED' if passed else 'FAILED'}")

    return {
        'check': 'schema_validation',
        'table': table_name,
        'expected_count': len(expected),
        'actual_count': len(actual_columns),
        'missing_columns': missing_columns,
        'passed': passed
    }

def check_value_ranges(df, table_name):
    """
    Check 4: Value range checks
    Numeric values should be within reasonable bounds
    Only applies to clean_statistics table
    MAX_VIEWS set to 500 million to account for viral YouTube videos
    """
    results = []

    # ---- Only applies to statistics table ----
    if table_name != 'clean_statistics':
        return results

    logger.info(f"Check 4 - Value Range Checks [{table_name}]:")

    # ---- Check views range ----
    if 'views' in df.columns:
        invalid_views = len(df[
            (df['views'] < 0) |
            (df['views'] > MAX_VIEWS)
        ])
        passed = invalid_views == 0

        logger.info(f"  Views range check:")
        logger.info(f"    Max allowed:  {MAX_VIEWS:,}")
        logger.info(f"    Invalid rows: {invalid_views}")
        logger.info(f"    Result:       {'PASSED' if passed else 'FAILED'}")

        results.append({
            'check': 'value_range',
            'table': table_name,
            'column': 'views',
            'invalid_rows': invalid_views,
            'passed': passed
        })

    # ---- Check likes range ----
    if 'likes' in df.columns:
        invalid_likes = len(df[df['likes'] < 0])
        passed = invalid_likes == 0

        logger.info(f"  Likes range check:")
        logger.info(f"    Invalid rows: {invalid_likes}")
        logger.info(f"    Result:       {'PASSED' if passed else 'FAILED'}")

        results.append({
            'check': 'value_range',
            'table': table_name,
            'column': 'likes',
            'invalid_rows': invalid_likes,
            'passed': passed
        })

    # ---- Check dislikes range ----
    if 'dislikes' in df.columns:
        invalid_dislikes = len(df[df['dislikes'] < 0])
        passed = invalid_dislikes == 0

        logger.info(f"  Dislikes range check:")
        logger.info(f"    Invalid rows: {invalid_dislikes}")
        logger.info(f"    Result:       {'PASSED' if passed else 'FAILED'}")

        results.append({
            'check': 'value_range',
            'table': table_name,
            'column': 'dislikes',
            'invalid_rows': invalid_dislikes,
            'passed': passed
        })

    return results

def check_freshness(df, table_name):
    """
    Check 5: Data freshness
    processed_at timestamp should be recent enough
    Data older than FRESHNESS_HOURS is considered stale
    FRESHNESS_HOURS configurable via DQ_FRESHNESS_HOURS environment variable
    """
    if 'processed_at' not in df.columns:
        logger.warning(f"processed_at not found in {table_name} - skipping freshness check")
        return {
            'check': 'freshness',
            'table': table_name,
            'passed': True,
            'message': 'processed_at column not found - skipped'
        }

    try:
        # ---- Get most recent processed_at ----
        latest = pd.to_datetime(df['processed_at']).max()
        now = datetime.now(timezone.utc)

        # ---- Handle timezone aware vs naive datetime ----
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)

        # ---- Calculate age in hours ----
        age_hours = (now - latest).total_seconds() / 3600
        passed = age_hours <= FRESHNESS_HOURS

        logger.info(f"Check 5 - Freshness [{table_name}]:")
        logger.info(f"  Latest processed_at: {latest}")
        logger.info(f"  Age hours:           {age_hours:.1f}")
        logger.info(f"  Threshold hours:     {FRESHNESS_HOURS}")
        logger.info(f"  Result:              {'PASSED' if passed else 'FAILED'}")

        return {
            'check': 'freshness',
            'table': table_name,
            'latest_processed_at': str(latest),
            'age_hours': round(age_hours, 1),
            'threshold_hours': FRESHNESS_HOURS,
            'passed': passed
        }

    except Exception as e:
        logger.error(f"Error checking freshness: {str(e)}")
        return {
            'check': 'freshness',
            'table': table_name,
            'passed': False,
            'message': str(e)
        }

def run_quality_checks(s3_path, table_name):
    """
    Run all quality checks for a given table
    Returns summary of all checks and overall pass/fail
    Uses column filtering and sampling to avoid Lambda memory issues
    """
    logger.info(f"{'=' * 50}")
    logger.info(f"Running quality checks for: {table_name}")
    logger.info(f"S3 path: {s3_path}")
    logger.info(f"{'=' * 50}")

    try:
        # ---- Read only needed columns to save memory ----
        # Do not read description and tags - they are large text fields
        # Not needed for quality checks
        columns_needed = [
            'video_id',
            'title',
            'channel_title',
            'category_id',
            'views',
            'likes',
            'dislikes',
            'comment_count',
            'trending_date',
            'processed_at'
        ]

        # ---- Read with column filter ----
        # Only reads needed columns from Parquet
        # Much less memory than reading all columns
        try:
            if table_name == 'clean_statistics':
                df = wr.s3.read_parquet(
                    path=s3_path,
                    dataset=True,
                    columns=columns_needed
                )
            else:
                # ---- Reference data is small - read all columns ----
                df = wr.s3.read_parquet(
                    path=s3_path,
                    dataset=True
                )
            
        except Exception:
            # ---- Fallback: read all columns if specific columns fail ----
            df = wr.s3.read_parquet(path=s3_path, dataset=True)

        total_rows = len(df)
        logger.info(f"Data loaded - total rows: {total_rows} columns: {len(df.columns)}")

        # ---- Sample if dataset is large ----
        # 10000 rows is enough for quality checks
        # Avoids Lambda OutOfMemory error on large datasets
        if total_rows > 10000:
            df = df.sample(n=10000, random_state=42)
            logger.info(f"Sampled 10000 rows from {total_rows} total for quality checks")

    except Exception as e:
        logger.error(f"Failed to read data from {s3_path}: {str(e)}")
        return {
            'table': table_name,
            'overall_passed': False,
            'error': str(e),
            'checks': []
        }

    # ---- Run all 5 checks ----
    all_checks = []

    # ---- Check 1: Row count ----
    all_checks.append(check_row_count(df, table_name))

    # ---- Check 2: Null percentage ----
    null_results = check_null_percentage(df, table_name)
    all_checks.extend(null_results)

    # ---- Check 3: Schema validation ----
    all_checks.append(check_schema_validation(df, table_name))

    # ---- Check 4: Value ranges ----
    range_results = check_value_ranges(df, table_name)
    all_checks.extend(range_results)

    # ---- Check 5: Freshness ----
    all_checks.append(check_freshness(df, table_name))

    # ---- Calculate overall result ----
    failed_checks = [c for c in all_checks if not c.get('passed', True)]
    overall_passed = len(failed_checks) == 0

    logger.info(f"{'=' * 50}")
    logger.info(f"Quality Check Summary [{table_name}]:")
    logger.info(f"  Total checks:  {len(all_checks)}")
    logger.info(f"  Passed:        {len(all_checks) - len(failed_checks)}")
    logger.info(f"  Failed:        {len(failed_checks)}")
    logger.info(f"  Overall:       {'PASSED' if overall_passed else 'FAILED'}")
    logger.info(f"{'=' * 50}")

    return {
        'table': table_name,
        'overall_passed': overall_passed,
        'total_checks': len(all_checks),
        'passed_checks': len(all_checks) - len(failed_checks),
        'failed_checks': len(failed_checks),
        'failed_details': failed_checks,
        'checks': all_checks
    }

def lambda_handler(event, context):
    """
    Main Lambda handler
    Runs quality checks on both silver tables
    Sends SNS alert with results
    Returns pass/fail for Step Functions to act on
    """
    logger.info("Starting Data Quality Checks for Silver Layer")

    # ---- Define tables to check ----
    tables_to_check = [
        {
            'name': 'clean_statistics',
            'path': f's3://{S3_BUCKET_SILVER}/youtube/clean_statistics/'
        },
        {
            'name': 'clean_reference_data',
            'path': f's3://{S3_BUCKET_SILVER}/youtube/raw_statistics_reference_data/'
        }
    ]

    # ---- Run checks for all tables ----
    all_results = []
    overall_passed = True

    for table in tables_to_check:
        result = run_quality_checks(
            s3_path=table['path'],
            table_name=table['name']
        )
        all_results.append(result)

        # ---- Track overall pipeline status ----
        if not result['overall_passed']:
            overall_passed = False

    # ---- Build summary message for SNS ----
    summary = f"""
Data Quality Check Summary
Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Overall Result: {'PASSED' if overall_passed else 'FAILED'}

Tables Checked:
"""
    for result in all_results:
        summary += f"""
Table: {result['table']}
  Overall: {'PASSED' if result['overall_passed'] else 'FAILED'}
  Total Checks: {result.get('total_checks', 0)}
  Passed: {result.get('passed_checks', 0)}
  Failed: {result.get('failed_checks', 0)}
"""
        if result.get('failed_details'):
            summary += "  Failed Details:\n"
            for failed in result['failed_details']:
                summary += f"    - {failed.get('check')}: {failed}\n"

    logger.info(summary)

    # ---- Send SNS notification ----
    send_sns_alert(
        subject=f"{'SUCCESS' if overall_passed else 'FAILED'} - Silver Layer Quality Checks",
        message=summary
    )

    # ---- Return result for Step Functions ----
    # Step Functions reads quality_check_passed to decide next step
    # TRUE  → proceed to Gold layer
    # FALSE → stop pipeline and alert team
    return {
    'statusCode': 200,
    'quality_check_passed': bool(overall_passed),
    'summary': summary
}