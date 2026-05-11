import json
import os
import logging
import urllib.request
import urllib.error
import urllib.parse
import boto3
from datetime import datetime, timezone

# ---- Setup logging ----
# Better than print() for production Lambda
# Shows timestamps and log levels automatically
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- AWS clients ----
# S3 client to save data to Bronze bucket
s3_client = boto3.client('s3')
# SNS client to send notifications on success or failure
sns_client = boto3.client('sns')

# ---- Environment variables ----
# These are set in Lambda configuration not hardcoded for security
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
# Split by comma to get list of regions
REGIONS = os.environ['YOUTUBE_REGIONS'].split(',')

# ---- Regions to fetch data for ----
# Each region will get its own file in Bronze bucket
# REGIONS = ['CA', 'US', 'GB', 'DE', 'FR', 'IN', 'JP', 'KR', 'MX', 'RU']
# Split by comma to get list of regions
REGIONS = os.environ['YOUTUBE_REGIONS'].split(',')

def get_youtube_trending(region_code):
    """
    Call YouTube API to get trending videos for a specific region
    Returns JSON data or raises an error
    """
    # Build the API URL with parameters
    base_url = 'https://www.googleapis.com/youtube/v3/videos'
    params = urllib.parse.urlencode({
        'part': 'snippet,statistics,contentDetails',
        'chart': 'mostPopular',
        'regionCode': region_code,
        'maxResults': 50,  # maximum allowed per request
        'key': YOUTUBE_API_KEY
    })
    
    url = f'{base_url}?{params}'
    logger.info(f"Calling YouTube API for region: {region_code}")
    
    try:
        # Make the API request
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode('utf-8'))
            logger.info(f"Successfully fetched {len(data.get('items', []))} videos for {region_code}")
            return data
            
    except urllib.error.HTTPError as e:
        # HTTP errors like 403 forbidden, 404 not found etc
        logger.error(f"HTTP Error for region {region_code}: {e.code} - {e.reason}")
        raise
    except urllib.error.URLError as e:
        # Network errors like no internet connection
        logger.error(f"URL Error for region {region_code}: {e.reason}")
        raise

def get_youtube_categories(region_code):
    """
    Call YouTube API to get video categories for a specific region
    This is the JSON file like CA_category_id.json you already have
    """
    base_url = 'https://www.googleapis.com/youtube/v3/videoCategories'
    params = urllib.parse.urlencode({
        'part': 'snippet',
        'regionCode': region_code,
        'key': YOUTUBE_API_KEY
    })
    
    url = f'{base_url}?{params}'
    logger.info(f"Calling YouTube Categories API for region: {region_code}")
    
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode('utf-8'))
            logger.info(f"Successfully fetched categories for {region_code}")
            return data
            
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP Error fetching categories for {region_code}: {e.code} - {e.reason}")
        raise
    except urllib.error.URLError as e:
        logger.error(f"URL Error fetching categories for {region_code}: {e.reason}")
        raise

def save_to_s3(data, bucket, key):
    """
    Save JSON data to S3 Bronze bucket
    """
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Successfully saved to s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise

def send_sns_notification(subject, message, success=True):
    """
    Send SNS notification on success or failure
    """
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"SNS notification sent: {subject}")
        
    except Exception as e:
        # Do not raise here - notification failure should not fail the whole pipeline
        logger.error(f"Failed to send SNS notification: {str(e)}")

def lambda_handler(event, context):
    """
    Main Lambda handler
    Fetches trending videos and categories for all regions
    Saves raw JSON to Bronze S3 bucket
    """
    # ---- Timestamp for file naming ----
    # Using UTC timezone for consistency
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    logger.info(f"Starting YouTube data ingestion for date: {timestamp}")
    
    # Track results for SNS notification
    successful_regions = []
    failed_regions = []
    
    for region in REGIONS:
        try:
            region_lower = region.lower()
            
            # ---- Fetch and save trending videos ----
            videos_data = get_youtube_trending(region)
            videos_key = f'youtube/raw_statistics/region={region_lower}/{region}videos_{timestamp}.json'
            save_to_s3(videos_data, BRONZE_BUCKET, videos_key)
            
            # ---- Fetch and save categories ----
            categories_data = get_youtube_categories(region)
            categories_key = f'youtube/raw_statistics_reference_data/region={region_lower}/{region}_category_id_{timestamp}.json'
            save_to_s3(categories_data, BRONZE_BUCKET, categories_key)
            
            successful_regions.append(region)
            logger.info(f"Completed ingestion for region: {region}")
            
        except Exception as e:
            logger.error(f"Failed ingestion for region {region}: {str(e)}")
            failed_regions.append(region)
            # Continue with other regions even if one fails
            continue
    
    # ---- Build summary ----
    summary = f"""
    YouTube Data Ingestion Summary - {timestamp}
    
    Successful Regions ({len(successful_regions)}): {', '.join(successful_regions)}
    Failed Regions ({len(failed_regions)}): {', '.join(failed_regions) if failed_regions else 'None'}
    """
    
    logger.info(summary)
    
    # ---- Send SNS notification ----
    if failed_regions:
        send_sns_notification(
            subject=f'YouTube Ingestion - Partial Success {timestamp}',
            message=summary,
            success=False
        )
    else:
        send_sns_notification(
            subject=f'YouTube Ingestion - Success {timestamp}',
            message=summary,
            success=True
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'date': timestamp,
            'successful_regions': successful_regions,
            'failed_regions': failed_regions
        })
    }