import awswrangler as wr
import pandas as pd
import urllib.parse
import boto3
import json
import os

# ---- AWS clients ----
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# ---- Environment Variables ----
# Target bucket for cleansed data
BUCKET_SILVER = os.environ['BUCKET_SILVER']
# Glue catalog database name
GLUE_DB_SILVER = os.environ['GLUE_DB_SILVER']
# Glue catalog table name
GLUE_TABLE_REFERENCE = os.environ['GLUE_TABLE_REFERENCE']
# SNS topic for alerts
SNS_ALERT_TOPIC_ARN = os.environ['SNS_ALERT_TOPIC_ARN']

def send_sns_alert(subject, message):
    """
    Send SNS alert notification on success or failure
    Does not raise error if SNS fails to avoid stopping the pipeline
    """
    try:
        sns_client.publish(
            TopicArn=SNS_ALERT_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"SNS alert sent: {subject}")
        
    except Exception as e:
        # Do not raise here - SNS failure should not stop the pipeline
        print(f"Failed to send SNS alert: {str(e)}")

def lambda_handler(event, context):
    # Get the bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print(f"Processing file: {key} from bucket: {bucket}")
    
    try:
        # Extract region from the path (region=ca, region=us, etc.)
        region = key.split('/')[-2]
        
        # Detect file type
        file_extension = key.split('.')[-1].lower()
        
        # ---- Handle JSON Files Only ----
        if file_extension == 'json':
            print("JSON file detected...")
            
            # Read JSON from bronze bucket
            response = s3_client.get_object(Bucket=bucket, Key=key)
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            
            # ---- Quality Check 1: Check JSON structure is valid ----
            if 'items' not in json_content:
                raise ValueError("Invalid JSON structure - 'items' key not found")
            
            if len(json_content['items']) == 0:
                raise ValueError("JSON file is empty - no items found")
            
            print(f"Quality Check 1 Passed: Valid JSON structure with {len(json_content['items'])} items")
            
            # Extract only the useful fields
            records = []
            for item in json_content['items']:
                
                # ---- Quality Check 2: Check required fields exist ----
                if 'id' not in item or 'snippet' not in item:
                    print(f"Skipping invalid item - missing required fields: {item}")
                    continue
                
                records.append({
                    'id': item['id'],
                    'title': item['snippet']['title'],
                    'assignable': item['snippet']['assignable']
                })
            
            print("Quality Check 2 Passed: All required fields present")
            
            # Convert to dataframe
            df = pd.DataFrame(records)
            
            # ---- Transformation 1: Remove duplicates ----
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['id'])
            after_dedup = len(df)
            if before_dedup != after_dedup:
                print(f"Transformation 1: Removed {before_dedup - after_dedup} duplicate category IDs")
            else:
                print("Transformation 1 Passed: No duplicates found")
            
            # ---- Transformation 2: Standardize data types ----
            # Make sure id is always string
            df['id'] = df['id'].astype(str)
            # Make sure title is always string
            df['title'] = df['title'].astype(str)
            # Make sure assignable is always boolean
            df['assignable'] = df['assignable'].astype(bool)
            
            print("Transformation 2 Passed: Data types standardized")
            
            # ---- Transformation 3: Clean text fields ----
            # Remove leading/trailing spaces from title
            df['title'] = df['title'].str.strip()
            # Convert title to title case (e.g. "music" -> "Music")
            df['title'] = df['title'].str.title()
            
            print("Transformation 3 Passed: Text fields cleaned")
            
            # ---- Quality Check 3: Check for null values ----
            null_counts = df.isnull().sum()
            if null_counts.any():
                print(f"Warning - Null values found: {null_counts[null_counts > 0]}")
                # Drop rows with null values
                df = df.dropna()
                print(f"Null rows removed - remaining records: {len(df)}")
            else:
                print("Quality Check 3 Passed: No null values found")
            
            # ---- Quality Check 4: Make sure we still have data after cleaning ----
            if len(df) == 0:
                raise ValueError("No valid records remaining after quality checks")
            
            print(f"Quality Check 4 Passed: {len(df)} valid records ready to save")
            
            # ---- Add metadata columns ----
            # Extract region code (e.g., CA, US, GB) from path
            df['region'] = region.split('=')[-1].upper()
            # Add processing timestamp for tracking
            df['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print("Metadata added: region and processed_at columns")
            
            # ---- Save as Parquet to Silver bucket ----
            # Using environment variable BUCKET_SILVER as destination
            output_path = f's3://{BUCKET_SILVER}/youtube/raw_statistics_reference_data/{region}/'
            
            wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True
            )
            
            print(f"Successfully saved to: {output_path}")
            print(f"Final dataframe shape: {df.shape}")
            
            # ---- Send success SNS alert ----
            send_sns_alert(
                subject=f'SUCCESS - Category JSON processed for {region.split("=")[-1].upper()}',
                message=f'File: {key}\nRegion: {region}\nRecords processed: {len(df)}\nSaved to: {output_path}'
            )
        
        else:
            # CSV and other files are handled by AWS Glue
            print(f"File type {file_extension} is handled by AWS Glue - skipping")
            return {
                'statusCode': 200,
                'body': f'File {key} skipped - handled by AWS Glue'
            }
        
        return {
            'statusCode': 200,
            'body': f'Successfully converted {key} to parquet in silver bucket'
        }
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        
        # ---- Send failure SNS alert ----
        send_sns_alert(
            subject=f'FAILED - Error processing file {key.split("/")[-1]}',
            message=f'File: {key}\nError: {str(e)}'
        )
        
        raise e