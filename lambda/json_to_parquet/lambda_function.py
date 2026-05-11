import awswrangler as wr
import pandas as pd
import urllib.parse
import boto3
import json

s3_client = boto3.client('s3')

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
        
        silver_bucket = 'youtube-silver-may-202'
        
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
            
            print(f"Quality Check 2 Passed: All required fields present")
            
            # Convert to dataframe
            df = pd.DataFrame(records)
            
            # ---- Transformation 1: Clean and standardize ----
            # Remove duplicates
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['id'])
            after_dedup = len(df)
            if before_dedup != after_dedup:
                print(f"Transformation 1: Removed {before_dedup - after_dedup} duplicate category IDs")
            else:
                print("Transformation 1 Passed: No duplicates found")
            
            # ---- Transformation 2: Data type checks ----
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
            df['region'] = region.split('=')[-1].upper()  # e.g., CA, US, GB
            df['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"Metadata added: region and processed_at columns")
            
            # Save as parquet to silver bucket
            output_path = f's3://{silver_bucket}/youtube/raw_statistics_reference_data/{region}/'
            
            wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True
            )
            
            print(f"Successfully saved to: {output_path}")
            print(f"Final dataframe shape: {df.shape}")
        
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
        raise e

##----------------------------------------------
# import awswrangler as wr
# import pandas as pd
# import urllib.parse
# import boto3
# import json

# s3_client = boto3.client('s3')

# def lambda_handler(event, context):
#     # Get the bucket and key from the event
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
#     print(f"Processing file: {key} from bucket: {bucket}")
    
#     try:
#         # Extract region from the path (region=ca, region=us, etc.)
#         region = key.split('/')[-2]
        
#         # Detect file type
#         file_extension = key.split('.')[-1].lower()
        
#         silver_bucket = 'youtube-silver-may-202'
        
#         # ---- Handle CSV Files ----
#         if file_extension == 'csv':
#             print("CSV file detected...")
            
#             df = wr.s3.read_csv(f's3://{bucket}/{key}')
            
#             output_path = f's3://{silver_bucket}/youtube/raw_statistics/{region}/'
            
#             wr.s3.to_parquet(
#                 df=df,
#                 path=output_path,
#                 dataset=True,
#                 partition_cols=['category_id']
#             )
            
#             print(f"CSV converted to parquet at: {output_path}")
        
#         # ---- Handle JSON Files ----
#         elif file_extension == 'json':
#             print("JSON file detected...")
            
#             response = s3_client.get_object(Bucket=bucket, Key=key)
#             json_content = json.loads(response['Body'].read().decode('utf-8'))
            
#             records = []
#             for item in json_content['items']:
#                 records.append({
#                     'id': item['id'],
#                     'title': item['snippet']['title'],
#                     'assignable': item['snippet']['assignable']
#                 })
            
#             df = pd.DataFrame(records)
            
#             output_path = f's3://{silver_bucket}/youtube/raw_statistics_reference_data/{region}/'
            
#             wr.s3.to_parquet(
#                 df=df,
#                 path=output_path,
#                 dataset=True
#             )
            
#             print(f"JSON converted to parquet at: {output_path}")
        
#         # ---- Handle Parquet Files (already in correct format) ----
#         elif file_extension == 'parquet':
#             print("Parquet file detected - copying directly to silver bucket...")
            
#             # Just copy directly to silver bucket no conversion needed
#             file_name = key.split('/')[-1]
            
#             # Detect if it is statistics or reference data based on path
#             if 'raw_statistics_reference_data' in key:
#                 output_key = f'youtube/raw_statistics_reference_data/{region}/{file_name}'
#             else:
#                 output_key = f'youtube/raw_statistics/{region}/{file_name}'
            
#             # Copy directly without reading or converting
#             s3_client.copy_object(
#                 CopySource={'Bucket': bucket, 'Key': key},
#                 Bucket=silver_bucket,
#                 Key=output_key
#             )
            
#             print(f"Parquet file copied directly to: s3://{silver_bucket}/{output_key}")
        
#         else:
#             print(f"Unsupported file type: {file_extension}")
#             return {
#                 'statusCode': 400,
#                 'body': f'Unsupported file type: {file_extension}'
#             }
        
#         return {
#             'statusCode': 200,
#             'body': f'Successfully processed {key}'
#         }
        
#     except Exception as e:
#         print(f"Error processing file: {str(e)}")
#         raise e