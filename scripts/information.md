Bronze Bucket Name >> youtube-bronze-may-202 
Silver Bucket Name >> youtube-silver-may-202 
Gold Bucket Name >> youtube-gold-may-202 

Script bucket - youtube-script-may-202 

SNS ARN >> arn:aws:sns:us-east-1:248949156360:data-pipeline-sns-alerts-202:e9fe799d-7f8b-4efa-8bee-9e694d1b6eff

---------------------------------------------------------------------
JOB_NAME >> youtube-202-pipeline-bronze-to-silver-glue-spark-json-csv-dev

--bronze_database  youtube-bronze-glue-database-202

--bronze_table  raw_statistics

--silver_bucket  youtube-silver-may-202

--silver_database  youtube-silver-glue-database-202

--silver_table  clean_statistics

--silver_path s3://youtube-silver-may-202/youtube/clean_statistics/

--bronze_path  s3://youtube-bronze-may-202/youtube/raw_statistics/
-------------------------------------------------------------------

