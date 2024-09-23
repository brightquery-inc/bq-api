import boto3
import os

def download_s3_directory(bucket_name, directory_name, local_path):
    s3 = boto3.client('s3',aws_access_key_id='AKIA4OW7DFTPPGE3DT7C',aws_secret_access_key='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')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_name)

    for obj in response['Contents']:
        if not obj['Key'].endswith('/'):  # Skip directories
            file_name = os.path.join(local_path, os.path.basename(obj['Key']))
            # if "LE0001_part_01" in file_name or "LE0001_part_02" in file_name or "LE0002_part_08" in file_name:
            s3.download_file(bucket_name, obj['Key'], file_name)
            print(f"Downloaded: {file_name}")
                
bucket_name = 'vespa-dev-bq'

directory_name = 'update/bq_company_capital_markets_universe/JSONL/'
local_path = '/home/ubuntu/terminal/Backend/Operations/from_s3'

download_s3_directory(bucket_name, directory_name, local_path)