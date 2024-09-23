import boto3
import os

def upload_file_to_s3(access_key, secret_key, bucket_name, object_key, local_file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3.upload_file(local_file_path, bucket_name, object_key)
        print(f"File uploaded successfully to S3: s3://{bucket_name}/{object_key}")
    except Exception as e:
        print("An error occurred:", e)
        
dir_path = '/home/ubuntu/terminal/Backend/Operations/from_s3/firmo'


# # Get a list of files in the directory
file_list = os.listdir(dir_path)

# # Filter the list to  include only files (not directories)
file_list = [file for file in file_list if os.path.isfile(os.path.join(dir_path, file))]

for file in file_list:
    object_key = f'multinode_firmo/jsonl/{file}'
    local_file_path = f'/home/ubuntu/terminal/Backend/Operations/from_s3/firmo/{file}'
    upload_file_to_s3('AKIA4OW7DFTPPGE3DT7C', 'Pxf65W7+G1cgOBlCPRmsT6SOYsikas2eptFJU6F0', 'vespa-dev-bq', object_key, local_file_path)
