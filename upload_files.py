import boto3
import os
import logging
from botocore.exceptions import ClientError

def upload_files(dir_name, bucket):
    # file_name= File to upload
    # bucket= Bucket Name

    s3_client = boto3.resource('s3')
    try:
        for file_name in os.listdir(dir_name):
            logging.info('Upload %s to Amazon s3 bucket %s' %(file_name,bucket))
            s3_client.Object(bucket,file_name).put(Body=open(os.path.join(dir_name,file_name),"rb"))
    except ClientError as ce:
        logging.error(ce)
        return False
    return True


upload_files('resources', 'kumara5-test-bucket-via-boto3')