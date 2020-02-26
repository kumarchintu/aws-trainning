import boto3
import logging
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    # file_name= File to upload
    # bucket= Bucket Name
    # object_name= object name, file name is used if object_name is None
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as ce:
        logging.error(ce)
        return False
    return True


upload_file('resources/mock.pdf', 'kumara5-test-bucket-via-boto3', 'mock.pdf')
