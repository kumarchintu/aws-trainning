import boto3

s3_resource=boto3.resource('s3')
s3_resource.Bucket('kumara5-test-bucket-via-boto3').delete()