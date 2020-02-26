import boto3

def create_bucket(bucket_name,region=None):
    if region is None:
        s3_client=boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client=boto3.client('s3',region_name=region)
        location={'LocationConstraint':region}
        s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location)

create_bucket('kumara5-test-bucket')
create_bucket('kumara5-mum-test-bucket','ap-south-1')