import json
from boto3 import client
from boto3 import resource

def lambda_handler(event, context):
    s3_client=client('s3')
    s3_resource=resource('s3')

    src_bucket=s3_resource.Bucket('prd-kumara5-poc')
    tgt_bucket=s3_resource.Bucket('uat-kumara5-poc')
    
    new_bucket_name = "uat-kumara5-poc"
    bucket_to_copy = "prd-kumara5-poc"
    
    for object in src_bucket.objects.all():
        tgt_key=object.key
        print('Target File Name: ',tgt_key)
        s3_resource.Object(tgt_bucket.name,tgt_key).copy_from(CopySource={
            'Bucket':object.bucket_name,
            'Key':object.key
        })

    '''
    for key in s3_client.list_objects(Bucket=bucket_to_copy)['Contents']:
        files = key['Key']
        copy_source = {'Bucket': "bucket_to_copy",'Key': files}
        s3_resource.meta.client.copy(copy_source, new_bucket_name, files)
        print(files)
        print(key['Key'])
    '''
    
    
------------------
import json
import boto3

s3_client=boto3.resource('s3')

def lambda_handler(event, context):
    my_event=event['Records'][0]
    my_bucket=my_event['s3']['bucket']
    my_object=my_event['s3']['object']
    
    uat_bucket_name=my_bucket['name']
    input_file_name=my_object['key']
    
    prd_bucket_name=s3_client.Bucket('prd-kumara5-poc').name
    prd_file_name=input_file_name
    
    uat_object={
        'Bucket':uat_bucket_name,
        'Key':input_file_name
    }
    
    print(uat_object)
    print('****************')
    print('UAT-Bucket Name : ',uat_bucket_name)
    print('Input File Name:',input_file_name)
    
    print('Copying '+input_file_name+' file from '+uat_bucket_name+' to '+ prd_bucket_name+'----')
    s3_client.Object(prd_bucket_name, prd_file_name).copy_from(CopySource={
        'Bucket':uat_bucket_name,
        'Key':input_file_name
    })
#   s3_client.meta.copy(uat_object,prd_bucket_name,prd_file_name)
    print('Copy operation finished')
    
    
    
