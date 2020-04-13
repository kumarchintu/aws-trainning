import json
import pandas as pd
import boto3

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    int_event = event['Records'][0]
    int_bucket = int_event['s3']['bucket']
    int_object = int_event['s3']['object']

    bucket_name = int_bucket['name']
    file_name = int_object['key']

    print('Bucket Name: ', bucket_name)
    print('File Name: ', file_name)

    read_s3_file(bucket_name, file_name)


def read_s3_file(bucket, file):
    print('*********************************')
    path = 's3://' + bucket + '/' + file
    data_frame = pd.read_csv(path, header=None)

    data_frame.columns = ["emp_id", "corp_id", "emp_name", "salary", "currency", "country"]

    for column in data_frame.columns:
        data_frame[column] = data_frame[column].astype(str)

    # Convert DataFrame to list of dictionaries
    json_list = data_frame.T.to_dict().values()

    print('---------Data in file---------')
    print(json_list)

    # Connect to DynamoDB  Table
    table = dynamodb.Table('employees')

    # Load data from json_list to dynamoDB table
    print('--------Loading data to DynamoDB Table started-------------')
    for employee in json_list:
        table.put_item(Item=employee)
    print('--------Data has been loaded to DyamoDB Table--------------')

