import boto3
import pandas as pd

#Use header=None else first row will be treated as header
data_frame=pd.read_csv("emp_data.csv",header=None)

#Create headers manually
data_frame.columns=["emp_id","corp_id","emp_name","salary","currency","country"]

for i in data_frame.columns:
    data_frame[i]=data_frame[i].astype(str)

#Convert dataframe to list of dictionaries(JSON) to be consumed by no-sql db
json_list=data_frame.T.to_dict().values()

#Print row data form json_list
print(json_list)

#Convert to dynamodb using boto3
dynamodb=boto3.resource('dynamodb',region_name='ap-south-1')

#Connect to DynamoDB table
table=dynamodb.Table("emp_data")

#Load the JSON object using put_item method
for employee in json_list:
    table.put_item(Item=employee)
