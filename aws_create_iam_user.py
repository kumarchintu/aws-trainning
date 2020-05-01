import boto3

#Create IAM Client
iam=boto3.client('iam')

#Create IAM user
response=iam.create_user(UserName='NEW_USER_AWS_TRAINING')

print(response)