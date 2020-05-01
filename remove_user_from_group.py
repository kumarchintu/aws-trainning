import boto3

#Create IAM Client
iam = boto3.client('iam')

#Remove IAM user from group
response = iam.remove_user_from_group(
    GroupName='AWS-Developer',
    UserName='NEW_IAM_USER_NAME'
)

print(response)
