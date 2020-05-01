import boto3

#Create IAM Client
iam = boto3.client('iam')

#Add IAM user to group
response = iam.add_user_to_group(
    GroupName='AWS-Developer',
    UserName='NEW_IAM_USER_NAME'
)

print(response)
