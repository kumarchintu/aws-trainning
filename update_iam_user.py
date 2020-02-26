import boto3

iam=boto3.client('iam')

iam.update_user(
    UserName='NEW_USER_AWS_TRAINING',
    NewUserName='NEW_IAM_USER_NAME'
)
