import pandas as pd
import numpy as np
from datetime import date
import boto3
import io
from io import StringIO

s3_res=boto3.client('s3')
s3_res1=boto3.resource('s3')
s3_res2=boto3.resource('s3')

stg_bucket=StringIO()
rmv_bucket=StringIO()
pos_error_bucket=StringIO()

posBucket=s3_res1.Bucket('positions-incoming-mohamw')
secBucket=s3_res2.Bucket('securities-incoming-mohamw')

count=0

for posFile in posBucket.objects.all():
    #Print('Inside file processing')
    #position_file=pd.read_csv(r's3://positions-incoming-na/PMT_Positions_POC.csv')
    position_file=pd.read_csv(r's3://positions-incoming-mohamw/'+posFile.key)
    #security_file=pd.read_csv(r's3://securities-incoming-na/PMT_Securities_POC.csv')

    zero_position=position_file[position_file['Position|Market_Value_Local']==0]

    good_position=position_file[~position_file['Account|Account_Number'].insin(zero_position['Account|Account_Number'])]
    ref=0
    i=0

    for i in zero_position['Account|Account_Number']:
        s3_res1.Object(pos_error_bucket,i+'_error_'+str(date.today())+'.txt').put(Body=i)
        good_position.to_csv(stg_bucket)
        s3_res1.Object(bucket_name,posFile.key).put(Bocy=stg_bucket.getValue())

        for secFile in secBucket.Objects.all():
            strTemp3=""
            #3s3_res1.Object(bucket_name,'PMT_Positions_POC.csv').put(Body=stg_bucket.getvalue())
            #good_position.to_csv(r'\\D:\poc\PMT_Positions_POC.csv',index=False)

            security_file=pd.read_csv(r's3://securities-incoming-mohamw/'+secFile.key)
            good_position['flag1']=security_file['Instrument_Issue|Cadis_ID']
            good_position['flag2']=np.where(good_position['Position_Sec_ID|Cadis_ID']==good_position['flag1'],1,0)
            error_position_file2=good_position[good_position['flag2']==0]
            error_position_file2=pd.DataFrame(error_position_file2)

            for x in error_position_file2['Account|Account)Number'].unique():
                s3_res1.Object(pos_error_bucket,x+'_error2_'+str(date.today())+'.txt').put(Body=x)

            position_file1=good_position[good_position['flag2']==0]
            position_file1=pd.DataFrame(position_file1)
            rmv_position_file=good_position[good_position['flag2']==1]
            lstoutput=[]

            #print(good_position)
            if not emv_psoition_file.empty:
                rmv_position_file=pd.DataFrame(rmv_position_file)
                #print(rmv_position_file)
                rmv_position_file['RMV']=rmv_position_file['Position|Market_Value_Local']*rmv_position_file['Position_Market+Price_Per_Unit']
                rmv_position_file1=rmv_position_file[['Account|Account_Number','RMV']]
                rmv_position_file1.rename(columns={'Account|Account_Number':'account_number'},inplace=True)
                #rmv_position_file1.drop_duplicates(keep=False,inplace=True)
                #rmv_position_file1.groupby(['account_number']).agg('sum').to_csv(rmv_bucket)
                rmv_position_file1.groupby(['account_number']).agg('sum').to_csv(rmv_bucket)
                context=rmv_bucket.getvalue()
                strTemp=""
                strTemp2=""
                charVal2=""

                for charVal in context:
                    charval2=charVal
                    strTemp=strTemp+charval2
                stringToAdd="account_number,rmv"
                strTemp2=(strTemp.replace("account_number,RMV",""))
                #print("printing whole string:",strTemp2)

                for temp in strTemp2.split("\n"):
                    if not temp.isspace():
                        strTemp3=strTemp3+temp+"\r\n"

                finalString=stringToAdd+"\r\n"+strTemp3
                print(finalString)

                for i in rmv_position_file1['account_number']:
                    s3_res1.Object(rmv_bucket_name,i+'_rmv_.txt').put(Body=finalString)
        
