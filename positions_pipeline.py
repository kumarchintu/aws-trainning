import pandas as pd
import os
import shutil
import glob
import sys

def process_position(file):
    #Read file name from incoming bucket
    if file.startswith('PMT_Position'):
        #Read positions file and store in dataframe
        position_df=pd.read_csv(file,header=0)
        print('Incoming file is Position file')
        print('Position dataframe created with' , str(position_df.shape[0])+' records')
        print('Sample position data')
        print(position_df.head())

        #Converting column data to string type to process further
        for column in position_df.columns:
            position_df[column]=position_df[column].astype('str')

    #    position_df['Position|Market_Value_Local']=position_df['Position|Market_Value_Local'].astype(float)
    #    position_df['Position|Position_Quantity']=position_df['Position|Position_Quantity'].astype(float)

        #Create dataframe for storing valid positions
        #valid_position_df=pd.DataFrame(columns=["account_name","account_number","market_price_per_unit","market_value","position_date","cadis_id","position_quantity"])
        valid_position_df=pd.DataFrame(columns=position_df.columns)

        for index,position in position_df.iterrows():
            #Checking for validation 1 and creating error file for invalid data
            if float(position['Position|Market_Value_Local'])==0.0 or float(position['Position|Position_Quantity'])==0.0:
                f_name=position['Account|Account_Number']+'_error_'+position['Position|Sec_ID|CADIS_ID']+'_'+position['Position|Position_Date']+'.txt'
                f_name=os.path.join('error-kumara5',f_name)
                f=open(f_name,'w+')
                f.close()
                #print('Position in invalid \t Account_Name: '+position['account_name']+'  Market Value: '+position['market_value']+'  Position Quantity: '+position['position_quantity'])
            else:
                #Storing valid data in a dataframe
                valid_position_df=valid_position_df.append({
                    'Account|Account_Name':position['Account|Account_Name'],
                    'Account|Account_Number':position['Account|Account_Number'],
                    'Account|Account_Domicile_Country|Country_Code':position['Account|Account_Domicile_Country|Country_Code'],
                    'Portfolio|Portfolio_Number':position['Portfolio|Portfolio_Number'],
                    'Position|Market_Price_Per_Unit':position['Position|Market_Price_Per_Unit'],
                    'Position|Market_Value_Local':position['Position|Market_Value_Local'],
                    'Position|Position_Date':position['Position|Position_Date'],
                    'Position|Sec_ID|CADIS_ID':position['Position|Sec_ID|CADIS_ID'],
                    'Position|Position_Quantity':position['Position|Position_Quantity']
                    },ignore_index=True)

        #Print sample data from valid_positons
        print('Valid positions dataframe created with ',str(valid_position_df.shape[0])+' records')

        print('Sample Valid positions data')
        print(valid_position_df.head())
                
        #Write valid position data to valid_positions.csv file                
        valid_position_df.to_csv('positions-stg-kumara5/valid_positions.csv',index=False)

        #Check if security file is available
        sec_path='security-stg-kumara5/pmt_security.csv'
        if os.path.exists(sec_path):
            security_df=pd.concat(map(pd.read_csv, glob.glob('security-stg-kumara5/*.csv')))
            calculate_rmv(valid_position_df,security_df,sec_path)
        
        else:
            print('Only position file is available.Hence, exiting...')
            sys.exit()
        
    else:
        #Move incoming file to security staging bucket
        sec_inc='security-stg-kumara5/pmt_security.csv'
        shutil.copy(file,sec_inc)
        security_df=pd.read_csv(sec_inc,header=0)
        #print incoming security records
        #print('Security dataframe created with' , str(security_df.shape[0])+' records')
        #Concat current file and existing file from security staging bucket
        security_df=pd.concat(map(pd.read_csv, glob.glob('security-stg-kumara5/*.csv')))
        #print updated security records
        #print('Security dataframe after merging with last data has ' , str(security_df.shape[0])+' records')
    
        print(security_df.head())        

        pos_path='positions-stg-kumara5/valid_positions.csv'
        if os.path.exists(pos_path):
            position_df=pd.concat(map(pd.read_csv, glob.glob('positions-stg-kumara5/*.csv')))
            calculate_rmv(position_df,security_df,pos_path)
        else:
            print('Only security file is available')
            sys.exit()
            
def calculate_rmv(df_pos,df_sec,path):
    print('Calculating RMV...')

    for column in df_pos.columns:
        df_pos[column]=df_pos[column].astype('str')

    for column in df_sec.columns:
        df_sec[column]=df_sec[column].astype('str')

    #Merging security and valid positions to get positions eligible for RMV calculation
    #merged_df=pd.merge(df_pos,df_sec,how='inner', left_on=['Position|Sec_ID|CADIS_ID'], right_on=['Instrument_Issue|Cadis_ID'])
#    mergedCSV = table_1[['t1_a','id']].merge(table_2[['t2_a','id']], on = 'id',how = 'left')
    merged_df=df_pos[['Account|Account_Name','Position|Market_Price_Per_Unit','Position|Market_Value_Local','Position|Sec_ID|CADIS_ID']].merge(df_sec[['Instrument_Issue|Cadis_ID']],how='inner', left_on=['Position|Sec_ID|CADIS_ID'], right_on=['Instrument_Issue|Cadis_ID'])
    print('Positions eligible for RMV calcuration returned ',str(merged_df.shape[0])+' records')
    merged_df.to_csv('rmv-kumara5/merged_data.csv',index=False)    

    #Getting records which are invalid against validation #3
    rmv_non_eligible_df=df_pos
    cond=rmv_non_eligible_df['Position|Sec_ID|CADIS_ID'].isin(df_sec['Instrument_Issue|Cadis_ID'])
    rmv_non_eligible_df.drop(rmv_non_eligible_df[cond].index, inplace = True)
    print('Positions not eligible for RMV calculation are: ',str(rmv_non_eligible_df.shape[0]))
    for index,t in rmv_non_eligible_df.iterrows():
        f_name=str(t['Account|Account_Number']+'_'+t['Position|Sec_ID|CADIS_ID']+'_error2_'+t['Position|Position_Date']+'.txt')
        print(f_name)
        f_name=os.path.join('error-kumara5',f_name)
        f=open(f_name,"w+")
        f.close()

    rmv_df=pd.DataFrame(columns=['Account|Account_Name','RMV'])

#    print(merged_df[['Account|Account_Name','Position|Position_Date']].head())
    #Calculating RMV for valid positions
    merged_df.groupby(['Account|Account_Name']).agg('sum').to_csv('rmv-kumara5/rmv.csv')
    for index,valid_pos in merged_df.iterrows():
#        rmv_df['RMV']=float(valid_pos['Position|Market_Price_Per_Unit'])*float(valid_pos['Position|Market_Value_Local'])
        rmv_df=rmv_df.append(
            {'Account|Account_Name':valid_pos['Account|Account_Name']
                },ignore_index=True)

    rmv_df.to_csv('rmv-kumara5/pos_rmv.csv',index=False)
        
process_position('PMT_Positions_POC.csv')
