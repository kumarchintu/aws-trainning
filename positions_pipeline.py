import pandas as pd
import os

def process_position():
    #Read positions file and store in dataframe
    position_df=pd.read_csv('PMT_Positions_POC.csv',header=0)
    #position_df.columns = ["account_name", "account_number", "country_code", "portfolio_number", "market_price_per_unit", "market_value","position_date","cadis_id","position_quantity"]
    print('Position dataframe created with ',str(position_df.shape[0])+' records')
    print('Sample Positions data')
    print(position_df.head())

    for column in position_df.columns:
        position_df[column]=position_df[column].astype('str')

#    position_df['Position|Market_Value_Local']=position_df['Position|Market_Value_Local'].astype(float)
#    position_df['Position|Position_Quantity']=position_df['Position|Position_Quantity'].astype(float)

    #Read securities file and store in dataframe
    security_df=pd.read_csv('PMT_Securities_POC.csv',header=0)
    #security_df.columns = ["as_of_date", "cadis_id", "data_quality_ind_pl_id", "country_code","interest_accrual_date","issue_short_name","maturity_date","security_type_code","security_type_desc","issuer_name"]
    print('Securities dataframe created with ',str(security_df.shape[0])+' records')
    print('Sample Securities data')
    print(security_df.head())

    for column in security_df.columns:
        security_df[column]=security_df[column].astype('str')
    
        
    #Create dataframe for storing valid positions
    #valid_position_df=pd.DataFrame(columns=["account_name","account_number","market_price_per_unit","market_value","position_date","cadis_id","position_quantity"])
    valid_position_df=pd.DataFrame(columns=position_df.columns)
    
    for index,position in position_df.iterrows():
        #Checking for validation 1 and creating error file for invalid data
        if float(position['Position|Market_Value_Local'])==0.0 or float(position['Position|Position_Quantity'])==0.0:
            f_name=position['Account|Account_Number']+'_error_'+position['Position|Sec_ID|CADIS_ID']+'_'+position['Position|Position_Date']+'.txt'
            f_name=os.path.join('positions-error-kumara5',f_name)
            f=open(f_name,'w+')
            f.close()
#            print('Position in invalid \t Account_Name: '+position['account_name']+'  Market Value: '+position['market_value']+'  Position Quantity: '+position['position_quantity'])
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
        
    for column in valid_position_df.columns:
        valid_position_df[column]=valid_position_df[column].astype('str')

    #Merging security and valid positions to get positions eligible for RMV calculation
    merged_df=pd.merge(valid_position_df,security_df,how='inner', left_on='Position|Sec_ID|CADIS_ID', right_on='Instrument_Issue|Cadis_ID')
    print('Positions eligible for RMV calcuration returned ',str(merged_df.shape[0])+' records')
    merged_df.to_csv('stg/merged_data.csv',index=False)

    #Getting records which are invalid against validation #3
    rmv_non_eligible_df=valid_position_df
    cond=rmv_non_eligible_df['Position|Sec_ID|CADIS_ID'].isin(security_df['Instrument_Issue|Cadis_ID'])
    rmv_non_eligible_df.drop(rmv_non_eligible_df[cond].index, inplace = True)
    print('Positions not eligible for RMV calculation are: ',str(rmv_non_eligible_df.shape[0]))
    for index,t in rmv_non_eligible_df.iterrows():
        f_name=str(t['Account|Account_Number']+'_'+t['Position|Sec_ID|CADIS_ID']+'_error2_'+t['Position|Position_Date']+'.txt')
        print(f_name)
        f_name=os.path.join('positions-error-kumara5',f_name)
        f=open(f_name,"w+")
        f.close()
                
    rmv_df=pd.DataFrame(columns=['Account|Account_Number','Position|Sec_ID|CADIS_ID','Position|Position_Date','RMV'])

    #Calculating RMV for valid positions
    for index,valid_pos in merged_df.iterrows():
        rmv_pos=float(valid_pos['Position|Market_Price_Per_Unit'])*float(valid_pos['Position|Market_Value_Local'])
        rmv_df=rmv_df.append(
            {'Account|Account_Number':valid_pos['Account|Account_Number'],
             'Position|Sec_ID|CADIS_ID':valid_pos['Position|Sec_ID|CADIS_ID'],
             'Position|Position_Date':valid_pos['Position|Position_Date'],
             'RMV':rmv_pos
                },ignore_index=True)

    rmv_df.to_csv('rmv/pos_rmv.csv',index=False)
    
process_position()
