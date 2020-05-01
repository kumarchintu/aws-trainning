import pandas as pd

position_df=pd.read_csv('PMT_Positions_POC.csv',header=0)
security_df=pd.read_csv('PMT_Securities_POC.csv',header=0)

print('Position DataFrame\n')
print(position_df.head())
print('Security DataFrame\n')
print(security_df.head())

merged_df=pd.merge(position_df,security_df,how='inner', left_on='Position|Sec_ID|CADIS_ID', right_on='Instrument_Issue|Cadis_ID')
print('Merged DataFrame\n')
print(merged_df.head())
merged_df.to_csv('stg/merged_data.csv',index=False)
