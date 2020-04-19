import pandas as pd

position_df=pd.read_csv('PMT_Positions_POC.csv',header=0)
security_df=pd.read_csv('PMT_Securities_POC.csv',header=0)

print('Position DataFrame\n')
print(position_df.head())
print('Security DataFrame\n')
print(security_df.head())

position_df.where(position_df.'Position|Sec_ID|CADIS_ID'==security_df.'Instrument_Issue|Cadis_ID')
#Merging positions and securities dataframe
#merged_frames=[position_df,security_df]
#merged_dataframe=pd.concat(merged_frames,sort=False)
#merged_dataframe=pd.concat(merged_frames, axis=0, join='outer', ignore_index=False, keys=None,levels=None, names=None, verify_integrity=False, copy=True,sort=False)
#merged_df=merge(data.frame(position_df,row.names=NULL), data.frame(security_df, row.names=NULL), by = 0, all = TRUE)[-1]

#merged_df=position_df.merge(security_df,how='inner', left_on='Position|Sec_ID|CADIS_ID', right_on='Instrument_Issue|Cadis_ID')
merged_df=pd.merge(position_df,security_df,how='inner', left_on='Position|Sec_ID|CADIS_ID', right_on='Instrument_Issue|Cadis_ID')
print('Merged DataFrame\n')
print(merged_df.head())
merged_df.to_csv('stg/merged_data.csv',index=False)
