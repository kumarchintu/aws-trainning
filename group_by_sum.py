import pandas as pd

df=pd.read_csv('group_by_sum.csv')
print(df)
df['Total']=df['MRP']*df['Qty']
df=df.groupby('Item').agg('sum')
df.to_csv('total.csv')
df=pd.read_csv('total.csv',header=0)
df[['Item','Total']].to_csv('total.csv')
