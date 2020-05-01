import pandas as pd

dfObj = pd.DataFrame(columns=['User_ID', 'UserName', 'Action'])
print("Empty Dataframe ", dfObj, sep='\n')
dfObj = dfObj.append({'User_ID': 23, 'UserName': 'Riti', 'Action': 'Login'}, ignore_index=True)
dfObj = dfObj.append({'User_ID': 24, 'UserName': 'Aadi', 'Action': 'Logout'}, ignore_index=True)
dfObj = dfObj.append({'User_ID': 25, 'UserName': 'Jack', 'Action': 'Login'}, ignore_index=True)
 
print("Dataframe Contens ", dfObj, sep='\n')
