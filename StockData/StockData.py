from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd 
import pytz
import json
import decimal

client = MongoClient("mongodb://34.132.27.77:27017")
database = client["Stock"]
collection = database["StockAgreegateDatanew"]
print("hi")
current_timezone = pytz.timezone("US/Eastern")
my_timestamp = datetime.now(current_timezone)
final_time = my_timestamp + timedelta(minutes=-5)
new=final_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
datestr=final_time.strftime("%Y-%m-%d");
query = {
    "date": datestr,
    "Saveddate": {
        "$gte": datetime.strptime(new,"%Y-%m-%dT%H:%M:%S.%fZ") }
}
sort = [("av", -1)]

# Created with NoSQLBooster, the essential IDE for MongoDB - https://nosqlbooster.com
cursor = collection.find(query, sort=sort)
try:
   rows = []
   json_data_list = [];
   list_cur = list(cursor)
   df = DataFrame(list_cur)
   grouped_single = df.groupby('sym')
   for sym, df_group in grouped_single:
       #print(df_group)
       #ll=list(df_group)
       #print(ll)
       
      
       df2 = DataFrame(df_group)
       countsofdf=len(df2.index)-1
       if countsofdf+1>=4:
           datafirst=df2.iloc[[0]]
           datalast=df2.iloc[[countsofdf]]
           aval=float(datalast["a"].values[0]) - float(datafirst["a"].values[0])
           avval=float(datafirst["av"].values[0]) - float(datalast["av"].values[0])
           eval=float(datalast["e"].values[0]) - float(datafirst["e"].values[0])
           avaval=float(avval)*float(aval)
           if avval != 0 and eval != 0:
               finalcal=float(avaval) / float(eval)
           else:
               finalcal = 0.00
           data = {}
           data['sym'] = sym
           data['calval'] = finalcal
           data['a'] = datafirst["a"].values[0]
           data['lasta'] = datalast["a"].values[0]
           data['firstav'] = datafirst["av"].values[0]
           data['lastav'] = datalast["av"].values[0]
           data['firsttime'] = datafirst["Saveddate"].values[0]
           data['lasttime'] = datalast["Saveddate"].values[0]
           #print(data)
           json_data_list.append(data);
       
       
   print(json_data_list)
   filedateadd=datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
   filename='data'+str(filedateadd)+'.csv'
   dfnew = DataFrame(json_data_list)
   dfnew.to_csv('D:\\polygon\\'+filename, index=False)
   #twitterDataFile = open(r'D:\polygon\''+str(new)+'.json', "w")
    # magic happens here to make it pretty-printed
   #twitterDataFile.write(json_data_list.dumps(json_data_list.loads(output), indent=4, sort_keys=True))
   #twitterDataFile.close()
  
finally:
    cursor.close()