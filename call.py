import os
import random
import sys
import json
import time
import uuid
import pandas as pd
import numpy as np
import pika
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysql_connecter import mysql_connecter
from datetime import date, timedelta
mysql_config = {
        'user': 'ffcs',
        'passwd': 'fAFic@#2022',
        'db': 'bestone_db',
        'host': '10.142.157.2',
        'port': 3306
    }
yesterday = str((date.today() + timedelta(days=-1)).strftime("%Y-%m-%d"))
db_util = mysql_connecter(db_cfg=mysql_config)
db_util.set_transaction_isolation_read_committed()
start_time = f"{yesterday} 00:00:00"
end_time = f"{yesterday} 23:59:59"
sql = f"select * from t_bestone where starttime >= '{start_time}' and starttime <= '{end_time}'"
print(sql)
df = db_util.pandas_read_sql(sql=sql)
df = df.applymap(lambda x: np.where(x!='',x,None))
df.dropna(subset=['current_intent'],how='any',inplace = True)
df = df[~df['current_intent'].str.contains('留言')]
df = df[~df['current_intent'].str.contains('其他')]
current_intent = df['current_intent']
#print(current_intent)
#print(current_intent.isnull())
current_intent = np.array(current_intent).tolist()
count = {}
for i in current_intent:
        count[i] = count.get(i,0)+1
tlist = [(count_value,count_key) for count_key,count_value in count.items()]
tlist = sorted(tlist,reverse=True)
top15 = []
for item in tlist:
   if len(top15) <15:
       top15.append(item[1])
   else:
       break
data = None
print(top15)
for i in top15:
    dft = df[df['current_intent'].str.contains(i)]
    if dft.shape[0] >= 100:
        dfs=dft.sample(100)
    else:
        dfs = dft
    callid = np.array(dfs['call_id']).tolist()
    for x in callid:
          data=pd.concat([data,df[df['call_id'].str.contains(x)]],axis=0)
print(data)
