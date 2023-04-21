import keepvariable_core as kv
import pandas as pd
import datetime

kv_redis=kv.KeepVariableRedisServer(host="app.forloop.ai",port=6379,password="redisforloop2023#-")
#kv_redis=kv.KeepVariableDummyRedisServer()

    

kv_redis.set("test","abc123")
result=kv_redis.get("test")

print(result) 
#abc123



#even pandas dataframes, and numpy arrays can be stored
df=pd.DataFrame([[1,2,3,4],[4,5,6,7]],columns=["a","b","c","d"])    
array=df.values    

    
kv_redis.set("test_df",df)
result=kv_redis.get("test_df")

print(result)
#   a  b  c  d
#0  1  2  3  4
#1  4  5  6  7
    
kv_redis.set("test_array",array)
result=kv_redis.get("test_array")

print(result)
# #[[1 2 3 4]
# # [4 5 6 7]]

datetime_test = datetime.datetime(year=2023, month=4, day=15, hour=14, minute=35)

print(f'Datetime original: {datetime_test}')

name = "datetime_test"
kv_redis.set(name, datetime_test)
result = kv_redis.get(name)

print(f'Datetime from redis: {result}')
# 2023-04-14 14:35:00


