import keepvariable_core as kv
import pandas as pd

kv_redis=kv.KeepVariableRedisServer(host="127.0.0.1",port=6379)
    

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